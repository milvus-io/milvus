package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	mock_message "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	walregistry "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newTestAlterWALMessage(
	target commonpb.WALName,
	timeTick uint64,
	messageID message.MessageID,
	lastConfirmed message.MessageID,
) message.ImmutableMessage {
	return message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: target}).
		WithBody(&message.AlterWALMessageBody{}).
		WithAllVChannel().
		MustBuildMutable().
		WithTimeTick(timeTick).
		WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(messageID)
}

func newTestTimeTickMessage(
	timeTick uint64,
	messageID message.MessageID,
	lastConfirmed message.MessageID,
) message.ImmutableMessage {
	return message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		MustBuildMutable().
		WithTimeTick(timeTick).
		WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(messageID)
}

func TestHistoricalScannerSwitchesAfterAlterWALTimeTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()

	messageCh := make(chan message.ImmutableMessage)
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messageCh).Maybe()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
		return ok
	})).Return(innerScanner, nil).Once()

	outputCh := make(chan message.ImmutableMessage, 3)
	type readerInfo struct {
		walName message.WALName
		role    string
	}
	switchedCh := make(chan readerInfo, 1)
	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     outputCh,
			onReaderSwitch: func(walName message.WALName, role string) {
				switchedCh <- readerInfo{walName: walName, role: role}
			},
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	type result struct {
		next switchableScanner
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		next, err := scanner.Do(ctx)
		resultCh <- result{next: next, err: err}
	}()

	alterWAL := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	messageCh <- alterWAL
	require.Equal(t, message.MessageTypeAlterWAL, (<-outputCh).MessageType())

	select {
	case result := <-resultCh:
		t.Fatalf("historical scanner switched before the TimeTick barrier: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}

	timeTickBeforeBarrier := newTestTimeTickMessage(99, rmq.NewRmqID(3), rmq.NewRmqID(2))
	messageCh <- timeTickBeforeBarrier
	require.Equal(t, uint64(99), (<-outputCh).TimeTick())

	select {
	case result := <-resultCh:
		t.Fatalf("historical scanner switched below the TimeTick barrier: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}

	timeTick := newTestTimeTickMessage(100, rmq.NewRmqID(4), rmq.NewRmqID(3))
	messageCh <- timeTick
	require.Equal(t, message.MessageTypeTimeTick, (<-outputCh).MessageType())

	resultValue := <-resultCh
	require.NoError(t, resultValue.err)
	catchup, ok := resultValue.next.(*catchupScanner)
	require.True(t, ok)
	assert.Equal(t, uint64(100), catchup.exclusiveStartTimeTick)
	_, ok = catchup.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
	assert.True(t, ok)
	assert.Equal(t, readerInfo{walName: message.WALNameTest, role: metrics.WALReaderRoleCurrent}, <-switchedCh)
}

func TestHistoricalScannerFollowsMigrationChain(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	rocksmqWAL := mock_walimpls.NewMockWALImpls(t)
	rocksmqWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	rocksmqWAL.EXPECT().Close().Return().Once()

	rocksmqMessages := make(chan message.ImmutableMessage, 2)
	rocksmqMessages <- newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	rocksmqMessages <- newTestTimeTickMessage(100, rmq.NewRmqID(3), rmq.NewRmqID(2))
	rocksmqScanner := mock_walimpls.NewMockScannerImpls(t)
	rocksmqScanner.EXPECT().Chan().Return(rocksmqMessages).Maybe()
	rocksmqScanner.EXPECT().Close().Return(nil).Once()
	rocksmqWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(rocksmqScanner, nil).Once()

	woodpeckerWAL := mock_walimpls.NewMockWALImpls(t)
	woodpeckerWAL.EXPECT().WALName().Return(message.WALNameWoodpecker).Maybe()
	woodpeckerWAL.EXPECT().Close().Return().Once()
	woodpeckerMessages := make(chan message.ImmutableMessage, 3)
	woodpeckerMessages <- newTestTimeTickMessage(100, rmq.NewRmqID(4), rmq.NewRmqID(3))
	woodpeckerMessages <- newTestAlterWALMessage(commonpb.WALName_Test, 200, rmq.NewRmqID(5), rmq.NewRmqID(4))
	woodpeckerMessages <- newTestTimeTickMessage(200, rmq.NewRmqID(6), rmq.NewRmqID(5))
	woodpeckerScanner := mock_walimpls.NewMockScannerImpls(t)
	woodpeckerScanner.EXPECT().Chan().Return(woodpeckerMessages).Maybe()
	woodpeckerScanner.EXPECT().Close().Return(nil).Once()
	woodpeckerWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
		return ok
	})).Return(woodpeckerScanner, nil).Once()

	type readerInfo struct {
		walName message.WALName
		role    string
	}
	switchedCh := make(chan readerInfo, 2)
	outputCh := make(chan message.ImmutableMessage, 4)

	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-chain-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     outputCh,
			historicalWALOpener: func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
				assert.Equal(t, message.WALNameWoodpecker, walName)
				return woodpeckerWAL, nil
			},
			onReaderSwitch: func(walName message.WALName, role string) {
				switchedCh <- readerInfo{walName: walName, role: role}
			},
		},
		historicalWAL: rocksmqWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	historicalNext, ok := next.(*historicalScanner)
	require.True(t, ok)
	assert.Equal(t, message.WALNameWoodpecker, historicalNext.historicalWAL.WALName())

	next, err = historicalNext.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	assert.Equal(t, uint64(200), catchup.exclusiveStartTimeTick)
	assert.Equal(t, readerInfo{walName: message.WALNameWoodpecker, role: metrics.WALReaderRoleHistorical}, <-switchedCh)
	assert.Equal(t, readerInfo{walName: message.WALNameTest, role: metrics.WALReaderRoleCurrent}, <-switchedCh)

	var timeTicks []uint64
	for len(outputCh) > 0 {
		msg := <-outputCh
		if msg.MessageType() == message.MessageTypeTimeTick {
			timeTicks = append(timeTicks, msg.TimeTick())
		}
	}
	assert.Equal(t, []uint64{100, 200}, timeTicks)
}

func TestHistoricalScannerFollowsMigrationChainWithRepeatedWALType(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	firstRocksmqWAL := mock_walimpls.NewMockWALImpls(t)
	firstRocksmqWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	firstRocksmqWAL.EXPECT().Close().Return().Once()
	firstRocksmqMessages := make(chan message.ImmutableMessage, 2)
	firstRocksmqMessages <- newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	firstRocksmqMessages <- newTestTimeTickMessage(100, rmq.NewRmqID(3), rmq.NewRmqID(2))
	firstRocksmqScanner := mock_walimpls.NewMockScannerImpls(t)
	firstRocksmqScanner.EXPECT().Chan().Return(firstRocksmqMessages).Maybe()
	firstRocksmqScanner.EXPECT().Close().Return(nil).Once()
	firstRocksmqWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(firstRocksmqScanner, nil).Once()

	woodpeckerWAL := mock_walimpls.NewMockWALImpls(t)
	woodpeckerWAL.EXPECT().WALName().Return(message.WALNameWoodpecker).Maybe()
	woodpeckerWAL.EXPECT().Close().Return().Once()
	woodpeckerMessages := make(chan message.ImmutableMessage, 2)
	woodpeckerMessages <- newTestAlterWALMessage(commonpb.WALName_RocksMQ, 200, rmq.NewRmqID(4), rmq.NewRmqID(3))
	woodpeckerMessages <- newTestTimeTickMessage(200, rmq.NewRmqID(5), rmq.NewRmqID(4))
	woodpeckerScanner := mock_walimpls.NewMockScannerImpls(t)
	woodpeckerScanner.EXPECT().Chan().Return(woodpeckerMessages).Maybe()
	woodpeckerScanner.EXPECT().Close().Return(nil).Once()
	woodpeckerWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(woodpeckerScanner, nil).Once()

	secondRocksmqWAL := mock_walimpls.NewMockWALImpls(t)
	secondRocksmqWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	secondRocksmqWAL.EXPECT().Close().Return().Once()
	secondRocksmqMessages := make(chan message.ImmutableMessage, 2)
	secondRocksmqMessages <- newTestAlterWALMessage(commonpb.WALName_Test, 300, rmq.NewRmqID(6), rmq.NewRmqID(5))
	secondRocksmqMessages <- newTestTimeTickMessage(300, rmq.NewRmqID(7), rmq.NewRmqID(6))
	secondRocksmqScanner := mock_walimpls.NewMockScannerImpls(t)
	secondRocksmqScanner.EXPECT().Chan().Return(secondRocksmqMessages).Maybe()
	secondRocksmqScanner.EXPECT().Close().Return(nil).Once()
	secondRocksmqWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(secondRocksmqScanner, nil).Once()

	type readerInfo struct {
		walName message.WALName
		role    string
	}
	switchedCh := make(chan readerInfo, 3)
	outputCh := make(chan message.ImmutableMessage, 6)
	openCount := 0
	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-repeated-wal-chain-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     outputCh,
			historicalWALOpener: func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
				openCount++
				switch openCount {
				case 1:
					assert.Equal(t, message.WALNameWoodpecker, walName)
					return woodpeckerWAL, nil
				case 2:
					assert.Equal(t, message.WALNameRocksmq, walName)
					return secondRocksmqWAL, nil
				default:
					t.Fatalf("unexpected historical WAL open %d for %s", openCount, walName)
					return nil, nil
				}
			},
			onReaderSwitch: func(walName message.WALName, role string) {
				switchedCh <- readerInfo{walName: walName, role: role}
			},
		},
		historicalWAL: firstRocksmqWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	firstNext, ok := next.(*historicalScanner)
	require.True(t, ok)

	next, err = firstNext.Do(context.Background())
	require.NoError(t, err)
	secondNext, ok := next.(*historicalScanner)
	require.True(t, ok)

	next, err = secondNext.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	assert.Equal(t, uint64(300), catchup.exclusiveStartTimeTick)
	assert.Equal(t, 2, openCount)
	assert.Equal(t, readerInfo{walName: message.WALNameWoodpecker, role: metrics.WALReaderRoleHistorical}, <-switchedCh)
	assert.Equal(t, readerInfo{walName: message.WALNameRocksmq, role: metrics.WALReaderRoleHistorical}, <-switchedCh)
	assert.Equal(t, readerInfo{walName: message.WALNameTest, role: metrics.WALReaderRoleCurrent}, <-switchedCh)

	var timeTicks []uint64
	for len(outputCh) > 0 {
		msg := <-outputCh
		if msg.MessageType() == message.MessageTypeTimeTick {
			timeTicks = append(timeTicks, msg.TimeTick())
		}
	}
	assert.Equal(t, []uint64{100, 200, 300}, timeTicks)
}

func TestHistoricalScannerFallsBackWhenNextHopIsUnavailable(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	messageCh := make(chan message.ImmutableMessage, 2)
	messageCh <- newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	messageCh <- newTestTimeTickMessage(100, rmq.NewRmqID(3), rmq.NewRmqID(2))
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messageCh).Maybe()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(innerScanner, nil).Once()

	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-fallback-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     make(chan message.ImmutableMessage, 2),
			historicalWALOpener: func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
				return nil, status.NewWALNameMismatchError(message.WALNameTest.String(), message.WALNameWoodpecker.String())
			},
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	assert.Equal(t, uint64(100), catchup.exclusiveStartTimeTick)
}

func TestHistoricalScannerStartAfterMarkerStillObservesBoundary(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	markerID := rmq.NewRmqID(2)
	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	messageCh := make(chan message.ImmutableMessage, 2)
	messageCh <- newTestAlterWALMessage(commonpb.WALName_Test, 100, markerID, rmq.NewRmqID(1))
	messageCh <- newTestTimeTickMessage(100, rmq.NewRmqID(3), markerID)
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messageCh).Maybe()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
		return ok
	})).Return(innerScanner, nil).Once()

	outputCh := make(chan message.ImmutableMessage, 2)
	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-start-after-marker-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     outputCh,
		},
		historicalWAL:           historicalWAL,
		deliverPolicy:           options.DeliverPolicyStartFrom(markerID),
		exclusiveStartMessageID: markerID,
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	_, ok := next.(*catchupScanner)
	require.True(t, ok)
	require.Len(t, outputCh, 1)
	assert.Equal(t, message.MessageTypeTimeTick, (<-outputCh).MessageType())
}

func TestHistoricalScannerIdleTailFallsBackToCurrentWAL(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	messageCh := make(chan message.ImmutableMessage)
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messageCh).Maybe()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(innerScanner, nil).Once()

	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName:                  "historical-idle-tail-test",
			logger:                       mlog.With(),
			innerWAL:                     currentWAL,
			msgChan:                      make(chan message.ImmutableMessage),
			historicalWALFallbackTimeout: 20 * time.Millisecond,
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(3)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	_, ok := next.(*catchupScanner)
	require.True(t, ok)
}

func TestHistoricalScannerReaderCreationTimeoutFallsBackToCurrentWAL(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(nil, errors.New("historical reader unavailable")).Once()

	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName:                  "historical-reader-create-timeout-test",
			logger:                       mlog.With(),
			innerWAL:                     currentWAL,
			msgChan:                      make(chan message.ImmutableMessage),
			historicalWALFallbackTimeout: 20 * time.Millisecond,
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(3)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	_, ok := next.(*catchupScanner)
	require.True(t, ok)
}

func TestHistoricalScannerMissingTopicFallsBackWithoutRetry(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).
		Return(nil, merr.WrapErrMqTopicNotFound("test-channel")).Once()

	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName:                  "historical-missing-topic-test",
			logger:                       mlog.With(),
			innerWAL:                     currentWAL,
			msgChan:                      make(chan message.ImmutableMessage),
			historicalWALFallbackTimeout: time.Second,
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(3)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	_, ok := next.(*catchupScanner)
	require.True(t, ok)
}

func TestHistoricalScannerUnavailableRocksMQFallsBackToCurrentWAL(t *testing.T) {
	originalRocksMQ := server.Rmq
	server.Rmq = nil
	defer func() { server.Rmq = originalRocksMQ }()

	builder, ok := walregistry.GetBuilder(message.WALNameRocksmq)
	require.True(t, ok)
	opener, err := builder.Build()
	require.NoError(t, err)
	defer opener.Close()
	historicalWAL, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "unavailable-historical-rocksmq",
			AccessMode: types.AccessModeRO,
		},
	})
	require.NoError(t, err)

	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()
	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName:                  "unavailable-historical-rocksmq-test",
			logger:                       mlog.With(),
			innerWAL:                     currentWAL,
			msgChan:                      make(chan message.ImmutableMessage),
			historicalWALFallbackTimeout: 20 * time.Millisecond,
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	_, ok = next.(*catchupScanner)
	require.True(t, ok)
}

func TestHistoricalScannerRetriesAfterMidStreamReaderFailure(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	transientErr := errors.New("transient historical reader failure")
	failedMessages := make(chan message.ImmutableMessage, 1)
	failedMessages <- newTestTimeTickMessage(90, rmq.NewRmqID(2), rmq.NewRmqID(1))
	close(failedMessages)
	failedScanner := mock_walimpls.NewMockScannerImpls(t)
	failedScanner.EXPECT().Chan().Return(failedMessages).Maybe()
	failedScanner.EXPECT().Error().Return(transientErr).Once()
	failedScanner.EXPECT().Close().Return(nil).Once()

	recoveredMessages := make(chan message.ImmutableMessage, 2)
	recoveredMessages <- newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	recoveredMessages <- newTestTimeTickMessage(100, rmq.NewRmqID(3), rmq.NewRmqID(2))
	recoveredScanner := mock_walimpls.NewMockScannerImpls(t)
	recoveredScanner.EXPECT().Chan().Return(recoveredMessages).Maybe()
	recoveredScanner.EXPECT().Close().Return(nil).Once()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
		return ok
	})).Return(failedScanner, nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
		return ok
	})).Return(recoveredScanner, nil).Once()

	outputCh := make(chan message.ImmutableMessage, 3)
	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-retry-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     outputCh,
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	_, ok := next.(*catchupScanner)
	require.True(t, ok)
	assert.Equal(t, uint64(90), (<-outputCh).TimeTick())
	assert.Equal(t, message.MessageTypeAlterWAL, (<-outputCh).MessageType())
	assert.Equal(t, message.MessageTypeTimeTick, (<-outputCh).MessageType())
}

func TestOldVersionLastConfirmedTracker_DefaultWindowSize(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(0)
	assert.Equal(t, 30, tracker.windowSize)
}

func TestOldVersionLastConfirmedTracker_BeforeWindowFull(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(3)

	ids := make([]*mock_message.MockMessageID, 3)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	// First message: should return itself (the first one)
	result := tracker.Track(ids[0])
	assert.Equal(t, ids[0], result)

	// Second message: still returns the first one (window not full)
	result = tracker.Track(ids[1])
	assert.Equal(t, ids[0], result)

	// Third message: still returns the first one (window size = 3, need 4th to start sliding)
	result = tracker.Track(ids[2])
	assert.Equal(t, ids[0], result)
}

func TestOldVersionLastConfirmedTracker_WindowSliding(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(3)

	ids := make([]*mock_message.MockMessageID, 6)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	// Fill the window: track ids[0], ids[1], ids[2]
	tracker.Track(ids[0])
	tracker.Track(ids[1])
	tracker.Track(ids[2])

	// 4th message (ids[3]): window is now [ids[0], ids[1], ids[2], ids[3]]
	// len=4 > windowSize=3, return ids[4-3-1] = ids[0]
	result := tracker.Track(ids[3])
	assert.Equal(t, ids[0], result, "should return the message 3 positions back")

	// 5th message (ids[4]): window is [ids[0]..ids[4]]
	// return ids[5-3-1] = ids[1]
	result = tracker.Track(ids[4])
	assert.Equal(t, ids[1], result, "should return the message 3 positions back")

	// 6th message (ids[5]): window is [ids[0]..ids[5]]
	// return ids[6-3-1] = ids[2]
	result = tracker.Track(ids[5])
	assert.Equal(t, ids[2], result, "should return the message 3 positions back")
}

func TestOldVersionLastConfirmedTracker_WindowSizeOne(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(1)

	ids := make([]*mock_message.MockMessageID, 3)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	// First message: returns itself
	result := tracker.Track(ids[0])
	assert.Equal(t, ids[0], result)

	// Second message: returns the one 1 position back = ids[0]
	result = tracker.Track(ids[1])
	assert.Equal(t, ids[0], result)

	// Third message: returns ids[1]
	result = tracker.Track(ids[2])
	assert.Equal(t, ids[1], result)
}

func TestOldVersionLastConfirmedTracker_LargeWindow(t *testing.T) {
	windowSize := 30
	tracker := newOldVersionLastConfirmedTracker(windowSize)

	totalMessages := 100
	ids := make([]*mock_message.MockMessageID, totalMessages)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	for i := 0; i < totalMessages; i++ {
		result := tracker.Track(ids[i])
		if i < windowSize {
			// Before window is full, always return the first ID
			assert.Equal(t, ids[0], result, "before window full, should return first ID at i=%d", i)
		} else {
			// After window is full, return the ID from windowSize positions back
			expected := ids[i-windowSize]
			assert.Equal(t, expected, result, "should return ID from %d positions back at i=%d", windowSize, i)
		}
	}
}
