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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	mock_message "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
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
	switchedCh := make(chan message.WALName, 1)
	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     outputCh,
			onWALSwitch: func(walName message.WALName) {
				switchedCh <- walName
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
	assert.Equal(t, message.WALNameTest, <-switchedCh)
}

func TestHistoricalScannerRejectsUnsupportedMigrationChain(t *testing.T) {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-channel"}).Maybe()

	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()

	messageCh := make(chan message.ImmutableMessage, 1)
	messageCh <- newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messageCh).Maybe()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(innerScanner, nil).Once()

	scanner := &historicalScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName: "historical-test",
			logger:      mlog.With(),
			innerWAL:    currentWAL,
			msgChan:     make(chan message.ImmutableMessage, 1),
		},
		historicalWAL: historicalWAL,
		deliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
	}

	next, err := scanner.Do(context.Background())
	require.Error(t, err)
	assert.Nil(t, next)
	assert.Contains(t, err.Error(), "unsupported WAL migration chain")
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
