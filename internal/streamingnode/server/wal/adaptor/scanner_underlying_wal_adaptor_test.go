package adaptor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
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

func newTestNonPersistedTimeTickMessage(
	timeTick uint64,
	messageID message.MessageID,
	lastConfirmed message.MessageID,
) message.ImmutableMessage {
	return message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		WithNotPersisted().
		MustBuildMutable().
		WithTimeTick(timeTick).
		WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(messageID)
}

func newTestReadWAL(
	t *testing.T,
	walName message.WALName,
	channel types.PChannelInfo,
	messages ...message.ImmutableMessage,
) walimpls.ROWALImpls {
	messageCh := make(chan message.ImmutableMessage, len(messages))
	for _, msg := range messages {
		messageCh <- msg
	}
	scanner := mock_walimpls.NewMockScannerImpls(t)
	scanner.EXPECT().Chan().Return(messageCh).Maybe()
	scanner.EXPECT().Close().Return(nil).Once()

	underlyingWAL := mock_walimpls.NewMockWALImpls(t)
	underlyingWAL.EXPECT().WALName().Return(walName).Maybe()
	underlyingWAL.EXPECT().Channel().Return(channel).Maybe()
	underlyingWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner, nil).Once()
	underlyingWAL.EXPECT().Close().Return().Once()
	return underlyingWAL
}

func newTestCurrentWAL(t *testing.T, channel types.PChannelInfo) walimpls.ROWALImpls {
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()
	return currentWAL
}

type switchableScannerResult struct {
	next switchableScanner
	err  error
}

func runSwitchableScannerUntil(
	t *testing.T,
	scanner switchableScanner,
	outputCh <-chan message.ImmutableMessage,
	stopWhen func(message.ImmutableMessage) bool,
) []message.ImmutableMessage {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan switchableScannerResult, 1)
	go func() {
		next, err := scanner.Do(ctx)
		resultCh <- switchableScannerResult{next: next, err: err}
	}()

	messages := make([]message.ImmutableMessage, 0)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case msg := <-outputCh:
			messages = append(messages, msg)
			if !stopWhen(msg) {
				continue
			}
			cancel()
			result := <-resultCh
			require.Nil(t, result.next)
			require.ErrorIs(t, result.err, context.Canceled)
			return messages
		case result := <-resultCh:
			t.Fatalf("switchable scanner stopped before the expected message: next=%T err=%v", result.next, result.err)
		case <-deadline.C:
			t.Fatal("timed out waiting for switchable scanner output")
		}
	}
}

func TestUnderlyingWALScannerAdaptorFollowsAlterWAL(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	oldWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentReadWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)
	outputCh := make(chan message.ImmutableMessage, 2)
	opened := make([]message.WALName, 0, 2)

	scanner := newSwithableScanner(
		"switch-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(_ context.Context, walName message.WALName, gotChannel types.PChannelInfo) (walimpls.ROWALImpls, error) {
			opened = append(opened, walName)
			require.Equal(t, channel, gotChannel)
			switch walName {
			case message.WALNameRocksmq:
				return oldWAL, nil
			case message.WALNameTest:
				return currentReadWAL, nil
			default:
				t.Fatalf("unexpected WAL name %s", walName)
				return nil, nil
			}
		},
		nil,
	)

	messages := runSwitchableScannerUntil(t, scanner, outputCh, func(msg message.ImmutableMessage) bool {
		return msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() == 101
	})
	require.Equal(t, []message.WALName{message.WALNameRocksmq, message.WALNameTest}, opened)
	require.Len(t, messages, 2)
	require.Equal(t, marker, messages[0])
	require.Equal(t, currentMessage, messages[1])
}

func TestUnderlyingWALScannerAdaptorPassesThroughRawV0Message(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	legacyMessage := message.NewImmutableMesasge(rmq.NewRmqID(1), []byte("legacy-message"), map[string]string{})
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	oldWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, legacyMessage, marker)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)

	scanner, err := newUnderlyingWALScannerAdaptor(
		mlog.With(),
		channel,
		walimpls.ReadOption{
			Name:          "legacy-reader",
			DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		},
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			switch walName {
			case message.WALNameRocksmq:
				return oldWAL, nil
			case message.WALNameTest:
				return currentWAL, nil
			default:
				t.Fatalf("unexpected WAL name %s", walName)
				return nil, nil
			}
		},
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, message.VersionOld, (<-scanner.Chan()).Version())
	require.Equal(t, marker, <-scanner.Chan())
	require.Equal(t, currentMessage, <-scanner.Chan())
	require.NoError(t, scanner.Close())
}

func TestUnderlyingWALScannerAdaptorSkipsRawV0MessageAfterAlterWAL(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	oldWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	staleLegacyMessage := message.NewImmutableMesasge(
		walimplstest.NewTestMessageID(0),
		[]byte("stale-legacy-message"),
		map[string]string{},
	)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentWAL := newTestReadWAL(t, message.WALNameTest, channel, staleLegacyMessage, currentMessage)

	scanner, err := newUnderlyingWALScannerAdaptor(
		mlog.With(),
		channel,
		walimpls.ReadOption{
			Name:          "legacy-reader-after-switch",
			DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		},
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			switch walName {
			case message.WALNameRocksmq:
				return oldWAL, nil
			case message.WALNameTest:
				return currentWAL, nil
			default:
				t.Fatalf("unexpected WAL name %s", walName)
				return nil, nil
			}
		},
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, marker, <-scanner.Chan())
	require.Equal(t, currentMessage, <-scanner.Chan())
	require.NoError(t, scanner.Close())
}

func TestUnderlyingWALScannerAdaptorFollowsChainWithCleanRepeatedWAL(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	firstRocksmqWAL := newTestReadWAL(
		t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1)),
	)
	woodpeckerWAL := newTestReadWAL(
		t, message.WALNameWoodpecker, channel,
		newTestTimeTickMessage(150, rmq.NewRmqID(3), rmq.NewRmqID(2)),
		newTestAlterWALMessage(commonpb.WALName_RocksMQ, 200, rmq.NewRmqID(4), rmq.NewRmqID(3)),
	)
	secondRocksmqWAL := newTestReadWAL(
		t, message.WALNameRocksmq, channel,
		// A WAL backend must be empty before it is selected as the switch target.
		newTestTimeTickMessage(250, rmq.NewRmqID(1), rmq.NewRmqID(1)),
		newTestAlterWALMessage(commonpb.WALName_Test, 300, rmq.NewRmqID(2), rmq.NewRmqID(1)),
	)
	currentReadWAL := newTestReadWAL(
		t, message.WALNameTest, channel,
		newTestTimeTickMessage(350, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1)),
	)

	opened := 0
	readerWALNames := make([]message.WALName, 0, 4)
	outputCh := make(chan message.ImmutableMessage, 10)
	scanner := newSwithableScanner(
		"migration-chain-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			opened++
			switch opened {
			case 1:
				require.Equal(t, message.WALNameRocksmq, walName)
				return firstRocksmqWAL, nil
			case 2:
				require.Equal(t, message.WALNameWoodpecker, walName)
				return woodpeckerWAL, nil
			case 3:
				require.Equal(t, message.WALNameRocksmq, walName)
				return secondRocksmqWAL, nil
			case 4:
				require.Equal(t, message.WALNameTest, walName)
				return currentReadWAL, nil
			default:
				t.Fatalf("unexpected WAL open %d for %s", opened, walName)
				return nil, nil
			}
		},
		func(walName message.WALName) {
			readerWALNames = append(readerWALNames, walName)
		},
	)

	messages := runSwitchableScannerUntil(t, scanner, outputCh, func(msg message.ImmutableMessage) bool {
		return msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() == 350
	})
	require.Equal(t, 4, opened)
	require.Equal(t, []message.WALName{
		message.WALNameRocksmq,
		message.WALNameWoodpecker,
		message.WALNameRocksmq,
		message.WALNameTest,
	}, readerWALNames)

	var timeTicks []uint64
	for _, msg := range messages {
		if msg.MessageType() == message.MessageTypeTimeTick {
			timeTicks = append(timeTicks, msg.TimeTick())
		}
	}
	require.Equal(t, []uint64{150, 250, 350}, timeTicks)
}

func TestUnderlyingWALScannerAdaptorStartAfterAlterWALMarker(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	markerID := rmq.NewRmqID(2)
	oldWAL := newTestReadWAL(
		t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName_Test, 100, markerID, rmq.NewRmqID(1)),
	)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentReadWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)
	outputCh := make(chan message.ImmutableMessage, 2)

	scanner := newSwithableScanner(
		"start-after-marker-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartAfter(markerID),
		outputCh,
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			if walName == message.WALNameRocksmq {
				return oldWAL, nil
			}
			require.Equal(t, message.WALNameTest, walName)
			return currentReadWAL, nil
		},
		nil,
	)

	messages := runSwitchableScannerUntil(t, scanner, outputCh, func(msg message.ImmutableMessage) bool {
		return msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() == 101
	})
	require.Equal(t, []message.ImmutableMessage{currentMessage}, messages)
}

func TestUnderlyingWALScannerAdaptorFailsWhenWALNameMismatches(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	scanner := newSwithableScanner(
		"missing-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		make(chan message.ImmutableMessage),
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return nil, status.NewWALNameMismatchError(message.WALNameTest.String(), message.WALNameRocksmq.String())
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.Nil(t, next)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestUnderlyingWALScannerAdaptorWaitsForAlterWALTarget(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	historicalWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	targetMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	targetWAL := newTestReadWAL(t, message.WALNameTest, channel, targetMessage)
	var targetOpenAttempts atomic.Int32

	scanner, err := newUnderlyingWALScannerAdaptor(
		mlog.With(),
		channel,
		walimpls.ReadOption{
			Name:          "wait-for-switch-target",
			DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		},
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			switch walName {
			case message.WALNameRocksmq:
				return historicalWAL, nil
			case message.WALNameTest:
				if targetOpenAttempts.Add(1) <= 2 {
					return nil, merr.WrapErrMqTopicNotFound(channel.Name)
				}
				return targetWAL, nil
			default:
				t.Fatalf("unexpected WAL name %s", walName)
				return nil, nil
			}
		},
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, marker, <-scanner.Chan())
	select {
	case msg := <-scanner.Chan():
		require.Equal(t, targetMessage, msg)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for switch target WAL")
	}
	require.Equal(t, int32(3), targetOpenAttempts.Load())
	require.NoError(t, scanner.Close())
}

func TestUnderlyingWALScannerAdaptorWaitsForAlterWALTargetScanner(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	historicalWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	targetMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	targetWAL := newTestReadWAL(t, message.WALNameTest, channel, targetMessage)
	var targetScannerReadAttempts atomic.Int32

	// The target WAL exists as soon as its writer opened it, but its scanner is
	// only creatable once the writer published the topic, so the reader retries
	// the read on the same WAL instance instead of reopening it.
	unreadyTargetWAL := mock_walimpls.NewMockWALImpls(t)
	unreadyTargetWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	unreadyTargetWAL.EXPECT().Close().Run(func() { targetWAL.Close() }).Return().Maybe()
	unreadyTargetWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
			if targetScannerReadAttempts.Add(1) <= 2 {
				return nil, merr.WrapErrMqTopicNotFound(channel.Name)
			}
			return targetWAL.Read(ctx, opt)
		})

	scanner, err := newUnderlyingWALScannerAdaptor(
		mlog.With(),
		channel,
		walimpls.ReadOption{
			Name:          "wait-for-switch-target-scanner",
			DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		},
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			if walName == message.WALNameRocksmq {
				return historicalWAL, nil
			}
			require.Equal(t, message.WALNameTest, walName)
			return unreadyTargetWAL, nil
		},
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, marker, <-scanner.Chan())
	select {
	case msg := <-scanner.Chan():
		require.Equal(t, targetMessage, msg)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for switch target WAL scanner")
	}
	require.Equal(t, int32(3), targetScannerReadAttempts.Load())
	require.NoError(t, scanner.Close())
}

func TestUnderlyingWALScannerAdaptorCanCloseWhileWaitingForAlterWALTarget(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	historicalWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	targetOpenAttempted := make(chan struct{}, 1)

	scanner, err := newUnderlyingWALScannerAdaptor(
		mlog.With(),
		channel,
		walimpls.ReadOption{
			Name:          "close-while-waiting-for-switch-target",
			DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		},
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			if walName == message.WALNameRocksmq {
				return historicalWAL, nil
			}
			require.Equal(t, message.WALNameTest, walName)
			select {
			case targetOpenAttempted <- struct{}{}:
			default:
			}
			return nil, merr.WrapErrMqTopicNotFound(channel.Name)
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, marker, <-scanner.Chan())

	select {
	case <-targetOpenAttempted:
	case <-time.After(time.Second):
		t.Fatal("target WAL open was not attempted")
	}
	require.NoError(t, scanner.Close())
}

func TestUnderlyingWALScannerAdaptorStopsWhenScannerCreationIsUnrecoverable(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	underlyingWAL := mock_walimpls.NewMockWALImpls(t)
	underlyingWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	underlyingWAL.EXPECT().Read(mock.Anything, mock.Anything).
		Return(nil, status.NewUnrecoverableError("reader cannot be created")).Once()
	underlyingWAL.EXPECT().Close().Return().Once()

	scanner, err := newUnderlyingWALScannerAdaptor(
		mlog.With(),
		channel,
		walimpls.ReadOption{
			Name:          "unrecoverable-reader",
			DeliverPolicy: options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		},
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return underlyingWAL, nil
		},
		nil,
	)
	require.NoError(t, err)

	select {
	case _, ok := <-scanner.Chan():
		require.False(t, ok)
		require.True(t, status.AsStreamingError(scanner.Error()).IsUnrecoverable())
	case <-time.After(time.Second):
		t.Fatal("scanner retried an unrecoverable creation error")
	}
}

func TestUnderlyingWALScannerAdaptorRejectsInvalidStartPosition(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	invalidPolicy := &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_StartFrom{
			StartFrom: &commonpb.MessageID{
				WALName: commonpb.WALName_RocksMQ,
				Id:      "invalid-rocksmq-id",
			},
		},
	}

	scanner := newSwithableScanner(
		"invalid-start-reader",
		mlog.With(),
		currentWAL,
		nil,
		invalidPolicy,
		make(chan message.ImmutableMessage),
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			t.Fatal("historical WAL opener must not be called for an invalid start position")
			return nil, nil
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.Nil(t, next)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestUnderlyingWALScannerAdaptorFailsWhenWALExhaustsBeforeBoundary(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	messages := make(chan message.ImmutableMessage)
	close(messages)
	innerScanner := mock_walimpls.NewMockScannerImpls(t)
	innerScanner.EXPECT().Chan().Return(messages).Maybe()
	innerScanner.EXPECT().Error().Return(nil).Once()
	innerScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Channel().Return(channel).Maybe()
	historicalWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(innerScanner, nil).Once()
	historicalWAL.EXPECT().Close().Return().Once()

	scanner := newSwithableScanner(
		"exhausted-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		make(chan message.ImmutableMessage),
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return historicalWAL, nil
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.Nil(t, next)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestUnderlyingWALScannerAdaptorRejectsInvalidAlterWALTarget(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	historicalWAL := newTestReadWAL(
		t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName(12345), 100, rmq.NewRmqID(2), rmq.NewRmqID(1)),
	)

	scanner := newSwithableScanner(
		"invalid-target-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		make(chan message.ImmutableMessage, 1),
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return historicalWAL, nil
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.Nil(t, next)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestUnderlyingWALScannerAdaptorReopensWALAfterReaderFailure(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	failedMessages := make(chan message.ImmutableMessage, 1)
	failedMessages <- newTestTimeTickMessage(90, rmq.NewRmqID(2), rmq.NewRmqID(1))
	close(failedMessages)
	failedScanner := mock_walimpls.NewMockScannerImpls(t)
	failedScanner.EXPECT().Chan().Return(failedMessages).Maybe()
	failedScanner.EXPECT().Error().Return(errors.New("transient reader failure")).Once()
	failedScanner.EXPECT().Close().Return(nil).Once()
	failedWAL := mock_walimpls.NewMockWALImpls(t)
	failedWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	failedWAL.EXPECT().Channel().Return(channel).Maybe()
	failedWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(failedScanner, nil).Once()
	failedWAL.EXPECT().Close().Return().Once()

	recoveredWAL := newTestReadWAL(
		t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(3), rmq.NewRmqID(2)),
	)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentReadWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)
	openCount := 0
	outputCh := make(chan message.ImmutableMessage, 4)
	scanner := newSwithableScanner(
		"retry-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			openCount++
			if walName == message.WALNameTest {
				return currentReadWAL, nil
			}
			require.Equal(t, message.WALNameRocksmq, walName)
			if openCount == 1 {
				return failedWAL, nil
			}
			return recoveredWAL, nil
		},
		nil,
	)

	messages := runSwitchableScannerUntil(t, scanner, outputCh, func(msg message.ImmutableMessage) bool {
		return msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() == 101
	})
	require.Equal(t, 3, openCount)
	require.Len(t, messages, 3)
	require.Equal(t, uint64(90), messages[0].TimeTick())
	require.Equal(t, message.MessageTypeAlterWAL, messages[1].MessageType())
	require.Equal(t, currentMessage, messages[2])
}

func TestUnderlyingWALScannerAdaptorRetriesWALOpenFailure(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	historicalWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentReadWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)

	historicalOpenAttempts := 0
	outputCh := make(chan message.ImmutableMessage, 2)
	scanner := newSwithableScanner(
		"retry-open-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			switch walName {
			case message.WALNameRocksmq:
				historicalOpenAttempts++
				if historicalOpenAttempts == 1 {
					return nil, merr.WrapErrMqInternalMsg("transient historical WAL open failure")
				}
				return historicalWAL, nil
			case message.WALNameTest:
				return currentReadWAL, nil
			default:
				t.Fatalf("unexpected WAL name %s", walName)
				return nil, nil
			}
		},
		nil,
	)

	messages := runSwitchableScannerUntil(t, scanner, outputCh, func(msg message.ImmutableMessage) bool {
		return msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() == 101
	})
	require.Equal(t, 2, historicalOpenAttempts)
	require.Equal(t, []message.ImmutableMessage{marker, currentMessage}, messages)
}

func TestNormalizeUnderlyingWALDeliverPolicy(t *testing.T) {
	messageID := rmq.NewRmqID(10)
	normalized, excluded, err := normalizeUnderlyingWALDeliverPolicy(options.DeliverPolicyStartAfter(messageID))
	require.NoError(t, err)
	require.True(t, excluded.EQ(messageID))
	policy, ok := normalized.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
	require.True(t, ok)
	normalizedID, err := message.UnmarshalMessageID(policy.StartFrom)
	require.NoError(t, err)
	require.True(t, normalizedID.EQ(messageID))
}
