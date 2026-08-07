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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	mock_message "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
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

func TestNewSwitchableScannerUsesCurrentWALDirectly(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	openerCalled := false

	scanner := newSwitchableScanner(
		"current-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(1)),
		make(chan message.ImmutableMessage),
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			openerCalled = true
			return nil, nil
		},
		nil,
	)

	_, ok := scanner.(*catchupScanner)
	require.True(t, ok)
	require.False(t, openerCalled)
}

func TestSwitchableScannerSwitchesImmediatelyOnAlterWAL(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	oldWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	currentMessage := newTestTimeTickMessage(101, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentReadWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)
	outputCh := make(chan message.ImmutableMessage, 2)
	opened := make([]message.WALName, 0, 2)

	scanner := newSwitchableScanner(
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

func TestSwitchableScannerFollowsMigrationChainWithCleanRepeatedWAL(t *testing.T) {
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
	readerSwitches := make([]message.WALName, 0, 3)
	outputCh := make(chan message.ImmutableMessage, 10)
	scanner := newSwitchableScanner(
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
		func(walName message.WALName, _ string) {
			readerSwitches = append(readerSwitches, walName)
		},
	)

	messages := runSwitchableScannerUntil(t, scanner, outputCh, func(msg message.ImmutableMessage) bool {
		return msg.MessageType() == message.MessageTypeTimeTick && msg.TimeTick() == 350
	})
	require.Equal(t, 4, opened)
	require.Equal(t, []message.WALName{
		message.WALNameWoodpecker,
		message.WALNameRocksmq,
		message.WALNameTest,
	}, readerSwitches)

	var timeTicks []uint64
	for _, msg := range messages {
		if msg.MessageType() == message.MessageTypeTimeTick {
			timeTicks = append(timeTicks, msg.TimeTick())
		}
	}
	require.Equal(t, []uint64{150, 250, 350}, timeTicks)
}

func TestSwitchableScannerStartAfterAlterWALMarker(t *testing.T) {
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

	scanner := newSwitchableScanner(
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

func TestSwitchableScannerFailsWhenReadWALIsUnavailable(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	scanner := newSwitchableScanner(
		"missing-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		make(chan message.ImmutableMessage),
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return nil, merr.WrapErrMqTopicNotFound(channel.Name)
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.Nil(t, next)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	require.Contains(t, err.Error(), merr.ErrMqTopicNotFound.Error())
}

func TestSwitchableScannerRejectsInvalidHistoricalStartPosition(t *testing.T) {
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

	scanner := newSwitchableScanner(
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

func TestSwitchableScannerFailsWhenHistoricalWALExhaustsBeforeBoundary(t *testing.T) {
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

	scanner := newSwitchableScanner(
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

func TestSwitchableScannerRejectsInvalidAlterWALTarget(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	historicalWAL := newTestReadWAL(
		t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName(12345), 100, rmq.NewRmqID(2), rmq.NewRmqID(1)),
	)

	scanner := newSwitchableScanner(
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

func TestSwitchableScannerReopensWALAfterReaderFailure(t *testing.T) {
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
	scanner := newSwitchableScanner(
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

func TestNormalizeSwitchableDeliverPolicy(t *testing.T) {
	messageID := rmq.NewRmqID(10)
	normalized, excluded, err := normalizeSwitchableDeliverPolicy(options.DeliverPolicyStartAfter(messageID))
	require.NoError(t, err)
	require.True(t, excluded.EQ(messageID))
	policy, ok := normalized.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
	require.True(t, ok)
	normalizedID, err := message.UnmarshalMessageID(policy.StartFrom)
	require.NoError(t, err)
	require.True(t, normalizedID.EQ(messageID))
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

	result := tracker.Track(ids[0])
	assert.Equal(t, ids[0], result)
	result = tracker.Track(ids[1])
	assert.Equal(t, ids[0], result)
	result = tracker.Track(ids[2])
	assert.Equal(t, ids[0], result)
}

func TestOldVersionLastConfirmedTracker_WindowSliding(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(3)

	ids := make([]*mock_message.MockMessageID, 6)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	tracker.Track(ids[0])
	tracker.Track(ids[1])
	tracker.Track(ids[2])
	assert.Equal(t, ids[0], tracker.Track(ids[3]))
	assert.Equal(t, ids[1], tracker.Track(ids[4]))
	assert.Equal(t, ids[2], tracker.Track(ids[5]))
}

func TestOldVersionLastConfirmedTracker_WindowSizeOne(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(1)

	ids := make([]*mock_message.MockMessageID, 3)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	assert.Equal(t, ids[0], tracker.Track(ids[0]))
	assert.Equal(t, ids[0], tracker.Track(ids[1]))
	assert.Equal(t, ids[1], tracker.Track(ids[2]))
}

func TestOldVersionLastConfirmedTracker_LargeWindow(t *testing.T) {
	const windowSize = 30
	tracker := newOldVersionLastConfirmedTracker(windowSize)

	const totalMessages = 100
	ids := make([]*mock_message.MockMessageID, totalMessages)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	for _, id := range ids[:windowSize] {
		assert.Equal(t, ids[0], tracker.Track(id))
	}

	expectedIDs := ids[:totalMessages-windowSize]
	remainingIDs := ids[windowSize:]
	for len(remainingIDs) > 0 {
		assert.Equal(t, expectedIDs[0], tracker.Track(remainingIDs[0]))
		expectedIDs = expectedIDs[1:]
		remainingIDs = remainingIDs[1:]
	}
}
