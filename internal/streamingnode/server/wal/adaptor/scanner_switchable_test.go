package adaptor

import (
	"context"
	"testing"

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
	outputCh := make(chan message.ImmutableMessage, 1)

	scanner := newSwitchableScanner(
		"switch-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(_ context.Context, walName message.WALName, gotChannel types.PChannelInfo) (walimpls.ROWALImpls, error) {
			require.Equal(t, message.WALNameRocksmq, walName)
			require.Equal(t, channel, gotChannel)
			return oldWAL, nil
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	require.Equal(t, uint64(100), catchup.exclusiveStartTimeTick)
	_, ok = catchup.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
	require.True(t, ok)
	require.Equal(t, marker, <-outputCh)
}

func TestSwitchableScannerFollowsMigrationChainAndFiltersRepeatedWAL(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	firstRocksmqWAL := newTestReadWAL(t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1)),
	)
	woodpeckerWAL := newTestReadWAL(t, message.WALNameWoodpecker, channel,
		newTestTimeTickMessage(150, rmq.NewRmqID(3), rmq.NewRmqID(2)),
		newTestAlterWALMessage(commonpb.WALName_RocksMQ, 200, rmq.NewRmqID(4), rmq.NewRmqID(3)),
	)
	secondRocksmqWAL := newTestReadWAL(t, message.WALNameRocksmq, channel,
		// These records belong to the first RocksMQ epoch and must be skipped.
		newTestTimeTickMessage(50, rmq.NewRmqID(1), rmq.NewRmqID(1)),
		newTestAlterWALMessage(commonpb.WALName_WoodPecker, 100, rmq.NewRmqID(2), rmq.NewRmqID(1)),
		newTestTimeTickMessage(250, rmq.NewRmqID(5), rmq.NewRmqID(4)),
		newTestAlterWALMessage(commonpb.WALName_Test, 300, rmq.NewRmqID(6), rmq.NewRmqID(5)),
	)

	opened := 0
	readerSwitches := make([]message.WALName, 0, 3)
	outputCh := make(chan message.ImmutableMessage, 8)
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
			default:
				t.Fatalf("unexpected WAL open %d for %s", opened, walName)
				return nil, nil
			}
		},
		func(walName message.WALName, _ string) {
			readerSwitches = append(readerSwitches, walName)
		},
	)

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	require.Equal(t, uint64(300), catchup.exclusiveStartTimeTick)
	require.Equal(t, 3, opened)
	require.Equal(t, []message.WALName{
		message.WALNameWoodpecker,
		message.WALNameRocksmq,
		message.WALNameTest,
	}, readerSwitches)

	var timeTicks []uint64
	for len(outputCh) > 0 {
		msg := <-outputCh
		if msg.MessageType() == message.MessageTypeTimeTick {
			timeTicks = append(timeTicks, msg.TimeTick())
		}
	}
	require.Equal(t, []uint64{150, 250}, timeTicks)
}

func TestSwitchableScannerStartAfterAlterWALMarker(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	markerID := rmq.NewRmqID(2)
	oldWAL := newTestReadWAL(t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName_Test, 100, markerID, rmq.NewRmqID(1)),
	)
	outputCh := make(chan message.ImmutableMessage, 1)

	scanner := newSwitchableScanner(
		"start-after-marker-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartAfter(markerID),
		outputCh,
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			return oldWAL, nil
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	require.Equal(t, uint64(100), catchup.exclusiveStartTimeTick)
	require.Empty(t, outputCh)
}

func TestSwitchableScannerFallsBackWhenReadWALIsUnavailable(t *testing.T) {
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
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	_, ok = catchup.deliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
	require.True(t, ok)
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

	recoveredWAL := newTestReadWAL(t, message.WALNameRocksmq, channel,
		newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(3), rmq.NewRmqID(2)),
	)
	openCount := 0
	outputCh := make(chan message.ImmutableMessage, 2)
	scanner := newSwitchableScanner(
		"retry-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(context.Context, message.WALName, types.PChannelInfo) (walimpls.ROWALImpls, error) {
			openCount++
			if openCount == 1 {
				return failedWAL, nil
			}
			return recoveredWAL, nil
		},
		nil,
	)

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	catchup, ok := next.(*catchupScanner)
	require.True(t, ok)
	require.Equal(t, uint64(100), catchup.exclusiveStartTimeTick)
	require.Equal(t, 2, openCount)
	require.Equal(t, uint64(90), (<-outputCh).TimeTick())
	require.Equal(t, message.MessageTypeAlterWAL, (<-outputCh).MessageType())
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

	for i := 0; i < totalMessages; i++ {
		result := tracker.Track(ids[i])
		if i < windowSize {
			assert.Equal(t, ids[0], result)
		} else {
			expected := ids[i-windowSize]
			assert.Equal(t, expected, result)
		}
	}
}
