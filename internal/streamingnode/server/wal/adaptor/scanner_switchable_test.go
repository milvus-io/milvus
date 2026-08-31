package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	mock_message "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestCatchupScannerUsesAdaptorForCurrentWALPosition(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)
	currentMessage := newTestTimeTickMessage(100, walimplstest.NewTestMessageID(2), walimplstest.NewTestMessageID(1))
	readWAL := newTestReadWAL(t, message.WALNameTest, channel, currentMessage)
	openedWALNames := make(chan message.WALName, 4)

	scanner := newSwithableScanner(
		"current-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(1)),
		make(chan message.ImmutableMessage),
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			openedWALNames <- walName
			return readWAL, nil
		},
		nil,
	)

	catchup, ok := scanner.(*catchupScanner)
	require.True(t, ok)
	openedScanner, err := catchup.openCatchupScannerImpls(context.Background())
	require.NoError(t, err)
	require.Equal(t, currentMessage, <-openedScanner.Chan())
	require.Equal(t, message.WALNameTest, <-openedWALNames)
	require.NoError(t, openedScanner.Close())
}

// A position of an older migration generation names the backend that is current
// again after an A->B->A migration. Reading it directly from the current WAL
// would resume inside the reused topic and silently skip everything that was
// written while B was current, so it must replay the whole marker chain.
func TestCatchupScannerReplaysChainForOldGenerationPosition(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel"}
	currentWAL := newTestCurrentWAL(t, channel)

	oldGenerationMessage := newTestTimeTickMessage(50, walimplstest.NewTestMessageID(2), walimplstest.NewTestMessageID(1))
	toRocksmq := newTestAlterWALMessage(commonpb.WALName_RocksMQ, 100, walimplstest.NewTestMessageID(3), walimplstest.NewTestMessageID(2))
	// The generation that must not be skipped.
	middleGenerationMessage := newTestTimeTickMessage(150, rmq.NewRmqID(2), rmq.NewRmqID(1))
	backToTest := newTestAlterWALMessage(commonpb.WALName_Test, 200, rmq.NewRmqID(3), rmq.NewRmqID(2))
	currentGenerationMessage := newTestTimeTickMessage(250, walimplstest.NewTestMessageID(5), walimplstest.NewTestMessageID(4))

	oldGenerationWAL := newTestReadWAL(t, message.WALNameTest, channel, oldGenerationMessage, toRocksmq)
	middleGenerationWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, middleGenerationMessage, backToTest)
	currentGenerationWAL := newTestReadWAL(t, message.WALNameTest, channel, currentGenerationMessage)
	testWALs := []walimpls.ROWALImpls{oldGenerationWAL, currentGenerationWAL}

	scanner := newSwithableScanner(
		"old-generation-reader",
		mlog.With(),
		currentWAL,
		nil,
		options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(1)),
		make(chan message.ImmutableMessage),
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
			if walName == message.WALNameRocksmq {
				return middleGenerationWAL, nil
			}
			require.Equal(t, message.WALNameTest, walName)
			require.NotEmpty(t, testWALs)
			opened := testWALs[0]
			testWALs = testWALs[1:]
			return opened, nil
		},
		nil,
	)

	catchup, ok := scanner.(*catchupScanner)
	require.True(t, ok)
	openedScanner, err := catchup.openCatchupScannerImpls(context.Background())
	require.NoError(t, err)
	for _, expected := range []message.ImmutableMessage{
		oldGenerationMessage,
		toRocksmq,
		middleGenerationMessage,
		backToTest,
		currentGenerationMessage,
	} {
		select {
		case msg := <-openedScanner.Chan():
			require.Equal(t, expected, msg)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s at time tick %d", expected.MessageType(), expected.TimeTick())
		}
	}
	require.NoError(t, openedScanner.Close())
}

func TestCrossWALCatchupSwitchesToTailingWAB(t *testing.T) {
	channel := types.PChannelInfo{Name: "test-channel", AccessMode: types.AccessModeRW}
	marker := newTestAlterWALMessage(commonpb.WALName_Test, 100, rmq.NewRmqID(2), rmq.NewRmqID(1))
	oldWAL := newTestReadWAL(t, message.WALNameRocksmq, channel, marker)
	currentTimeTick := newTestTimeTickMessage(200, walimplstest.NewTestMessageID(1), walimplstest.NewTestMessageID(1))
	currentWAL := newTestCurrentWAL(t, channel)
	currentReadWAL := newTestReadWAL(t, message.WALNameTest, channel, currentTimeTick)
	writeAheadBuffer := wab.NewWriteAheadBuffer(
		channel.Name,
		mlog.With(),
		1024*1024,
		time.Minute,
		currentTimeTick,
	)
	defer writeAheadBuffer.Close()

	outputCh := make(chan message.ImmutableMessage, 4)
	readerWALNames := make([]message.WALName, 0, 2)
	scanner := newSwithableScanner(
		"cross-wal-catchup-reader",
		mlog.With(),
		currentWAL,
		writeAheadBuffer,
		options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
		outputCh,
		func(_ context.Context, walName message.WALName, _ types.PChannelInfo) (walimpls.ROWALImpls, error) {
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
		func(walName message.WALName) {
			readerWALNames = append(readerWALNames, walName)
		},
	)

	next, err := scanner.Do(context.Background())
	require.NoError(t, err)
	tailing, ok := next.(*tailingScanner)
	require.True(t, ok)
	require.Equal(t, marker, <-outputCh)
	require.Equal(t, currentTimeTick, <-outputCh)
	require.Equal(t, []message.WALName{message.WALNameRocksmq, message.WALNameTest}, readerWALNames)

	idleTimeTick := newTestNonPersistedTimeTickMessage(
		201,
		walimplstest.NewTestMessageID(1),
		walimplstest.NewTestMessageID(1),
	)
	writeAheadBuffer.Append(nil, idleTimeTick)

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan switchableScannerResult, 1)
	go func() {
		next, err := tailing.Do(ctx)
		resultCh <- switchableScannerResult{next: next, err: err}
	}()

	receivedIdleTimeTick, isTailing := isTailingScanImmutableMessage(<-outputCh)
	require.True(t, isTailing)
	require.Equal(t, idleTimeTick, receivedIdleTimeTick)
	cancel()
	result := <-resultCh
	require.Nil(t, result.next)
	require.ErrorIs(t, result.err, context.Canceled)
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
