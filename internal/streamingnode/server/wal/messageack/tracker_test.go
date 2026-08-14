package messageack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTrackerDerivesCheckpointFromMessage(t *testing.T) {
	lastConfirmed := walimplstest.NewTestMessageID(100)
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 200, lastConfirmed).
		IntoImmutableMessage(walimplstest.NewTestMessageID(101))
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil)

	owner := tracker.Track(raw)
	owner.Release()

	point := tracker.CompletedPoint()
	require.NotNil(t, point.MessageID)
	assert.True(t, lastConfirmed.EQ(point.MessageID))
	assert.Equal(t, uint64(200), point.TimeTick)
}

func TestTrackerAdvancesOnlyContinuousCompletedPrefix(t *testing.T) {
	initial := utility.WALConsumeCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
	}
	advanced := make([]utility.WALConsumeCheckpoint, 0, 2)
	tracker := NewTracker(initial, func(point utility.WALConsumeCheckpoint) {
		advanced = append(advanced, point)
	})

	first := tracker.Track(testMessage(t, 2, 20))
	second := tracker.Track(testMessage(t, 3, 30))
	firstHandle := first.Clone()
	secondHandle := second.Clone()
	first.Release()
	second.Release()

	secondHandle.Release()
	point := tracker.CompletedPoint()
	require.True(t, initial.MessageID.EQ(point.MessageID))
	assert.Equal(t, initial.TimeTick, point.TimeTick)
	assert.Empty(t, advanced)
	assert.Equal(t, 2, tracker.Pending())
	assert.Panics(t, func() { _ = second.Message() })
	tracker.mu.Lock()
	assert.Nil(t, tracker.pending[1].message)
	assert.True(t, tracker.pending[1].completed)
	tracker.mu.Unlock()

	firstHandle.Release()
	point = tracker.CompletedPoint()
	require.True(t, walimplstest.NewTestMessageID(3).EQ(point.MessageID))
	assert.Equal(t, uint64(30), point.TimeTick)
	require.Len(t, advanced, 1)
	require.True(t, walimplstest.NewTestMessageID(3).EQ(advanced[0].MessageID))
	assert.Equal(t, uint64(30), advanced[0].TimeTick)
	assert.Zero(t, tracker.Pending())
}

func TestTrackerTreatsBroadcastAsOrdinaryTrackedMessage(t *testing.T) {
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil)
	raw := testBroadcastMessage(t, 2, 20)
	owner := tracker.Track(raw)

	owner.Release()

	assert.Equal(t, raw.TimeTick(), tracker.CompletedPoint().TimeTick)
}

func TestTrackerCompletedPointReturnsCopy(t *testing.T) {
	tracker := NewTracker(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)

	point := tracker.CompletedPoint()
	point.TimeTick = 100

	assert.Equal(t, uint64(10), tracker.CompletedPoint().TimeTick)
}

func TestTrackerCompletedPointDoesNotRegressOnReplay(t *testing.T) {
	initial := utility.WALConsumeCheckpoint{
		MessageID: walimplstest.NewTestMessageID(3),
		TimeTick:  30,
	}
	advanceCount := 0
	tracker := NewTracker(initial, func(utility.WALConsumeCheckpoint) {
		advanceCount++
	})

	owner := tracker.Track(testMessage(t, 1, 20))
	owner.Release()

	completed := tracker.CompletedPoint()
	require.True(t, initial.MessageID.EQ(completed.MessageID))
	assert.Equal(t, initial.TimeTick, completed.TimeTick)
	assert.Zero(t, advanceCount)
	assert.Zero(t, tracker.Pending())
}

func testMessage(t *testing.T, messageID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	id := walimplstest.NewTestMessageID(messageID)
	return message.CreateTestTimeTickSyncMessage(t, 1, timetick, id).
		IntoImmutableMessage(walimplstest.NewTestMessageID(messageID + 100))
}

func testBroadcastMessage(t *testing.T, messageID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	broadcast := message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildBroadcast().
		WithBroadcastID(1)
	return broadcast.SplitIntoMutableMessage()[0].WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(messageID)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(messageID + 100))
}
