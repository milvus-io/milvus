package messageack

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type recordingDataPersister struct {
	mu       sync.Mutex
	requests []persistRequest
}

func (p *recordingDataPersister) RequestPersistThrough(vchannel string, targetTimeTick uint64) {
	p.mu.Lock()
	p.requests = append(p.requests, persistRequest{vchannel: vchannel, targetTimeTick: targetTimeTick})
	p.mu.Unlock()
}

func (p *recordingDataPersister) snapshot() []persistRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]persistRequest(nil), p.requests...)
}

func TestTrackerDerivesCheckpointFromMessage(t *testing.T) {
	lastConfirmed := walimplstest.NewTestMessageID(100)
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 200, lastConfirmed).
		IntoImmutableMessage(walimplstest.NewTestMessageID(101))
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil, nil)

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
	}, nil)

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
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil, nil)
	raw := testBroadcastMessage(t, 2, 20)
	owner := tracker.Track(raw)

	owner.Release()

	assert.Equal(t, raw.TimeTick(), tracker.CompletedPoint().TimeTick)
}

func TestTrackerCompletedPointReturnsCopy(t *testing.T) {
	tracker := NewTracker(utility.WALConsumeCheckpoint{TimeTick: 10}, nil, nil)

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
	}, nil)

	owner := tracker.Track(testMessage(t, 1, 20))
	owner.Release()

	completed := tracker.CompletedPoint()
	require.True(t, initial.MessageID.EQ(completed.MessageID))
	assert.Equal(t, initial.TimeTick, completed.TimeTick)
	assert.Zero(t, advanceCount)
	assert.Zero(t, tracker.Pending())
}

func TestTrackerRequestsPersistencePerStalledVChannel(t *testing.T) {
	persister := &recordingDataPersister{}
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil, persister)
	v1First := retainTrackedMessage(tracker, testVChannelMessage(t, "v1", 2, 20))
	v1Second := retainTrackedMessage(tracker, testVChannelMessage(t, "v1", 3, 30))
	v1Third := retainTrackedMessage(tracker, testVChannelMessage(t, "v1", 5, 50))
	v2 := retainTrackedMessage(tracker, testVChannelMessage(t, "v2", 4, 40))

	now := time.Now()
	tracker.mu.Lock()
	tracker.vchannels["v1"].pending[0].trackedAt = now.Add(-2 * time.Minute)
	tracker.vchannels["v1"].pending[1].trackedAt = now.Add(-2 * time.Minute)
	tracker.vchannels["v1"].pending[2].trackedAt = now
	tracker.vchannels["v2"].pending[0].trackedAt = now.Add(time.Hour)
	tracker.mu.Unlock()

	// The target is the greatest stalled TimeTick, not the latest TimeTick
	// observed on the VChannel.
	tracker.triggerStalledVChannels(now, time.Minute)
	require.Equal(t, []persistRequest{{vchannel: "v1", targetTimeTick: 30}}, persister.snapshot())

	// The same stalled frontier requests persistence only once.
	tracker.triggerStalledVChannels(now.Add(30*time.Second), time.Minute)
	require.Len(t, persister.snapshot(), 1)

	// Completing the first head does not repeat an already-requested frontier.
	v1First.Release()
	tracker.triggerStalledVChannels(now.Add(30*time.Second), time.Minute)
	require.Len(t, persister.snapshot(), 1)

	// Once the newer message also stalls, it advances the requested frontier.
	tracker.mu.Lock()
	tracker.vchannels["v1"].pending[1].trackedAt = now.Add(-2 * time.Minute)
	tracker.mu.Unlock()
	tracker.triggerStalledVChannels(now.Add(time.Minute), time.Minute)
	require.Equal(t, []persistRequest{
		{vchannel: "v1", targetTimeTick: 30},
		{vchannel: "v1", targetTimeTick: 50},
	}, persister.snapshot())

	v1Second.Release()
	v1Third.Release()
	v2.Release()
}

func TestTrackerRemovesCompletedVChannelFromStallDetection(t *testing.T) {
	persister := &recordingDataPersister{}
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil, persister)
	v1 := retainTrackedMessage(tracker, testVChannelMessage(t, "v1", 2, 20))
	v2Owner := tracker.Track(testVChannelMessage(t, "v2", 3, 30))
	v2Owner.Release()

	now := time.Now()
	tracker.mu.Lock()
	tracker.vchannels["v1"].pending[0].trackedAt = now.Add(-2 * time.Minute)
	_, v2Pending := tracker.vchannels["v2"]
	tracker.mu.Unlock()
	require.False(t, v2Pending)

	tracker.triggerStalledVChannels(now, time.Minute)
	require.Equal(t, []persistRequest{{vchannel: "v1", targetTimeTick: 20}}, persister.snapshot())
	v1.Release()
}

func TestTrackerRunStopsWithContext(t *testing.T) {
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil, &recordingDataPersister{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tracker.Run(ctx, time.Hour)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tracker stall detector did not stop")
	}
}

func retainTrackedMessage(tracker *Tracker, raw message.ImmutableMessage) message.RetainedImmutableMessage {
	owner := tracker.Track(raw)
	handle := owner.Clone()
	owner.Release()
	return handle
}

func testMessage(t *testing.T, messageID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	id := walimplstest.NewTestMessageID(messageID)
	return message.CreateTestTimeTickSyncMessage(t, 1, timetick, id).
		IntoImmutableMessage(walimplstest.NewTestMessageID(messageID + 100))
}

func testBroadcastMessage(t *testing.T, messageID int64, timetick uint64) message.ImmutableMessage {
	return testVChannelMessage(t, "v1", messageID, timetick)
}

func testVChannelMessage(t *testing.T, vchannel string, messageID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	broadcast := message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{vchannel}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildBroadcast().
		WithBroadcastID(1)
	return broadcast.SplitIntoMutableMessage()[0].WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(messageID)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(messageID + 100))
}
