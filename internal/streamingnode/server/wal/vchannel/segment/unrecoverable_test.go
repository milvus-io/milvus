package segment

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// newTrackedRetained builds an owned message whose retained handle reports,
// via the exclusive callback, when it has been released back to the owner. The
// owner is returned too so the test can drain it at the end.
func newTrackedRetained(t *testing.T, timetick uint64, released *atomic.Int32) (message.RetainedImmutableMessage, message.OwnedImmutableMessage) {
	t.Helper()
	raw := message.CreateTestInsertMessage(t, 1, 1, timetick, walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	owner.RegisterExclusiveCallback(func() {
		released.Add(1)
	})
	return retained, owner
}

// TestMarkUnrecoverablePoisonsAllPending covers the poison contract of the
// terminal pivot: every retained handle in all three pending structures is
// poisoned (so a consumer can enumerate and handle each message separately)
// and released, and the structures are drained so a failed segment pins nothing
// in memory. The poison mark is message-level (shared on the core), so it
// survives on the owner's copy after the pending handle is released.
func TestMarkUnrecoverablePoisonsAllPending(t *testing.T) {
	var released atomic.Int32
	h10, o10 := newTrackedRetained(t, 10, &released)
	h20, o20 := newTrackedRetained(t, 20, &released)
	h30, o30 := newTrackedRetained(t, 30, &released)
	defer func() {
		o10.Release()
		o20.Release()
		o30.Release()
	}()

	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	view.mu.Lock()
	view.pending.appendMessage(h10, 1, 1)
	view.pending.appendMessage(h20, 1, 1)
	view.pendingDataHandles = append(view.pendingDataHandles, pendingDataHandle{timetick: 30, message: h30})
	view.mu.Unlock()

	terminalErr := errors.New("terminal")
	view.markUnrecoverable(context.Background(), terminalErr)

	require.Equal(t, terminalErr, view.unrecoverableErr())
	view.mu.Lock()
	assert.Empty(t, view.pending.entries)
	assert.Empty(t, view.pendingDataHandles)
	assert.Empty(t, view.pendingFlushChunks)
	view.mu.Unlock()
	assert.Equal(t, int32(3), released.Load(), "every pending handle is poisoned and released")

	// The poison mark is message-level and survives on the shared core: a
	// consumer holding any handle of the same message observes it, even though
	// the pending handle itself is gone.
	for _, owner := range []message.OwnedImmutableMessage{o10, o20, o30} {
		probe := owner.Clone()
		require.True(t, probe.IsPoisoned(), "message must be observable as poisoned through any handle")
		probe.Release()
	}
}

// TestMarkUnrecoverablePoisonsFlushChunk covers the same poison contract when
// the unpersisted messages live in a pending flush chunk rather than the
// pending buffer or data handles.
func TestMarkUnrecoverablePoisonsFlushChunk(t *testing.T) {
	var released atomic.Int32
	h10, o10 := newTrackedRetained(t, 10, &released)
	h20, o20 := newTrackedRetained(t, 20, &released)
	defer func() {
		o10.Release()
		o20.Release()
	}()

	chunk := writeOnlyInsertBuffer{}
	chunk.appendMessage(h10, 1, 1)
	chunk.appendMessage(h20, 1, 1)

	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	view.mu.Lock()
	view.pendingFlushChunks = append(view.pendingFlushChunks, chunk)
	view.mu.Unlock()

	view.markUnrecoverable(context.Background(), errors.New("terminal"))
	view.mu.Lock()
	assert.Empty(t, view.pendingFlushChunks)
	view.mu.Unlock()
	assert.Equal(t, int32(2), released.Load(), "every flush chunk handle is poisoned and released")

	for _, owner := range []message.OwnedImmutableMessage{o10, o20} {
		probe := owner.Clone()
		require.True(t, probe.IsPoisoned(), "message must be observable as poisoned through any handle")
		probe.Release()
	}
}

// TestTerminalSegmentObservationGates verifies that every observe entry point
// that can retain data poisons the incoming message instead once the segment is
// unrecoverable: the message is not silently dropped (the poison lets a
// consumer handle it separately) and is never retained, so no handle is pinned
// after a terminal failure and no new flush/commit work can be enqueued.
func TestTerminalSegmentObservationGates(t *testing.T) {
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId: 1,
			Vchannel:  "v1",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	view.markUnrecoverable(context.Background(), errors.New("terminal"))

	// ObserveInsert poisons the incoming handle and retains nothing.
	insertRaw := newObserveTestInsert(t, 1, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 3, 4),
	})
	insertOwner := message.NewOwnedImmutableMessage(insertRaw, nil)
	insertDispatch := insertOwner.Clone()
	batches, err := BuildInsertBatches(insertRaw)
	require.NoError(t, err)
	assert.False(t, view.ObserveInsert(context.Background(), insertDispatch, batches[1]))
	require.True(t, insertDispatch.IsPoisoned(), "terminal ObserveInsert must poison the incoming message")
	view.mu.Lock()
	assert.Empty(t, view.pending.entries, "terminal ObserveInsert must not retain the insert handle")
	view.mu.Unlock()
	insertDispatch.Release()
	insertOwner.Release()

	// ObserveCreateSegmentMessageV2 poisons the incoming handle and retains
	// nothing.
	createRaw := message.CreateTestCreateSegmentMessage(t, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	createOwner := message.NewOwnedImmutableMessage(createRaw, nil)
	createRetained := message.MustAsOwnedImmutableCreateSegmentMessageV2(createOwner).Clone()
	assert.False(t, view.ObserveCreateSegmentMessageV2(context.Background(), createRetained))
	require.True(t, createRetained.IsPoisoned(), "terminal ObserveCreateSegmentMessageV2 must poison the incoming message")
	view.mu.Lock()
	assert.Empty(t, view.pendingDataHandles, "terminal ObserveCreateSegmentMessageV2 must not retain the create handle")
	view.mu.Unlock()
	createRetained.Release()
	createOwner.Release()

	// Flush poisons the incoming flush message and retains nothing.
	flushRaw := message.NewFlushMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	flushOwner := message.NewOwnedImmutableMessage(flushRaw, nil)
	flushDispatch := flushOwner.Clone()
	assert.False(t, view.Flush(context.Background(), flushDispatch))
	require.True(t, flushDispatch.IsPoisoned(), "terminal Flush must poison the incoming flush message")
	view.mu.Lock()
	assert.Empty(t, view.pendingDataHandles, "terminal Flush must not retain the flush handle")
	view.mu.Unlock()
	flushDispatch.Release()
	flushOwner.Release()

	// RequestPersistThrough rejects without enqueueing.
	assert.False(t, view.RequestPersistThrough(1))

	// FlushInsertChunk rejects with the terminal error instead of writing.
	require.Error(t, view.FlushInsertChunk(context.Background(), 1))

	// EnsureFinalCommit must not report "durably committed" for a segment that
	// will never commit.
	require.False(t, view.EnsureFinalCommit())

	// IsGrowing and WritePathRecoveryState are pure lifecycle-state predicates:
	// the unrecoverable health marker only fast-fails persistence tasks, it
	// never changes what state the segment reports, so a GROWING terminal
	// segment still reports itself as GROWING.
	require.True(t, view.IsGrowing())
	_, ok := view.WritePathRecoveryState()
	require.True(t, ok)
}
