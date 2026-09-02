package segment

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TestSegmentViewMetaConstructors covers the meta-driven view constructors and
// the create-segment-message path that builds a view from WAL observations.
func TestSegmentViewMetaConstructors(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        2,
		SegmentId:          3,
		Vchannel:           "v1",
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick: 10,
		Stat:               &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
	}
	schema := &schemapb.CollectionSchema{}

	view := NewSegmentViewFromMetaWithConfig(meta, schema, ViewConfig{})
	require.NotNil(t, view)
	assert.Equal(t, int64(3), view.AssignmentMeta().GetSegmentId())
	assert.Equal(t, int64(1), view.CollectionID())
	assert.Equal(t, int64(2), view.PartitionID())
	assert.Equal(t, "v1", view.VChannel())
	id, vch := view.IDAndVChannel()
	assert.Equal(t, int64(3), id)
	assert.Equal(t, "v1", vch)
	assert.Equal(t, uint64(5), view.CreateTimeTick())
	assert.True(t, view.IsGrowing())
	assert.Equal(t, uint64(10), view.PersistedCheckpointTimeTick(), "recovery restores the durable checkpoint as the persisted anchor")

	// shouldObserveLocked is the durable-checkpoint watermark.
	view.mu.Lock()
	assert.False(t, view.shouldObserveLocked(10), "equal to durable checkpoint is already persisted")
	assert.True(t, view.shouldObserveLocked(11), "beyond the durable checkpoint is not yet persisted")
	view.mu.Unlock()
}

// TestSegmentViewFromCreateSegmentMessage covers the create-segment-message
// constructor and the assignment-meta projection it builds.
func TestSegmentViewFromCreateSegmentMessage(t *testing.T) {
	raw := message.CreateTestCreateSegmentMessage(t, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	msg := message.MustAsImmutableCreateSegmentMessageV2(raw)

	meta := newSegmentAssignmentMetaFromCreateSegmentMessage(msg)
	assert.Equal(t, int64(1), meta.GetCollectionId())
	assert.Equal(t, int64(1), meta.GetPartitionId())
	assert.Equal(t, int64(1), meta.GetSegmentId())
	assert.Equal(t, "v1", meta.GetVchannel())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, meta.GetState())
	assert.Equal(t, uint64(10), meta.GetStat().GetCreateSegmentTimeTick())
	assert.Equal(t, uint64(1024), meta.GetStat().GetMaxBinarySize())

	view := NewSegmentViewFromCreateSegmentMessageWithConfig(msg, nil, ViewConfig{})
	require.NotNil(t, view)
	assert.Equal(t, int64(1), view.AssignmentMeta().GetSegmentId())
	assert.True(t, view.IsGrowing())
}

// TestShouldRetryRecoveredFinalCommit covers the legacy recovered final-commit
// predicate across all assignment states.
func TestShouldRetryRecoveredFinalCommit(t *testing.T) {
	flushed := &streamingpb.SegmentAssignmentMeta{
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick: 10,
	}
	assert.True(t, shouldRetryRecoveredFinalCommit(flushed))

	flushedNoCheckpoint := &streamingpb.SegmentAssignmentMeta{
		State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
	}
	assert.False(t, shouldRetryRecoveredFinalCommit(flushedNoCheckpoint))

	done := &streamingpb.SegmentAssignmentMeta{
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick: 10,
		L1CommitDone:       true,
	}
	assert.False(t, shouldRetryRecoveredFinalCommit(done))

	growing := &streamingpb.SegmentAssignmentMeta{
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick: 10,
	}
	assert.False(t, shouldRetryRecoveredFinalCommit(growing))

	tombstoned := &streamingpb.SegmentAssignmentMeta{
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		CheckpointTimeTick: 10,
	}
	assert.True(t, shouldRetryRecoveredFinalCommit(tombstoned))
}

// TestSegmentViewObserveCreateSegmentMessageV2 covers observing a create
// segment message: the view retains the message handle as the durability
// anchor for the whole segment and schedules the ensure-growing task.
func TestSegmentViewObserveCreateSegmentMessageV2(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
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
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)

	raw := message.CreateTestCreateSegmentMessage(t, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	owned := message.MustAsOwnedImmutableCreateSegmentMessageV2(owner)
	retained := owned.Clone()
	defer retained.Release()
	owner.Release()

	require.True(t, view.ObserveCreateSegmentMessageV2(context.Background(), retained))

	view.mu.Lock()
	assert.Len(t, view.pendingDataHandles, 1)
	assert.Equal(t, uint64(10), view.pendingDataHandles[0].timetick)
	view.mu.Unlock()
	require.Len(t, scheduler.tasks, 1, "create segment observation schedules the ensure-growing task")

	// A duplicate delivery at or below the observation watermark is rejected.
	require.False(t, view.ObserveCreateSegmentMessageV2(context.Background(), retained))

	view.mu.Lock()
	for _, h := range view.pendingDataHandles {
		h.message.Release()
	}
	view.pendingDataHandles = nil
	view.mu.Unlock()
}

// TestSegmentViewFlushTransitionsToFlushed covers the Flush success path:
// observeFlushMeta moves GROWING -> FLUSHED, the flush handle is retained as
// pending data, and the final-commit task is scheduled.
func TestSegmentViewFlushTransitionsToFlushed(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
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
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)

	flushRaw := message.NewFlushMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(10).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	flushOwner := message.NewOwnedImmutableMessage(flushRaw, nil)
	flushDispatch := flushOwner.Clone()
	require.True(t, view.Flush(context.Background(), flushDispatch))
	flushDispatch.Release()
	flushOwner.Release()

	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, view.AssignmentMeta().GetState())
	require.Equal(t, uint64(10), view.AssignmentMeta().GetCheckpointTimeTick())
	require.Len(t, scheduler.tasks, 1, "flush schedules the final commit task")

	// observeFlushMeta is idempotent once FLUSHED.
	view.mu.Lock()
	closed, _, changed := view.observeFlushMeta(11)
	assert.True(t, closed)
	assert.False(t, changed)
	view.mu.Unlock()

	// A tombstoned segment rejects any flush observation.
	tombstoned := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId: 1,
			Vchannel:  "v1",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)
	tombstoned.mu.Lock()
	closed, _, changed = tombstoned.observeFlushMeta(10)
	assert.False(t, closed)
	assert.False(t, changed)
	tombstoned.mu.Unlock()

	// An at-or-below-durable flush is skipped (already persisted).
	durable := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 5,
		},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)
	durable.mu.Lock()
	durable.durableMeta.CheckpointTimeTick = 10
	closed, checkpoint, changed := durable.observeFlushMeta(10)
	assert.False(t, closed, "at-or-below durable checkpoint is already persisted")
	assert.Equal(t, uint64(10), checkpoint)
	assert.False(t, changed)
	durable.mu.Unlock()

	view.mu.Lock()
	for _, h := range view.pendingDataHandles {
		h.message.Release()
	}
	view.pendingDataHandles = nil
	view.mu.Unlock()
}

// TestSegmentViewTombstoneLifecycle covers the tombstone state transitions and
// the persistence/cleanup readiness predicates.
func TestSegmentViewTombstoneLifecycle(t *testing.T) {
	// A FLUSHED, finally-committed segment with a durable checkpoint at or past
	// its observation watermark can be finalized to TOMBSTONED.
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
			L1CommitDone:       true,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	require.True(t, view.TryFinalizeTombstone())
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, view.AssignmentMeta().GetState())

	// Not dirty and persisted up to the checkpoint: tombstone is fully durable.
	view.mu.Lock()
	view.dirty = false
	view.persistedCheckpointTimeTick = 10
	view.mu.Unlock()
	assert.True(t, view.TombstonePersisted())
	assert.False(t, view.TombstonedCleanupReady(10), "cleanup needs physical time past the checkpoint")
	assert.True(t, view.TombstonedCleanupReady(11))

	// A dirty tombstoned segment is not persisted.
	view.mu.Lock()
	view.dirty = true
	view.mu.Unlock()
	assert.False(t, view.TombstonePersisted())
	assert.False(t, view.TombstonedCleanupReady(11))

	// A still-growing segment cannot be finalized.
	growing := newSegmentView(
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
	require.False(t, growing.TryFinalizeTombstone())

	// A flushed segment whose durable checkpoint trails its observation
	// watermark cannot be finalized either.
	pending := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
			L1CommitDone:       true,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	pending.mu.Lock()
	pending.durableMeta.CheckpointTimeTick = 5
	pending.mu.Unlock()
	require.False(t, pending.TryFinalizeTombstone())
}

// TestSegmentViewResumePendingRecovery covers retrying legacy recovered
// final-commit work: a durable-but-uncommitted FLUSHED segment gets a recovered
// commit task scheduled.
func TestSegmentViewResumePendingRecovery(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)
	view.ResumePendingRecovery()
	require.Len(t, scheduler.tasks, 1, "durable uncommitted segment schedules a recovered final commit")
	view.mu.Lock()
	require.NotNil(t, view.pendingFinalCommit,
		"non-terminal recovered commit registers the task as the pending final commit")
	view.mu.Unlock()

	// A GROWING segment has nothing to resume.
	growing := newSegmentView(
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
	growing.ResumePendingRecovery()
	assert.Empty(t, growing.runtime.Scheduler.(*recordingSegmentScheduler).tasks)

	// A finally-committed segment has nothing to resume.
	done := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
			L1CommitDone:       true,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	done.ResumePendingRecovery()
	assert.Empty(t, done.runtime.Scheduler.(*recordingSegmentScheduler).tasks)

	// A terminal segment has nothing to resume: its final commit can never be
	// accepted by the coordinator, and the explicit gate rejects before any
	// recovered commit task is enqueued.
	terminal := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	terminal.markUnrecoverable(context.Background(), errors.New("terminal"))
	terminal.ResumePendingRecovery()
	assert.Empty(t, terminal.runtime.Scheduler.(*recordingSegmentScheduler).tasks,
		"terminal segment must not enqueue a recovered final commit")
	terminal.mu.Lock()
	assert.Nil(t, terminal.pendingFinalCommit,
		"terminal segment must not register a recovered final commit; contrast with the non-terminal case above")
	terminal.mu.Unlock()
}

// TestMarkUnrecoverablePoisonsIdempotent covers the idempotence of the
// terminal sweep: the first markUnrecoverable poisons and releases every
// pending handle and clears the structures; a second markUnrecoverable (today
// prevented by the serial-task invariant, but kept structural by the
// empty-structures no-op) collects nothing and re-reports the same terminal
// state. It goes through markUnrecoverable twice so the reentrant guard and
// the log branch are exercised through the production entry point rather than
// by calling the locked helper directly.
func TestMarkUnrecoverablePoisonsIdempotent(t *testing.T) {
	var released atomic.Int32
	h10, o10 := newTrackedRetained(t, 10, &released)
	defer o10.Release()

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
	view.mu.Unlock()

	view.markUnrecoverable(context.Background(), errors.New("terminal"))
	view.mu.Lock()
	assert.Empty(t, view.pending.entries)
	view.mu.Unlock()
	assert.Equal(t, int32(1), released.Load(), "the first sweep poisons and releases the pending handle")

	probe := o10.Clone()
	require.True(t, probe.IsPoisoned(), "the first sweep poisons the pending message")
	probe.Release()

	// A second markUnrecoverable collects nothing (the structures are already
	// empty) and releases nothing.
	view.markUnrecoverable(context.Background(), errors.New("terminal again"))
	assert.Equal(t, int32(1), released.Load(), "idempotent second markUnrecoverable releases nothing")
	probe = o10.Clone()
	require.True(t, probe.IsPoisoned(), "the poison mark survives across a second sweep")
	probe.Release()
}

// TestSegmentTaskBaseDone covers the Done getter on the task base.
func TestSegmentTaskBaseDone(t *testing.T) {
	task := &segmentTaskBase{}
	assert.False(t, task.Done())
	task.done.Store(true)
	assert.True(t, task.Done())
}

// TestRuntimeConfigFromViewConfig covers the ViewConfig -> runtimeConfig
// projection used by the exported constructors.
func TestRuntimeConfigFromViewConfig(t *testing.T) {
	owner := testSegmentOwner{}
	cfg := runtimeConfigFromViewConfig(ViewConfig{
		Lifecycle: &testSegmentLifecycle{},
		Runtime:   moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}},
		Owner:     owner,
	})
	assert.NotNil(t, cfg.lifecycle)
	assert.NotNil(t, cfg.runtime.Scheduler)
	assert.NotNil(t, cfg.owner)
}

// TestPublicConstructors covers the exported lifecycle and pack-writer
// constructors: they are pure wiring that binds the given collaborators, so
// construction alone is the whole contract to verify.
func TestPublicConstructors(t *testing.T) {
	lifecycle := NewSegmentLifecycleWriter(nil, 1)
	require.NotNil(t, lifecycle)

	writer := NewBulkPackWriter(nil, nil, nil)
	require.NotNil(t, writer)
}

// TestWriteOnlyFlushPolicy covers the static flush-policy constructor and its
// ShouldFlush thresholds.
func TestWriteOnlyFlushPolicy(t *testing.T) {
	policy := newWriteOnlyFlushPolicy(10, 100, time.Second)
	assert.False(t, policy.ShouldFlush(writeOnlyInsertBuffer{}, 20), "empty buffer never flushes")

	start := tsoutil.ComposeTSByTime(time.Now())
	raw := message.CreateTestInsertMessage(t, 1, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	buffer := writeOnlyInsertBuffer{}
	buffer.appendMessage(retained, 10, 10)
	defer func() {
		retained.Release()
		owner.Release()
	}()
	assert.True(t, policy.ShouldFlush(buffer, start), "row threshold reached")
	assert.True(t, newWriteOnlyFlushPolicy(0, 10, 0).ShouldFlush(buffer, start), "byte threshold reached")
	assert.False(t, newWriteOnlyFlushPolicy(0, 0, 0).ShouldFlush(buffer, start), "no thresholds configured")

	old := writeOnlyInsertBuffer{}
	rawOld := message.CreateTestInsertMessage(t, 1, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	ownerOld := message.NewOwnedImmutableMessage(rawOld, nil)
	retainedOld := ownerOld.Clone()
	defer func() {
		retainedOld.Release()
		ownerOld.Release()
	}()
	old.appendMessage(retainedOld, 10, 10)
	old.fromTimeTick = tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour))
	assert.True(t, newWriteOnlyFlushPolicy(0, 0, time.Millisecond).ShouldFlush(old, start), "age threshold reached")
}
