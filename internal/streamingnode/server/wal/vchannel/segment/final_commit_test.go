package segment

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type testSegmentLifecycle struct {
	err   error
	calls int
}

type testSegmentOwner struct{}

func (testSegmentOwner) SegmentDataUpdated(int64, *SegmentView) {}

func (*testSegmentLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return nil
}

func (l *testSegmentLifecycle) CommitL1Segment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	l.calls++
	return l.err
}

func TestFinalCommitPersistsStorageOwnedCompletionMarker(t *testing.T) {
	lifecycle := &testSegmentLifecycle{}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)

	view.mu.Lock()
	task := view.newCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()
	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, lifecycle.calls)
	require.True(t, view.AssignmentMeta().GetL1CommitDone())
	view.mu.Lock()
	require.True(t, view.dirty)
	view.mu.Unlock()

	recovered := newSegmentViewFromMeta(view.AssignmentMeta(), nil)
	require.True(t, recovered.finalCommitDone.Load())
	require.True(t, recovered.EnsureFinalCommit())
}

func TestFinalCommitRejectsReplayInsert(t *testing.T) {
	// A FLUSHED segment can never persist new data again: its buffer is
	// already fully covered by the L1 commit and no flush task is ever
	// scheduled for it. shouldObserveInsertLocked encodes this directly —
	// only a GROWING segment accepts inserts, so replay/out-of-order inserts
	// are rejected and their handles released instead of stalling the
	// checkpoint.
	flushed := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: &testSegmentLifecycle{}, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	flushed.mu.Lock()
	defer flushed.mu.Unlock()
	assert.False(t, flushed.finalCommitDone.Load())
	assert.False(t, flushed.shouldObserveInsertLocked(11), "a flushed segment accepts nothing, pre-commit")

	flushed.finalCommitDone.Store(true)
	assert.False(t, flushed.shouldObserveInsertLocked(11), "a flushed segment accepts nothing, post-commit")

	growing := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          2,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: &testSegmentLifecycle{}, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)
	growing.mu.Lock()
	defer growing.mu.Unlock()
	assert.True(t, growing.shouldObserveInsertLocked(11), "a growing segment accepts inserts beyond its checkpoint")
	assert.False(t, growing.shouldObserveInsertLocked(10), "a growing segment skips at-or-below its checkpoint")
}

func TestFinalCommitFailureKeepsMessageDurabilityPending(t *testing.T) {
	lifecycle := &testSegmentLifecycle{err: errors.New("commit failed")}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)

	view.mu.Lock()
	task := view.newCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()
	err := task.Execute(context.Background())
	require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	require.False(t, view.AssignmentMeta().GetL1CommitDone())
	require.False(t, view.finalCommitDone.Load())
}

// TestFinalCommitPoisonsAbandonedBuffer covers the low-1 anomaly branch: a
// non-empty pending buffer after the final commit (out-of-timetick-order
// inserts that slipped past the replay guards) can never be persisted, so the
// commit task poisons those handles instead of plain-releasing them — the loss
// stays observable to a consumer, not silently dropped.
func TestFinalCommitPoisonsAbandonedBuffer(t *testing.T) {
	lifecycle := &testSegmentLifecycle{}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)

	view.mu.Lock()
	task := view.newCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()

	// A stray insert lands in the pending buffer after the flush chunk was
	// drained: it can never be persisted, so the final commit must poison it.
	var released atomic.Int32
	stray, strayOwner := newTrackedRetained(t, 20, &released)
	view.mu.Lock()
	view.pending.appendMessage(stray, 1, 1)
	view.mu.Unlock()

	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, lifecycle.calls)
	view.mu.Lock()
	assert.Empty(t, view.pending.entries, "the abandoned handles are drained from the buffer")
	view.mu.Unlock()
	assert.Equal(t, int32(1), released.Load(), "the abandoned handle is released (poisoned)")

	probe := strayOwner.Clone()
	require.True(t, probe.IsPoisoned(), "the abandoned handle is poisoned, observable through any handle")
	probe.Release()
	strayOwner.Release()
}

func TestBuildCommitL1SegmentRequestPreservesDurableStorageState(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        2,
		SegmentId:          3,
		Vchannel:           "v1",
		CheckpointTimeTick: 50,
		Stat: &streamingpb.SegmentAssignmentStat{
			ModifiedRows:          10,
			CreateSegmentTimeTick: 20,
		},
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			DeltaBinlog: []*datapb.FieldBinlog{{FieldID: 100}},
			Statistics:  &datapb.Statistics{InsertBinlogSize: 123, DeltaBinlogSize: 45},
		},
	}

	req := buildCommitL1SegmentRequest(10, meta)

	require.Len(t, req.GetDeltalogs(), 1)
	assert.Equal(t, int64(100), req.GetDeltalogs()[0].GetFieldID())
	require.NotNil(t, req.GetStats())
	assert.Equal(t, int64(123), req.GetStats().GetInsertBinlogSize())
	assert.Equal(t, int64(45), req.GetStats().GetDeltaBinlogSize())
	assert.True(t, req.GetWithFullBinlogs())

	// The checkpoint position must be non-nil or DataCoord skips the update
	// and the flushed segment drops out of channel recovery.
	require.Len(t, req.GetCheckPoints(), 1)
	cp := req.GetCheckPoints()[0]
	assert.Equal(t, int64(3), cp.GetSegmentID())
	assert.Equal(t, int64(10), cp.GetNumOfRows())
	require.NotNil(t, cp.GetPosition())
	assert.Equal(t, "v1", cp.GetPosition().GetChannelName())
	assert.Equal(t, uint64(50), cp.GetPosition().GetTimestamp())

	require.Len(t, req.GetStartPositions(), 1)
	sp := req.GetStartPositions()[0]
	assert.Equal(t, int64(3), sp.GetSegmentID())
	require.NotNil(t, sp.GetStartPosition())
	assert.Equal(t, "v1", sp.GetStartPosition().GetChannelName())
	assert.Equal(t, uint64(20), sp.GetStartPosition().GetTimestamp())
}

// TestEnsureFinalCommitSurvivesTerminalError covers the low-1 ordering fix:
// a segment whose L1 commit already landed (finalCommitDone == true, e.g.
// restored from meta.L1CommitDone on recovery) is durably committed regardless
// of any later task failure. finalCommitDone is the authoritative fact and is
// checked before the terminal gate, so a terminal error must not invert the
// answer for an already-committed segment (EnsureFinalCommit stays true), and
// the two accessors (EnsureFinalCommit vs tombstoneFinalizeReadyLocked, which
// reads finalCommitDone directly) agree about the same fact.
func TestEnsureFinalCommitSurvivesTerminalError(t *testing.T) {
	committed := newSegmentView(
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
	require.True(t, committed.finalCommitDone.Load(), "L1CommitDone is restored into finalCommitDone")

	committed.markUnrecoverable(context.Background(), errors.New("terminal"))
	require.True(t, committed.EnsureFinalCommit(),
		"a durably committed segment stays committed even after a later terminal task error")
	require.True(t, committed.AssignmentMeta().GetL1CommitDone())
}
