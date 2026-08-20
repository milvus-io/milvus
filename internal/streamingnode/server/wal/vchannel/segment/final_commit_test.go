package segment

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}},
	)

	view.mu.Lock()
	task := view.newCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()
	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, 1, lifecycle.calls)
	require.True(t, view.AssignmentMeta().GetL1CommitDone())
	require.True(t, view.HasDirty())

	recovered := NewSegmentViewFromMeta(view.AssignmentMeta(), nil)
	require.True(t, recovered.finalCommitDone)
	require.True(t, recovered.EnsureFinalCommit())
}

func TestFinalCommitRejectsReplayInsert(t *testing.T) {
	// A FLUSHED segment whose final commit is done can never persist new data
	// again: no flush task is ever scheduled for it. Replay inserts must be
	// rejected so their handles are released instead of stalling the
	// checkpoint, and before the commit completes re-deliveries of data
	// already covered by the L1 commit (timetick <= checkpoint) stay accepted.
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: &testSegmentLifecycle{}, owner: testSegmentOwner{}},
	)

	view.mu.Lock()
	defer view.mu.Unlock()
	assert.False(t, view.finalCommitDone)
	assert.True(t, view.canReplayInsertLocked(5), "pre-commit re-delivery within flushed window should be accepted")
	assert.False(t, view.canReplayInsertLocked(11), "data beyond the flushed window should never be accepted")

	view.finalCommitDone = true
	assert.False(t, view.canReplayInsertLocked(5), "post-commit nothing may be accepted into the buffer")
}

func TestFinalCommitFailureKeepsMessageDurabilityPending(t *testing.T) {
	lifecycle := &testSegmentLifecycle{err: errors.New("commit failed")}
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
		},
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}},
	)

	view.mu.Lock()
	task := view.newCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()
	err := task.Execute(context.Background())
	require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	require.False(t, view.AssignmentMeta().GetL1CommitDone())
	require.False(t, view.finalCommitDone)
}

func TestBuildCommitL1SegmentRequestPreservesDurableStorageState(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId: 1,
		PartitionId:  2,
		SegmentId:    3,
		Vchannel:     "v1",
		Stat:         &streamingpb.SegmentAssignmentStat{ModifiedRows: 10},
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
}
