package segment

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

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
			SegmentId:              1,
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
		},
		10,
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

func TestFinalCommitFailureKeepsMessageDurabilityPending(t *testing.T) {
	lifecycle := &testSegmentLifecycle{err: errors.New("commit failed")}
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              1,
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
		},
		10,
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
