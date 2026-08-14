// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segment

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSegmentTaskRetriesErrorsUntilSuccess(t *testing.T) {
	businessErr := errors.New("business failure")
	predecessor := &testSegmentTask{err: businessErr}
	successor := &testSegmentTask{
		segmentTaskBase: segmentTaskBase{predecessors: []segmentTask{predecessor}},
	}

	require.ErrorIs(t, successor.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.Equal(t, int32(0), successor.calls.Load())
	assert.False(t, successor.Done())

	err := predecessor.Execute(context.Background())
	require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	require.ErrorIs(t, err, businessErr)
	assert.False(t, predecessor.Done())
	require.ErrorIs(t, successor.Execute(context.Background()), nodescheduler.ErrDelay)

	predecessor.err = nil
	require.NoError(t, predecessor.Execute(context.Background()))
	assert.True(t, predecessor.Done())
	require.NoError(t, successor.Execute(context.Background()))
	assert.Equal(t, int32(1), successor.calls.Load())
	assert.True(t, successor.Done())
}

func TestSegmentTaskDoesNotInterpretCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	task := &testSegmentTask{err: context.Canceled}
	err := task.Execute(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.False(t, task.Done())
}

func TestStaleFinalCommitTaskSkipsAfterDataVersionAdvances(t *testing.T) {
	ctx := context.Background()
	recorder := &segmentTaskRecorder{
		commitVersions: []*viewpb.DataVersion{
			{StreamingVersion: 1},
			{StreamingVersion: 2},
			{StreamingVersion: 3},
		},
	}
	first := newFinalCommitTestSegment(recorder, 100)
	newer := newFinalCommitTestSegment(recorder, 200)

	require.NoError(t, (&commitL1SegmentTask{
		segmentTaskBase: segmentTaskBase{segment: first},
		timetick:        30,
	}).Execute(ctx))
	require.NoError(t, (&commitL1SegmentTask{
		segmentTaskBase: segmentTaskBase{segment: newer},
		timetick:        40,
	}).Execute(ctx))

	stale := &commitL1SegmentTask{
		segmentTaskBase: segmentTaskBase{segment: first},
		timetick:        30,
	}
	require.NoError(t, stale.Execute(ctx))
	assert.True(t, stale.Done())
	assert.Equal(t, []int64{100, 200}, recorder.commitSegmentIDs)
	assert.Equal(t, int64(1), first.AssignmentMeta().GetSealedAtDataVersion().GetStreamingVersion())
}

func TestRecoveredFinalCommitIsNotRepeated(t *testing.T) {
	recorder := &segmentTaskRecorder{}
	meta := newFinalCommitTestMeta(100)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	meta.CheckpointTimeTick = 30
	meta.DataCheckpointTimeTick = 30
	meta.SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 10}
	segment := NewSegmentViewFromMeta(
		meta,
		&schemapb.CollectionSchema{},
		runtimeConfig{lifecycle: recorder, metaAndData: true, owner: &recordingSegmentViewOwner{}},
	)
	task := &commitL1SegmentTask{
		segmentTaskBase: segmentTaskBase{segment: segment},
		timetick:        30,
	}

	require.NoError(t, task.Execute(context.Background()))
	assert.True(t, task.Done())
	assert.Empty(t, recorder.commitSegmentIDs)
}

func TestRecoveredDataCheckpointDoesNotProveFinalCommit(t *testing.T) {
	recorder := &segmentTaskRecorder{}
	meta := newFinalCommitTestMeta(100)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	meta.CheckpointTimeTick = 30
	meta.DataCheckpointTimeTick = 30
	segment := NewSegmentViewFromMeta(
		meta,
		&schemapb.CollectionSchema{},
		runtimeConfig{lifecycle: recorder, metaAndData: true, owner: &recordingSegmentViewOwner{}},
	)
	segment.mu.Lock()
	task := segment.newRecoveredCommitL1SegmentTaskLocked(30)
	segment.mu.Unlock()
	recovered, ok := task.(*commitL1SegmentTask)
	require.True(t, ok)
	assert.Zero(t, recovered.flushTimeTick)

	require.NoError(t, recovered.Execute(context.Background()))

	assert.True(t, recovered.Done())
	assert.Equal(t, []int64{100}, recorder.commitSegmentIDs)
}

func TestEnsureFinalCommitSchedulesOneTaskUntilSealedVersionIsInstalled(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	recorder := &segmentTaskRecorder{}
	meta := newFinalCommitTestMeta(100)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	meta.CheckpointTimeTick = 30
	meta.DataCheckpointTimeTick = 20
	segment := NewSegmentViewFromMeta(
		meta,
		&schemapb.CollectionSchema{},
		runtimeConfig{
			lifecycle:   recorder,
			metaAndData: true,
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)

	assert.False(t, segment.EnsureFinalCommit())
	require.Len(t, scheduler.tasks, 1)
	assert.False(t, segment.EnsureFinalCommit())
	require.Len(t, scheduler.tasks, 1)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, segment.EnsureFinalCommit())
	require.Len(t, scheduler.tasks, 1)
	require.NotNil(t, segment.AssignmentMeta().GetSealedAtDataVersion())
}

type testSegmentTask struct {
	segmentTaskBase
	err   error
	calls atomic.Int32
}

func (t *testSegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(context.Context) error {
		t.calls.Add(1)
		return t.err
	})
}

type segmentTaskRecorder struct {
	commitSegmentIDs []int64
	commitVersions   []*viewpb.DataVersion
}

func (r *segmentTaskRecorder) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return nil
}

func (r *segmentTaskRecorder) CommitL1Segment(_ context.Context, meta *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	r.commitSegmentIDs = append(r.commitSegmentIDs, meta.GetSegmentId())
	if len(r.commitVersions) == 0 {
		return &viewpb.DataVersion{StreamingVersion: 1}, nil
	}
	version := r.commitVersions[0]
	r.commitVersions = r.commitVersions[1:]
	return version, nil
}

func newFinalCommitTestSegment(recorder *segmentTaskRecorder, segmentID int64) *SegmentView {
	return NewSegmentViewFromMeta(
		newFinalCommitTestMeta(segmentID),
		&schemapb.CollectionSchema{},
		runtimeConfig{lifecycle: recorder, metaAndData: true, owner: &recordingSegmentViewOwner{}},
	)
}

func newFinalCommitTestMeta(segmentID int64) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        10,
		SegmentId:          segmentID,
		Vchannel:           "v1",
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick: 10,
		PersistedStorage:   &streamingpb.L1SegmentPersistedStorage{},
		Stat:               &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 10},
	}
}
