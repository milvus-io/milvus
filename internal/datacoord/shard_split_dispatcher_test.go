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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakePlanReader struct {
	byTrigger map[int64][]*datapb.CompactionTask
}

func (f *fakePlanReader) GetCompactionTasksByTriggerID(_ context.Context, triggerID int64) []*datapb.CompactionTask {
	return f.byTrigger[triggerID]
}

type fakeInspector struct {
	enqueued []*datapb.CompactionTask
	err      error
}

func (f *fakeInspector) enqueueCompaction(task *datapb.CompactionTask) error {
	if f.err != nil {
		return f.err
	}
	f.enqueued = append(f.enqueued, task)
	return nil
}

type fakeAllocator struct {
	next  int64
	idErr error
	nErr  error
}

func (a *fakeAllocator) AllocTimestamp(context.Context) (uint64, error) {
	a.next++
	return uint64(a.next), nil
}

func (a *fakeAllocator) AllocID(context.Context) (int64, error) {
	if a.idErr != nil {
		return 0, a.idErr
	}
	a.next++
	return a.next, nil
}

func (a *fakeAllocator) AllocN(n int64) (int64, int64, error) {
	if a.nErr != nil {
		return 0, 0, a.nErr
	}
	begin := a.next + 1
	a.next += n
	return begin, a.next + 1, nil
}

func newTestRewriteDispatcher(t *testing.T, sourceSegments []int64) (*inspectorRewriteDispatcher, *fakeInspector, *fakePlanReader, *meta) {
	m := newHashRewriteMeta(t, sourceSegments)
	inspector := &fakeInspector{}
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}
	return newInspectorRewriteDispatcher(context.Background(), m, inspector, reader, &fakeAllocator{next: 900}), inspector, reader, m
}

func TestDispatcherBuildsHashSplitPlan(t *testing.T) {
	d, inspector, _, _ := newTestRewriteDispatcher(t, []int64{301})
	task := newHashTask([]int64{301})

	planID, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.NotZero(t, planID)
	require.Len(t, inspector.enqueued, 1)

	plan := inspector.enqueued[0]
	assert.Equal(t, planID, plan.GetPlanID())
	assert.Equal(t, datapb.CompactionType_HashSplitCompaction, plan.GetType())
	assert.Equal(t, datapb.CompactionTaskState_pipelining, plan.GetState())
	// The plan runs on the source channel, where its input lives...
	assert.Equal(t, hashSrcVChannel, plan.GetChannel())
	assert.EqualValues(t, 10, plan.GetPartitionID())
	assert.Equal(t, []int64{301}, plan.GetInputSegments())
	// ...and carries the targets and the modulus their residues are taken
	// against, which is how the datanode routes every row.
	require.Len(t, plan.GetHashSplitTargets(), 2)
	assert.Equal(t, hashTgtA, plan.GetHashSplitTargets()[0].GetVchannel())
	assert.Equal(t, hashTgtB, plan.GetHashSplitTargets()[1].GetVchannel())
	assert.EqualValues(t, 4, plan.GetHashSplitModulus())
	// The split task id is the trigger id, so the task's plans are findable.
	assert.Equal(t, task.GetTaskId(), plan.GetTriggerID())
	// One output id per target, and an output size bound.
	assert.EqualValues(t, 2, plan.GetPreAllocatedSegmentIDs().GetEnd()-plan.GetPreAllocatedSegmentIDs().GetBegin())
	assert.Positive(t, plan.GetMaxSize())
	assert.NotNil(t, plan.GetSchema())
}

func TestDispatcherIsIdempotentPerSegment(t *testing.T) {
	d, inspector, reader, _ := newTestRewriteDispatcher(t, []int64{301})
	task := newHashTask([]int64{301})

	planID, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	reader.byTrigger[task.GetTaskId()] = []*datapb.CompactionTask{
		// Another compaction type and another segment's plan are not it.
		{PlanID: 1, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{301}, State: datapb.CompactionTaskState_executing},
		{PlanID: 2, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_HashSplitCompaction, InputSegments: []int64{302}, State: datapb.CompactionTaskState_executing},
		{PlanID: planID, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_HashSplitCompaction, InputSegments: []int64{301}, State: datapb.CompactionTaskState_executing},
	}

	again, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.Equal(t, planID, again)
	assert.Len(t, inspector.enqueued, 1, "no second rewrite of the same segment")

	// A failed plan is not live: the segment is dispatched again.
	reader.byTrigger[task.GetTaskId()][2].State = datapb.CompactionTaskState_failed
	third, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.NotEqual(t, planID, third)
	assert.Len(t, inspector.enqueued, 2)
}

func TestDispatcherRefusesAPlanItCannotBuild(t *testing.T) {
	cases := []struct {
		name      string
		segmentID int64
		mutate    func(task *datapb.SplitShardTask, m *meta, alloc *fakeAllocator, inspector *fakeInspector)
		errIs     error
	}{
		{
			name:      "a segment absent from meta",
			segmentID: 999,
			mutate:    func(*datapb.SplitShardTask, *meta, *fakeAllocator, *fakeInspector) {},
			errIs:     merr.ErrSegmentNotFound,
		},
		{
			name:      "a segment of another channel",
			segmentID: 404,
			mutate: func(_ *datapb.SplitShardTask, m *meta, _ *fakeAllocator, _ *fakeInspector) {
				m.segments.SetSegment(404, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID: 404, CollectionID: splitMgrCollection, InsertChannel: splitMgrV3, State: commonpb.SegmentState_Flushed,
				}})
			},
			errIs: merr.ErrServiceInternal,
		},
		{
			name: "a collection datacoord does not hold",
			mutate: func(_ *datapb.SplitShardTask, m *meta, _ *fakeAllocator, _ *fakeInspector) {
				m.collections.Remove(splitMgrCollection)
			},
			errIs: merr.ErrCollectionNotFound,
		},
		{
			name: "a task without a modulus",
			mutate: func(task *datapb.SplitShardTask, _ *meta, _ *fakeAllocator, _ *fakeInspector) {
				task.RoutingModulus = 0
			},
			errIs: merr.ErrServiceInternal,
		},
		{
			name: "a task without its target vchannels",
			mutate: func(task *datapb.SplitShardTask, _ *meta, _ *fakeAllocator, _ *fakeInspector) {
				task.Targets[1].Vchannel = ""
			},
			errIs: merr.ErrServiceInternal,
		},
		{
			name: "no plan id",
			mutate: func(_ *datapb.SplitShardTask, _ *meta, alloc *fakeAllocator, _ *fakeInspector) {
				alloc.idErr = merr.WrapErrServiceUnavailable("allocator down")
			},
			errIs: merr.ErrServiceUnavailable,
		},
		{
			name: "no output ids",
			mutate: func(_ *datapb.SplitShardTask, _ *meta, alloc *fakeAllocator, _ *fakeInspector) {
				alloc.nErr = merr.WrapErrServiceUnavailable("allocator down")
			},
			errIs: merr.ErrServiceUnavailable,
		},
		{
			name: "the inspector refuses it",
			mutate: func(_ *datapb.SplitShardTask, _ *meta, _ *fakeAllocator, inspector *fakeInspector) {
				inspector.err = merr.WrapErrCompactionPlanConflict("segment is compacting")
			},
			errIs: merr.ErrCompactionPlanConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newHashRewriteMeta(t, []int64{301})
			inspector := &fakeInspector{}
			alloc := &fakeAllocator{next: 900}
			d := newInspectorRewriteDispatcher(context.Background(), m, inspector,
				&fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}, alloc)
			task := newHashTask([]int64{301})
			tc.mutate(task, m, alloc, inspector)
			segmentID := tc.segmentID
			if segmentID == 0 {
				segmentID = 301
			}
			_, err := d.DispatchHashSplit(task, segmentID)
			assert.ErrorIs(t, err, tc.errIs)
			assert.Empty(t, inspector.enqueued)
		})
	}
}

func TestHashSplitPlanStateMapping(t *testing.T) {
	cases := []struct {
		state   datapb.CompactionTaskState
		done    bool
		running bool
	}{
		{datapb.CompactionTaskState_completed, true, false},
		{datapb.CompactionTaskState_pipelining, false, true},
		{datapb.CompactionTaskState_executing, false, true},
		{datapb.CompactionTaskState_meta_saved, false, true},
		{datapb.CompactionTaskState_statistic, false, true},
		{datapb.CompactionTaskState_indexing, false, true},
		{datapb.CompactionTaskState_failed, false, false},
		{datapb.CompactionTaskState_timeout, false, false},
		{datapb.CompactionTaskState_cleaned, false, false},
	}
	for _, tc := range cases {
		done, running := hashSplitPlanTerminalState(tc.state)
		assert.Equal(t, tc.done, done, tc.state.String())
		assert.Equal(t, tc.running, running, tc.state.String())
	}
}

func TestDispatcherPlanStateLookup(t *testing.T) {
	d, _, reader, _ := newTestRewriteDispatcher(t, nil)
	reader.byTrigger[hashTaskID] = []*datapb.CompactionTask{
		{PlanID: 555, TriggerID: hashTaskID, State: datapb.CompactionTaskState_completed, InputSegments: []int64{301}},
		{PlanID: 556, TriggerID: hashTaskID, State: datapb.CompactionTaskState_executing, InputSegments: []int64{302}},
		// Cleaned by housekeeping: committed or failed is not decidable from
		// the state alone, but the record still names its input.
		{PlanID: 557, TriggerID: hashTaskID, State: datapb.CompactionTaskState_cleaned, InputSegments: []int64{303}},
	}
	scoped := d.forTask(hashTaskID)

	done, running, inputs := scoped.HashSplitPlanState(555)
	assert.True(t, done)
	assert.False(t, running)
	assert.Empty(t, inputs)

	done, running, inputs = scoped.HashSplitPlanState(556)
	assert.False(t, done)
	assert.True(t, running)
	assert.Empty(t, inputs)

	done, running, inputs = scoped.HashSplitPlanState(557)
	assert.False(t, done)
	assert.False(t, running)
	assert.Equal(t, []int64{303}, inputs)

	// Absent from meta: neither, and nothing to check against.
	done, running, inputs = scoped.HashSplitPlanState(999)
	assert.False(t, done)
	assert.False(t, running)
	assert.Empty(t, inputs)

	// Scoped to another task, the same plan is not found.
	done, running, _ = d.forTask(hashTaskID + 1).HashSplitPlanState(555)
	assert.False(t, done)
	assert.False(t, running)
}
