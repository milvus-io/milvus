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

package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	globaltask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNewCompactionTaskHasNoAdmissionSideEffects(t *testing.T) {
	inspector := &Inspector{} // No metadata/allocator is needed for construction.
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction,
		datapb.CompactionType_Level0DeleteCompaction, datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			plan := &datapb.CompactionTask{PlanID: 10, Type: kind, State: datapb.CompactionTaskState_executing}
			before := proto.Clone(plan)
			task, err := inspector.newCompactionTask(plan)
			require.NoError(t, err)
			require.Same(t, plan, task.GetTaskProto())
			require.True(t, proto.Equal(before, plan))
		})
	}
}

func TestRecoverCompactionTaskFromDurableGroup(t *testing.T) {
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction,
		datapb.CompactionType_ClusteringCompaction, datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		for _, state := range []datapb.CompactionTaskState{
			datapb.CompactionTaskState_executing, datapb.CompactionTaskState_pipelining,
			datapb.CompactionTaskState_failed, datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_completed,
		} {
			t.Run(fmt.Sprintf("%s/%s", kind, state), func(t *testing.T) {
				plan := &datapb.CompactionTask{
					PlanID: 10, CollectionID: 100, InputSegments: []int64{1},
					Type: kind, State: state, NodeID: 7, CreateTs: 123, StartTime: 45,
					FailReason: "persisted decision", RetryTimes: 2,
				}
				before := proto.Clone(plan)
				group := &model.SegmentChangeGroup{GroupID: 10, NewSegmentIDs: []int64{2, 3}}
				wantResults := []int64{2, 3}
				if kind == datapb.CompactionType_SortCompaction {
					group.SortJobs = map[int64]model.SegmentChangeSortJob{10: {InputSegmentID: 1, OutputSegmentID: 2}}
					wantResults = []int64{2}
				}
				mt := ctask.NewMockCompactionMeta(t)
				mt.EXPECT().GetCompactionChangeGroup(plan).Return(group, nil).Once()
				mt.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{1}, true).Once()
				inspector := &Inspector{meta: mt}
				task, err := inspector.recoverCompactionTask(plan)
				require.NoError(t, err)
				want := state
				if state == datapb.CompactionTaskState_executing || state == datapb.CompactionTaskState_pipelining {
					want = datapb.CompactionTaskState_meta_saved
					if kind == datapb.CompactionType_ClusteringCompaction {
						want = datapb.CompactionTaskState_indexing
					}
					require.Equal(t, taskcommon.Finished, task.GetTaskState(), "durable output must not rerun on a worker")
					require.False(t, task.NeedReAssignNodeID())
				}
				restored := task.GetTaskProto()
				require.Equal(t, want, restored.GetState())
				require.Equal(t, wantResults, restored.GetResultSegments())
				require.Equal(t, plan.GetCreateTs(), restored.GetCreateTs())
				require.Equal(t, plan.GetStartTime(), restored.GetStartTime())
				require.Equal(t, plan.GetFailReason(), restored.GetFailReason())
				require.Equal(t, plan.GetRetryTimes(), restored.GetRetryTimes())
				require.True(t, proto.Equal(before, plan), "recovery must not mutate the persisted proto")
			})
		}
	}
}

func TestEnqueueCompactionRejectsDurableGroup(t *testing.T) {
	mt := ctask.NewMockCompactionMeta(t)
	plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_MixCompaction,
		State: datapb.CompactionTaskState_executing, InputSegments: []int64{1}, CreateTs: 123}
	before := proto.Clone(plan)
	mt.EXPECT().GetCompactionChangeGroup(plan).Return(&model.SegmentChangeGroup{GroupID: 10}, nil).Once()
	// No allocator/catalog is installed: duplicate admission must stop before
	// stamping a new create time, saving task state or releasing group parents.
	inspector := &Inspector{meta: mt}
	require.ErrorIs(t, inspector.EnqueueCompaction(plan), merr.ErrCompactionPlanConflict)
	require.True(t, proto.Equal(before, plan))
}

func TestRecoverL0DoesNotApplyNewTaskExclusion(t *testing.T) {
	for _, state := range []datapb.CompactionTaskState{datapb.CompactionTaskState_executing, datapb.CompactionTaskState_pipelining} {
		t.Run(state.String(), func(t *testing.T) {
			mt := ctask.NewMockCompactionMeta(t)
			plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_Level0DeleteCompaction,
				State: state, Channel: "ch-1", InputSegments: []int64{1}, NodeID: 7}
			mt.EXPECT().GetCompactionChangeGroup(plan).Return(nil, nil).Once()
			mt.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(plan).Return(nil).Once()
			mt.EXPECT().ReserveGroupSort(plan).Return(false).Once()
			mt.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, []int64{1}).Return(true, true).Once()
			// SegmentChangeL0Channels must not be queried during recovery.
			inspector := &Inspector{meta: mt}
			recovered, err := inspector.recoverCompactionTask(plan)
			require.NoError(t, err)
			require.Equal(t, state, recovered.GetTaskProto().GetState())
			require.Equal(t, int64(7), recovered.GetTaskProto().GetNodeID())
		})
	}
}

func TestRecoverUnfinishedGroupSortClaimsPrivateInput(t *testing.T) {
	mt := ctask.NewMockCompactionMeta(t)
	plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_SortCompaction,
		State: datapb.CompactionTaskState_pipelining, InputSegments: []int64{1}}
	mt.EXPECT().GetCompactionChangeGroup(plan).Return(nil, nil).Once()
	mt.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(plan).Return(nil).Once()
	mt.EXPECT().ReserveGroupSort(plan).Return(true).Once()
	// Generic input reservation would reject a staged, private group member.
	inspector := &Inspector{meta: mt}
	recovered, err := inspector.recoverCompactionTask(plan)
	require.NoError(t, err)
	require.Equal(t, datapb.CompactionTaskState_pipelining, recovered.GetTaskProto().GetState())
}

func TestLoadMetaPreservesTasksOnRecoveryError(t *testing.T) {
	for _, failure := range []error{merr.WrapErrDataIntegrityMsg("conflicting lineage"), merr.WrapErrServiceUnavailableMsg("retry"), context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			mt := ctask.NewMockCompactionMeta(t)
			catalog := mocks.NewDataCoordCatalog(t)
			taskMeta := ctask.NewCompactionTaskMetaOverCatalog(catalog)
			plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_MixCompaction,
				State: datapb.CompactionTaskState_executing, InputSegments: []int64{1}}
			taskMeta.SaveCompactionTaskMemory(plan)
			mt.EXPECT().GetCompactionChangeGroup(mock.Anything).Return(nil, failure).Once()
			inspector := &Inspector{meta: mt, compactionTaskMeta: taskMeta, stopCh: make(chan struct{})}
			require.ErrorIs(t, inspector.LoadMeta(), failure)
			inspector.Stop()
			require.Len(t, taskMeta.GetCompactionTasks()[0], 1)
			catalog.AssertNotCalled(t, "DropCompactionTask", mock.Anything, mock.Anything)
		})
	}
}

func TestLoadMetaDiscardsObsoletePlan(t *testing.T) {
	mt := ctask.NewMockCompactionMeta(t)
	catalog := mocks.NewDataCoordCatalog(t)
	taskMeta := ctask.NewCompactionTaskMetaOverCatalog(catalog)
	plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_MixCompaction,
		State: datapb.CompactionTaskState_pipelining, InputSegments: []int64{1}}
	taskMeta.SaveCompactionTaskMemory(plan)
	mt.EXPECT().GetCompactionChangeGroup(mock.Anything).Return(nil, nil).Once()
	mt.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(merr.WrapErrSegmentNotFound(1)).Once()
	dropped := make(chan struct{})
	catalog.EXPECT().DropCompactionTask(mock.Anything, mock.Anything).Run(func(context.Context, *datapb.CompactionTask) { close(dropped) }).Return(nil).Once()
	inspector := &Inspector{meta: mt, compactionTaskMeta: taskMeta, stopCh: make(chan struct{})}
	defer inspector.Stop()
	require.NoError(t, inspector.LoadMeta())
	select {
	case <-dropped:
	case <-time.After(5 * time.Second):
		t.Fatal("obsolete task was not cleaned")
	}
}

func TestLoadMetaRestoresDurableGroupWithoutWorkerAssignment(t *testing.T) {
	mt := ctask.NewMockCompactionMeta(t)
	scheduler := globaltask.NewMockGlobalScheduler(t)
	taskMeta := ctask.NewCompactionTaskMetaOverCatalog(nil)
	plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_MixCompaction,
		State: datapb.CompactionTaskState_pipelining, InputSegments: []int64{1}}
	taskMeta.SaveCompactionTaskMemory(plan)
	mt.EXPECT().GetCompactionChangeGroup(mock.Anything).Return(&model.SegmentChangeGroup{GroupID: 10, NewSegmentIDs: []int64{2}}, nil).Once()
	mt.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{1}, true).Once()
	scheduler.EXPECT().Enqueue(mock.MatchedBy(func(task globaltask.Task) bool {
		return task.GetTaskState() == taskcommon.Finished
	})).Once()
	inspector := &Inspector{meta: mt, compactionTaskMeta: taskMeta, scheduler: scheduler,
		executingTasks: make(map[int64]ctask.CompactionTask), stopCh: make(chan struct{})}
	defer inspector.Stop()
	require.NoError(t, inspector.LoadMeta())
	require.Contains(t, inspector.executingTasks, int64(10))
	require.Equal(t, datapb.CompactionTaskState_meta_saved, inspector.executingTasks[10].GetTaskProto().GetState())
	require.Equal(t, datapb.CompactionTaskState_pipelining, plan.GetState())
}

func TestLoadMetaQueuesUnfinishedTaskWithoutRestamping(t *testing.T) {
	mt := ctask.NewMockCompactionMeta(t)
	taskMeta := ctask.NewCompactionTaskMetaOverCatalog(nil)
	plan := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_MixCompaction,
		State: datapb.CompactionTaskState_pipelining, InputSegments: []int64{1}, CreateTs: 123, StartTime: 45}
	taskMeta.SaveCompactionTaskMemory(plan)
	mt.EXPECT().GetCompactionChangeGroup(mock.Anything).Return(nil, nil).Once()
	mt.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
	mt.EXPECT().ReserveGroupSort(mock.Anything).Return(false).Once()
	mt.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, []int64{1}).Return(true, true).Once()
	inspector := &Inspector{meta: mt, compactionTaskMeta: taskMeta, stopCh: make(chan struct{}),
		queueTasks: NewCompactionQueue(8, func(ctask.CompactionTask) int { return 0 })}
	defer inspector.Stop()
	require.NoError(t, inspector.LoadMeta())
	require.Equal(t, 1, inspector.queueTasks.Len())
	restored, err := inspector.queueTasks.Dequeue()
	require.NoError(t, err)
	require.True(t, proto.Equal(plan, restored.GetTaskProto()))
}
