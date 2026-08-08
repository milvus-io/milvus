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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/magiconair/properties/assert"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCompactionPlanHandlerSuite(t *testing.T) {
	suite.Run(t, new(CompactionPlanHandlerSuite))
}

type CompactionPlanHandlerSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockAlloc   *allocator.MockAllocator
	handler     *compactionInspector
	mockHandler *NMockHandler
}

type stateRewritingCompactionTask struct {
	CompactionTask
}

// blockingCleanCompactionTask stands in for a cleanup that has to wait out an
// in-flight worker callback. calls records every Clean invocation so a test can
// assert the inspector does not re-dispatch a cleanup that is still running.
type blockingCleanCompactionTask struct {
	CompactionTask
	release chan struct{}
	calls   chan struct{}
}

func (t *blockingCleanCompactionTask) Clean() bool {
	t.calls <- struct{}{}
	<-t.release
	return true
}

// terminalThenRewrittenCompactionTask terminates and then has its state rewritten
// back to pipelining, standing in for a scheduler callback that fails to probe
// its worker in the window between Process and the cleaningTasks insert.
type terminalThenRewrittenCompactionTask struct {
	CompactionTask
}

func (t *terminalThenRewrittenCompactionTask) Process() bool {
	t.SetTask(t.ShadowClone(setState(datapb.CompactionTaskState_pipelining)))
	return true
}

func (t *stateRewritingCompactionTask) Process() bool {
	// Simulate a stale scheduler callback rewriting timeout after the inspector
	// reads the terminal state but before Process gets to inspect it.
	t.SetTask(t.ShadowClone(setState(datapb.CompactionTaskState_pipelining)))
	return t.CompactionTask.Process()
}

func (s *CompactionPlanHandlerSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	mockScheduler := newOwnershipScheduler(s.T())
	s.handler = newCompactionInspector(s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, mockScheduler, newMockVersionManager())
	s.mockHandler = NewNMockHandler(s.T())
	s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil).Maybe()
}

func (s *CompactionPlanHandlerSuite) TestScheduleEmpty() {
	s.SetupTest()

	s.handler.schedule()
	s.Empty(s.handler.executingTasks)
}

func (s *CompactionPlanHandlerSuite) TestScheduleExcludesChannelsOfTasksAwaitingCleanup() {
	s.SetupTest()

	// A task awaiting cleanup has already left executingTasks but still owns its
	// input segments, and a worker callback for it may still be submitting
	// results. Cleanup is dispatched off the scheduling loop, so unlike the old
	// synchronous cleanup it is generally still running when schedule() picks the
	// next batch -- the exclusion must therefore outlive executingTasks.
	cleaning := &mixCompactionTask{meta: s.mockMeta}
	cleaning.SetTask(&datapb.CompactionTask{
		PlanID:  1,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_failed,
		Channel: "ch-1",
	})
	s.handler.cleaningTasks[1] = cleaning

	// L0 is channel-exclusive with Mix/Sort, so it is what the stale Mix task
	// must keep out. (Mix does not exclude Mix.)
	queued := &l0CompactionTask{meta: s.mockMeta}
	queued.SetTask(&datapb.CompactionTask{
		PlanID:  2,
		Type:    datapb.CompactionType_Level0DeleteCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-1",
	})
	s.Require().NoError(s.handler.queueTasks.Enqueue(queued))

	s.Empty(s.handler.schedule(),
		"an L0 task sharing the channel of a task awaiting cleanup must not start yet")
	s.Equal(1, s.handler.queueTasks.Len(), "the excluded task must go back on the queue")
}

func (s *CompactionPlanHandlerSuite) generateInitTasksForSchedule() {
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()
	task1 := &mixCompactionTask{
		meta: s.mockMeta,
	}
	task1.SetTask(&datapb.CompactionTask{
		PlanID:  1,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-1",
		NodeID:  100,
	})

	task2 := &mixCompactionTask{
		meta: s.mockMeta,
	}
	task2.SetTask(&datapb.CompactionTask{
		PlanID:  2,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-1",
		NodeID:  100,
	})

	task3 := &mixCompactionTask{
		meta: s.mockMeta,
	}
	task3.SetTask(&datapb.CompactionTask{
		PlanID:  3,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-2",
		NodeID:  101,
	})

	task4 := &mixCompactionTask{
		meta: s.mockMeta,
	}
	task4.SetTask(&datapb.CompactionTask{
		PlanID:  4,
		Type:    datapb.CompactionType_Level0DeleteCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-3",
		NodeID:  102,
	})

	ret := []CompactionTask{task1, task2, task3, task4}
	for _, t := range ret {
		s.handler.restoreTask(t)
	}
}

func (s *CompactionPlanHandlerSuite) TestScheduleNodeWith1ParallelTask() {
	tests := []struct {
		description string
		tasks       []CompactionTask
		plans       []*datapb.CompactionPlan
		expectedOut []UniqueID // planID
	}{
		{
			"with L0 tasks diff channel",
			[]CompactionTask{
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  101,
				}, nil, s.mockMeta),
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction},
				{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
			},
			[]UniqueID{10, 11},
		},
		{
			"with L0 tasks same channel",
			[]CompactionTask{
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, newMockVersionManager()),
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				{PlanID: 10, Channel: "ch-11", Type: datapb.CompactionType_Level0DeleteCompaction},
			},
			[]UniqueID{10},
		},
		{
			"without L0 tasks",
			[]CompactionTask{
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-2",
					NodeID:  101,
				}, nil, s.mockMeta, newMockVersionManager()),
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, newMockVersionManager()),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 14, Channel: "ch-2", Type: datapb.CompactionType_MixCompaction},
				{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
			},
			[]UniqueID{14, 13},
		},
		{
			"empty tasks",
			[]CompactionTask{},
			[]*datapb.CompactionPlan{},
			[]UniqueID{},
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			if len(test.tasks) > 0 {
				s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()
			}
			s.generateInitTasksForSchedule()
			// submit the testing tasks
			for _, t := range test.tasks {
				// t.SetPlan(test.plans[i])
				s.handler.submitTask(t)
			}

			gotTasks := s.handler.schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetTaskProto().GetPlanID()
			}))
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestScheduleNodeWithL0Executing() {
	tests := []struct {
		description string
		tasks       []CompactionTask
		plans       []*datapb.CompactionPlan
		expectedOut []UniqueID // planID
	}{
		{
			"with L0 tasks diff channel",
			[]CompactionTask{
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  102,
				}, nil, s.mockMeta),
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
			},
			[]*datapb.CompactionPlan{{}, {}},
			[]UniqueID{10, 11},
		},
		{
			"with L0 tasks same channel",
			[]CompactionTask{
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-3",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 10, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction},
				{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				{PlanID: 13, Channel: "ch-3", Type: datapb.CompactionType_MixCompaction},
			},
			[]UniqueID{10, 13},
		},
		{
			"with multiple L0 tasks same channel",
			[]CompactionTask{
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
				newL0CompactionTask(&datapb.CompactionTask{
					PlanID:  12,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 10, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction},
				{PlanID: 11, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction},
				{PlanID: 12, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction},
			},
			[]UniqueID{10, 11, 12},
		},
		{
			"without L0 tasks",
			[]CompactionTask{
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-3",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
				newMixCompactionTask(&datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 14, Channel: "ch-3", Type: datapb.CompactionType_MixCompaction},
				{},
			},
			[]UniqueID{13, 14},
		},
		{"empty tasks", []CompactionTask{}, []*datapb.CompactionPlan{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			if len(test.tasks) > 0 {
				s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()
			}

			// submit the testing tasks
			for _, t := range test.tasks {
				s.handler.submitTask(t)
			}
			gotTasks := s.handler.schedule()
			gotPlanIDs := lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetTaskProto().GetPlanID()
			})
			s.ElementsMatch(test.expectedOut, gotPlanIDs)
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionConflictsWithExecutingL0SameChannel() {
	s.SetupTest()
	s.handler.executingTasks[1] = newL0CompactionTask(&datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_Level0DeleteCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta)
	s.NoError(s.handler.submitTask(newBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, newMockVersionManager())))

	gotTasks := s.handler.schedule()
	s.Empty(gotTasks)
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionBlocksQueuedL0SameChannel() {
	s.SetupTest()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.CompactionTaskPrioritizer.Key, "mix")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.CompactionTaskPrioritizer.Key)
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return().Once()

	s.NoError(s.handler.submitTask(newBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, newMockVersionManager())))
	s.NoError(s.handler.submitTask(newL0CompactionTask(&datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_Level0DeleteCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta)))

	gotTasks := s.handler.schedule()
	s.Equal([]UniqueID{2}, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
		return t.GetTaskProto().GetPlanID()
	}))
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionBlocksClusteringSameLabel() {
	s.SetupTest()
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return().Once()

	s.NoError(s.handler.submitTask(newBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, newMockVersionManager())))
	s.NoError(s.handler.submitTask(newClusteringCompactionTask(&datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_ClusteringCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager())))

	gotTasks := s.handler.schedule()
	s.Equal([]UniqueID{1}, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
		return t.GetTaskProto().GetPlanID()
	}))
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestRemoveTasksByChannel() {
	s.SetupTest()
	ch := "ch1"

	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()

	t1 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:  19530,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: ch,
		NodeID:  1,
	}, nil, s.mockMeta, newMockVersionManager())

	t2 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:  19531,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: ch,
		NodeID:  1,
	}, nil, s.mockMeta, newMockVersionManager())

	s.handler.submitTask(t1)
	s.handler.restoreTask(t2)
	s.handler.removeTasksByChannel(ch)
}

func (s *CompactionPlanHandlerSuite) TestGetCompactionTask() {
	s.SetupTest()

	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()

	t1 := newMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, newMockVersionManager())

	t2 := newMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_completed,
	}, nil, s.mockMeta, newMockVersionManager())

	t3 := newL0CompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    3,
		Type:      datapb.CompactionType_Level0DeleteCompaction,
		Channel:   "ch-02",
		State:     datapb.CompactionTaskState_failed,
	}, nil, s.mockMeta)

	inTasks := map[int64]CompactionTask{
		1: t1,
		2: t2,
		3: t3,
	}
	s.mockMeta.EXPECT().GetCompactionTasksByTriggerID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) []*datapb.CompactionTask {
		var ret []*datapb.CompactionTask
		for _, t := range inTasks {
			if t.GetTaskProto().GetTriggerID() != i {
				continue
			}
			ret = append(ret, t.ShadowClone())
		}
		return ret
	})

	for _, t := range inTasks {
		s.handler.submitTask(t)
	}

	s.handler.schedule()

	info := s.handler.getCompactionInfo(context.TODO(), 1)
	s.Equal(1, info.completedCnt)
	s.Equal(1, info.executingCnt)
	s.Equal(1, info.failedCnt)
}

func (s *CompactionPlanHandlerSuite) TestCompactionQueueFull() {
	s.SetupTest()
	paramtable.Get().Save("dataCoord.compaction.taskQueueCapacity", "1")
	defer paramtable.Get().Reset("dataCoord.compaction.taskQueueCapacity")

	mockScheduler := newOwnershipScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.Init {
			cluster := session.NewMockCluster(s.T())
			t.QueryTaskOnWorker(cluster)
		}
	}).Maybe()
	s.handler = newCompactionInspector(s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, mockScheduler, newMockVersionManager())

	t1 := newMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, newMockVersionManager())

	s.NoError(s.handler.submitTask(t1))

	t2 := newMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_completed,
	}, nil, s.mockMeta, newMockVersionManager())

	s.Error(s.handler.submitTask(t2))
}

func (s *CompactionPlanHandlerSuite) TestExecCompactionPlan() {
	s.SetupTest()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Maybe()

	mockScheduler := newOwnershipScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.Init {
			cluster := session.NewMockCluster(s.T())
			t.QueryTaskOnWorker(cluster)
		}
	}).Maybe()
	handler := newCompactionInspector(s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, mockScheduler, newMockVersionManager())

	task := &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Channel:   "ch-1",
		Type:      datapb.CompactionType_MixCompaction,
	}
	err := handler.enqueueCompaction(task)
	s.NoError(err)
	t := handler.getCompactionTask(1)
	s.NotNil(t)
	task.PlanID = 2
	err = s.handler.enqueueCompaction(task)
	s.NoError(err)
}

func (s *CompactionPlanHandlerSuite) TestCheckCompaction() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(CompactionTask).GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
			t.DropTaskOnWorker(cluster)
		}
	})

	cluster.EXPECT().QueryCompaction(UniqueID(111), &datapb.CompactionStateRequest{PlanID: 1}).Return(
		&datapb.CompactionPlanResult{PlanID: 1, State: datapb.CompactionTaskState_executing}, nil).Once()

	cluster.EXPECT().QueryCompaction(UniqueID(111), &datapb.CompactionStateRequest{PlanID: 2}).Return(
		&datapb.CompactionPlanResult{
			PlanID:   2,
			State:    datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{{PlanID: 2}},
		}, nil).Once()

	cluster.EXPECT().QueryCompaction(UniqueID(111), &datapb.CompactionStateRequest{PlanID: 6}).Return(
		&datapb.CompactionPlanResult{
			PlanID:   6,
			Channel:  "ch-2",
			State:    datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{{PlanID: 6}},
		}, nil).Once()

	cluster.EXPECT().DropCompaction(mock.Anything, mock.Anything).Return(nil)
	// Reaching a terminal state must not unlock the inputs; only doClean does,
	// and this test does not run cleanup.

	t1 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t2 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t3 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    3,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_timeout,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t4 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    4,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_timeout,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t6 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    6,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-2",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	inTasks := map[int64]CompactionTask{
		1: t1,
		2: t2,
		3: t3,
		4: t4,
		6: t6,
	}

	// s.mockSessMgr.EXPECT().SyncSegments(int64(111), mock.Anything).Return(nil)
	// s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
			if t.GetPlanID() == 2 {
				segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
				return []*SegmentInfo{segment}, &segMetricMutation{}, nil
			} else if t.GetPlanID() == 6 {
				return nil, nil, errors.Errorf("intended error")
			}
			return nil, nil, errors.Errorf("unexpected error")
		}).Twice()

	for _, t := range inTasks {
		s.handler.submitTask(t)
	}

	s.handler.schedule()
	// time.Sleep(2 * time.Second)
	s.handler.checkCompaction()

	t := s.handler.getCompactionTask(1)
	s.NotNil(t)

	t = s.handler.getCompactionTask(2)
	// completed
	s.Nil(t)

	t = s.handler.getCompactionTask(3)
	s.Nil(t)

	t = s.handler.getCompactionTask(4)
	s.Nil(t)

	t = s.handler.getCompactionTask(5)
	// not exist
	s.Nil(t)

	t = s.handler.getCompactionTask(6)
	s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
}

func (s *CompactionPlanHandlerSuite) TestCompactionGC() {
	s.SetupTest()
	inTasks := []*datapb.CompactionTask{
		{
			PlanID:    1,
			Type:      datapb.CompactionType_MixCompaction,
			State:     datapb.CompactionTaskState_completed,
			StartTime: time.Now().Add(-time.Second * 100000).Unix(),
		},
		{
			PlanID:    2,
			Type:      datapb.CompactionType_MixCompaction,
			State:     datapb.CompactionTaskState_cleaned,
			StartTime: time.Now().Add(-time.Second * 100000).Unix(),
		},
		{
			PlanID:    3,
			Type:      datapb.CompactionType_MixCompaction,
			State:     datapb.CompactionTaskState_cleaned,
			StartTime: time.Now().Unix(),
		},
	}

	catalog := &datacoord.Catalog{MetaKv: NewMetaMemoryKV()}
	compactionTaskMeta, err := newCompactionTaskMeta(context.TODO(), catalog)
	s.NoError(err)
	s.handler.meta = &meta{compactionTaskMeta: compactionTaskMeta}
	for _, t := range inTasks {
		s.handler.meta.SaveCompactionTask(context.TODO(), t)
	}

	s.handler.cleanCompactionTaskMeta()
	// two task should be cleaned, one remains
	tasks := s.handler.meta.GetCompactionTaskMeta().GetCompactionTasks()
	s.Equal(1, len(tasks))
}

func (s *CompactionPlanHandlerSuite) TestProcessCompleteCompaction() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(CompactionTask).GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
			t.DropTaskOnWorker(cluster)
		}
	})

	// s.mockSessMgr.EXPECT().SyncSegments(mock.Anything, mock.Anything).Return(nil).Once()
	// No SetSegmentsCompacting expectation: this test drives the task to
	// completed without running cleanup, and reaching completed must not unlock
	// the inputs. doClean is the single place that does, so a second unlock
	// cannot release segments another compaction has meanwhile re-acquired.
	// An unexpected call here fails the test.
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*SegmentInfo{segment},
		&segMetricMutation{}, nil).Once()

	dataNodeID := UniqueID(111)

	seg1 := &datapb.SegmentInfo{
		ID:        1,
		Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
		Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 2)},
		Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)},
	}

	seg2 := &datapb.SegmentInfo{
		ID:        2,
		Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(101, 4)},
		Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 5)},
		Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 6)},
	}

	plan := &datapb.CompactionPlan{
		PlanID: 1,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID:           seg1.ID,
				FieldBinlogs:        seg1.GetBinlogs(),
				Field2StatslogPaths: seg1.GetStatslogs(),
				Deltalogs:           seg1.GetDeltalogs(),
			},
			{
				SegmentID:           seg2.ID,
				FieldBinlogs:        seg2.GetBinlogs(),
				Field2StatslogPaths: seg2.GetStatslogs(),
				Deltalogs:           seg2.GetDeltalogs(),
			},
		},
		Type: datapb.CompactionType_MixCompaction,
	}

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        plan.GetPlanID(),
		TriggerID:     1,
		Type:          plan.GetType(),
		State:         datapb.CompactionTaskState_executing,
		NodeID:        dataNodeID,
		InputSegments: []UniqueID{1, 2},
	}, nil, s.mockMeta, newMockVersionManager())

	compactionResult := datapb.CompactionPlanResult{
		PlanID: 1,
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           3,
				NumOfRows:           15,
				InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(101, 301)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 302)},
				Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogIDs(101, 303)},
			},
		},
	}

	cluster.EXPECT().QueryCompaction(UniqueID(111), &datapb.CompactionStateRequest{PlanID: 1}).Return(&compactionResult, nil).Once()
	cluster.EXPECT().DropCompaction(mock.Anything, mock.Anything).Return(nil)

	s.handler.submitTask(task)

	s.handler.schedule()
	err := s.handler.checkCompaction()
	s.NoError(err)
}

func (s *CompactionPlanHandlerSuite) TestCheckCompactionCleansNonMixTaskWhenStateRewrittenAfterProcess() {
	// The cleanup decision must not be re-read after Process: a task dropped from
	// executingTasks without entering cleaningTasks is never cleaned, and its
	// input segments stay isCompacting until DataCoord restarts. This holds for
	// every compaction type, not just Mix/Sort.
	planID := int64(1)
	l0Task := &terminalThenRewrittenCompactionTask{CompactionTask: newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        planID,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		State:         datapb.CompactionTaskState_timeout,
		StartTime:     time.Now().Unix(),
		InputSegments: []int64{100},
	}, nil, s.mockMeta)}
	s.handler.executingTasks[planID] = l0Task

	s.Require().NoError(s.handler.checkCompaction())
	s.NotContains(s.handler.executingTasks, planID)
	s.Contains(s.handler.cleaningTasks, planID,
		"a terminal L0 task must still be cleaned after a stale scheduler rewrite")
	s.Equal(datapb.CompactionTaskState_pipelining, l0Task.GetTaskProto().GetState())
}

func (s *CompactionPlanHandlerSuite) TestCheckCompactionDoesNotRecleanFinishedCleanedTask() {
	// clusteringCompactionTask.Process reports true for an already-cleaned task.
	// Widening the cleanup decision must not drag it back into cleaningTasks.
	planID := int64(1)
	cleanedTask := newClusteringCompactionTask(&datapb.CompactionTask{
		PlanID:    planID,
		Type:      datapb.CompactionType_ClusteringCompaction,
		State:     datapb.CompactionTaskState_cleaned,
		StartTime: time.Now().Unix(),
	}, nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager())
	s.handler.executingTasks[planID] = cleanedTask

	s.Require().NoError(s.handler.checkCompaction())
	s.NotContains(s.handler.executingTasks, planID)
	s.NotContains(s.handler.cleaningTasks, planID, "an already-cleaned task must not be cleaned again")
}

func (s *CompactionPlanHandlerSuite) TestCleanupDropsThePlanOnTheWorker() {
	// Finalize takes the task out of dispatch, so the scheduler's own terminal
	// branch never runs DropTaskOnWorker for it. Cleanup must send the drop
	// itself, or the worker keeps the plan and its result binlogs until that
	// DataNode restarts.
	planID := int64(1)
	nodeID := int64(11)
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(nodeID, planID).Return(nil).Once()
	s.handler.cluster = cluster

	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        planID,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_failed,
		NodeID:        nodeID,
		InputSegments: []int64{100},
	}, nil, s.mockMeta, newMockVersionManager())
	s.handler.cleaningTasks[planID] = task

	s.cleanFailedTasksAndWait(s.handler)
	s.NotContains(s.handler.cleaningTasks, planID)
}

func (s *CompactionPlanHandlerSuite) TestCleanFailedTasksDispatchesCleanupOffTheScheduleLoop() {
	planID := int64(1)
	blocking := &blockingCleanCompactionTask{
		CompactionTask: newMixCompactionTask(&datapb.CompactionTask{
			PlanID: planID,
			Type:   datapb.CompactionType_SortCompaction,
			State:  datapb.CompactionTaskState_timeout,
		}, nil, s.mockMeta, newMockVersionManager()),
		release: make(chan struct{}),
		calls:   make(chan struct{}, 8),
	}
	releaseOnce := sync.Once{}
	release := func() { releaseOnce.Do(func() { close(blocking.release) }) }
	defer release()

	s.handler.cleaningTasks[planID] = blocking

	// checkSchedule drives checkCompaction and schedule for every other task in
	// the same goroutine, so dispatching a cleanup must never wait for it.
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.handler.cleanFailedTasks()
	}()
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		s.FailNow("cleanFailedTasks blocked on a slow cleanup, stalling the checkSchedule loop")
	}
	s.Contains(s.handler.cleaningTasks, planID, "a cleanup still in flight must stay queued")

	// The dispatched goroutine has to actually reach Clean before the next
	// assertion means anything.
	s.Eventually(func() bool { return len(blocking.calls) == 1 },
		10*time.Second, 5*time.Millisecond, "cleanup was never dispatched")

	// A later round must not dispatch the same cleanup a second time.
	s.handler.cleanFailedTasks()
	s.Require().Len(blocking.calls, 1)

	release()
	s.Eventually(func() bool {
		s.handler.cleaningGuard.RLock()
		defer s.handler.cleaningGuard.RUnlock()
		return len(s.handler.cleaningTasks) == 0
	}, 30*time.Second, 5*time.Millisecond, "a finished cleanup must leave cleaningTasks")
	s.Len(blocking.calls, 1)
}

// cleanFailedTasksAndWait dispatches a cleanup round and waits for the
// goroutines it spawned. cleanFailedTasks is asynchronous so that a slow
// cleanup cannot stall the checkSchedule loop, but the assertions below are
// about the outcome of a completed round.
func (s *CompactionPlanHandlerSuite) cleanFailedTasksAndWait(handler *compactionInspector) {
	s.T().Helper()
	handler.cleanFailedTasks()
	s.Eventually(func() bool {
		return len(handler.cleaningInFlight.Collect()) == 0
	}, 30*time.Second, 5*time.Millisecond, "cleanup goroutines did not finish")
}

func (s *CompactionPlanHandlerSuite) TestCleanCompaction() {
	s.SetupTest()

	tests := []struct {
		task CompactionTask
	}{
		{
			newMixCompactionTask(
				&datapb.CompactionTask{
					PlanID:        1,
					TriggerID:     1,
					Type:          datapb.CompactionType_MixCompaction,
					State:         datapb.CompactionTaskState_failed,
					NodeID:        1,
					InputSegments: []UniqueID{1, 2},
				},
				nil, s.mockMeta, newMockVersionManager()),
		},
		{
			newL0CompactionTask(&datapb.CompactionTask{
				PlanID:        1,
				TriggerID:     1,
				Type:          datapb.CompactionType_Level0DeleteCompaction,
				State:         datapb.CompactionTaskState_failed,
				NodeID:        1,
				InputSegments: []UniqueID{1, 2},
			},
				nil, s.mockMeta),
		},
	}
	for _, test := range tests {
		task := test.task
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		s.handler.executingTasks[1] = task
		s.Equal(1, len(s.handler.executingTasks))

		err := s.handler.checkCompaction()
		s.NoError(err)
		s.Equal(0, len(s.handler.executingTasks))
		s.Equal(1, len(s.handler.cleaningTasks))
		s.cleanFailedTasksAndWait(s.handler)
		s.Equal(0, len(s.handler.cleaningTasks))
	}
}

func (s *CompactionPlanHandlerSuite) TestRecoveredTimeoutSortTaskCleanupOrder() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID := int64(10)
	segmentID := int64(100)
	planID := int64(1)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	realMeta.SetSegmentsCompacting(context.Background(), []int64{segmentID}, true)

	meta := NewMockCompactionMeta(s.T())
	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(meta, nil, nil, nil, scheduler, scheduler, newMockVersionManager())
	compactionTask := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        planID,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_timeout,
		StartTime:     time.Now().Unix(),
		NodeID:        11,
		InputSegments: []int64{segmentID},
	}, nil, meta, newMockVersionManager())
	events := make([]string, 0, 4)

	scheduler.EXPECT().Enqueue(compactionTask).Once()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, saved *datapb.CompactionTask) error {
			segment := realMeta.GetSegment(ctx, segmentID)
			switch saved.GetState() {
			case datapb.CompactionTaskState_cleaned:
				events = append(events, "save-cleaned")
				s.False(segment.GetIsInvisible(), "visibility must be persisted before the task is marked cleaned")
				s.True(segment.isCompacting, "the compaction lock must be released last")
			default:
				s.Fail("unexpected saved compaction state", saved.GetState().String())
			}
			return nil
		}).Once()
	meta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, operators ...UpdateOperator) error {
			events = append(events, "restore-visible")
			return realMeta.UpdateSegmentsInfo(ctx, operators...)
		}).Once()
	meta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{segmentID}, false).
		Run(func(ctx context.Context, segmentIDs []int64, compacting bool) {
			events = append(events, "unlock")
			s.Equal(datapb.CompactionTaskState_cleaned, compactionTask.GetTaskProto().GetState())
			realMeta.SetSegmentsCompacting(ctx, segmentIDs, compacting)
		}).Once()

	handler.restoreTask(compactionTask)
	s.Require().NoError(handler.checkCompaction())
	s.NotContains(handler.executingTasks, planID)
	s.Contains(handler.cleaningTasks, planID)

	s.Equal(datapb.CompactionTaskState_timeout, compactionTask.GetTaskProto().GetState())
	s.Contains(handler.cleaningTasks, planID)

	s.cleanFailedTasksAndWait(handler)
	s.NotContains(handler.cleaningTasks, planID)
	s.Equal(datapb.CompactionTaskState_cleaned, compactionTask.GetTaskProto().GetState())
	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.False(segment.GetIsInvisible())
	s.False(segment.isCompacting)
	s.Equal([]string{"restore-visible", "save-cleaned", "unlock"}, events)

	persisted, err := realMeta.catalog.ListSegments(context.Background(), collectionID)
	s.Require().NoError(err)
	s.Require().Len(persisted, 1)
	s.False(persisted[0].GetIsInvisible())
}

func (s *CompactionPlanHandlerSuite) TestCheckCompactionKeepsTerminalCleanupDecisionWhenStateRewritePrecedesProcess() {
	planID := int64(1)
	task := &stateRewritingCompactionTask{CompactionTask: newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    planID,
		Type:      datapb.CompactionType_SortCompaction,
		State:     datapb.CompactionTaskState_timeout,
		StartTime: time.Now().Unix(),
	}, nil, s.mockMeta, newMockVersionManager())}
	s.handler.executingTasks[planID] = task

	s.Require().NoError(s.handler.checkCompaction())
	s.NotContains(s.handler.executingTasks, planID)
	s.Contains(s.handler.cleaningTasks, planID,
		"the terminal state observed before Process must survive a stale scheduler rewrite")
	s.Equal(datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
}

func (s *CompactionPlanHandlerSuite) TestLoadMetaCleansRecoveredTerminalSortTaskSynchronously() {
	Params.Save(Params.DataCoordCfg.EnableCompaction.Key, "false")
	defer Params.Reset(Params.DataCoordCfg.EnableCompaction.Key)

	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID := int64(10)
	segmentID := int64(100)
	planID := int64(1)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        planID,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_timeout,
		NodeID:        11,
		InputSegments: []int64{segmentID},
	}))

	scheduler := newOwnershipScheduler(s.T())
	// The synchronous cleanup bypasses the scheduler, which owns the only path
	// that sends DropCompaction, so the task must be queued for a drop. The drop
	// itself is an RPC and is deliberately deferred until after start(): no
	// expectation is registered on the cluster, so issuing it inside loadMeta --
	// on the DataCoord readiness path -- fails this test.
	cluster := session.NewMockCluster(s.T())
	handler := newCompactionInspector(realMeta, nil, nil, cluster, scheduler, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())
	s.Require().Len(handler.pendingWorkerDrops, 1, "the recovered task must be queued for a worker drop")
	s.Equal(planID, handler.pendingWorkerDrops[0].GetTaskProto().GetPlanID())

	s.NotContains(handler.executingTasks, planID)
	s.NotContains(handler.cleaningTasks, planID)
	s.False(realMeta.GetSegment(context.Background(), segmentID).GetIsInvisible())
	s.False(realMeta.GetSegment(context.Background(), segmentID).isCompacting)
	persistedTasks := realMeta.GetCompactionTasks(context.Background())
	s.Require().Len(persistedTasks[0], 1)
	s.Equal(datapb.CompactionTaskState_cleaned, persistedTasks[0][0].GetState())
}

func (s *CompactionPlanHandlerSuite) TestLoadMetaRepairsCleanedSortInputWithoutExposingWaitingSortInput() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID := int64(10)
	for _, segment := range []*datapb.SegmentInfo{
		{
			ID:           100,
			CollectionID: collectionID,
			State:        commonpb.SegmentState_Flushed,
			IsInvisible:  true,
		},
		{
			ID:           101,
			CollectionID: collectionID,
			State:        commonpb.SegmentState_Flushed,
			IsInvisible:  true,
		},
		{
			ID:                  102,
			CollectionID:        collectionID,
			State:               commonpb.SegmentState_Flushed,
			IsInvisible:         true,
			CreatedByCompaction: true,
		},
		{
			ID:           103,
			CollectionID: collectionID,
			State:        commonpb.SegmentState_Flushed,
			IsInvisible:  true,
		},
	} {
		s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(segment)))
	}
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        1,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_cleaned,
		InputSegments: []int64{100},
	}))
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        2,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_executing,
		NodeID:        11,
		InputSegments: []int64{103},
	}))
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        3,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_cleaned,
		InputSegments: []int64{103},
	}))

	scheduler := newOwnershipScheduler(s.T())
	scheduler.EXPECT().Enqueue(mock.MatchedBy(func(compactionTask task.Task) bool {
		return compactionTask.GetTaskID() == 2
	})).Once()
	handler := newCompactionInspector(realMeta, nil, nil, nil, scheduler, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())

	s.False(realMeta.GetSegment(context.Background(), 100).GetIsInvisible(),
		"a source referenced only by an old cleaned task must be repaired")
	s.True(realMeta.GetSegment(context.Background(), 101).GetIsInvisible(),
		"a flushed original without terminal task evidence may still be legitimately waiting for sort")
	s.True(realMeta.GetSegment(context.Background(), 102).GetIsInvisible(),
		"compaction-created intermediate segments must remain hidden")
	s.True(realMeta.GetSegment(context.Background(), 103).GetIsInvisible(),
		"an active sort task must fence an older cleaned task for the same input")
	s.True(realMeta.GetSegment(context.Background(), 103).isCompacting)

	persisted, err := realMeta.catalog.ListSegments(context.Background(), collectionID)
	s.Require().NoError(err)
	persistedByID := lo.SliceToMap(persisted, func(segment *datapb.SegmentInfo) (int64, *datapb.SegmentInfo) {
		return segment.GetID(), segment
	})
	s.False(persistedByID[100].GetIsInvisible())
	s.True(persistedByID[101].GetIsInvisible())
	s.True(persistedByID[102].GetIsInvisible())
	s.True(persistedByID[103].GetIsInvisible())
}

func (s *CompactionPlanHandlerSuite) TestLoadMetaFailsWhenCleanedSortVisibilityRepairCannotPersist() {
	meta := NewMockCompactionMeta(s.T())
	meta.EXPECT().GetCompactionTasks(mock.Anything).Return(map[int64][]*datapb.CompactionTask{
		0: {
			{
				PlanID:        1,
				Type:          datapb.CompactionType_SortCompaction,
				State:         datapb.CompactionTaskState_cleaned,
				InputSegments: []int64{100},
			},
		},
	}).Once()
	meta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceInternalMsg("mock cleaned task repair persistence failure")).Once()

	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(meta, nil, nil, nil, scheduler, scheduler, newMockVersionManager())
	err := handler.loadMeta()
	s.Error(err)
	s.Contains(err.Error(), "restore cleaned sort compaction input visibility")
}

func (s *CompactionPlanHandlerSuite) TestCleanClusteringCompaction() {
	s.SetupTest()

	task := newClusteringCompactionTask(
		&datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     1,
			CollectionID:  1001,
			Type:          datapb.CompactionType_ClusteringCompaction,
			State:         datapb.CompactionTaskState_failed,
			NodeID:        1,
			InputSegments: []UniqueID{1, 2},
		},
		nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager())
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

	s.handler.executingTasks[1] = task
	s.Equal(1, len(s.handler.executingTasks))
	s.handler.checkCompaction()
	s.Equal(0, len(s.handler.executingTasks))
	s.Equal(1, len(s.handler.cleaningTasks))
	s.cleanFailedTasksAndWait(s.handler)
	s.Equal(0, len(s.handler.cleaningTasks))
}

func (s *CompactionPlanHandlerSuite) TestCleanClusteringCompactionCommitFail() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(CompactionTask).GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
			t.DropTaskOnWorker(cluster)
		}
	})

	task := newClusteringCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     1,
		CollectionID:  1001,
		Channel:       "ch-1",
		Type:          datapb.CompactionType_ClusteringCompaction,
		State:         datapb.CompactionTaskState_executing,
		NodeID:        1,
		InputSegments: []UniqueID{1, 2},
		ClusteringKeyField: &schemapb.FieldSchema{
			FieldID:         100,
			Name:            Int64Field,
			IsPrimaryKey:    true,
			DataType:        schemapb.DataType_Int64,
			AutoID:          true,
			IsClusteringKey: true,
		},
	},
		nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager())

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
	cluster.EXPECT().QueryCompaction(UniqueID(1), &datapb.CompactionStateRequest{PlanID: 1}).Return(
		&datapb.CompactionPlanResult{
			PlanID: 1,
			State:  datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{
				{
					PlanID:    1,
					SegmentID: 101,
				},
			},
		}, nil).Once()
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.New("mock error"))

	s.handler.submitTask(task)
	s.handler.schedule()
	s.handler.checkCompaction()
	s.Equal(0, len(task.GetTaskProto().GetResultSegments()))

	s.Equal(datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
	s.Equal(0, len(s.handler.executingTasks))
	s.Equal(1, len(s.handler.cleaningTasks))

	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil)
	s.cleanFailedTasksAndWait(s.handler)
	s.Equal(0, len(s.handler.cleaningTasks))
}

// test inspector should keep clean the failed task until it become cleaned
func (s *CompactionPlanHandlerSuite) TestKeepClean() {
	s.SetupTest()

	tests := []struct {
		task CompactionTask
	}{
		{
			newClusteringCompactionTask(&datapb.CompactionTask{
				PlanID:        1,
				TriggerID:     1,
				Type:          datapb.CompactionType_ClusteringCompaction,
				State:         datapb.CompactionTaskState_failed,
				NodeID:        1,
				InputSegments: []UniqueID{1, 2},
			},
				nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager()),
		},
	}
	for _, test := range tests {
		task := test.task
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return()
		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.mockMeta.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		s.handler.executingTasks[1] = task

		s.Equal(1, len(s.handler.executingTasks))
		s.handler.checkCompaction()
		s.Equal(0, len(s.handler.executingTasks))
		s.Equal(1, len(s.handler.cleaningTasks))
		s.cleanFailedTasksAndWait(s.handler)
		s.Equal(1, len(s.handler.cleaningTasks))
		s.mockMeta.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil).Once()
		s.cleanFailedTasksAndWait(s.handler)
		s.Equal(0, len(s.handler.cleaningTasks))
	}
}

func getFieldBinlogIDs(fieldID int64, logIDs ...int64) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(logIDs)),
	}
	for _, id := range logIDs {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogID: id})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

func getFieldBinlogPaths(fieldID int64, paths ...string) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(paths)),
	}
	for _, path := range paths {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogPath: path})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

func getFieldBinlogIDsWithEntry(fieldID int64, entry int64, logIDs ...int64) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(logIDs)),
	}
	for _, id := range logIDs {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogID: id, EntriesNum: entry})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

func getInsertLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildInsertLogPath(rootPath, 10, 100, segmentID, 1000, 10000)
}

func getStatsLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildStatsLogPath(rootPath, 10, 100, segmentID, 1000, 10000)
}

func getDeltaLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildDeltaLogPath(rootPath, 10, 100, segmentID, 10000)
}

func TestCheckDelay(t *testing.T) {
	handler := &compactionInspector{}
	t1 := newMixCompactionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, newMockVersionManager())
	handler.checkDelay(t1)
	t2 := newL0CompactionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil)
	handler.checkDelay(t2)
	t3 := newClusteringCompactionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, nil, nil, newMockVersionManager())
	handler.checkDelay(t3)
	t4 := newBumpSchemaVersionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, newMockVersionManager())
	handler.checkDelay(t4)
}

func TestGetCompactionTasksNum(t *testing.T) {
	queueTasks := NewCompactionQueue(10, DefaultPrioritizer)
	queueTasks.Enqueue(
		newMixCompactionTask(&datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 1,
			Type:         datapb.CompactionType_MixCompaction,
		}, nil, nil, newMockVersionManager()),
	)
	queueTasks.Enqueue(
		newL0CompactionTask(&datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 1,
			Type:         datapb.CompactionType_Level0DeleteCompaction,
		}, nil, nil),
	)
	queueTasks.Enqueue(
		newClusteringCompactionTask(&datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 10,
			Type:         datapb.CompactionType_ClusteringCompaction,
		}, nil, nil, nil, nil, newMockVersionManager()),
	)
	executingTasks := make(map[int64]CompactionTask, 0)
	executingTasks[1] = newMixCompactionTask(&datapb.CompactionTask{
		StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
		CollectionID: 1,
		Type:         datapb.CompactionType_MixCompaction,
	}, nil, nil, newMockVersionManager())
	executingTasks[2] = newL0CompactionTask(&datapb.CompactionTask{
		StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
		CollectionID: 10,
		Type:         datapb.CompactionType_Level0DeleteCompaction,
	}, nil, nil)

	handler := &compactionInspector{
		queueTasks:     queueTasks,
		executingTasks: executingTasks,
	}
	t.Run("no filter", func(t *testing.T) {
		i := handler.getCompactionTasksNum()
		assert.Equal(t, 5, i)
	})
	t.Run("collection id filter", func(t *testing.T) {
		i := handler.getCompactionTasksNum(CollectionIDCompactionTaskFilter(1))
		assert.Equal(t, 3, i)
	})
	t.Run("l0 compaction filter", func(t *testing.T) {
		i := handler.getCompactionTasksNum(L0CompactionCompactionTaskFilter())
		assert.Equal(t, 2, i)
	})
	t.Run("collection id and l0 compaction filter", func(t *testing.T) {
		i := handler.getCompactionTasksNum(CollectionIDCompactionTaskFilter(1), L0CompactionCompactionTaskFilter())
		assert.Equal(t, 1, i)
	})
}

func (s *CompactionPlanHandlerSuite) TestCreateCompactTask_BumpSchemaVersionCompaction() {
	s.SetupTest()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Maybe()

	mockScheduler := newOwnershipScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Maybe()
	handler := newCompactionInspector(s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, mockScheduler, newMockVersionManager())

	t := &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    10,
		Channel:   "ch-1",
		Type:      datapb.CompactionType_BumpSchemaVersionCompaction,
	}

	compactTask, err := handler.createCompactTask(t)
	s.NoError(err)
	s.NotNil(compactTask)
	s.Equal(datapb.CompactionType_BumpSchemaVersionCompaction, compactTask.GetTaskProto().GetType())
}

func (s *CompactionPlanHandlerSuite) TestCreateCompactTask_UnknownType() {
	s.SetupTest()

	mockScheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, mockScheduler, newMockVersionManager())

	t := &datapb.CompactionTask{
		TriggerID: 2,
		PlanID:    20,
		Channel:   "ch-1",
		Type:      datapb.CompactionType(9999),
	}

	compactTask, err := handler.createCompactTask(t)
	s.Nil(compactTask)
	s.Error(err)
	s.True(errors.Is(err, merr.ErrIllegalCompactionPlan))
}
