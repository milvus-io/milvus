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
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCompactionPlanHandlerSuite(t *testing.T) {
	suite.Run(t, new(CompactionPlanHandlerSuite))
}

func TestSummaryCompactionState(t *testing.T) {
	testCases := []struct {
		name            string
		taskState       datapb.CompactionTaskState
		terminalState   datapb.CompactionTaskState
		publicState     commonpb.CompactionState
		executingCnt    int
		completedCnt    int
		failedCnt       int
		timeoutCnt      int
		unclassifiedCnt int
	}{
		{name: "unknown", taskState: datapb.CompactionTaskState_unknown, publicState: commonpb.CompactionState_UndefiedState, unclassifiedCnt: 1},
		{name: "executing", taskState: datapb.CompactionTaskState_executing, publicState: commonpb.CompactionState_Executing, executingCnt: 1},
		{name: "pipelining", taskState: datapb.CompactionTaskState_pipelining, publicState: commonpb.CompactionState_Executing, executingCnt: 1},
		{name: "completed", taskState: datapb.CompactionTaskState_completed, publicState: commonpb.CompactionState_Completed, completedCnt: 1},
		{name: "failed", taskState: datapb.CompactionTaskState_failed, publicState: commonpb.CompactionState_Completed, failedCnt: 1},
		{name: "timeout", taskState: datapb.CompactionTaskState_timeout, publicState: commonpb.CompactionState_Completed, timeoutCnt: 1},
		{name: "analyzing", taskState: datapb.CompactionTaskState_analyzing, publicState: commonpb.CompactionState_Executing, executingCnt: 1},
		{name: "indexing", taskState: datapb.CompactionTaskState_indexing, publicState: commonpb.CompactionState_Executing, executingCnt: 1},
		{name: "cleaned legacy", taskState: datapb.CompactionTaskState_cleaned, publicState: commonpb.CompactionState_Completed, completedCnt: 1},
		{name: "cleaned after success", taskState: datapb.CompactionTaskState_cleaned, terminalState: datapb.CompactionTaskState_completed, publicState: commonpb.CompactionState_Completed, completedCnt: 1},
		{name: "cleaned after failure", taskState: datapb.CompactionTaskState_cleaned, terminalState: datapb.CompactionTaskState_failed, publicState: commonpb.CompactionState_Completed, failedCnt: 1},
		{name: "cleaned after timeout", taskState: datapb.CompactionTaskState_cleaned, terminalState: datapb.CompactionTaskState_timeout, publicState: commonpb.CompactionState_Completed, timeoutCnt: 1},
		{name: "cleaned with nonterminal outcome", taskState: datapb.CompactionTaskState_cleaned, terminalState: datapb.CompactionTaskState_executing, publicState: commonpb.CompactionState_UndefiedState, unclassifiedCnt: 1},
		{name: "meta_saved", taskState: datapb.CompactionTaskState_meta_saved, publicState: commonpb.CompactionState_Executing, executingCnt: 1},
		{name: "statistic", taskState: datapb.CompactionTaskState_statistic, publicState: commonpb.CompactionState_Executing, executingCnt: 1},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			info := policy.SummaryCompactionState(100, []*datapb.CompactionTask{{
				TriggerID:     100,
				PlanID:        1,
				State:         testCase.taskState,
				TerminalState: testCase.terminalState,
			}})

			assert.Equal(t, testCase.publicState, info.State)
			assert.Equal(t, testCase.executingCnt, info.ExecutingCnt)
			assert.Equal(t, testCase.completedCnt, info.CompletedCnt)
			assert.Equal(t, testCase.failedCnt, info.FailedCnt)
			assert.Equal(t, testCase.timeoutCnt, info.TimeoutCnt)
			assert.Equal(t, testCase.unclassifiedCnt, info.UnclassifiedCnt)
			assert.Equal(t, 1, len(info.MergeInfos))
		})
	}

	t.Run("mixed states remain undefined when any task is unclassified", func(t *testing.T) {
		tasks := make([]*datapb.CompactionTask, 0, len(testCases)+1)
		for idx, testCase := range testCases {
			tasks = append(tasks, &datapb.CompactionTask{
				TriggerID:     100,
				PlanID:        int64(idx + 1),
				State:         testCase.taskState,
				TerminalState: testCase.terminalState,
			})
		}
		tasks = append(tasks, nil)

		info := policy.SummaryCompactionState(100, tasks)

		assert.Equal(t, commonpb.CompactionState_UndefiedState, info.State)
		assert.Equal(t, 6, info.ExecutingCnt)
		assert.Equal(t, 3, info.CompletedCnt)
		assert.Equal(t, 2, info.FailedCnt)
		assert.Equal(t, 2, info.TimeoutCnt)
		assert.Equal(t, 2, info.UnclassifiedCnt)
		assert.Equal(t, len(testCases), len(info.MergeInfos))
	})

	t.Run("completed task cannot mask an unknown task", func(t *testing.T) {
		info := policy.SummaryCompactionState(100, []*datapb.CompactionTask{
			{PlanID: 1, State: datapb.CompactionTaskState_completed},
			{PlanID: 2, State: datapb.CompactionTaskState_unknown},
		})

		assert.Equal(t, commonpb.CompactionState_UndefiedState, info.State)
		assert.Equal(t, 1, info.CompletedCnt)
		assert.Equal(t, 1, info.UnclassifiedCnt)
	})

	t.Run("no-op id completes without tasks", func(t *testing.T) {
		info := policy.SummaryCompactionState(-1, nil)

		assert.Equal(t, commonpb.CompactionState_Completed, info.State)
		assert.Equal(t, 0, info.ExecutingCnt)
		assert.Equal(t, 0, info.CompletedCnt)
		assert.Equal(t, 0, len(info.MergeInfos))
	})

	t.Run("unknown id stays undefined without tasks", func(t *testing.T) {
		info := policy.SummaryCompactionState(999999, nil)

		assert.Equal(t, commonpb.CompactionState_UndefiedState, info.State)
		assert.Equal(t, 0, info.ExecutingCnt)
		assert.Equal(t, 0, info.CompletedCnt)
		assert.Equal(t, 0, len(info.MergeInfos))
	})
}

func TestSetCompactionTaskStatePreservesTerminalOutcome(t *testing.T) {
	for _, terminalState := range []datapb.CompactionTaskState{
		datapb.CompactionTaskState_completed,
		datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_timeout,
	} {
		t.Run(terminalState.String(), func(t *testing.T) {
			task := &datapb.CompactionTask{State: datapb.CompactionTaskState_executing}
			ctask.SetState(terminalState)(task)

			assert.Equal(t, terminalState, task.GetState())
			assert.Equal(t, terminalState, task.GetTerminalState())

			ctask.SetState(datapb.CompactionTaskState_cleaned)(task)

			assert.Equal(t, datapb.CompactionTaskState_cleaned, task.GetState())
			assert.Equal(t, terminalState, task.GetTerminalState())
		})
	}

	t.Run("capture terminal state from task created before upgrade", func(t *testing.T) {
		task := &datapb.CompactionTask{State: datapb.CompactionTaskState_failed}
		ctask.SetState(datapb.CompactionTaskState_cleaned)(task)

		assert.Equal(t, datapb.CompactionTaskState_failed, task.GetTerminalState())
	})

	t.Run("terminal outcome is immutable", func(t *testing.T) {
		task := &datapb.CompactionTask{State: datapb.CompactionTaskState_executing}
		ctask.SetState(datapb.CompactionTaskState_failed)(task)
		ctask.SetState(datapb.CompactionTaskState_completed)(task)

		assert.Equal(t, datapb.CompactionTaskState_failed, task.GetTerminalState())
	})
}

type CompactionPlanHandlerSuite struct {
	suite.Suite

	mockMeta           *ctask.MockCompactionMeta
	mockCleaner        *partitionstats.MockPartitionStatsCleaner
	mockCatalog        *mocks.DataCoordCatalog
	compactionTaskMeta *ctask.CompactionTaskMeta
	mockAlloc          allocator.Allocator
	handler            *Inspector
	mockHandler        *meta.NMockHandler
}

func (s *CompactionPlanHandlerSuite) SetupTest() {
	s.mockMeta = ctask.NewMockCompactionMeta(s.T())
	s.mockCleaner = partitionstats.NewMockPartitionStatsCleaner(s.T())
	s.mockCatalog = mocks.NewDataCoordCatalog(s.T())
	s.compactionTaskMeta = ctask.NewCompactionTaskMetaOverCatalog(s.mockCatalog)
	s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Maybe()
	s.mockAlloc = mockAlloc
	mockScheduler := task.NewMockGlobalScheduler(s.T())
	s.handler = NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, s.mockCleaner, s.mockAlloc, nil, mockScheduler, mockScheduler, index.NewTestVersionManager())
	s.mockHandler = meta.NewNMockHandler(s.T())
	s.mockHandler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&meta.CollectionInfo{}, nil).Maybe()
}

func (s *CompactionPlanHandlerSuite) TestScheduleEmpty() {
	s.SetupTest()

	s.handler.schedule()
	s.Empty(s.handler.executingTasks)
}

func (s *CompactionPlanHandlerSuite) generateInitTasksForSchedule() {
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()
	task1 := &ctask.MixCompactionTask{
		Meta: s.mockMeta,
	}
	task1.SetTask(&datapb.CompactionTask{
		PlanID:  1,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-1",
		NodeID:  100,
	})

	task2 := &ctask.MixCompactionTask{
		Meta: s.mockMeta,
	}
	task2.SetTask(&datapb.CompactionTask{
		PlanID:  2,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-1",
		NodeID:  100,
	})

	task3 := &ctask.MixCompactionTask{
		Meta: s.mockMeta,
	}
	task3.SetTask(&datapb.CompactionTask{
		PlanID:  3,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-2",
		NodeID:  101,
	})

	task4 := &ctask.MixCompactionTask{
		Meta: s.mockMeta,
	}
	task4.SetTask(&datapb.CompactionTask{
		PlanID:  4,
		Type:    datapb.CompactionType_Level0DeleteCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: "ch-3",
		NodeID:  102,
	})

	ret := []ctask.CompactionTask{task1, task2, task3, task4}
	for _, t := range ret {
		s.handler.restoreTask(t)
	}
}

func (s *CompactionPlanHandlerSuite) TestScheduleNodeWith1ParallelTask() {
	tests := []struct {
		description string
		tasks       []ctask.CompactionTask
		plans       []*datapb.CompactionPlan
		expectedOut []UniqueID // planID
	}{
		{
			"with L0 tasks diff channel",
			[]ctask.CompactionTask{
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  101,
				}, nil, s.mockMeta, s.compactionTaskMeta),
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, s.compactionTaskMeta),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction},
				{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
			},
			[]UniqueID{10, 11},
		},
		{
			"with L0 tasks same channel",
			[]ctask.CompactionTask{
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, s.compactionTaskMeta),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				{PlanID: 10, Channel: "ch-11", Type: datapb.CompactionType_Level0DeleteCompaction},
			},
			[]UniqueID{10},
		},
		{
			"without L0 tasks",
			[]ctask.CompactionTask{
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-2",
					NodeID:  101,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 14, Channel: "ch-2", Type: datapb.CompactionType_MixCompaction},
				{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
			},
			[]UniqueID{14, 13},
		},
		{
			"empty tasks",
			[]ctask.CompactionTask{},
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
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t ctask.CompactionTask, _ int) int64 {
				return t.GetTaskProto().GetPlanID()
			}))
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestScheduleNodeWithL0Executing() {
	tests := []struct {
		description string
		tasks       []ctask.CompactionTask
		plans       []*datapb.CompactionPlan
		expectedOut []UniqueID // planID
	}{
		{
			"with L0 tasks diff channel",
			[]ctask.CompactionTask{
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta),
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
			},
			[]*datapb.CompactionPlan{{}, {}},
			[]UniqueID{10, 11},
		},
		{
			"with L0 tasks same channel",
			[]ctask.CompactionTask{
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta),
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-3",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
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
			[]ctask.CompactionTask{
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta),
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta),
				ctask.NewL0CompactionTask(&datapb.CompactionTask{
					PlanID:  12,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta),
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
			[]ctask.CompactionTask{
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-3",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
				ctask.NewMixCompactionTask(&datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
			},
			[]*datapb.CompactionPlan{
				{PlanID: 14, Channel: "ch-3", Type: datapb.CompactionType_MixCompaction},
				{},
			},
			[]UniqueID{13, 14},
		},
		{"empty tasks", []ctask.CompactionTask{}, []*datapb.CompactionPlan{}, []UniqueID{}},
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
			gotPlanIDs := lo.Map(gotTasks, func(t ctask.CompactionTask, _ int) int64 {
				return t.GetTaskProto().GetPlanID()
			})
			s.ElementsMatch(test.expectedOut, gotPlanIDs)
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionConflictsWithExecutingL0SameChannel() {
	s.SetupTest()
	s.handler.executingTasks[1] = ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_Level0DeleteCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.compactionTaskMeta)
	s.NoError(s.handler.submitTask(ctask.NewBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())))

	gotTasks := s.handler.schedule()
	s.Empty(gotTasks)
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionBlocksQueuedL0SameChannel() {
	s.SetupTest()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.CompactionTaskPrioritizer.Key, "mix")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.CompactionTaskPrioritizer.Key)
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return().Once()

	s.NoError(s.handler.submitTask(ctask.NewBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())))
	s.NoError(s.handler.submitTask(ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_Level0DeleteCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.compactionTaskMeta)))

	gotTasks := s.handler.schedule()
	s.Equal([]UniqueID{2}, lo.Map(gotTasks, func(t ctask.CompactionTask, _ int) int64 {
		return t.GetTaskProto().GetPlanID()
	}))
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionBlocksClusteringSameLabel() {
	s.SetupTest()
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return().Once()

	s.NoError(s.handler.submitTask(ctask.NewBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())))
	s.NoError(s.handler.submitTask(ctask.NewClusteringCompactionTask(&datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_ClusteringCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, s.mockHandler, nil, index.NewTestVersionManager())))

	gotTasks := s.handler.schedule()
	s.Equal([]UniqueID{1}, lo.Map(gotTasks, func(t ctask.CompactionTask, _ int) int64 {
		return t.GetTaskProto().GetPlanID()
	}))
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestRemoveTasksByChannel() {
	s.SetupTest()
	ch := "ch1"

	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()

	t1 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:  19530,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: ch,
		NodeID:  1,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t2 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:  19531,
		Type:    datapb.CompactionType_MixCompaction,
		Channel: ch,
		NodeID:  1,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	s.handler.submitTask(t1)
	s.handler.restoreTask(t2)
	s.handler.RemoveTasksByChannel(ch)
}

func (s *CompactionPlanHandlerSuite) TestGetCompactionTask() {
	s.SetupTest()

	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()

	t1 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t2 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_completed,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t3 := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    3,
		Type:      datapb.CompactionType_Level0DeleteCompaction,
		Channel:   "ch-02",
		State:     datapb.CompactionTaskState_failed,
	}, nil, s.mockMeta, s.compactionTaskMeta)

	inTasks := map[int64]ctask.CompactionTask{
		1: t1,
		2: t2,
		3: t3,
	}
	for _, t := range inTasks {
		s.handler.submitTask(t)
	}

	s.handler.schedule()

	for _, t := range inTasks {
		s.handler.compactionTaskMeta.SaveCompactionTaskMemory(t.ShadowClone())
	}

	info := s.handler.GetCompactionInfo(context.TODO(), 1)
	s.Equal(1, info.CompletedCnt)
	s.Equal(1, info.ExecutingCnt)
	s.Equal(1, info.FailedCnt)
}

func (s *CompactionPlanHandlerSuite) TestCompactionQueueFull() {
	s.SetupTest()
	paramtable.Get().Save("dataCoord.compaction.taskQueueCapacity", "1")
	defer paramtable.Get().Reset("dataCoord.compaction.taskQueueCapacity")

	mockScheduler := task.NewMockGlobalScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.Init {
			cluster := session.NewMockCluster(s.T())
			t.QueryTaskOnWorker(cluster)
		}
	}).Maybe()
	s.handler = NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, s.mockAlloc, nil, mockScheduler, mockScheduler, index.NewTestVersionManager())

	t1 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	s.NoError(s.handler.submitTask(t1))

	t2 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_completed,
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	s.Error(s.handler.submitTask(t2))
}

func (s *CompactionPlanHandlerSuite) TestIsFullAtMaxTaskNum() {
	for _, tc := range []struct {
		name             string
		maxTaskNum       string
		pending, running int
		full             bool
	}{
		{"0 is unlimited", "0", 2, 2, false},
		{"-1 is unlimited", "-1", 2, 2, false},
		{"below the limit", "5", 2, 2, false},
		{"pending and running tasks reach the limit", "4", 2, 2, true},
	} {
		s.Run(tc.name, func() {
			paramtable.Get().Save(Params.DataCoordCfg.CompactionMaxTaskNum.Key, tc.maxTaskNum)
			defer paramtable.Get().Reset(Params.DataCoordCfg.CompactionMaxTaskNum.Key)
			inspector := NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, nil, nil, nil, nil, index.NewTestVersionManager())
			for i := 0; i < tc.pending; i++ {
				s.Require().NoError(inspector.submitTask(ctask.NewMixCompactionTask(&datapb.CompactionTask{PlanID: int64(1 + i)}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())))
			}
			for i := 0; i < tc.running; i++ {
				inspector.executingTasks[int64(100+i)] = ctask.NewMixCompactionTask(&datapb.CompactionTask{PlanID: int64(100 + i)}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())
			}
			s.Equal(tc.full, inspector.isFull())
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestSharedAdmissionLimitsNewTasksInLegacyMode() {
	params := paramtable.Get()
	params.Save("dataCoord.compaction.rateLimitTokens", "1")
	params.Save("dataCoord.compaction.rateLimitInterval", "3600")
	s.T().Cleanup(func() {
		params.Reset("dataCoord.compaction.rateLimitTokens")
		params.Reset("dataCoord.compaction.rateLimitInterval")
	})

	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Maybe()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Maybe()
	inspector := NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, s.mockAlloc, nil, nil, nil, index.NewTestVersionManager())
	s.Require().NoError(inspector.InitAdmission())
	s.Require().NoError(inspector.EnqueueCompaction(s.T().Context(), &datapb.CompactionTask{
		PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10, 11},
	}, AdmissionOptions{Producer: ProducerLegacyTrigger}))
	s.Require().Error(inspector.EnqueueCompaction(s.T().Context(), &datapb.CompactionTask{
		PlanID: 2, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{12},
	}, AdmissionOptions{Producer: ProducerTriggerManager}))
	s.Nil(inspector.getCompactionTask(2))
	s.Len(s.compactionTaskMeta.GetCompactionTasksByTriggerID(0), 1)
}

func (s *CompactionPlanHandlerSuite) TestAdmissionRefundsAndRetainsLastValidConfiguration() {
	params := paramtable.Get()
	cfg := &Params.DataCoordCfg
	params.Save(cfg.CompactionRateLimitTokens.Key, "1")
	params.Save(cfg.CompactionRateLimitInterval.Key, "3600")
	s.T().Cleanup(func() {
		params.Reset(cfg.CompactionRateLimitTokens.Key)
		params.Reset(cfg.CompactionRateLimitInterval.Key)
	})
	inspector := s.handler
	s.Require().NoError(inspector.InitAdmission())
	clock := inspector.admission.refilledAt
	inspector.admission.now = func() time.Time { return clock }

	ctx := s.T().Context()
	catalog := mocks.NewDataCoordCatalog(s.T())
	inspector.compactionTaskMeta = ctask.NewCompactionTaskMetaOverCatalog(catalog)
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.MatchedBy(func(task *datapb.CompactionTask) bool {
		return task.GetPlanID() == 1
	})).Return(merr.ErrIoFailed).Once()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.MatchedBy(func(task *datapb.CompactionTask) bool {
		return task.GetPlanID() == 2
	})).Return(nil).Once()
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Twice()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, []int64{10}).Return(true, true).Once()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, []int64{20}).Return(true, true).Once()
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{10}, false).Once()

	failed := &datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}}
	s.Require().ErrorIs(inspector.EnqueueCompaction(ctx, failed, AdmissionOptions{Producer: ProducerImport}), merr.ErrIoFailed)
	next := &datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, InputSegments: []int64{20}}
	params.Save(cfg.CompactionRateLimitTokens.Key, "0")
	s.Require().NoError(inspector.EnqueueCompaction(ctx, next, AdmissionOptions{Producer: ProducerRestore}))
	s.Require().ErrorIs(inspector.EnqueueCompaction(ctx, failed, AdmissionOptions{Producer: ProducerImport}), ErrAdmissionLimited)
	s.Nil(inspector.getCompactionTask(1))
	s.NotNil(inspector.getCompactionTask(2))

	// The same invalid input must fail initial configuration.
	s.Require().Error(inspector.InitAdmission())
}

func (s *CompactionPlanHandlerSuite) TestAdmissionAllowsOnlyExplicitDependencyPastBacklogCap() {
	params := paramtable.Get()
	cfg := &Params.DataCoordCfg
	params.Save(cfg.CompactionMaxTaskNum.Key, "1")
	params.Save(cfg.CompactionRateLimitTokens.Key, "2")
	params.Save(cfg.CompactionRateLimitInterval.Key, "3600")
	s.T().Cleanup(func() {
		params.Reset(cfg.CompactionMaxTaskNum.Key)
		params.Reset(cfg.CompactionRateLimitTokens.Key)
		params.Reset(cfg.CompactionRateLimitInterval.Key)
	})
	inspector := s.handler
	s.Require().NoError(inspector.InitAdmission())
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Twice()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Twice()
	ctx := s.T().Context()

	parent := &datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_ClusteringCompaction, InputSegments: []int64{10}}
	opts := AdmissionOptions{Producer: ProducerTriggerManager}
	s.Require().NoError(inspector.EnqueueCompaction(ctx, parent, opts))
	sort := &datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{20}}
	s.Require().ErrorIs(inspector.EnqueueCompaction(ctx, sort, opts), ErrAdmissionLimited)
	opts.DependencySort = true
	s.Require().NoError(inspector.EnqueueCompaction(ctx, sort, opts))
	sort = &datapb.CompactionTask{PlanID: 3, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{30}}
	s.Require().ErrorIs(inspector.EnqueueCompaction(ctx, sort, opts), ErrAdmissionLimited)
	s.Nil(inspector.getCompactionTask(3))
}

func (s *CompactionPlanHandlerSuite) TestAdmissionRefundsQueueRejectionAndRecoveryDoesNotCharge() {
	params := paramtable.Get()
	cfg := &Params.DataCoordCfg
	params.Save(cfg.CompactionTaskQueueCapacity.Key, "1")
	params.Save(cfg.CompactionRateLimitTokens.Key, "1")
	params.Save(cfg.CompactionRateLimitInterval.Key, "3600")
	s.T().Cleanup(func() {
		params.Reset(cfg.CompactionTaskQueueCapacity.Key)
		params.Reset(cfg.CompactionRateLimitTokens.Key)
		params.Reset(cfg.CompactionRateLimitInterval.Key)
	})

	ctx := s.T().Context()
	inspector := NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, s.mockAlloc, nil, nil, nil, index.NewTestVersionManager())
	s.Require().NoError(inspector.InitAdmission())
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Times(3)
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Times(3)
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{10}, false).Once()

	failed := &datapb.CompactionTask{
		PlanID: 1, TriggerID: 100, Type: datapb.CompactionType_RestoreLevel0DeleteCompaction,
		State: datapb.CompactionTaskState_pipelining, InputSegments: []int64{10}, NodeID: meta.NullNodeID,
	}
	// Fill the last queue slot through an existing-task requeue while the new
	// task's metadata write is in progress.
	catalog := mocks.NewDataCoordCatalog(s.T())
	inspector.compactionTaskMeta = ctask.NewCompactionTaskMetaOverCatalog(catalog)
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.MatchedBy(func(task *datapb.CompactionTask) bool {
		return task.GetPlanID() == 1
	})).RunAndReturn(func(context.Context, *datapb.CompactionTask) error {
		requeued := ctask.NewMixCompactionTask(&datapb.CompactionTask{PlanID: 50}, nil, s.mockMeta, inspector.compactionTaskMeta, nil)
		return inspector.submitTask(requeued)
	}).Once()
	s.Require().ErrorIs(inspector.EnqueueCompaction(ctx, failed, AdmissionOptions{Producer: ProducerRestore}), ErrAdmissionLimited)
	s.Require().Len(inspector.compactionTaskMeta.GetCompactionTasksByTriggerID(100), 1)
	s.False(inspector.HasCompactionTask(1))
	_, err := inspector.queueTasks.Dequeue()
	s.Require().NoError(err)

	// Recovery sees the durable task even though its initial enqueue failed.
	inspector.LoadMeta()
	s.True(inspector.HasCompactionTask(1))
	_, err = inspector.queueTasks.Dequeue()
	s.Require().NoError(err)

	next := &datapb.CompactionTask{PlanID: 2, TriggerID: 200, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{20}}
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.MatchedBy(func(task *datapb.CompactionTask) bool {
		return task.GetPlanID() == 2
	})).Return(nil).Once()
	s.Require().NoError(inspector.EnqueueCompaction(ctx, next, AdmissionOptions{Producer: ProducerImport}))
	s.True(inspector.HasCompactionTask(2))
}

func (s *CompactionPlanHandlerSuite) TestAdmissionAlternatesContendedPartialRefills() {
	params := paramtable.Get()
	cfg := &Params.DataCoordCfg
	params.Save(cfg.CompactionRateLimitTokens.Key, "2")
	params.Save(cfg.CompactionRateLimitInterval.Key, "2")
	s.T().Cleanup(func() {
		params.Reset(cfg.CompactionRateLimitTokens.Key)
		params.Reset(cfg.CompactionRateLimitInterval.Key)
	})

	inspector := s.handler
	s.Require().NoError(inspector.InitAdmission())
	clock := inspector.admission.refilledAt
	inspector.admission.now = func() time.Time { return clock }
	ctx := s.T().Context()
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Times(5)
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Times(5)
	submit := func(id int64, producer Producer) error {
		return inspector.EnqueueCompaction(ctx, &datapb.CompactionTask{
			PlanID: id, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{id},
		}, AdmissionOptions{Producer: producer})
	}
	s.Require().NoError(submit(1, ProducerTBC))
	s.Require().NoError(submit(2, ProducerTBC))
	s.Require().ErrorIs(submit(3, ProducerRestore), ErrAdmissionLimited)
	clock = clock.Add(time.Second)
	s.Require().ErrorIs(submit(4, ProducerTBC), ErrAdmissionLimited)
	s.Require().NoError(submit(3, ProducerRestore))
	clock = clock.Add(time.Second)
	s.Require().ErrorIs(submit(4, ProducerTBC), ErrAdmissionLimited)
	s.Require().NoError(submit(5, ProducerRestore))
	clock = clock.Add(time.Second)
	limit, _ := inspector.attemptLimit(ctx, ProducerTBC, 2)
	s.Equal(1, limit)
	s.Require().NoError(submit(4, ProducerTBC))
}

func (s *CompactionPlanHandlerSuite) TestExecCompactionPlan() {
	s.SetupTest()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Maybe()
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Twice()

	mockScheduler := task.NewMockGlobalScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.Init {
			cluster := session.NewMockCluster(s.T())
			t.QueryTaskOnWorker(cluster)
		}
	}).Maybe()
	handler := NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, s.mockAlloc, nil, mockScheduler, mockScheduler, index.NewTestVersionManager())

	task := &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Channel:   "ch-1",
		Type:      datapb.CompactionType_MixCompaction,
	}
	err := handler.EnqueueCompaction(s.T().Context(), task, AdmissionOptions{Producer: ProducerLegacyTrigger})
	s.NoError(err)
	t := handler.getCompactionTask(1)
	s.NotNil(t)
	s.Equal(uint64(1000), t.GetTaskProto().GetCreateTs())
	s.Equal(t.GetTaskProto().GetStartTime(), tsoutil.PhysicalTime(t.GetTaskProto().GetCreateTs()).Unix())
	task.PlanID = 2
	err = s.handler.EnqueueCompaction(s.T().Context(), task, AdmissionOptions{Producer: ProducerLegacyTrigger})
	s.NoError(err)
}

func (s *CompactionPlanHandlerSuite) TestCheckCompaction() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(ctask.CompactionTask).GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
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
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return()

	t1 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t2 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t3 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:    3,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_timeout,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t4 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:    4,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_timeout,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	t6 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:    6,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-2",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	inTasks := map[int64]ctask.CompactionTask{
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
		func(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*meta.SegmentInfo, error) {
			if t.GetPlanID() == 2 {
				segment := meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
				return []*meta.SegmentInfo{segment}, nil
			} else if t.GetPlanID() == 6 {
				return nil, errors.Errorf("intended error")
			}
			return nil, errors.Errorf("unexpected error")
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
		{
			PlanID:    4,
			Type:      datapb.CompactionType_MixCompaction,
			State:     datapb.CompactionTaskState_cleaned,
			StartTime: time.Now().Add(-time.Second * 100000).Unix(),
			EndTime:   time.Now().Unix(),
		},
		{
			PlanID:    5,
			Type:      datapb.CompactionType_MixCompaction,
			State:     datapb.CompactionTaskState_cleaned,
			StartTime: time.Now().Add(-time.Second * 100000).Unix(),
			EndTime:   time.Now().Add(-time.Second * 100000).Unix(),
		},
	}

	catalog := &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()}
	compactionTaskMeta, err := ctask.NewCompactionTaskMeta(context.TODO(), catalog)
	s.NoError(err)
	s.handler.meta = &meta.Meta{}
	s.handler.compactionTaskMeta = compactionTaskMeta
	for _, t := range inTasks {
		s.handler.compactionTaskMeta.SaveCompactionTask(context.TODO(), t)
	}

	s.handler.cleanCompactionTaskMeta()
	tasks := s.handler.compactionTaskMeta.GetCompactionTasksByTriggerID(0)
	remainingPlanIDs := lo.Map(tasks, func(task *datapb.CompactionTask, _ int) int64 {
		return task.GetPlanID()
	})
	s.ElementsMatch([]int64{1, 3, 4}, remainingPlanIDs)
}

func (s *CompactionPlanHandlerSuite) TestProcessCompleteCompaction() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(ctask.CompactionTask).GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
			t.DropTaskOnWorker(cluster)
		}
	})

	// s.mockSessMgr.EXPECT().SyncSegments(mock.Anything, mock.Anything).Return(nil).Once()
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Twice()
	segment := meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*meta.SegmentInfo{segment}, nil).Once()

	dataNodeID := UniqueID(111)

	seg1 := &datapb.SegmentInfo{
		ID:        1,
		Binlogs:   []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 1)},
		Statslogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 2)},
		Deltalogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 3)},
	}

	seg2 := &datapb.SegmentInfo{
		ID:        2,
		Binlogs:   []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 4)},
		Statslogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 5)},
		Deltalogs: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 6)},
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

	task := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:        plan.GetPlanID(),
		TriggerID:     1,
		Type:          plan.GetType(),
		State:         datapb.CompactionTaskState_executing,
		NodeID:        dataNodeID,
		InputSegments: []UniqueID{1, 2},
	}, nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager())

	compactionResult := datapb.CompactionPlanResult{
		PlanID: 1,
		State:  datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID:           3,
				NumOfRows:           15,
				InsertLogs:          []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 301)},
				Field2StatslogPaths: []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 302)},
				Deltalogs:           []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 303)},
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

func (s *CompactionPlanHandlerSuite) TestCleanCompaction() {
	s.SetupTest()

	tests := []struct {
		task ctask.CompactionTask
	}{
		{
			ctask.NewMixCompactionTask(
				&datapb.CompactionTask{
					PlanID:        1,
					TriggerID:     1,
					Type:          datapb.CompactionType_MixCompaction,
					State:         datapb.CompactionTaskState_failed,
					NodeID:        1,
					InputSegments: []UniqueID{1, 2},
				},
				nil, s.mockMeta, s.compactionTaskMeta, index.NewTestVersionManager()),
		},
		{
			ctask.NewL0CompactionTask(&datapb.CompactionTask{
				PlanID:        1,
				TriggerID:     1,
				Type:          datapb.CompactionType_Level0DeleteCompaction,
				State:         datapb.CompactionTaskState_failed,
				NodeID:        1,
				InputSegments: []UniqueID{1, 2},
			},
				nil, s.mockMeta, s.compactionTaskMeta),
		},
	}
	for _, test := range tests {
		task := test.task
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		s.handler.executingTasks[1] = task
		s.Equal(1, len(s.handler.executingTasks))

		err := s.handler.checkCompaction()
		s.NoError(err)
		s.Equal(0, len(s.handler.executingTasks))
		s.Equal(1, len(s.handler.cleaningTasks))
		s.handler.cleanFailedTasks()
		s.Equal(0, len(s.handler.cleaningTasks))
	}
}

func (s *CompactionPlanHandlerSuite) TestCleanClusteringCompaction() {
	s.SetupTest()

	task := ctask.NewClusteringCompactionTask(
		&datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     1,
			CollectionID:  1001,
			Type:          datapb.CompactionType_ClusteringCompaction,
			State:         datapb.CompactionTaskState_failed,
			NodeID:        1,
			InputSegments: []UniqueID{1, 2},
		},
		nil, s.mockMeta, s.compactionTaskMeta, nil, nil, nil, s.mockCleaner, s.mockHandler, nil, index.NewTestVersionManager())
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mockCleaner.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil)
	s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

	s.handler.executingTasks[1] = task
	s.Equal(1, len(s.handler.executingTasks))
	s.handler.checkCompaction()
	s.Equal(0, len(s.handler.executingTasks))
	s.Equal(1, len(s.handler.cleaningTasks))
	s.handler.cleanFailedTasks()
	s.Equal(0, len(s.handler.cleaningTasks))
}

func (s *CompactionPlanHandlerSuite) TestCleanClusteringCompactionCommitFail() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(ctask.CompactionTask).GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
			t.DropTaskOnWorker(cluster)
		}
	})

	task := ctask.NewClusteringCompactionTask(&datapb.CompactionTask{
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
			Name:            ctask.Int64Field,
			IsPrimaryKey:    true,
			DataType:        schemapb.DataType_Int64,
			AutoID:          true,
			IsClusteringKey: true,
		},
	},
		nil, s.mockMeta, s.compactionTaskMeta, nil, nil, nil, s.mockCleaner, s.mockHandler, nil, index.NewTestVersionManager())

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil)
	s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
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
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))

	s.handler.submitTask(task)
	s.handler.schedule()
	s.handler.checkCompaction()
	s.Equal(0, len(task.GetTaskProto().GetResultSegments()))

	s.Equal(datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
	s.Equal(0, len(s.handler.executingTasks))
	s.Equal(1, len(s.handler.cleaningTasks))

	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mockCleaner.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil)
	s.handler.cleanFailedTasks()
	s.Equal(0, len(s.handler.cleaningTasks))
}

// test inspector should keep clean the failed task until it become cleaned
func (s *CompactionPlanHandlerSuite) TestKeepClean() {
	s.SetupTest()

	tests := []struct {
		task ctask.CompactionTask
	}{
		{
			ctask.NewClusteringCompactionTask(&datapb.CompactionTask{
				PlanID:        1,
				TriggerID:     1,
				Type:          datapb.CompactionType_ClusteringCompaction,
				State:         datapb.CompactionTaskState_failed,
				NodeID:        1,
				InputSegments: []UniqueID{1, 2},
			},
				nil, s.mockMeta, s.compactionTaskMeta, nil, nil, nil, s.mockCleaner, s.mockHandler, nil, index.NewTestVersionManager()),
		},
	}
	for _, test := range tests {
		task := test.task
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return()
		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.mockCleaner.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		s.handler.executingTasks[1] = task

		s.Equal(1, len(s.handler.executingTasks))
		s.handler.checkCompaction()
		s.Equal(0, len(s.handler.executingTasks))
		s.Equal(1, len(s.handler.cleaningTasks))
		s.handler.cleanFailedTasks()
		s.Equal(1, len(s.handler.cleaningTasks))
		s.mockCleaner.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil).Once()
		s.handler.cleanFailedTasks()
		s.Equal(0, len(s.handler.cleaningTasks))
	}
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
	handler := &Inspector{}
	t1 := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, nil, index.NewTestVersionManager())
	handler.checkDelay(t1)
	t2 := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, nil)
	handler.checkDelay(t2)
	t3 := ctask.NewClusteringCompactionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, nil, nil, nil, nil, nil, nil, nil, index.NewTestVersionManager())
	handler.checkDelay(t3)
	t4 := ctask.NewBumpSchemaVersionTask(&datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, nil, index.NewTestVersionManager())
	handler.checkDelay(t4)
}

func TestGetCompactionTasksNum(t *testing.T) {
	queueTasks := NewCompactionQueue(10, DefaultPrioritizer)
	queueTasks.Enqueue(
		ctask.NewMixCompactionTask(&datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 1,
			Type:         datapb.CompactionType_MixCompaction,
		}, nil, nil, nil, index.NewTestVersionManager()),
	)
	queueTasks.Enqueue(
		ctask.NewL0CompactionTask(&datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 1,
			Type:         datapb.CompactionType_Level0DeleteCompaction,
		}, nil, nil, nil),
	)
	queueTasks.Enqueue(
		ctask.NewClusteringCompactionTask(&datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 10,
			Type:         datapb.CompactionType_ClusteringCompaction,
		}, nil, nil, nil, nil, nil, nil, nil, nil, nil, index.NewTestVersionManager()),
	)
	executingTasks := make(map[int64]ctask.CompactionTask, 0)
	executingTasks[1] = ctask.NewMixCompactionTask(&datapb.CompactionTask{
		StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
		CollectionID: 1,
		Type:         datapb.CompactionType_MixCompaction,
	}, nil, nil, nil, index.NewTestVersionManager())
	executingTasks[2] = ctask.NewL0CompactionTask(&datapb.CompactionTask{
		StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
		CollectionID: 10,
		Type:         datapb.CompactionType_Level0DeleteCompaction,
	}, nil, nil, nil)

	handler := &Inspector{
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
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()

	mockScheduler := task.NewMockGlobalScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Maybe()
	handler := NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, s.mockAlloc, nil, mockScheduler, mockScheduler, index.NewTestVersionManager())

	t := &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    10,
		Channel:   "ch-1",
		Type:      datapb.CompactionType_BumpSchemaVersionCompaction,
	}

	compactTask, err := handler.createCompactTask(s.T().Context(), t)
	s.NoError(err)
	s.NotNil(compactTask)
	s.Equal(datapb.CompactionType_BumpSchemaVersionCompaction, compactTask.GetTaskProto().GetType())
}

func (s *CompactionPlanHandlerSuite) TestCreateCompactTaskRejectsSnapshotProtectedInputs() {
	tests := []struct {
		name  string
		block func(*snapshot.SnapshotMeta)
	}{
		{
			name: "collection snapshot block",
			block: func(snapshotMeta *snapshot.SnapshotMeta) {
				snapshotMeta.SetSnapshotPending(100)
			},
		},
		{
			name: "segment snapshot protection",
			block: func(snapshotMeta *snapshot.SnapshotMeta) {
				snapshotMeta.SegmentProtectionUntil[1] = uint64(time.Now().Add(time.Hour).Unix())
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			snapshotMeta := snapshot.CreateTestSnapshotMetaLoaded(s.T())
			test.block(snapshotMeta)
			store := metacache.NewMetaStore(nil)
			mt := &meta.Meta{
				Segments:  meta.NewSegmentsInfo(store),
				MetaStore: store,
			}
			mt.CompactionProtection = snapshotMeta
			mt.Segments.SetSegment(1, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				Level:        datapb.SegmentLevel_L1,
			}})
			inspector := NewCompactionInspector(mt, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, index.NewTestVersionManager())

			compactTask, err := inspector.createCompactTask(s.T().Context(), &datapb.CompactionTask{
				PlanID:        10,
				CollectionID:  100,
				Type:          datapb.CompactionType_MixCompaction,
				InputSegments: []int64{1},
			})

			s.Nil(compactTask)
			s.ErrorIs(err, merr.ErrCompactionBlocked)
			s.False(mt.IsSegmentCompacting(1))
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestCreateCompactTask_UnknownType() {
	s.SetupTest()

	mockScheduler := task.NewMockGlobalScheduler(s.T())
	handler := NewCompactionInspector(s.mockMeta, s.compactionTaskMeta, nil, nil, nil, nil, nil, s.mockAlloc, nil, mockScheduler, mockScheduler, index.NewTestVersionManager())

	t := &datapb.CompactionTask{
		TriggerID: 2,
		PlanID:    20,
		Channel:   "ch-1",
		Type:      datapb.CompactionType(9999),
	}

	compactTask, err := handler.createCompactTask(s.T().Context(), t)
	s.Nil(compactTask)
	s.Error(err)
	s.True(errors.Is(err, merr.ErrIllegalCompactionPlan))
}

func TestCompactionTargetIDDoesNotRefineLegacyTaskStatus(t *testing.T) {
	ctx := t.Context()
	manager := targets.NewLoadedCompactionTargetMeta(t, ctx, &datapb.CompactionTarget{
		TargetID: 1234, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS: 100,
	})
	taskMeta := ctask.NewCompactionTaskMetaOverCatalog(nil)
	inspector := &Inspector{compactionTaskMeta: taskMeta, compactionTargetMeta: manager}
	assert.Equal(t, commonpb.CompactionState_UndefiedState, inspector.GetCompactionInfo(ctx, 1234).State)

	taskMeta.SaveCompactionTaskMemory(&datapb.CompactionTask{
		PlanID: 1, TriggerID: 1234, State: datapb.CompactionTaskState_completed,
	})
	info := inspector.GetCompactionInfo(ctx, 1234)
	assert.Equal(t, commonpb.CompactionState_Completed, info.State)
	assert.Equal(t, 1, info.CompletedCnt)
}
