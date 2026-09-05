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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/magiconair/properties/assert"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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

func TestCleanPartitionStatsConfirmsCollectionDrop(t *testing.T) {
	newInspector := func(t *testing.T, lookup func(context.Context, int64) (*collectionInfo, error)) (*compactionInspector, *meta) {
		mt, err := newMemoryMeta(t)
		require.NoError(t, err)
		t.Cleanup(mt.snapshotMeta.Close)
		info := &datapb.PartitionStatsInfo{
			CollectionID: 100,
			PartitionID:  10,
			VChannel:     "channel",
			Version:      1,
		}
		seedPartitionStatsInfo(mt.partitionStatsMeta, info)

		handler := NewNMockHandler(t)
		handler.EXPECT().GetCollection(mock.Anything, int64(100)).RunAndReturn(lookup).Once()
		return newCompactionInspector(context.Background(), mt, nil, handler, nil, nil, newMockVersionManager()), mt
	}

	t.Run("live collection missing only from cache", func(t *testing.T) {
		inspector, mt := newInspector(t, func(context.Context, int64) (*collectionInfo, error) {
			return &collectionInfo{ID: 100}, nil
		})
		inspector.cleanPartitionStats()
		require.Len(t, mt.partitionStatsMeta.ListAllPartitionStatsInfos(), 1)
	})

	t.Run("transient lookup failure", func(t *testing.T) {
		inspector, mt := newInspector(t, func(context.Context, int64) (*collectionInfo, error) {
			return nil, errors.New("rootcoord unavailable")
		})
		inspector.cleanPartitionStats()
		require.Len(t, mt.partitionStatsMeta.ListAllPartitionStatsInfos(), 1)
	})

	t.Run("confirmed dropped collection", func(t *testing.T) {
		inspector, mt := newInspector(t, func(context.Context, int64) (*collectionInfo, error) {
			return nil, merr.WrapErrCollectionNotFound(100)
		})
		inspector.cleanPartitionStats()
		require.Empty(t, mt.partitionStatsMeta.ListAllPartitionStatsInfos())
	})
}

func TestEnqueueCompactionSaveFailureFailStops(t *testing.T) {
	writeErr := errors.New("ambiguous catalog response")
	newHandler := func(t *testing.T, ctx context.Context) (*compactionInspector, *MockCompactionMeta) {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		meta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, []int64{100}).Return(true, true).Once()
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(writeErr).Once()

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Once()
		return newCompactionInspector(ctx, meta, alloc, nil, nil, newOwnershipScheduler(t), newMockVersionManager()), meta
	}
	newTask := func() *datapb.CompactionTask {
		return &datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     2,
			CollectionID:  3,
			Type:          datapb.CompactionType_MixCompaction,
			State:         datapb.CompactionTaskState_pipelining,
			InputSegments: []int64{100},
		}
	}

	t.Run("live process fail-stops without releasing inputs", func(t *testing.T) {
		handler, _ := newHandler(t, context.Background())
		fatalCalled := false
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
			Build()
		defer mockFatal.UnPatch()

		require.ErrorIs(t, handler.enqueueCompaction(newTask()), writeErr)
		require.True(t, fatalCalled)
	})

	t.Run("shutdown returns and releases inputs", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		handler, meta := newHandler(t, ctx)
		meta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Once()
		fatalCalled := false
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
			Build()
		defer mockFatal.UnPatch()

		require.ErrorIs(t, handler.enqueueCompaction(newTask()), writeErr)
		require.False(t, fatalCalled)
	})
}

type CompactionPlanHandlerSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockAlloc   allocator.Allocator
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
	t.SetTask(cloneCompactionTask(t.GetTask(), setState(datapb.CompactionTaskState_pipelining)))
	return true
}

func (t *stateRewritingCompactionTask) Process() bool {
	// Simulate a stale scheduler callback rewriting timeout after the inspector
	// reads the terminal state but before Process gets to inspect it.
	t.SetTask(cloneCompactionTask(t.GetTask(), setState(datapb.CompactionTaskState_pipelining)))
	return t.CompactionTask.Process()
}

func (s *CompactionPlanHandlerSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Maybe()
	// Every task in retrying state asks cleanup to renew it, including the ones
	// tests here build only to
	// exercise other parts of cleanup. Tests that care about the replan itself use their own
	// handler with a tailored allocator; this default makes an incidental
	// replan fail harmlessly at its first allocation instead of panicking
	// on an unstubbed mock call.
	mockAlloc.EXPECT().AllocID(mock.Anything).
		Return(int64(0), errors.New("the replan is not exercised by this test")).Maybe()
	s.mockAlloc = mockAlloc
	mockScheduler := newOwnershipScheduler(s.T())
	s.handler = newCompactionInspector(context.Background(), s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, newMockVersionManager())
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
	s.handler.queueTasks.Enqueue(queued)

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
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  101,
				}, nil, s.mockMeta),
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
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
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				}, nil, s.mockMeta, newMockVersionManager()),
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
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
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-2",
					NodeID:  101,
				}, nil, s.mockMeta, newMockVersionManager()),
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
				return t.GetTask().GetPlanID()
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
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  102,
				}, nil, s.mockMeta),
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				}, nil, s.mockMeta),
				newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
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
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-3",
					NodeID:  102,
				}, nil, s.mockMeta, newMockVersionManager()),
				newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
				return t.GetTask().GetPlanID()
			})
			s.ElementsMatch(test.expectedOut, gotPlanIDs)
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionConflictsWithExecutingL0SameChannel() {
	s.SetupTest()
	s.handler.executingTasks[1] = newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_Level0DeleteCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta)
	s.handler.submitTask(newBumpSchemaVersionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, newMockVersionManager()))

	gotTasks := s.handler.schedule()
	s.Empty(gotTasks)
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionBlocksQueuedL0SameChannel() {
	s.SetupTest()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.CompactionTaskPrioritizer.Key, "mix")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.CompactionTaskPrioritizer.Key)
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return().Once()

	s.handler.submitTask(newBumpSchemaVersionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, newMockVersionManager()))
	s.handler.submitTask(newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_Level0DeleteCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta))

	gotTasks := s.handler.schedule()
	s.Equal([]UniqueID{2}, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
		return t.GetTask().GetPlanID()
	}))
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestSchedule_BumpSchemaVersionBlocksClusteringSameLabel() {
	s.SetupTest()
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return().Once()

	s.handler.submitTask(newBumpSchemaVersionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:      1,
		Type:        datapb.CompactionType_BumpSchemaVersionCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, newMockVersionManager()))
	s.handler.submitTask(newClusteringCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:      2,
		Type:        datapb.CompactionType_ClusteringCompaction,
		State:       datapb.CompactionTaskState_pipelining,
		Channel:     "ch-1",
		PartitionID: 10,
		NodeID:      102,
	}, nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager()))

	gotTasks := s.handler.schedule()
	s.Equal([]UniqueID{1}, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
		return t.GetTask().GetPlanID()
	}))
	s.Equal(1, s.handler.queueTasks.Len())
}

func (s *CompactionPlanHandlerSuite) TestRemoveTasksByChannel() {
	s.SetupTest()
	ch := "ch1"

	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()

	t1 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:  19530,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_pipelining,
		Channel: ch,
		NodeID:  1,
	}, nil, s.mockMeta, newMockVersionManager())

	t2 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:  19531,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_executing,
		Channel: ch,
		NodeID:  1,
	}, nil, s.mockMeta, newMockVersionManager())

	s.handler.submitTask(t1)
	s.handler.restoreTask(t2)
	s.handler.removeTasksByChannel(ch)

	s.Equal(datapb.CompactionTaskState_failed, t1.GetTask().GetState())
	s.Equal(datapb.CompactionTaskState_failed, t2.GetTask().GetState())
	s.Equal("channel dropped", t1.GetTask().GetFailReason())
	s.Equal("channel dropped", t2.GetTask().GetFailReason())
	// Ownership is intentionally retained until normal cleanup consumes the
	// durable failed state and releases each task's input segment claims.
	s.Equal(1, s.handler.queueTasks.Len())
	s.Contains(s.handler.executingTasks, t2.GetTask().GetPlanID())
}

func (s *CompactionPlanHandlerSuite) TestRemoveTasksByChannelKeepsOwnerWhenMetaSaveFails() {
	meta := NewMockCompactionMeta(s.T())
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		Return(errors.New("catalog unavailable")).Once()
	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(context.Background(), meta, nil, nil, nil, scheduler, newMockVersionManager())

	const planID = int64(19532)
	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:  planID,
		Type:    datapb.CompactionType_MixCompaction,
		State:   datapb.CompactionTaskState_executing,
		Channel: "ch1",
		NodeID:  1,
	}, nil, meta, newMockVersionManager())
	handler.executingTasks[planID] = task

	handler.removeTasksByChannel("ch1")

	s.Equal(datapb.CompactionTaskState_executing, task.GetTask().GetState())
	s.Contains(handler.executingTasks, planID,
		"a failed terminal write must not orphan the task from its normal owner")
}

func (s *CompactionPlanHandlerSuite) TestGetCompactionTask() {
	s.SetupTest()

	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Return()

	t1 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, newMockVersionManager())

	t2 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_completed,
	}, nil, s.mockMeta, newMockVersionManager())

	// RetryTimes at the cap makes this a settled failure. Below the cap the
	// task would still be awaiting a rebuild, and the summary reports that as
	// executing rather than failed -- see
	// TestSummaryKeepsTriggerRunningWhileReplanIsOwed.
	t3 := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID:  1,
		PlanID:     3,
		Type:       datapb.CompactionType_Level0DeleteCompaction,
		Channel:    "ch-02",
		State:      datapb.CompactionTaskState_failed,
		RetryTimes: int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()),
	}, nil, s.mockMeta)

	inTasks := map[int64]CompactionTask{
		1: t1,
		2: t2,
		3: t3,
	}
	s.mockMeta.EXPECT().GetCompactionTasksByTriggerID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i int64) []*datapb.CompactionTask {
		var ret []*datapb.CompactionTask
		for _, t := range inTasks {
			if t.GetTask().GetTriggerID() != i {
				continue
			}
			ret = append(ret, cloneCompactionTask(t.GetTask()))
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

// A full queue tells producers to stop creating work; it never turns away a
// task that is already persisted. submitTask is therefore infallible, and the
// queue is allowed to sit over its limit until the scheduler drains it.
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
	s.handler = newCompactionInspector(context.Background(), s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, newMockVersionManager())

	t1 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, newMockVersionManager())

	s.handler.submitTask(t1)

	t2 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-01",
		State:     datapb.CompactionTaskState_completed,
	}, nil, s.mockMeta, newMockVersionManager())

	s.True(s.handler.isFull(), "producers are told the queue is full")
	s.handler.submitTask(t2)
	s.Equal(2, s.handler.queueTasks.Len(),
		"a persisted task is accepted over the limit rather than left ownerless")
}

func (s *CompactionPlanHandlerSuite) TestExecCompactionPlan() {
	s.SetupTest()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Maybe()
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Twice()

	mockScheduler := newOwnershipScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.Init {
			cluster := session.NewMockCluster(s.T())
			t.QueryTaskOnWorker(cluster)
		}
	}).Maybe()
	handler := newCompactionInspector(context.Background(), s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, newMockVersionManager())

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
	s.Equal(uint64(1000), t.GetTask().GetCreateTs())
	s.Equal(t.GetTask().GetStartTime(), tsoutil.PhysicalTime(t.GetTask().GetCreateTs()).Unix())
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
		if t.(CompactionTask).GetTask().GetState() == datapb.CompactionTaskState_completed {
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

	t1 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t2 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:    2,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t3 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:    3,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_timeout,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t4 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:    4,
		Type:      datapb.CompactionType_MixCompaction,
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_timeout,
		NodeID:    111,
		StartTime: time.Now().Unix(),
	}, s.mockAlloc, s.mockMeta, newMockVersionManager())

	t6 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
	s.Equal(datapb.CompactionTaskState_executing, t.GetTask().GetState())
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

func (s *CompactionPlanHandlerSuite) TestCompactionGCKeepsMetaUntilWorkerDrop() {
	ctx := context.Background()
	catalog := &datacoord.Catalog{MetaKv: NewMetaMemoryKV()}
	compactionTaskMeta, err := newCompactionTaskMeta(ctx, catalog)
	s.Require().NoError(err)
	const (
		planID    = int64(1)
		triggerID = int64(2)
		nodeID    = int64(11)
	)
	taskProto := &datapb.CompactionTask{
		PlanID:    planID,
		TriggerID: triggerID,
		Type:      datapb.CompactionType_MixCompaction,
		State:     datapb.CompactionTaskState_cleaned,
		NodeID:    nodeID,
		StartTime: time.Now().Add(-100000 * time.Second).Unix(),
	}
	meta := &meta{compactionTaskMeta: compactionTaskMeta}
	s.Require().NoError(meta.SaveCompactionTask(ctx, taskProto))

	dropAttempts := 0
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(nodeID, planID).RunAndReturn(func(int64, int64) error {
		dropAttempts++
		if dropAttempts == 1 {
			return errors.New("worker unavailable")
		}
		return merr.WrapErrNodeNotFound(nodeID)
	}).Twice()
	handler := newCompactionInspector(ctx, meta, nil, nil, nil, nil, newMockVersionManager())
	handler.cleanCompactionTaskMeta()
	s.Len(meta.GetCompactionTasksByTriggerID(ctx, triggerID), 1,
		"an assigned plan must retain its metadata when no worker client is configured")
	handler.cluster = cluster

	handler.cleanCompactionTaskMeta()
	s.Len(meta.GetCompactionTasksByTriggerID(ctx, triggerID), 1,
		"a failed worker drop must retain the metadata cleanup anchor")

	handler.cleanCompactionTaskMeta()
	s.Empty(meta.GetCompactionTasksByTriggerID(ctx, triggerID),
		"node-not-found proves there is no worker owner and permits metadata GC")
}

func (s *CompactionPlanHandlerSuite) TestFailedClusteringCleanupFencesAnalyzeCallback() {
	ctx := context.Background()
	meta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	s.T().Cleanup(meta.snapshotMeta.Close)
	const (
		planID      = int64(1)
		triggerID   = int64(2)
		analyzeID   = int64(3)
		analyzeNode = int64(11)
	)
	s.Require().NoError(meta.analyzeMeta.AddAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: analyzeID,
		NodeID: analyzeNode,
		State:  indexpb.JobState_JobStateInProgress,
	}))
	taskProto := &datapb.CompactionTask{
		PlanID:        planID,
		TriggerID:     triggerID,
		Type:          datapb.CompactionType_ClusteringCompaction,
		State:         datapb.CompactionTaskState_failed,
		AnalyzeTaskID: analyzeID,
	}
	s.Require().NoError(meta.SaveCompactionTask(ctx, taskProto))
	unconfigured := newCompactionInspector(ctx, meta, nil, nil, nil, nil, newMockVersionManager())
	cleanupCalled := false
	s.False(unconfigured.finalizeAnalyzeTaskCleanup(analyzeID, func() bool {
		cleanupCalled = true
		return true
	}),
		"an assigned analyze task must retain its metadata without both cleanup dependencies")
	s.False(cleanupCalled)

	inAnalyzeFinalize := false
	dropOutsideFence := false
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().Finalize(planID, mock.Anything).Run(func(_ int64, fn func()) { fn() }).Twice()
	scheduler.EXPECT().Finalize(analyzeID, mock.Anything).Run(func(_ int64, fn func()) {
		inAnalyzeFinalize = true
		fn()
		inAnalyzeFinalize = false
	}).Twice()
	scheduler.EXPECT().AbortAndRemoveTask(analyzeID).Return().Maybe()

	dropAttempts := 0
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropAnalyze(analyzeNode, analyzeID).RunAndReturn(func(int64, int64) error {
		if !inAnalyzeFinalize {
			dropOutsideFence = true
		}
		dropAttempts++
		if dropAttempts == 1 {
			return errors.New("worker unavailable")
		}
		return merr.WrapErrNodeNotFound(analyzeNode)
	}).Twice()

	handler := newCompactionInspector(ctx, meta, nil, nil, cluster, scheduler, newMockVersionManager())
	handler.cleaningTasks[planID] = newClusteringCompactionTask(ctx, taskProto, nil, meta, nil, scheduler, newMockVersionManager())

	s.cleanFailedTasksAndWait(handler)
	s.Contains(handler.cleaningTasks, planID)
	s.NotNil(meta.analyzeMeta.GetTask(analyzeID),
		"a failed worker drop must retain the analyze metadata cleanup anchor")
	s.Equal(datapb.CompactionTaskState_failed,
		meta.GetCompactionTasksByTriggerID(ctx, triggerID)[0].GetState())

	s.cleanFailedTasksAndWait(handler)
	handler.stopWg.Wait()
	s.NotContains(handler.cleaningTasks, planID)
	s.Nil(meta.analyzeMeta.GetTask(analyzeID))
	s.Equal(datapb.CompactionTaskState_cleaned,
		meta.GetCompactionTasksByTriggerID(ctx, triggerID)[0].GetState())
	s.False(dropOutsideFence, "DropAnalyze must run under the analyze task scheduler fence")
}

func (s *CompactionPlanHandlerSuite) TestRetryingClusteringCleanupFencesAnalyzeCallback() {
	ctx := context.Background()
	meta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	s.T().Cleanup(meta.snapshotMeta.Close)
	const (
		planID       = int64(1)
		triggerID    = int64(2)
		analyzeID    = int64(3)
		analyzeNode  = int64(11)
		newPlanID    = int64(20)
		newAnalyzeID = int64(21)
	)
	s.Require().NoError(meta.analyzeMeta.AddAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: analyzeID,
		State:  indexpb.JobState_JobStateInit,
	}))
	taskProto := &datapb.CompactionTask{
		PlanID:        planID,
		TriggerID:     triggerID,
		Type:          datapb.CompactionType_ClusteringCompaction,
		State:         datapb.CompactionTaskState_retrying,
		AnalyzeTaskID: analyzeID,
	}
	s.Require().NoError(meta.SaveCompactionTask(ctx, taskProto))

	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocID(mock.Anything).Return(newPlanID, nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(newAnalyzeID, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil).Once()

	inAnalyzeFinalize := false
	cleanupInsideFence := false
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().Finalize(planID, mock.Anything).Run(func(_ int64, fn func()) { fn() }).Once()
	scheduler.EXPECT().Finalize(analyzeID, mock.Anything).Run(func(_ int64, fn func()) {
		// Model an Analyze Create callback that passed its initial existence check
		// before the parent compaction entered retry cleanup. Finalize must drain
		// its assignment before cleanRetry removes the Analyze metadata.
		s.Require().NoError(meta.analyzeMeta.AssignTask(analyzeID, analyzeNode))
		inAnalyzeFinalize = true
		fn()
		inAnalyzeFinalize = false
	}).Once()
	scheduler.EXPECT().AbortAndRemoveTask(analyzeID).Return().Maybe()

	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropAnalyze(analyzeNode, analyzeID).RunAndReturn(func(int64, int64) error {
		cleanupInsideFence = inAnalyzeFinalize
		return nil
	}).Once()

	handler := newCompactionInspector(ctx, meta, alloc, nil, cluster, scheduler, newMockVersionManager())
	handler.cleaningTasks[planID] = newClusteringCompactionTask(
		ctx, taskProto, alloc, meta, nil, scheduler, newMockVersionManager())

	s.cleanFailedTasksAndWait(handler)
	handler.stopWg.Wait()

	s.True(cleanupInsideFence, "retry cleanup must drain Analyze ownership before deleting its metadata")
	s.Nil(meta.analyzeMeta.GetTask(analyzeID))
	tasks := meta.GetCompactionTasksByTriggerID(ctx, triggerID)
	s.Require().Len(tasks, 1)
	s.Equal(newPlanID, tasks[0].GetPlanID())
	s.Equal(newAnalyzeID, tasks[0].GetAnalyzeTaskID())
}

func (s *CompactionPlanHandlerSuite) TestAnalyzeCleanupFinalizesUnassignedTaskBeforeReadingOwner() {
	ctx := context.Background()
	meta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	s.T().Cleanup(meta.snapshotMeta.Close)
	const (
		analyzeID   = int64(3)
		analyzeNode = int64(11)
	)
	s.Require().NoError(meta.analyzeMeta.AddAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: analyzeID,
		State:  indexpb.JobState_JobStateInit,
	}))

	inAnalyzeFinalize := false
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().Finalize(analyzeID, mock.Anything).Run(func(_ int64, fn func()) {
		// Model an Assign/Create callback that was already in flight when cleanup
		// first observed the unassigned task. Finalize drains it before fn runs.
		s.Require().NoError(meta.analyzeMeta.AssignTask(analyzeID, analyzeNode))
		inAnalyzeFinalize = true
		fn()
		inAnalyzeFinalize = false
	}).Once()
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropAnalyze(analyzeNode, analyzeID).Return(nil).Once()

	handler := newCompactionInspector(ctx, meta, nil, nil, cluster, scheduler, newMockVersionManager())
	cleanupInsideFence := false
	s.True(handler.finalizeAnalyzeTaskCleanup(analyzeID, func() bool {
		cleanupInsideFence = inAnalyzeFinalize
		return true
	}))
	s.True(cleanupInsideFence, "Analyze meta cleanup must complete before Finalize releases the task key")
}

func (s *CompactionPlanHandlerSuite) TestProcessCompleteCompaction() {
	s.SetupTest()

	cluster := session.NewMockCluster(s.T())
	s.handler.scheduler.(*task.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		if t.GetTaskState() == taskcommon.InProgress {
			t.QueryTaskOnWorker(cluster)
		}
		if t.(CompactionTask).GetTask().GetState() == datapb.CompactionTaskState_completed {
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

	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
	s.handler.checkCompaction()
}

func (s *CompactionPlanHandlerSuite) TestCheckCompactionCleansNonMixTaskWhenStateRewrittenAfterProcess() {
	// The cleanup decision must not be re-read after Process: a task dropped from
	// executingTasks without entering cleaningTasks is never cleaned, and its
	// input segments stay isCompacting until DataCoord restarts. This holds for
	// every compaction type, not just Mix/Sort.
	planID := int64(1)
	l0Task := &terminalThenRewrittenCompactionTask{CompactionTask: newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        planID,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		State:         datapb.CompactionTaskState_timeout,
		StartTime:     time.Now().Unix(),
		InputSegments: []int64{100},
	}, nil, s.mockMeta)}
	s.handler.executingTasks[planID] = l0Task

	s.handler.checkCompaction()
	s.NotContains(s.handler.executingTasks, planID)
	s.Contains(s.handler.cleaningTasks, planID,
		"a terminal L0 task must still be cleaned after a stale scheduler rewrite")
	s.Equal(datapb.CompactionTaskState_pipelining, l0Task.GetTask().GetState())
}

func (s *CompactionPlanHandlerSuite) TestCheckCompactionDoesNotRecleanFinishedCleanedTask() {
	// clusteringCompactionTask.Process reports true for an already-cleaned task.
	// Widening the cleanup decision must not drag it back into cleaningTasks.
	planID := int64(1)
	cleanedTask := newClusteringCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:    planID,
		Type:      datapb.CompactionType_ClusteringCompaction,
		State:     datapb.CompactionTaskState_cleaned,
		StartTime: time.Now().Unix(),
	}, nil, s.mockMeta, s.mockHandler, nil, newMockVersionManager())
	s.handler.executingTasks[planID] = cleanedTask

	s.handler.checkCompaction()
	s.NotContains(s.handler.executingTasks, planID)
	s.NotContains(s.handler.cleaningTasks, planID, "an already-cleaned task must not be cleaned again")
}

func (s *CompactionPlanHandlerSuite) TestReleaseWorkerResourcesFiresWithoutStart() {
	// releaseWorkerResources spawns its own tracked goroutine; it does not depend on
	// start() -- or on dataCoord.enableCompaction, which only gates admission
	// inside schedule() -- so a plan queued for drop during recovery, before
	// any loop is running, must still fire.
	dropped := make(chan struct{})
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(int64(11), int64(1)).
		RunAndReturn(func(int64, int64) error { close(dropped); return nil }).Once()
	s.handler.cluster = cluster
	s.handler.releaseWorkerResources(&datapb.CompactionTask{PlanID: 1, NodeID: 11})

	select {
	case <-dropped:
	case <-time.After(10 * time.Second):
		s.FailNow("recovered drops must fire even before start() runs")
	}
}

func (s *CompactionPlanHandlerSuite) TestCleanupDropsThePlanOnTheWorker() {
	// Finalize takes the task out of dispatch, so the scheduler's own terminal
	// branch never runs DropTaskOnWorker for it. Cleanup must hand the drop to
	// releaseWorkerResources' own tracked goroutine -- asynchronously, so an
	// unreachable DataNode cannot pin the cleanup slot or the channel
	// exclusion for the RPC's duration.
	planID := int64(1)
	nodeID := int64(11)
	dropStarted := make(chan struct{})
	releaseDrop := make(chan struct{})
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(nodeID, planID).
		RunAndReturn(func(int64, int64) error {
			close(dropStarted)
			<-releaseDrop
			return nil
		}).Once()
	s.handler.cluster = cluster
	s.handler.start()
	defer s.handler.stop()

	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        planID,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_failed,
		NodeID:        nodeID,
		InputSegments: []int64{100},
	}, nil, s.mockMeta, newMockVersionManager())
	s.handler.cleaningTasks[planID] = task

	s.cleanFailedTasksAndWait(s.handler)

	// The drop is stuck mid-RPC, yet every cleanup resource is already free:
	// the exclusion entry is gone and the limiter has no occupied slot. This is
	// the invariant -- an unreachable DataNode must cost the drop loop, not
	// cleanup throughput.
	select {
	case <-dropStarted:
	case <-time.After(10 * time.Second):
		s.FailNow("the worker drop was never sent")
	}
	s.NotContains(s.handler.cleaningTasks, planID,
		"the channel exclusion must not wait for the drop RPC")
	s.handler.cleaningGuard.RLock()
	inFlight := len(s.handler.cleaningInFlight)
	s.handler.cleaningGuard.RUnlock()
	s.Zero(inFlight, "the cleanup slot must not wait for the drop RPC")
	close(releaseDrop)
}

func (s *CompactionPlanHandlerSuite) TestCleanFailedTasksDispatchesCleanupOffTheScheduleLoop() {
	planID := int64(1)
	blocking := &blockingCleanCompactionTask{
		CompactionTask: newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
			PlanID: planID,
			Type:   datapb.CompactionType_SortCompaction,
			State:  datapb.CompactionTaskState_timeout,
			// Spent, so cleanup settles it instead of first building a
			// replacement -- this test is about dispatch, not the rebuild.
			RetryTimes: int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()),
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
		handler.cleaningGuard.RLock()
		defer handler.cleaningGuard.RUnlock()
		return len(handler.cleaningInFlight) == 0
	}, 30*time.Second, 5*time.Millisecond, "cleanup goroutines did not finish")
}

func (s *CompactionPlanHandlerSuite) TestCleanCompaction() {
	s.SetupTest()

	tests := []struct {
		task CompactionTask
	}{
		{
			newMixCompactionTask(context.TODO(),
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
			newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
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

		s.handler.checkCompaction()
		s.Equal(0, len(s.handler.executingTasks))
		s.Equal(1, len(s.handler.cleaningTasks))
		s.cleanFailedTasksAndWait(s.handler)
		s.Equal(0, len(s.handler.cleaningTasks))
	}
}

func (s *CompactionPlanHandlerSuite) TestRecoveredTimeoutSortTaskReleasesLockLast() {
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
	handler := newCompactionInspector(context.Background(), meta, nil, nil, nil, scheduler, newMockVersionManager())
	// timeout owes no rebuild, so cleanup never reaches the replan -- which is
	// what lets this test pass a nil allocator.
	compactionTask := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
				s.True(segment.GetIsInvisible(), "cleanup never publishes an unsorted input")
				s.True(segment.isCompacting, "the compaction lock must be released last")
			default:
				s.Fail("unexpected saved compaction state", saved.GetState().String())
			}
			return nil
		}).Once()
	meta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{segmentID}, false).
		Run(func(ctx context.Context, segmentIDs []int64, compacting bool) {
			events = append(events, "unlock")
			s.Equal(datapb.CompactionTaskState_cleaned, compactionTask.GetTask().GetState())
			realMeta.SetSegmentsCompacting(ctx, segmentIDs, compacting)
		}).Once()

	handler.restoreTask(compactionTask)
	handler.checkCompaction()
	s.NotContains(handler.executingTasks, planID)
	s.Contains(handler.cleaningTasks, planID)

	s.Equal(datapb.CompactionTaskState_timeout, compactionTask.GetTask().GetState())
	s.Contains(handler.cleaningTasks, planID)

	s.cleanFailedTasksAndWait(handler)
	s.NotContains(handler.cleaningTasks, planID)
	s.Equal(datapb.CompactionTaskState_cleaned, compactionTask.GetTask().GetState())
	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.True(segment.GetIsInvisible(), "the input still owes a sort and must not be published")
	s.False(segment.isCompacting)
	s.True(canTriggerSortCompaction(segment), "and it is eligible for a fresh sort plan")
	s.Equal([]string{"save-cleaned", "unlock"}, events,
		"the lock is handed back only after cleaned is durable, so a crash in between retries")
}

func (s *CompactionPlanHandlerSuite) TestCheckCompactionKeepsTerminalCleanupDecisionWhenStateRewritePrecedesProcess() {
	planID := int64(1)
	task := &stateRewritingCompactionTask{CompactionTask: newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:    planID,
		Type:      datapb.CompactionType_SortCompaction,
		State:     datapb.CompactionTaskState_timeout,
		StartTime: time.Now().Unix(),
	}, nil, s.mockMeta, newMockVersionManager())}
	s.handler.executingTasks[planID] = task

	s.handler.checkCompaction()
	s.NotContains(s.handler.executingTasks, planID)
	s.Contains(s.handler.cleaningTasks, planID,
		"the terminal state observed before Process must survive a stale scheduler rewrite")
	s.Equal(datapb.CompactionTaskState_pipelining, task.GetTask().GetState())
}

func (s *CompactionPlanHandlerSuite) TestLoadMetaDefersRecoveredTerminalSortTaskCleanup() {
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
	// timeout owes no rebuild, so cleanup never reaches the replan: this test is
	// about deferred cleanup, and its allocator is nil.
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        planID,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_timeout,
		NodeID:        11,
		InputSegments: []int64{segmentID},
	}))

	scheduler := newOwnershipScheduler(s.T())
	// loadMeta runs on the DataCoord readiness path, so it must not clean here:
	// the metastore writes cleanup makes would block startup behind an
	// unbounded backlog. It queues the task for the same asynchronous cleanup
	// the runtime path uses. loadMeta itself sends no worker drop: that is
	// cleanup's job, once, and only after the task is settled.
	dropped := make(chan struct{}, 1)
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(int64(11), planID).
		RunAndReturn(func(int64, int64) error { dropped <- struct{}{}; return nil }).Once()
	handler := newCompactionInspector(context.Background(), realMeta, nil, nil, cluster, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())

	s.NotContains(handler.executingTasks, planID)
	s.Require().Contains(handler.cleaningTasks, planID, "the recovered terminal task must await cleanup")
	s.Empty(dropped, "readiness must not spend an RPC on the worker drop")
	s.True(realMeta.GetSegment(context.Background(), segmentID).isCompacting,
		"recovery must rebuild the cleanup owner's claim before producers start")
	s.ErrorIs(handler.enqueueCompaction(&datapb.CompactionTask{
		PlanID:        2,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_pipelining,
		InputSegments: []int64{segmentID},
	}), merr.ErrCompactionPlanConflict,
		"a producer must not claim inputs before recovered cleanup releases them")

	// Readiness did none of the work: the task is still terminal-not-cleaned.
	persistedTasks := realMeta.GetCompactionTasks(context.Background())
	s.Require().Len(persistedTasks[0], 1)
	s.Equal(datapb.CompactionTaskState_timeout, persistedTasks[0][0].GetState())

	// The drain settles it, off the startup path.
	handler.cleanFailedTasks()
	s.Eventually(func() bool {
		tasks := realMeta.GetCompactionTasks(context.Background())
		return len(tasks[0]) == 1 && tasks[0][0].GetState() == datapb.CompactionTaskState_cleaned
	}, 10*time.Second, 10*time.Millisecond, "cleanup must run once the inspector is up")
	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.True(segment.GetIsInvisible(), "an unsorted input is never published by cleanup")
	s.False(segment.isCompacting)
	s.True(canTriggerSortCompaction(segment), "it is eligible for a fresh sort plan")

	// Cleanup's worker drop, fired once, after the exclusion was lifted.
	select {
	case <-dropped:
	case <-time.After(10 * time.Second):
		s.FailNow("cleanup never released the worker-side plan entry")
	}
}

func (s *CompactionPlanHandlerSuite) TestLoadMetaRestoresMetaSavedTaskWithDroppedInputs() {
	ctx := context.Background()
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	const (
		collectionID = int64(10)
		segmentID    = int64(100)
		planID       = int64(1)
	)
	s.Require().NoError(realMeta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Dropped,
	})))
	taskProto := &datapb.CompactionTask{
		PlanID:        planID,
		TriggerID:     2,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_meta_saved,
		InputSegments: []int64{segmentID},
		ResultSegments: []int64{
			200,
		},
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 200, End: 201},
	}
	s.Require().NoError(realMeta.SaveCompactionTask(ctx, taskProto))
	s.Require().Error(realMeta.ValidateSegmentStateBeforeCompleteCompactionMutation(taskProto),
		"normal admission must reject the already-retired input")

	scheduler := newOwnershipScheduler(s.T())
	scheduler.EXPECT().Enqueue(mock.MatchedBy(func(t task.Task) bool {
		return t.GetTaskID() == planID && t.GetTaskState() != taskcommon.Init && t.GetTaskState() != taskcommon.InProgress
	})).Once()
	handler := newCompactionInspector(ctx, realMeta, nil, nil, nil, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())

	s.Contains(handler.executingTasks, planID,
		"the persisted post-worker state must continue without re-admitting dropped inputs")
	s.True(realMeta.GetSegment(ctx, segmentID).isCompacting,
		"recovery must reconstruct the task's process-local input claim")
}

// A sort task terminates precisely because a snapshot protects its inputs, so on
// restart that same protection is usually still in place. Admission rejects such
// a task, and dropping it there would leave its input locked as isCompacting --
// which canTriggerSortCompaction requires to be false -- so nothing would ever
// re-sort the segment and it would stay on the growing path for good. Terminal
// tasks must reach cleanup without being admitted.
func (s *CompactionPlanHandlerSuite) TestLoadMetaKeepsSnapshotProtectedTerminalSortTask() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID, planID := int64(10), int64(100), int64(1)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	// The snapshot that rejected the compaction result is still active.
	realMeta.snapshotMeta.segmentProtectionUntil[segmentID] = uint64(time.Now().Unix()) + 3600
	s.Require().Error(realMeta.ValidateSegmentStateBeforeCompleteCompactionMutation(&datapb.CompactionTask{
		CollectionID: collectionID, InputSegments: []int64{segmentID},
	}), "the task must be inadmissible for this test to mean anything")

	// failed owes no rebuild, so cleanup never reaches the replan: this test is
	// about cleanup under snapshot protection, and its allocator is nil.
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        planID,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_failed,
		NodeID:        11,
		InputSegments: []int64{segmentID},
	}))

	scheduler := newOwnershipScheduler(s.T())
	// Exactly one drop: cleanup's. loadMeta sends none.
	dropped := make(chan struct{}, 1)
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(int64(11), planID).
		RunAndReturn(func(int64, int64) error { dropped <- struct{}{}; return nil }).Once()
	handler := newCompactionInspector(context.Background(), realMeta, nil, nil, cluster, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())

	s.Require().Contains(handler.cleaningTasks, planID, "an inadmissible terminal task must still be cleaned")
	persisted := realMeta.GetCompactionTasks(context.Background())
	s.Require().Len(persisted[0], 1, "its metadata is the only evidence the input is invisible; it must survive")

	handler.cleanFailedTasks()
	s.Eventually(func() bool {
		return !realMeta.GetSegment(context.Background(), segmentID).isCompacting
	}, 10*time.Second, 10*time.Millisecond, "cleanup must re-arm the input even under snapshot protection")
	s.True(realMeta.GetSegment(context.Background(), segmentID).GetIsInvisible())

	select {
	case <-dropped:
	case <-time.After(10 * time.Second):
		s.FailNow("cleanup never released the worker-side plan entry")
	}
}

// A trigger keeps its ID across a replan, so an attempt that ended owing a
// rebuild must not make the trigger look settled: the caller polls that ID and
// would be told the compaction finished while cleanup was about to retry it.
// The record itself carries that distinction -- setAttemptEnded writes retrying
// while the cap leaves a rebuild and failed once it does not -- so the summary
// reads it rather than re-deriving it.
func (s *CompactionPlanHandlerSuite) TestSummaryKeepsTriggerRunningWhileReplanIsOwed() {
	owed := summaryCompactionState(context.Background(), 1, []*datapb.CompactionTask{{
		PlanID: 1, TriggerID: 1, State: datapb.CompactionTaskState_retrying,
		RetryTimes: int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()) + 5,
	}})
	s.Equal(commonpb.CompactionState_Executing, owed.state,
		"a rebuild is still owed, so the trigger has not settled")
	s.Equal(1, owed.executingCnt)
	s.Zero(owed.failedCnt)

	settled := summaryCompactionState(context.Background(), 1, []*datapb.CompactionTask{{
		PlanID: 1, TriggerID: 1, State: datapb.CompactionTaskState_failed,
	}})
	s.Equal(commonpb.CompactionState_Completed, settled.state,
		"failed is written only once no rebuild is left, so the trigger is done")
	s.Zero(settled.executingCnt)
	s.Equal(1, settled.failedCnt)

	// A cleaned task carries no claim either way: if the rebuild happened it is
	// a separate pipelining record under the same trigger, and if it did not
	// the trigger really is done.
	cleaned := summaryCompactionState(context.Background(), 1, []*datapb.CompactionTask{{
		PlanID: 1, TriggerID: 1, State: datapb.CompactionTaskState_cleaned,
	}})
	s.Equal(commonpb.CompactionState_Completed, cleaned.state)
}

// A full queue must be refused before anything is written. A record persisted
// and then left out of the queue is driven by nothing: it sits at pipelining --
// which GetCompactionState counts as executing -- until a restart happens to
// resubmit it.
func (s *CompactionPlanHandlerSuite) TestEnqueueRefusesAFullQueueWithoutPersisting() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID, planID, triggerID := int64(10), int64(100), int64(1), int64(19530)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		State:         commonpb.SegmentState_Flushed,
		InsertChannel: "ch-1",
	})))

	Params.Save(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key)

	alloc := allocator.NewMockAllocator(s.T())
	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(context.Background(), realMeta, alloc, nil, nil, scheduler, newMockVersionManager())
	handler.queueTasks.Enqueue(newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID: 999, Type: datapb.CompactionType_MixCompaction,
	}, nil, realMeta, newMockVersionManager()))

	// No allocator expectations: refusal must come before the timestamp is even
	// allocated, let alone before anything is written.
	s.Error(handler.enqueueCompaction(&datapb.CompactionTask{
		PlanID:        planID,
		TriggerID:     triggerID,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_pipelining,
		Channel:       "ch-1",
		InputSegments: []int64{segmentID},
	}))

	s.Empty(realMeta.GetCompactionTasksByTriggerID(context.Background(), triggerID),
		"nothing may be persisted for a task that never reached the queue")
	s.False(realMeta.GetSegment(context.Background(), segmentID).isCompacting,
		"and the inputs admission claimed must be released")
}

// The queue limit is advisory, so concurrent producers that all saw room can
// overshoot it. That is the deliberate trade: the alternative is refusing a task
// after it is durable, which strands work with no runtime owner. This pins the
// half that actually matters -- every task that got persisted is queued.
func (s *CompactionPlanHandlerSuite) TestConcurrentEnqueueOvershootsRatherThanStrandingWork() {
	ctx := context.Background()
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	const (
		collectionID  = int64(10)
		firstSegment  = int64(100)
		secondSegment = int64(101)
	)
	for _, segmentID := range []int64{firstSegment, secondSegment} {
		s.Require().NoError(realMeta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collectionID,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch-1",
		})))
	}

	Params.Save(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key)

	allocationStarted := make(chan struct{})
	releaseAllocation := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseAllocation)
		}
	}()
	// A one-shot token, not a sync.Once: Once would block the second caller
	// until the first returns, which is the opposite of what this test needs.
	parkToken := make(chan struct{}, 1)
	parkToken <- struct{}{}
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocTimestamp(mock.Anything).RunAndReturn(func(context.Context) (uint64, error) {
		// Park only the first producer, so the second runs to completion while
		// the first is still between its capacity check and its store.
		select {
		case <-parkToken:
			close(allocationStarted)
			<-releaseAllocation
		default:
		}
		return 1000, nil
	}).Twice()
	handler := newCompactionInspector(ctx, realMeta, alloc, nil, nil, newOwnershipScheduler(s.T()), newMockVersionManager())

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- handler.enqueueCompaction(&datapb.CompactionTask{
			PlanID: 1, TriggerID: 11, CollectionID: collectionID,
			Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_pipelining,
			Channel: "ch-1", InputSegments: []int64{firstSegment},
		})
	}()
	select {
	case <-allocationStarted:
	case <-time.After(5 * time.Second):
		s.FailNow("the first enqueue never reached timestamp allocation")
	}

	err = handler.enqueueCompaction(&datapb.CompactionTask{
		PlanID: 2, TriggerID: 12, CollectionID: collectionID,
		Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_pipelining,
		Channel: "ch-1", InputSegments: []int64{secondSegment},
	})
	s.Require().NoError(err, "a producer that saw room is not turned away after persisting")
	s.Len(realMeta.GetCompactionTasksByTriggerID(ctx, 12), 1)
	s.True(realMeta.GetSegment(ctx, secondSegment).isCompacting)

	close(releaseAllocation)
	released = true
	s.Require().NoError(<-firstResult)
	s.Len(realMeta.GetCompactionTasksByTriggerID(ctx, 11), 1)
	s.Equal(2, handler.queueTasks.Len(),
		"both durable tasks are queued; the limit of 1 is exceeded on purpose")
	s.NotNil(handler.getCompactionTask(1))
	s.NotNil(handler.getCompactionTask(2))
}

// Unbuildable records do not own process-local inputs or worker work, so their
// deletion must not block DataCoord readiness. The tracked cleanup goroutine
// still attempts the erase and stop waits for that attempt to finish.
func (s *CompactionPlanHandlerSuite) TestLoadMetaDefersUnbuildableRecordCleanup() {
	meta := NewMockCompactionMeta(s.T())
	// An illegal type is unbuildable, so recovery tries to erase the record.
	// A state is required: unknown reads as already cleaned, and recovery skips
	// those without ever trying to erase anything.
	meta.EXPECT().GetCompactionTasks(mock.Anything).Return(map[int64][]*datapb.CompactionTask{
		1: {{
			PlanID: 1, TriggerID: 1,
			Type:  datapb.CompactionType_UndefinedCompaction,
			State: datapb.CompactionTaskState_pipelining,
		}},
	}).Once()
	dropAttempted := make(chan struct{}, 1)
	meta.EXPECT().DropCompactionTask(mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, *datapb.CompactionTask) error {
			dropAttempted <- struct{}{}
			return errors.New("etcd unavailable")
		}).Once()

	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(context.Background(), meta, nil, nil, nil, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta(), "unbuildable record deletion must not block startup")
	select {
	case <-dropAttempted:
	case <-time.After(time.Second):
		s.FailNow("background cleanup did not attempt to erase the unbuildable record")
	}
	handler.stop()
}

// Recovery admits a task -- marking its inputs compacting -- and only then
// tries to queue it. If queueing fails the record is erased, so nothing will
// ever reach cleanup to release those inputs: they must be released right
// there, or canTriggerSortCompaction refuses them for the rest of this
// process's life.
func (s *CompactionPlanHandlerSuite) TestLoadMetaReleasesInputsWhenSubmitFails() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID, planID := int64(10), int64(100), int64(1)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		State:         commonpb.SegmentState_Flushed,
		IsInvisible:   true,
		InsertChannel: "ch-1",
	})))
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:       planID,
		CollectionID: collectionID,
		Type:         datapb.CompactionType_SortCompaction,
		// pipelining with no owner is what sends it down the submitTask path.
		State:         datapb.CompactionTaskState_pipelining,
		NodeID:        NullNodeID,
		Channel:       "ch-1",
		InputSegments: []int64{segmentID},
	}))

	Params.Save(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key)

	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(context.Background(), realMeta, nil, nil, session.NewMockCluster(s.T()), scheduler, newMockVersionManager())
	handler.queueTasks.Enqueue(newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID: 999, Type: datapb.CompactionType_MixCompaction,
	}, nil, realMeta, newMockVersionManager()))
	s.Require().True(handler.isFull(), "the queue is over its limit before recovery starts")

	s.Require().NoError(handler.loadMeta())

	s.Require().NotNil(handler.getCompactionTask(planID),
		"recovery takes the durable task whatever the queue length is")
	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(segment)
	s.True(segment.isCompacting, "the queued task owns its inputs and will release them on cleanup")
}

func (s *CompactionPlanHandlerSuite) TestLoadMetaKeepsDurableReplanWhenRecoveryQueueIsFull() {
	ctx := context.Background()
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	const (
		collectionID = int64(10)
		segmentID    = int64(100)
		planID       = int64(500)
		triggerID    = int64(19530)
	)
	s.Require().NoError(realMeta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: segmentID, CollectionID: collectionID, State: commonpb.SegmentState_Flushed,
	})))
	s.Require().NoError(realMeta.SaveCompactionTask(ctx, &datapb.CompactionTask{
		PlanID: planID, TriggerID: triggerID, CollectionID: collectionID,
		Type: datapb.CompactionType_SortCompaction, State: datapb.CompactionTaskState_pipelining,
		NodeID: NullNodeID, InputSegments: []int64{segmentID}, RetryTimes: 1,
	}))

	Params.Save(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.CompactionTaskQueueCapacity.Key)
	handler := newCompactionInspector(ctx, realMeta, nil, nil, session.NewMockCluster(s.T()),
		newOwnershipScheduler(s.T()), newMockVersionManager())
	handler.queueTasks.Enqueue(newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID: 999, Type: datapb.CompactionType_MixCompaction,
	}, nil, realMeta, newMockVersionManager()))
	s.Require().True(handler.isFull())

	s.Require().NoError(handler.loadMeta())
	tasks := realMeta.GetCompactionTasksByTriggerID(ctx, triggerID)
	s.Require().Len(tasks, 1, "queue pressure must not erase the only durable replan")
	s.Equal(planID, tasks[0].GetPlanID())
	s.Require().NotNil(handler.getCompactionTask(planID),
		"and it is queued now, not left waiting for capacity that may never come")
}

// An executing task can fail admission at recovery -- a snapshot that started
// protecting its inputs is enough. The attempt is then ended exactly as an
// unanswered worker round would: cleanup rebuilds the work under a fresh plan
// ID, and the reconciler admits it once the protection clears. No worker drop
// is sent: the drop is best-effort from cleanup's single call site only, and
// the DataNode executor's TTL sweep is what reclaims an entry no drop ever
// reaches.
func (s *CompactionPlanHandlerSuite) TestLoadMetaEndsInadmissibleExecutingTask() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID, planID := int64(10), int64(100), int64(1)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	realMeta.snapshotMeta.segmentProtectionUntil[segmentID] = uint64(time.Now().Unix()) + 3600

	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID:        planID,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_executing,
		NodeID:        11,
		InputSegments: []int64{segmentID},
	}))

	scheduler := newOwnershipScheduler(s.T())
	// No DropCompaction expectation: mockery fails the test on any RPC, which
	// is the assertion -- recovery spends none.
	cluster := session.NewMockCluster(s.T())
	handler := newCompactionInspector(context.Background(), realMeta, nil, nil, cluster, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())

	persisted := realMeta.GetCompactionTasks(context.Background())
	s.Require().Len(persisted[0], 1, "an inadmissible executing task ends its attempt, it is not dropped")
	s.Equal(datapb.CompactionTaskState_retrying, persisted[0][0].GetState())
	s.Contains(handler.cleaningTasks, planID, "the ended attempt is queued for cleanup")
}

// A cleaned record is inert during recovery, so loadMeta does not send worker
// RPCs. The retention GC owns the eventual handoff: before deleting this
// record it retries DropCompaction until success or NodeNotFound.
func (s *CompactionPlanHandlerSuite) TestLoadMetaSendsNoWorkerDropForCleanedTask() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID: 1,
		Type:   datapb.CompactionType_MixCompaction,
		State:  datapb.CompactionTaskState_cleaned,
		NodeID: 11,
	}))

	scheduler := newOwnershipScheduler(s.T())
	// No DropCompaction expectation: any RPC fails the test.
	cluster := session.NewMockCluster(s.T())
	handler := newCompactionInspector(context.Background(), realMeta, nil, nil, cluster, scheduler, newMockVersionManager())
	s.Require().NoError(handler.loadMeta())

	s.NotContains(handler.executingTasks, int64(1))
	s.NotContains(handler.cleaningTasks, int64(1))
}

func (s *CompactionPlanHandlerSuite) TestCleanClusteringCompaction() {
	s.SetupTest()

	task := newClusteringCompactionTask(context.TODO(),
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
		if t.(CompactionTask).GetTask().GetState() == datapb.CompactionTaskState_completed {
			t.DropTaskOnWorker(cluster)
		}
	})

	task := newClusteringCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:       1,
		TriggerID:    1,
		CollectionID: 1001,
		Channel:      "ch-1",
		Type:         datapb.CompactionType_ClusteringCompaction,
		State:        datapb.CompactionTaskState_executing,
		// Spent, so the failure this test injects settles the task instead of
		// first building a replacement -- the subject here is cleanup, not the
		// rebuild.
		RetryTimes:    int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()),
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
	s.Equal(0, len(task.GetTask().GetResultSegments()))
	s.Equal([]int64{101}, task.GetTask().GetTmpSegments())

	// The cap is spent, so the failure settles the attempt instead of leaving it
	// owing a rebuild -- which is what lets cleanup below run to completion.
	s.Equal(datapb.CompactionTaskState_failed, task.GetTask().GetState())
	s.Equal(0, len(s.handler.executingTasks))
	s.Equal(1, len(s.handler.cleaningTasks))

	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Once()
	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
			newClusteringCompactionTask(context.TODO(), &datapb.CompactionTask{
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
	t1 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, newMockVersionManager())
	handler.checkDelay(t1)
	t2 := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil)
	handler.checkDelay(t2)
	t3 := newClusteringCompactionTask(context.TODO(), &datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, nil, nil, newMockVersionManager())
	handler.checkDelay(t3)
	t4 := newBumpSchemaVersionTask(context.TODO(), &datapb.CompactionTask{
		StartTime: time.Now().Add(-100 * time.Minute).Unix(),
	}, nil, nil, newMockVersionManager())
	handler.checkDelay(t4)
}

func TestGetCompactionTasksNum(t *testing.T) {
	queueTasks := NewCompactionQueue(10, DefaultPrioritizer)
	queueTasks.Enqueue(
		newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 1,
			Type:         datapb.CompactionType_MixCompaction,
		}, nil, nil, newMockVersionManager()),
	)
	queueTasks.Enqueue(
		newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 1,
			Type:         datapb.CompactionType_Level0DeleteCompaction,
		}, nil, nil),
	)
	queueTasks.Enqueue(
		newClusteringCompactionTask(context.TODO(), &datapb.CompactionTask{
			StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
			CollectionID: 10,
			Type:         datapb.CompactionType_ClusteringCompaction,
		}, nil, nil, nil, nil, newMockVersionManager()),
	)
	executingTasks := make(map[int64]CompactionTask, 0)
	executingTasks[1] = newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		StartTime:    time.Now().Add(-100 * time.Minute).Unix(),
		CollectionID: 1,
		Type:         datapb.CompactionType_MixCompaction,
	}, nil, nil, newMockVersionManager())
	executingTasks[2] = newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
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
	s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()

	mockScheduler := newOwnershipScheduler(s.T())
	mockScheduler.EXPECT().Enqueue(mock.Anything).Maybe()
	handler := newCompactionInspector(context.Background(), s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, newMockVersionManager())

	t := &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    10,
		Channel:   "ch-1",
		Type:      datapb.CompactionType_BumpSchemaVersionCompaction,
	}

	compactTask, err := handler.createCompactTask(t)
	s.NoError(err)
	s.NotNil(compactTask)
	s.Equal(datapb.CompactionType_BumpSchemaVersionCompaction, compactTask.GetTask().GetType())
}

func (s *CompactionPlanHandlerSuite) TestCreateCompactTaskRejectsSnapshotProtectedInputs() {
	tests := []struct {
		name  string
		block func(*snapshotMeta)
	}{
		{
			name: "collection snapshot block",
			block: func(snapshotMeta *snapshotMeta) {
				snapshotMeta.SetSnapshotPending(100)
			},
		},
		{
			name: "segment snapshot protection",
			block: func(snapshotMeta *snapshotMeta) {
				snapshotMeta.segmentProtectionUntil[1] = uint64(time.Now().Add(time.Hour).Unix())
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			snapshotMeta := createTestSnapshotMetaLoaded(s.T())
			test.block(snapshotMeta)
			meta := &meta{
				segments:     NewSegmentsInfo(),
				snapshotMeta: snapshotMeta,
			}
			meta.segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				Level:        datapb.SegmentLevel_L1,
			}})
			inspector := newCompactionInspector(context.Background(), meta, nil, nil, nil, nil, newMockVersionManager())

			compactTask, err := inspector.createCompactTask(&datapb.CompactionTask{
				PlanID:        10,
				CollectionID:  100,
				Type:          datapb.CompactionType_MixCompaction,
				InputSegments: []int64{1},
			})

			s.Nil(compactTask)
			s.ErrorIs(err, merr.ErrCompactionBlocked)
			s.False(meta.IsSegmentCompacting(1))
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestCreateCompactTask_UnknownType() {
	s.SetupTest()

	mockScheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(context.Background(), s.mockMeta, s.mockAlloc, nil, nil, mockScheduler, newMockVersionManager())

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

// A retry is a new attempt: it keeps the trigger identity clients poll, but
// receives fresh worker and output identities. Building it is in-memory only;
// the cleanup path performs the one atomic metadata handoff.
func (s *CompactionPlanHandlerSuite) TestBuildReplacementUsesFreshAttemptIdentity() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)

	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(500), nil).Once()
	alloc.EXPECT().AllocN(int64(1000)).Return(int64(2000), int64(3000), nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil).Once()

	handler := newCompactionInspector(context.Background(), realMeta, alloc, nil, nil,
		newOwnershipScheduler(s.T()), newMockVersionManager())
	old := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:                 1,
		TriggerID:              19530,
		CollectionID:           10,
		Channel:                "ch-1",
		Type:                   datapb.CompactionType_SortCompaction,
		State:                  datapb.CompactionTaskState_retrying,
		InputSegments:          []int64{100},
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 1000, End: 2000},
		ResultSegments:         []int64{7},
		TmpSegments:            []int64{8},
		FailReason:             "create outcome unknown",
	}, alloc, realMeta, newMockVersionManager())

	replacement := handler.buildReplacement(old)
	s.Require().NotNil(replacement)
	s.EqualValues(500, replacement.GetPlanID())
	s.Equal(old.GetTask().GetTriggerID(), replacement.GetTriggerID())
	s.Equal(datapb.CompactionTaskState_pipelining, replacement.GetState())
	s.EqualValues(NullNodeID, replacement.GetNodeID())
	s.EqualValues(1, replacement.GetRetryTimes())
	s.Empty(replacement.GetFailReason())
	s.Empty(replacement.GetResultSegments())
	s.Empty(replacement.GetTmpSegments())
	s.EqualValues(2000, replacement.GetPreAllocatedSegmentIDs().GetBegin())
	s.EqualValues(3000, replacement.GetPreAllocatedSegmentIDs().GetEnd())
	s.Empty(realMeta.GetCompactionTasksByTriggerID(context.Background(), old.GetTask().GetTriggerID()),
		"building a replacement must not create a second metadata record")
}

func (s *CompactionPlanHandlerSuite) TestClusteringReplacementGetsFreshAnalyzeIdentity() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)

	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(500), nil).Once()
	alloc.EXPECT().AllocN(int64(1000)).Return(int64(2000), int64(3000), nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(900), nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil).Once()

	handler := newCompactionInspector(context.Background(), realMeta, alloc, nil, nil,
		newOwnershipScheduler(s.T()), newMockVersionManager())
	old := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:                 1,
		TriggerID:              19530,
		Type:                   datapb.CompactionType_ClusteringCompaction,
		State:                  datapb.CompactionTaskState_retrying,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 1000, End: 2000},
		AnalyzeTaskID:          77,
		AnalyzeVersion:         3,
	}, alloc, realMeta, newMockVersionManager())

	replacement := handler.buildReplacement(old)
	s.Require().NotNil(replacement)
	s.EqualValues(900, replacement.GetAnalyzeTaskID())
	s.Zero(replacement.GetAnalyzeVersion())
}

func (s *CompactionPlanHandlerSuite) TestRetryAtomicallyReplacesOldMetadataAndKeepsInputClaim() {
	ctx := context.Background()
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	const (
		collectionID = int64(10)
		segmentID    = int64(100)
		oldPlanID    = int64(1)
		newPlanID    = int64(500)
		triggerID    = int64(19530)
	)
	s.Require().NoError(realMeta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	old := &datapb.CompactionTask{
		PlanID: oldPlanID, TriggerID: triggerID, CollectionID: collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_retrying,
		RetryTimes:    int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()) + 5,
		InputSegments: []int64{segmentID},
	}
	s.Require().NoError(realMeta.SaveCompactionTask(ctx, old))
	realMeta.SetSegmentsCompacting(ctx, old.GetInputSegments(), true)

	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocID(mock.Anything).Return(newPlanID, nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil).Once()
	handler := newCompactionInspector(ctx, realMeta, alloc, nil, nil,
		newOwnershipScheduler(s.T()), newMockVersionManager())
	handler.cleaningTasks[oldPlanID] = newMixCompactionTask(
		ctx, old, alloc, realMeta, newMockVersionManager())

	s.cleanFailedTasksAndWait(handler)

	tasks := realMeta.GetCompactionTasksByTriggerID(ctx, triggerID)
	s.Require().Len(tasks, 1, "catalog and memory must contain exactly one attempt")
	s.Equal(newPlanID, tasks[0].GetPlanID())
	s.Equal(datapb.CompactionTaskState_pipelining, tasks[0].GetState())
	s.NotContains(handler.cleaningTasks, oldPlanID)
	s.Require().NotNil(handler.getCompactionTask(newPlanID))
	s.True(realMeta.GetSegment(ctx, segmentID).isCompacting,
		"the input claim transfers to the new attempt without an admission gap")
}

func (s *CompactionPlanHandlerSuite) TestCleanupIsSkippedWhenTheRebuildCannotBeBuilt() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID, planID, triggerID := int64(10), int64(100), int64(1), int64(19530)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	realMeta.SetSegmentsCompacting(context.Background(), []int64{segmentID}, true)
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID: planID, TriggerID: triggerID, CollectionID: collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_retrying,
		InputSegments: []int64{segmentID},
	}))

	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(0), errors.New("rootcoord unavailable"))
	handler := newCompactionInspector(context.Background(), realMeta, alloc, nil, nil,
		newOwnershipScheduler(s.T()), newMockVersionManager())
	handler.cleaningTasks[planID] = newMixCompactionTask(context.TODO(),
		realMeta.GetCompactionTasksByTriggerID(context.Background(), triggerID)[0],
		alloc, realMeta, newMockVersionManager())

	s.cleanFailedTasksAndWait(handler)

	tasks := realMeta.GetCompactionTasksByTriggerID(context.Background(), triggerID)
	s.Require().Len(tasks, 1)
	s.Equal(planID, tasks[0].GetPlanID())
	s.Equal(datapb.CompactionTaskState_retrying, tasks[0].GetState())
	s.Contains(handler.cleaningTasks, planID)
	s.True(realMeta.GetSegment(context.Background(), segmentID).isCompacting)
}

func (s *CompactionPlanHandlerSuite) TestResumePendingTaskOnBusinessInterval() {
	ctx := context.Background()
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	s.Require().NoError(realMeta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 100, CollectionID: 10, State: commonpb.SegmentState_Flushed,
	})))
	pending := &datapb.CompactionTask{
		PlanID: 500, TriggerID: 19530, CollectionID: 10,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_pipelining,
		InputSegments: []int64{100},
	}
	s.Require().NoError(realMeta.SaveCompactionTask(ctx, pending))

	handler := newCompactionInspector(ctx, realMeta, nil, nil, nil,
		newOwnershipScheduler(s.T()), newMockVersionManager())
	handler.resumePendingTasks()

	s.Require().NotNil(handler.getCompactionTask(pending.GetPlanID()))
	s.True(realMeta.GetSegment(ctx, int64(100)).isCompacting)
}

func (s *CompactionPlanHandlerSuite) TestCleanupDoesNotRebuildASettledFailure() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID, planID, triggerID := int64(10), int64(100), int64(1), int64(19530)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
	})))
	s.Require().NoError(realMeta.SaveCompactionTask(context.Background(), &datapb.CompactionTask{
		PlanID: planID, TriggerID: triggerID, CollectionID: collectionID,
		Type:  datapb.CompactionType_SortCompaction,
		State: datapb.CompactionTaskState_failed,
		// This is what a spent cap looks like once setAttemptEnded has run.
		RetryTimes:    int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt()),
		InputSegments: []int64{segmentID},
	}))

	// No allocator expectations: reaching the allocator at all fails this test.
	alloc := allocator.NewMockAllocator(s.T())
	scheduler := newOwnershipScheduler(s.T())
	handler := newCompactionInspector(context.Background(), realMeta, alloc, nil, nil, scheduler, newMockVersionManager())
	handler.cleaningTasks[planID] = newMixCompactionTask(context.TODO(),
		realMeta.GetCompactionTasksByTriggerID(context.Background(), triggerID)[0],
		alloc, realMeta, newMockVersionManager())

	s.cleanFailedTasksAndWait(handler)

	tasks := realMeta.GetCompactionTasksByTriggerID(context.Background(), triggerID)
	s.Require().Len(tasks, 1, "a settled failure is not rebuilt under a new planID")
	s.Equal(datapb.CompactionTaskState_cleaned, tasks[0].GetState())
	s.False(realMeta.GetSegment(context.Background(), segmentID).isCompacting,
		"cleanup still hands the inputs back")
}
