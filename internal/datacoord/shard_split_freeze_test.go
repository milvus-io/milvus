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
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newFreezeTestInspector(t *testing.T) (*compactionInspector, *MockCompactionMeta) {
	inspector, mockMeta, _ := newFreezeTestInspectorWithScheduler(t)
	return inspector, mockMeta
}

func newFreezeTestInspectorWithScheduler(t *testing.T) (*compactionInspector, *MockCompactionMeta, *task.MockGlobalScheduler) {
	paramtable.Init()
	mockMeta := NewMockCompactionMeta(t)
	mockAlloc := allocator.NewMockAllocator(t)
	mockScheduler := task.NewMockGlobalScheduler(t)
	mockScheduler.EXPECT().Enqueue(mock.Anything).Return().Maybe()
	mockAlloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Maybe()
	return newCompactionInspector(mockMeta, mockAlloc, nil, mockScheduler, mockScheduler, newMockVersionManager()), mockMeta, mockScheduler
}

func TestCompactionFrozenBySplit(t *testing.T) {
	inspector, mockMeta := newFreezeTestInspector(t)
	mix := &datapb.CompactionTask{Channel: splitMgrV0, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{1}}

	// Without a split manager nothing is frozen.
	assert.False(t, inspector.frozenBySplit(mix))

	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == splitMgrV0 })
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_Level0DeleteCompaction,
		datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		assert.True(t, inspector.frozenBySplit(&datapb.CompactionTask{Channel: splitMgrV0, Type: kind}), kind.String())
	}
	assert.False(t, inspector.frozenBySplit(&datapb.CompactionTask{Channel: splitMgrV1, Type: datapb.CompactionType_MixCompaction}))

	// A sort compaction of a flushed segment is frozen like any other; an
	// import's own sort step, over segments the import is still committing,
	// is not: the split's drain waits for that import.
	mockMeta.EXPECT().GetHealthySegment(mock.Anything, int64(1)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1}}).Maybe()
	mockMeta.EXPECT().GetHealthySegment(mock.Anything, int64(2)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, IsImporting: true}}).Maybe()
	mockMeta.EXPECT().GetHealthySegment(mock.Anything, int64(3)).Return(nil).Maybe()
	sort := func(inputs ...int64) *datapb.CompactionTask {
		return &datapb.CompactionTask{Channel: splitMgrV0, Type: datapb.CompactionType_SortCompaction, InputSegments: inputs}
	}
	assert.True(t, inspector.frozenBySplit(sort(1)))
	assert.False(t, inspector.frozenBySplit(sort(2)))
	assert.True(t, inspector.frozenBySplit(sort(2, 3)), "a missing input is not an import's")
	assert.True(t, inspector.frozenBySplit(sort()))
}

// The split's own rewrite runs on the frozen source by construction: it is the
// redistribution, so the freeze and the preemption both spare it.
func TestShardSplitRewriteIsExemptFromTheFreeze(t *testing.T) {
	inspector, mockMeta := newFreezeTestInspector(t)
	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == splitMgrV0 })
	rewrite := &datapb.CompactionTask{
		TriggerID: 1, PlanID: 7, Channel: splitMgrV0, Type: datapb.CompactionType_HashSplitCompaction,
		InputSegments: []int64{700},
	}
	assert.False(t, inspector.frozenBySplit(rewrite))

	mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, []int64{700}).Return(true, true).Once()
	mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	require.NoError(t, inspector.enqueueCompaction(rewrite))
	_, isMix := inspector.getCompactionTask(7).(*mixCompactionTask)
	assert.True(t, isMix, "a rewrite runs the mix task's lifecycle")

	inspector.preemptTasksByChannel(splitMgrV0)
	assert.NotNil(t, inspector.getCompactionTask(7), "the preemption spares the split's own rewrite")
}

func TestEnqueueCompactionRejectedOnSplittingChannel(t *testing.T) {
	inspector, _ := newFreezeTestInspector(t)
	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == splitMgrV0 })

	// Rejected before any task is created, so no input is marked compacting.
	err := inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Channel:   splitMgrV0,
		Type:      datapb.CompactionType_MixCompaction,
	})
	assert.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
	assert.Nil(t, inspector.getCompactionTask(1))
}

func TestPreemptTasksByChannel(t *testing.T) {
	inspector, mockMeta, scheduler := newFreezeTestInspectorWithScheduler(t)
	mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Times(3)
	mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().GetHealthySegment(mock.Anything, int64(300)).Return(
		&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 300, IsImporting: true}}).Maybe()
	require.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1, PlanID: 1, Channel: splitMgrV0, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{100},
	}))
	require.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1, PlanID: 2, Channel: splitMgrV3, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{200},
	}))
	require.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1, PlanID: 3, Channel: splitMgrV0, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{300},
	}))
	// One of the source's tasks is already executing.
	executing := inspector.getCompactionTask(1)
	require.NotNil(t, executing)
	inspector.queueTasks.RemoveAll(func(task CompactionTask) bool { return task.GetTaskProto().GetPlanID() == 1 })
	inspector.executingGuard.Lock()
	inspector.executingTasks[1] = executing
	inspector.executingGuard.Unlock()

	// The preemption cleans the source's task -- the cleaned state is
	// persisted and its inputs are released -- and leaves the other channel's
	// task and the import's own sort step alone.
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Return().Once()
	scheduler.EXPECT().AbortAndRemoveTask(int64(1)).Return().Once()
	inspector.preemptTasksByChannel(splitMgrV0)

	assert.Nil(t, inspector.getCompactionTask(1))
	assert.NotNil(t, inspector.getCompactionTask(2))
	assert.NotNil(t, inspector.getCompactionTask(3))

	// Idempotent: a second preemption finds nothing.
	inspector.preemptTasksByChannel(splitMgrV0)
}

// The freeze, the GC hold and the trigger's exclusion last until Done: an
// Adopting task still holds its source and targets.
func TestIsVChannelSplittingLastsUntilDone(t *testing.T) {
	manager, _ := newSplitTestManager(t, &fakeSplitCoordinator{})
	ctx := context.Background()
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskAdopting)
	require.NoError(t, manager.store.create(ctx, manager.catalog, task))

	for _, vchannel := range []string{splitMgrV0, splitMgrV1, splitMgrV2} {
		assert.True(t, manager.IsVChannelSplitting(vchannel), vchannel)
	}
	assert.False(t, manager.IsVChannelSplitting(splitMgrV3))

	manager.finishTask(task, "")
	for _, vchannel := range []string{splitMgrV0, splitMgrV1, splitMgrV2} {
		assert.False(t, manager.IsVChannelSplitting(vchannel), vchannel)
	}
}

type fakePreempter struct{ channels []string }

func (f *fakePreempter) preemptTasksByChannel(channel string) {
	f.channels = append(f.channels, channel)
}

// A compaction already running on the source is preempted before the write
// switch, and again on every redistribution tick -- the latter is what a
// secondary, whose task starts at Redistributing, preempts with.
func TestShardSplitPreemptsTheSourcesCompactions(t *testing.T) {
	manager, coordinator, _ := newPreparingCase(t)
	preempter := &fakePreempter{}
	manager.setCompactionPreempter(preempter)

	manager.advanceTasks()
	assert.Equal(t, []string{splitMgrV0}, preempter.channels)
	require.Len(t, coordinator.issued, 1)

	require.NoError(t, manager.store.create(context.Background(), manager.catalog, &datapb.SplitShardTask{
		TaskId: 101, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced:  true,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV3, SwitchTimeTick: 10}},
		Targets: []*datapb.SplitShardTaskTarget{{Vchannel: splitMgrV4}, {Vchannel: "by-dev-rootcoord-dml_5_1v5"}},
	}))
	preempter.channels = nil
	manager.advanceTask(mustTask(t, manager, 101))
	assert.Equal(t, []string{splitMgrV3}, preempter.channels)

	// No source, nothing to preempt.
	preempter.channels = nil
	manager.preemptSourceCompactions(&datapb.SplitShardTask{})
	assert.Empty(t, preempter.channels)
}

// A preempted compaction that is already executing on a worker must never
// commit: removed from the inspector alone, the global scheduler would keep
// polling the worker and, once it finished, commit a mix -- or an L0 that
// retires the source's L0s -- on a source past its fence.
func TestPreemptedExecutingCompactionNeverCommits(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.TaskScheduleInterval.Key, "10")
	defer params.Reset(params.DataCoordCfg.TaskScheduleInterval.Key)

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{}).Maybe()
	cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		PlanID:   1,
		State:    datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{{SegmentID: 101}},
	}, nil).Maybe()
	cluster.EXPECT().DropCompaction(mock.Anything, mock.Anything).Return(nil).Maybe()
	scheduler := task.NewGlobalTaskScheduler(context.Background(), cluster)

	mockMeta := NewMockCompactionMeta(t)
	mockAlloc := allocator.NewMockAllocator(t)
	inspector := newCompactionInspector(mockMeta, mockAlloc, nil, scheduler, scheduler, newMockVersionManager())
	committed := atomic.NewBool(false)
	mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *datapb.CompactionTask, *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
			committed.Store(true)
			return nil, nil, merr.WrapErrServiceInternal("must not commit")
		}).Maybe()
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

	executing := newMixCompactionTask(&datapb.CompactionTask{
		PlanID: 1, TriggerID: 1, Channel: splitMgrV0, NodeID: 7,
		Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_executing,
		InputSegments: []int64{100},
	}, mockAlloc, mockMeta, newMockVersionManager())
	inspector.restoreTask(executing)

	inspector.preemptTasksByChannel(splitMgrV0)
	scheduler.Start()
	time.Sleep(200 * time.Millisecond)
	scheduler.Stop()
	assert.False(t, committed.Load(), "a preempted compaction committed on the source")
	assert.Equal(t, datapb.CompactionTaskState_cleaned, executing.GetTaskProto().GetState())
}

// newFrozenScheduleCase enqueues a mix compaction on the source while it is not
// frozen yet and returns a switch that turns the freeze on.
func newFrozenScheduleCase(t *testing.T) (*compactionInspector, *MockCompactionMeta, *task.MockGlobalScheduler, *atomic.Bool) {
	inspector, mockMeta, scheduler := newFreezeTestInspectorWithScheduler(t)
	frozen := atomic.NewBool(false)
	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == splitMgrV0 && frozen.Load() })
	mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Maybe()
	mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	require.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1, PlanID: 1, Channel: splitMgrV0, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{100},
	}))
	return inspector, mockMeta, scheduler, frozen
}

// A compaction queued before the split froze its channel -- one the
// preemption missed because schedule() held it between its Dequeue and its
// re-Enqueue -- is never dispatched: the schedule loop checks the freeze where
// it dispatches, drops the task and releases its inputs.
func TestScheduleNeverDispatchesACompactionOnAFrozenChannel(t *testing.T) {
	inspector, mockMeta, _, frozen := newFrozenScheduleCase(t)
	frozen.Store(true)
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Return().Once()

	assert.Empty(t, inspector.schedule())
	assert.Nil(t, inspector.getCompactionTask(1), "a frozen compaction was dispatched or put back in the queue")
	assert.Zero(t, inspector.queueTasks.Len())
}

// A compaction the schedule loop set aside because its channel was busy is not
// put back in the queue once the channel is frozen: preemptTasksByChannel
// cannot see a task schedule() holds in its excluded list.
func TestScheduleDropsAnExcludedCompactionOnAFrozenChannel(t *testing.T) {
	inspector, mockMeta, _, frozen := newFrozenScheduleCase(t)
	// An L0 already running on the source excludes the mix this round.
	l0 := newL0CompactionTask(&datapb.CompactionTask{
		PlanID: 2, TriggerID: 1, Channel: splitMgrV0, Type: datapb.CompactionType_Level0DeleteCompaction,
		State: datapb.CompactionTaskState_executing,
	}, nil, mockMeta)
	inspector.executingGuard.Lock()
	inspector.executingTasks[2] = l0
	inspector.executingGuard.Unlock()

	assert.Empty(t, inspector.schedule())
	require.Equal(t, 1, inspector.queueTasks.Len(), "the mix waits behind the L0")

	// The freeze lands while the mix waits in the queue; the next round drops it.
	frozen.Store(true)
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Return().Once()
	assert.Empty(t, inspector.schedule())
	assert.Zero(t, inspector.queueTasks.Len(), "a frozen compaction was put back in the queue")
	inspector.executingGuard.RLock()
	defer inspector.executingGuard.RUnlock()
	assert.NotContains(t, inspector.executingTasks, int64(1))
}

// The freeze is checked again where the task is handed to the executing set,
// under the guard preemptTasksByChannel scans that set with: a split frozen
// after the task was dequeued either sees it executing and preempts it, or is
// seen here.
func TestScheduleRechecksTheFreezeWhereItDispatches(t *testing.T) {
	inspector, mockMeta, _, _ := newFrozenScheduleCase(t)
	calls := atomic.NewInt32(0)
	inspector.setChannelSplittingChecker(func(channel string) bool { return calls.Inc() > 1 })
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Return().Once()

	assert.Empty(t, inspector.schedule())
	assert.Nil(t, inspector.getCompactionTask(1), "a compaction frozen after its dequeue was dispatched")
}

// A compaction restored from the catalog after a restart on a channel a split
// freezes is preempted like one found running: aborted on its worker, cleaned,
// never committed.
func TestLoadMetaPreemptsACompactionOnAFrozenChannel(t *testing.T) {
	inspector, mockMeta, scheduler := newFreezeTestInspectorWithScheduler(t)
	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == splitMgrV0 })
	mockMeta.EXPECT().GetCompactionTasks(mock.Anything).Return(map[int64][]*datapb.CompactionTask{
		1: {
			{
				PlanID: 1, TriggerID: 1, Channel: splitMgrV0, NodeID: 7, Type: datapb.CompactionType_MixCompaction,
				State: datapb.CompactionTaskState_executing, InputSegments: []int64{100},
			},
			{
				PlanID: 2, TriggerID: 1, Channel: splitMgrV3, NodeID: 7, Type: datapb.CompactionType_MixCompaction,
				State: datapb.CompactionTaskState_executing, InputSegments: []int64{200},
			},
		},
	})
	mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Times(2)
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Return().Once()
	scheduler.EXPECT().AbortAndRemoveTask(int64(1)).Return().Once()

	inspector.loadMeta()

	assert.Nil(t, inspector.getCompactionTask(1), "a compaction on a frozen channel was revived from the catalog")
	assert.NotNil(t, inspector.getCompactionTask(2))
}

// The freeze predicate runs once per compaction plan and once per dropped
// segment of every GC round, and task records are never removed: it is an
// index lookup, not a walk of every task ever recorded. The index follows the
// task through every write -- the targets allocated, the task finished --
// whichever writer makes it.
func TestIsVChannelSplittingIsAnIndexLookup(t *testing.T) {
	manager, _ := newSplitTestManager(t, &fakeSplitCoordinator{})
	ctx := context.Background()
	for id := int64(1001); id <= 2000; id++ {
		manager.store.cache(&datapb.SplitShardTask{
			TaskId: id, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskDone,
			Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV3}},
			Targets: []*datapb.SplitShardTaskTarget{{Vchannel: splitMgrV4}, {Vchannel: "by-dev-rootcoord-dml_5_1v5"}},
		})
	}
	require.NoError(t, manager.store.create(ctx, manager.catalog, preparingTask()))
	assert.Zero(t, testing.AllocsPerRun(100, func() { manager.IsVChannelSplitting(splitMgrV3) }),
		"the freeze predicate allocates per call")
	assert.False(t, manager.IsVChannelSplitting(splitMgrV3), "a Done split freezes nothing")
	assert.True(t, manager.IsVChannelSplitting(splitMgrV0))
	assert.False(t, manager.IsVChannelSplitting(splitMgrV1), "no target is allocated yet")

	_, err := manager.store.modify(ctx, manager.catalog, 100, func(task *datapb.SplitShardTask) bool {
		task.Targets[0].Vchannel, task.Targets[1].Vchannel = splitMgrV1, splitMgrV2
		return true
	})
	require.NoError(t, err)
	for _, vchannel := range []string{splitMgrV0, splitMgrV1, splitMgrV2} {
		assert.True(t, manager.IsVChannelSplitting(vchannel), vchannel)
	}
	source, target := manager.store.activeSplitRoles(splitMgrV1)
	assert.False(t, source)
	assert.True(t, target)
	source, target = manager.store.activeSplitRoles(splitMgrV0)
	assert.True(t, source)
	assert.False(t, target)

	manager.finishTask(mustTask(t, manager, 100), "")
	for _, vchannel := range []string{splitMgrV0, splitMgrV1, splitMgrV2} {
		assert.False(t, manager.IsVChannelSplitting(vchannel), vchannel)
	}

	// A restart rebuilds it from the catalog.
	store := newShardSplitTasks()
	require.NoError(t, store.load(ctx, manager.catalog))
	source, target = store.activeSplitRoles(splitMgrV0)
	assert.False(t, source || target)
}

// A compaction taken off the queue by the freeze -- dropped at dequeue or
// preempted while queued -- is taken off the Pending gauge under the label it
// was counted under when queued (NullNodeID), whatever node the task names.
func TestFrozenQueuedCompactionsLeaveThePendingGaugeBalanced(t *testing.T) {
	pending := func() float64 {
		return testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.WithLabelValues(
			fmt.Sprintf("%d", NullNodeID), datapb.CompactionType_MixCompaction.String(), metrics.Pending))
	}
	for _, preempted := range []bool{false, true} {
		before := pending()
		inspector, mockMeta, scheduler, frozen := newFrozenScheduleCase(t)
		scheduler.EXPECT().AbortAndRemoveTask(mock.Anything).Return().Maybe()
		require.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
			TriggerID: 1, PlanID: 2, Channel: splitMgrV0, NodeID: 7, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{200},
		}))
		require.Equal(t, before+2, pending())
		mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).Return().Times(2)

		frozen.Store(true)
		if preempted {
			inspector.preemptTasksByChannel(splitMgrV0)
		} else {
			inspector.schedule()
		}
		assert.Equal(t, before, pending(), "preempted=%v", preempted)
	}
}

// A target of a split in flight is told from its source: the freeze exempts
// the sort of a rewrite output on a target only. A vchannel that is both -- a
// target of one split and the source of a later one -- is a source.
func TestIsVChannelSplitTarget(t *testing.T) {
	manager, _ := newSplitTestManager(t, &fakeSplitCoordinator{})
	ctx := context.Background()
	task := fencedTask(datapb.SplitShardTaskState_SplitShardTaskRedistributing)
	require.NoError(t, manager.store.create(ctx, manager.catalog, task))

	assert.False(t, manager.IsVChannelSplitTarget(splitMgrV0), "the source is no target")
	assert.True(t, manager.IsVChannelSplitTarget(splitMgrV1))
	assert.True(t, manager.IsVChannelSplitTarget(splitMgrV2))
	assert.False(t, manager.IsVChannelSplitTarget(splitMgrV3), "a channel no split names")

	cascade := &datapb.SplitShardTask{
		TaskId: 101, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskPreparing,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV1}},
		Targets: []*datapb.SplitShardTaskTarget{{Vchannel: splitMgrV3}, {Vchannel: splitMgrV4}},
	}
	require.NoError(t, manager.store.create(ctx, manager.catalog, cascade))
	assert.False(t, manager.IsVChannelSplitTarget(splitMgrV1), "the source of a later split is a source")
	assert.True(t, manager.IsVChannelSplitTarget(splitMgrV2))

	manager.finishTask(cascade, "")
	manager.finishTask(task, "")
	for _, vchannel := range []string{splitMgrV1, splitMgrV2, splitMgrV3, splitMgrV4} {
		assert.False(t, manager.IsVChannelSplitTarget(vchannel), vchannel)
	}
}

// The sort of a rewrite output on a target runs during the window: the output
// replaces nothing a child delegator holds, since no target WAL ever carried
// its rows. A target-flushed segment stays frozen -- the child consuming the
// target WAL tells the source's copy of it by segment id, and a sort changes
// the id -- and so does any sort on the source.
func TestSortOfARewriteOutputOnATargetIsExemptFromTheFreeze(t *testing.T) {
	inspector, mockMeta := newFreezeTestInspector(t)
	splitting := func(channel string) bool {
		return channel == splitMgrV0 || channel == splitMgrV1 || channel == splitMgrV2
	}
	inspector.setChannelSplittingChecker(splitting)
	inspector.setChannelSplitTargetChecker(func(channel string) bool {
		return channel == splitMgrV1 || channel == splitMgrV2
	})

	segments := map[int64]*SegmentInfo{
		// A rewrite output.
		10: {SegmentInfo: &datapb.SegmentInfo{ID: 10, CreatedByCompaction: true}},
		// Flushed from a target WAL: invisible until sorted.
		11: {SegmentInfo: &datapb.SegmentInfo{ID: 11, IsInvisible: true}},
		// A compaction's invisible staging output (clustering).
		12: {SegmentInfo: &datapb.SegmentInfo{ID: 12, CreatedByCompaction: true, IsInvisible: true}},
		13: {SegmentInfo: &datapb.SegmentInfo{ID: 13, CreatedByCompaction: true}},
	}
	mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, id int64) *SegmentInfo { return segments[id] }).Maybe()
	sortOn := func(channel string, inputs ...int64) *datapb.CompactionTask {
		return &datapb.CompactionTask{Channel: channel, Type: datapb.CompactionType_SortCompaction, InputSegments: inputs}
	}

	assert.False(t, inspector.frozenBySplit(sortOn(splitMgrV1, 10)), "a rewrite output sorts on its target")
	assert.False(t, inspector.frozenBySplit(sortOn(splitMgrV2, 10, 13)))
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV1, 11)), "a target-flushed segment stays frozen")
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV1, 10, 11)))
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV1, 12)), "an invisible staging output is no rewrite output")
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV1, 10, 99)), "a missing input is not a rewrite output")
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV1)))
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV0, 10)), "nothing but the rewrite compacts the source")
	assert.True(t, inspector.frozenBySplit(&datapb.CompactionTask{
		Channel: splitMgrV1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10},
	}), "only a sort is exempt")

	// Without the target predicate wired, nothing on a splitting channel is.
	inspector.setChannelSplitTargetChecker(nil)
	assert.True(t, inspector.frozenBySplit(sortOn(splitMgrV1, 10)))
}

// A rewrite commit that takes the chunked catalog path and is torn between
// chunks leaves an output published while its input is still Flushed; the
// recovery view then serves the input and hides the output. Sorting that output
// would give it a child the view no longer ties to the input, and the rows of
// that half would be served twice. So the exemption holds only once every
// parent of every input is Dropped (or already gone from meta).
func TestSortOfATornRewriteOutputStaysFrozen(t *testing.T) {
	inspector, mockMeta := newFreezeTestInspector(t)
	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == splitMgrV0 || channel == splitMgrV1 })
	inspector.setChannelSplitTargetChecker(func(channel string) bool { return channel == splitMgrV1 })

	const (
		liveInput    = int64(100)
		droppedInput = int64(101)
		goneInput    = int64(102)
	)
	parents := map[int64]*SegmentInfo{
		liveInput:    {SegmentInfo: &datapb.SegmentInfo{ID: liveInput, State: commonpb.SegmentState_Flushed}},
		droppedInput: {SegmentInfo: &datapb.SegmentInfo{ID: droppedInput, State: commonpb.SegmentState_Dropped}},
	}
	outputs := map[int64]*SegmentInfo{
		// Published by a torn commit: its input is still Flushed.
		20: {SegmentInfo: &datapb.SegmentInfo{ID: 20, CreatedByCompaction: true, CompactionFrom: []int64{liveInput}}},
		// Published by a whole commit: its input is Dropped.
		21: {SegmentInfo: &datapb.SegmentInfo{ID: 21, CreatedByCompaction: true, CompactionFrom: []int64{droppedInput}}},
		// Its input is already garbage-collected out of meta.
		22: {SegmentInfo: &datapb.SegmentInfo{ID: 22, CreatedByCompaction: true, CompactionFrom: []int64{goneInput}}},
	}
	mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, id int64) *SegmentInfo { return outputs[id] }).Maybe()
	mockMeta.EXPECT().GetSegment(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, id int64) *SegmentInfo { return parents[id] }).Maybe()
	sortOn := func(inputs ...int64) *datapb.CompactionTask {
		return &datapb.CompactionTask{Channel: splitMgrV1, Type: datapb.CompactionType_SortCompaction, InputSegments: inputs}
	}

	assert.True(t, inspector.frozenBySplit(sortOn(20)), "a torn commit's output stays frozen while its input is live")
	assert.True(t, inspector.frozenBySplit(sortOn(21, 20)), "one torn input freezes the whole sort")
	assert.False(t, inspector.frozenBySplit(sortOn(21)), "a whole commit's output sorts")
	assert.False(t, inspector.frozenBySplit(sortOn(22)), "a parent gone from meta was dropped")

	// Once the re-run commit drops the input, the output sorts.
	parents[liveInput] = &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: liveInput, State: commonpb.SegmentState_Dropped}}
	assert.False(t, inspector.frozenBySplit(sortOn(20)))
}
