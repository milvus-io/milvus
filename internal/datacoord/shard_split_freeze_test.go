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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newFreezeTestInspector(t *testing.T) (*compactionInspector, *MockCompactionMeta) {
	paramtable.Init()
	mockMeta := NewMockCompactionMeta(t)
	mockAlloc := allocator.NewMockAllocator(t)
	mockScheduler := task.NewMockGlobalScheduler(t)
	mockScheduler.EXPECT().Enqueue(mock.Anything).Return().Maybe()
	mockAlloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(1000), nil).Maybe()
	return newCompactionInspector(mockMeta, mockAlloc, nil, mockScheduler, mockScheduler, newMockVersionManager()), mockMeta
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
	inspector, mockMeta := newFreezeTestInspector(t)
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
