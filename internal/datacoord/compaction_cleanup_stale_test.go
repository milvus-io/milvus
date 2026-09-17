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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetTask is the first call after cleanFailedTasks releases its snapshot lock.
// Pause only that call in the second round, leaving cleanup itself unchanged.
type compactionCleanupSnapshotTask struct {
	CompactionTask
	pauseSnapshot atomic.Bool
	finishCleanup func()
}

func (t *compactionCleanupSnapshotTask) GetTask() *datapb.CompactionTask {
	if t.pauseSnapshot.CompareAndSwap(true, false) {
		t.finishCleanup()
	}
	return t.CompactionTask.GetTask()
}

func TestCompactionCleanupRejectsStaleSnapshot(t *testing.T) {
	ctx := context.Background()
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	t.Cleanup(mt.snapshotMeta.Close)
	const segmentID = int64(100)
	old := &datapb.CompactionTask{
		PlanID: 1, TriggerID: 10, CollectionID: 20,
		Type: datapb.CompactionType_SortCompaction, State: datapb.CompactionTaskState_retrying,
		InputSegments: []int64{segmentID},
	}
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: segmentID, CollectionID: old.GetCollectionID(), State: commonpb.SegmentState_Flushed,
	})))
	require.NoError(t, mt.SaveCompactionTask(ctx, old))
	mt.SetSegmentsCompacting(ctx, old.GetInputSegments(), true)

	var allocations atomic.Int64
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(context.Context) (int64, error) {
		return 500 + allocations.Add(1), nil
	})
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil)
	started, release := make(chan struct{}), make(chan struct{})
	var finalizeCalls atomic.Int64
	scheduler := task.NewMockGlobalScheduler(t)
	scheduler.EXPECT().Finalize(old.GetPlanID(), mock.Anything).Run(func(_ int64, fn func()) {
		if finalizeCalls.Add(1) == 1 {
			close(started)
			<-release
		}
		fn()
	})
	inspector := newCompactionInspector(ctx, mt, alloc, nil, nil, scheduler, newMockVersionManager())
	var releaseOnce sync.Once
	finishCleanup := func() {
		releaseOnce.Do(func() { close(release) })
		inspector.stopWg.Wait()
	}
	defer finishCleanup()
	wrapper := &compactionCleanupSnapshotTask{
		CompactionTask: newMixCompactionTask(ctx, old, alloc, mt, newMockVersionManager()),
		finishCleanup:  finishCleanup,
	}
	inspector.cleaningTasks[old.GetPlanID()] = wrapper
	inspector.cleanFailedTasks()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("first cleanup did not start")
	}

	// The second round has snapshotted the predecessor before the first cleanup
	// replaces it and removes both cleaning entries. Resume dispatch afterwards.
	wrapper.pauseSnapshot.Store(true)
	inspector.cleanFailedTasks()
	inspector.stopWg.Wait()

	assert.EqualValues(t, 1, finalizeCalls.Load(), "a retired cleanup must not run again")
	assert.EqualValues(t, 1, allocations.Load(), "a retired predecessor must not get a second replacement")
	assert.Equal(t, 1, inspector.queueTasks.Len())
	tasks := mt.GetCompactionTasksByTriggerID(ctx, old.GetTriggerID())
	require.Len(t, tasks, 1)
	assert.EqualValues(t, 501, tasks[0].GetPlanID())
	assert.True(t, mt.GetSegment(ctx, segmentID).isCompacting, "the replacement retains the input claim")
}

func TestCompactionCleanupDoesNotSubmitRejectedReplacement(t *testing.T) {
	ctx := context.Background()
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	t.Cleanup(mt.snapshotMeta.Close)
	old := &datapb.CompactionTask{
		PlanID: 1, TriggerID: 10,
		Type: datapb.CompactionType_SortCompaction, State: datapb.CompactionTaskState_retrying,
	}
	require.NoError(t, mt.SaveCompactionTask(ctx, old))
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(500), nil).Once()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(10000), nil).Once()
	scheduler := task.NewMockGlobalScheduler(t)
	scheduler.EXPECT().Finalize(old.GetPlanID(), mock.Anything).Run(func(_ int64, fn func()) {
		// Change the authoritative record while the wrapper still has retrying.
		retired := proto.Clone(old).(*datapb.CompactionTask)
		retired.State = datapb.CompactionTaskState_failed
		require.NoError(t, mt.SaveCompactionTask(ctx, retired))
		fn()
	}).Once()
	inspector := newCompactionInspector(ctx, mt, alloc, nil, nil, scheduler, newMockVersionManager())
	inspector.cleaningTasks[old.GetPlanID()] = newMixCompactionTask(ctx, old, alloc, mt, newMockVersionManager())
	inspector.cleanFailedTasks()
	inspector.stopWg.Wait()

	assert.Zero(t, inspector.queueTasks.Len(), "rejected catalog replacement must not be submitted")
	tasks := mt.GetCompactionTasksByTriggerID(ctx, old.GetTriggerID())
	require.Len(t, tasks, 1)
	assert.Equal(t, old.GetPlanID(), tasks[0].GetPlanID())
	assert.Equal(t, datapb.CompactionTaskState_failed, tasks[0].GetState())
}

func TestCompactionReplacementRejectsRetiredPredecessor(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		mt := newTestCompactionTaskMeta(t)
		catalog := mt.catalog.(*mocks.DataCoordCatalog)
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		err := mt.ReplaceCompactionTask(context.Background(),
			&datapb.CompactionTask{PlanID: 1, TriggerID: 10, State: datapb.CompactionTaskState_retrying},
			&datapb.CompactionTask{PlanID: 2, TriggerID: 10, State: datapb.CompactionTaskState_pipelining})
		assert.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
		catalog.AssertNotCalled(t, "Update", mock.Anything, mock.Anything, mock.Anything)
		assert.Empty(t, mt.GetCompactionTasksByTriggerID(10))
	})
	for _, state := range []datapb.CompactionTaskState{
		datapb.CompactionTaskState_pipelining,
		datapb.CompactionTaskState_executing,
		datapb.CompactionTaskState_completed,
		datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_timeout,
		datapb.CompactionTaskState_cleaned,
	} {
		t.Run(state.String(), func(t *testing.T) {
			mt := newTestCompactionTaskMeta(t)
			old := &datapb.CompactionTask{PlanID: 1, TriggerID: 10, State: state}
			require.NoError(t, mt.SaveCompactionTask(context.Background(), old))
			catalog := mt.catalog.(*mocks.DataCoordCatalog)
			catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			stale := proto.Clone(old).(*datapb.CompactionTask)
			stale.State = datapb.CompactionTaskState_retrying
			err := mt.ReplaceCompactionTask(context.Background(), stale,
				&datapb.CompactionTask{PlanID: 2, TriggerID: 10, State: datapb.CompactionTaskState_pipelining})
			assert.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
			catalog.AssertNotCalled(t, "Update", mock.Anything, mock.Anything, mock.Anything)
			tasks := mt.GetCompactionTasksByTriggerID(old.GetTriggerID())
			require.Len(t, tasks, 1)
			assert.True(t, proto.Equal(old, tasks[0]))
		})
	}
	t.Run("already replaced", func(t *testing.T) {
		mt := newTestCompactionTaskMeta(t)
		old := &datapb.CompactionTask{PlanID: 1, TriggerID: 10, State: datapb.CompactionTaskState_retrying}
		require.NoError(t, mt.SaveCompactionTask(context.Background(), old))
		catalog := mt.catalog.(*mocks.DataCoordCatalog)
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		next := &datapb.CompactionTask{PlanID: 2, TriggerID: 10, State: datapb.CompactionTaskState_pipelining}
		require.NoError(t, mt.ReplaceCompactionTask(context.Background(), old, next))
		err := mt.ReplaceCompactionTask(context.Background(), old,
			&datapb.CompactionTask{PlanID: 3, TriggerID: 10, State: datapb.CompactionTaskState_pipelining})
		assert.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
		catalog.AssertNumberOfCalls(t, "Update", 1)
		tasks := mt.GetCompactionTasksByTriggerID(old.GetTriggerID())
		require.Len(t, tasks, 1)
		assert.True(t, proto.Equal(next, tasks[0]))
	})
}

func TestCompactionReplacementWriteFailureKeepsPredecessor(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "live process", true: "shutdown"}[shutdown], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mt := newTestCompactionTaskMeta(t)
			old := &datapb.CompactionTask{PlanID: 1, TriggerID: 10, State: datapb.CompactionTaskState_retrying}
			require.NoError(t, mt.SaveCompactionTask(ctx, old))
			writeErr := merr.WrapErrServiceUnavailable("catalog response lost")
			catalog := mt.catalog.(*mocks.DataCoordCatalog)
			catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(writeErr).Once()
			fatalCalls := 0
			fatal := mockey.Mock(mlog.Fatal).To(func(context.Context, string, ...mlog.Field) { fatalCalls++ }).Build()
			defer fatal.UnPatch()
			if shutdown {
				cancel()
			}
			err := mt.ReplaceCompactionTask(ctx, old,
				&datapb.CompactionTask{PlanID: 2, TriggerID: 10, State: datapb.CompactionTaskState_pipelining})
			assert.ErrorIs(t, err, writeErr)
			expectedFatalCalls := 1
			if shutdown {
				expectedFatalCalls = 0
			}
			assert.Equal(t, expectedFatalCalls, fatalCalls, "only a live process must fail-stop on an ambiguous write")
			tasks := mt.GetCompactionTasksByTriggerID(old.GetTriggerID())
			require.Len(t, tasks, 1)
			assert.True(t, proto.Equal(old, tasks[0]), "memory must not publish the unconfirmed replacement")
		})
	}
}
