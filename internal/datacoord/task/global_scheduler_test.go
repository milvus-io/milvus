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

package task

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	paramtable.Init()
}

func TestGlobalScheduler_Enqueue(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1)
	task.EXPECT().GetTaskState().Return(taskcommon.Init)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 1, len(scheduler.(*globalTaskScheduler).pendingTasks.TaskIDs()))
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Compaction))
	scheduler.Enqueue(task)
	assert.Equal(t, 1, len(scheduler.(*globalTaskScheduler).pendingTasks.TaskIDs()))
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Compaction))

	task = NewMockTask(t)
	task.EXPECT().GetTaskID().Return(2)
	task.EXPECT().GetTaskState().Return(taskcommon.InProgress)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).runningTasks.Len())
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Compaction))
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).runningTasks.Len())
}

func TestGlobalScheduler_GetPendingTaskCountIsScopedByTaskType(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)

	enqueue := func(taskID int64, taskType taskcommon.Type) {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(taskID)
		task.EXPECT().GetTaskState().Return(taskcommon.Init)
		task.EXPECT().GetTaskType().Return(taskType)
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
		scheduler.Enqueue(task)
	}

	enqueue(1, taskcommon.Stats)
	enqueue(2, taskcommon.Compaction)
	enqueue(3, taskcommon.Index)
	enqueue(4, taskcommon.Compaction)

	// An index/compaction backlog must not consume the stats admission budget.
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Stats))
	assert.Equal(t, 2, scheduler.GetPendingTaskCount(taskcommon.Compaction))
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Index))
}

func TestGlobalScheduler_GetPendingTaskCountIncludesBackoff(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "60")
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffInterval.Key)

	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)
	globalScheduler := scheduler.(*globalTaskScheduler)

	tasks := make(map[int64]Task)
	for taskID := int64(1); taskID <= 2; taskID++ {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(taskID)
		task.EXPECT().GetTaskState().Return(taskcommon.Init)
		task.EXPECT().GetTaskType().Return(taskcommon.Stats)
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
		scheduler.Enqueue(task)
		tasks[taskID] = task
	}

	// A task waiting on its retry backoff still occupies queue depth: excluding it
	// would let a worker-side failure storm silently disable the admission gate.
	globalScheduler.recordTaskFailure(tasks[2])
	assert.Equal(t, 2, scheduler.GetPendingTaskCount(taskcommon.Stats))
}

func TestGlobalScheduler_AbortAndRemoveTask(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1)
	task.EXPECT().GetTaskState().Return(taskcommon.Init)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 1, len(scheduler.(*globalTaskScheduler).pendingTasks.TaskIDs()))
	scheduler.AbortAndRemoveTask(1)
	assert.Equal(t, 0, len(scheduler.(*globalTaskScheduler).pendingTasks.TaskIDs()))

	task = NewMockTask(t)
	task.EXPECT().GetTaskID().Return(2)
	task.EXPECT().GetTaskState().Return(taskcommon.InProgress)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).runningTasks.Len())
	scheduler.AbortAndRemoveTask(2)
	assert.Equal(t, 0, scheduler.(*globalTaskScheduler).runningTasks.Len())
}

func TestGlobalScheduler_pickNode(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)

	// Tie: either node may be returned, but the most-available is always picked.
	tie := newNodeSlotHeap(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 30},
		2: {NodeID: 2, AvailableSlots: 30},
	})
	nodeID := scheduler.pickNode(tie, 1)
	assert.True(t, nodeID == int64(1) || nodeID == int64(2))

	// Least-loaded selection: node 2 has more available slots, so it wins even
	// though node 1 also fits and might be iterated first in the map.
	leastLoaded := newNodeSlotHeap(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 20},
		2: {NodeID: 2, AvailableSlots: 80},
	})
	assert.Equal(t, int64(2), scheduler.pickNode(leastLoaded, 10))

	// Route by the QuerySlot map key instead of relying on WorkerSlots.NodeID.
	keyOnly := map[int64]*session.WorkerSlots{
		10: {AvailableSlots: 20},
		20: {AvailableSlots: 80},
	}
	assert.Equal(t, int64(20), scheduler.pickNode(newNodeSlotHeap(keyOnly), 10))
	assert.Equal(t, int64(70), keyOnly[20].AvailableSlots)

	// Fallback: no node can fully satisfy the request, pick the most-available
	// node and drain its slots to 0.
	noEnough := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 20},
		2: {NodeID: 2, AvailableSlots: 30},
	}
	noEnoughHeap := newNodeSlotHeap(noEnough)
	assert.Equal(t, int64(2), scheduler.pickNode(noEnoughHeap, 100))
	assert.Equal(t, int64(0), noEnough[2].AvailableSlots)

	// Single node: slots decrement across successive picks, then fall back.
	single := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
	}
	singleHeap := newNodeSlotHeap(single)
	assert.Equal(t, int64(1), scheduler.pickNode(singleHeap, 10))
	assert.Equal(t, int64(90), single[1].AvailableSlots)
	assert.Equal(t, int64(1), scheduler.pickNode(singleHeap, 10))
	assert.Equal(t, int64(80), single[1].AvailableSlots)
	assert.Equal(t, int64(1), scheduler.pickNode(singleHeap, 100)) // 80 < 100, fallback
	assert.Equal(t, int64(0), single[1].AvailableSlots)

	// No available slots at all.
	empty := newNodeSlotHeap(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 0},
		2: {NodeID: 2, AvailableSlots: 0},
	})
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(empty, 1))

	// Zero-slot cleanup work should still be dispatched even when every node is
	// exhausted, and it should not consume any slots.
	zeroSlot := map[int64]*session.WorkerSlots{
		10: {AvailableSlots: 0},
		20: {AvailableSlots: 0},
	}
	zeroSlotHeap := newNodeSlotHeap(zeroSlot)
	nodeID = scheduler.pickNode(zeroSlotHeap, 0)
	assert.True(t, nodeID == int64(10) || nodeID == int64(20))
	assert.Equal(t, int64(0), zeroSlot[10].AvailableSlots)
	assert.Equal(t, int64(0), zeroSlot[20].AvailableSlots)
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(zeroSlotHeap, 1))

	// Empty cluster.
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(newNodeSlotHeap(nil), 1))
}

// TestGlobalScheduler_pickNode_Balancing verifies that successive picks spread
// tasks evenly across nodes (water-filling) instead of packing one node first.
func TestGlobalScheduler_pickNode_Balancing(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)

	nodes := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
		2: {NodeID: 2, AvailableSlots: 100},
		3: {NodeID: 3, AvailableSlots: 100},
	}
	slotHeap := newNodeSlotHeap(nodes)

	assigned := map[int64]int{}
	// Each task needs 10 slots; 30 tasks should be spread 10 per node.
	for i := 0; i < 30; i++ {
		nodeID := scheduler.pickNode(slotHeap, 10)
		assert.NotEqual(t, int64(NullNodeID), nodeID)
		assigned[nodeID]++
	}

	for nodeID, ws := range nodes {
		assert.Equal(t, 10, assigned[nodeID], "node %d should receive an even share", nodeID)
		assert.Equal(t, int64(0), ws.AvailableSlots, "node %d should be fully drained", nodeID)
	}

	// All nodes are now empty: further picks return NullNodeID.
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(slotHeap, 1))
}

func TestGlobalScheduler_pickNode_MixedTaskSizes(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)

	nodes := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
		2: {NodeID: 2, AvailableSlots: 80},
		3: {NodeID: 3, AvailableSlots: 60},
	}
	slotHeap := newNodeSlotHeap(nodes)

	assert.Equal(t, int64(1), scheduler.pickNode(slotHeap, 30))
	assert.Equal(t, int64(2), scheduler.pickNode(slotHeap, 70))
	assert.Equal(t, int64(1), scheduler.pickNode(slotHeap, 50))
	assert.Equal(t, int64(3), scheduler.pickNode(slotHeap, 90))

	assert.Equal(t, int64(20), nodes[1].AvailableSlots)
	assert.Equal(t, int64(10), nodes[2].AvailableSlots)
	assert.Equal(t, int64(0), nodes[3].AvailableSlots)
}

func TestGlobalScheduler_TestSchedule(t *testing.T) {
	newCluster := func() session.Cluster {
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
			1: {
				NodeID:         1,
				AvailableSlots: 100,
			},
			2: {
				NodeID:         2,
				AvailableSlots: 100,
			},
		}).Maybe()
		return cluster
	}

	newTask := func() *MockTask {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(1).Maybe()
		task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
		task.EXPECT().GetTaskSlot().Return(1).Maybe()
		return task
	}

	t.Run("task retry when CreateTaskOnWorker", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), newCluster())
		scheduler.Start()
		defer scheduler.Stop()

		task := newTask()
		var stateCounter atomic.Int32

		// Set initial state
		task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
			counter := stateCounter.Load()
			if counter == 0 {
				return taskcommon.Init
			}
			return taskcommon.Retry
		}).Maybe()

		task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			stateCounter.Store(1) // Mark that CreateTaskOnWorker was called
		}).Maybe()

		scheduler.Enqueue(task)
		assert.Eventually(t, func() bool {
			s := scheduler.(*globalTaskScheduler)
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			return task.GetTaskState() == taskcommon.Retry &&
				s.runningTasks.Len() == 0 && len(s.pendingTasks.TaskIDs()) == 1
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("task retry when QueryTaskOnWorker", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), newCluster())
		scheduler.Start()
		defer scheduler.Stop()

		task := newTask()
		var stateCounter atomic.Int32

		task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
			counter := stateCounter.Load()
			switch counter {
			case 0:
				return taskcommon.Init
			case 1:
				return taskcommon.InProgress
			default:
				return taskcommon.Retry
			}
		}).Maybe()

		task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			stateCounter.Store(1) // CreateTaskOnWorker called
		}).Maybe()

		task.EXPECT().QueryTaskOnWorker(mock.Anything).Run(func(cluster session.Cluster) {
			stateCounter.Store(2) // QueryTaskOnWorker called
		}).Maybe()

		scheduler.Enqueue(task)
		assert.Eventually(t, func() bool {
			s := scheduler.(*globalTaskScheduler)
			s.mu.RLock(1)
			defer s.mu.RUnlock(1)
			return stateCounter.Load() >= 2 && s.runningTasks.Len() == 0
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("zero slot task dispatched when nodes exhausted", func(t *testing.T) {
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
			10: {AvailableSlots: 0},
			20: {AvailableSlots: 0},
		}).Once()

		scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(1).Maybe()
		task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
		task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
		task.EXPECT().GetTaskSlot().Return(int64(0)).Once()

		var dispatched atomic.Bool
		task.EXPECT().CreateTaskOnWorker(mock.MatchedBy(func(nodeID int64) bool {
			return nodeID == 10 || nodeID == 20
		}), mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			dispatched.Store(true)
		}).Once()

		scheduler.Enqueue(task)
		scheduler.schedule()

		assert.True(t, dispatched.Load())
	})

	t.Run("normal case", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), newCluster())
		scheduler.Start()
		defer scheduler.Stop()

		task := newTask()
		var stateCounter atomic.Int32

		task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
			counter := stateCounter.Load()
			switch counter {
			case 0:
				return taskcommon.Init
			case 1:
				return taskcommon.InProgress
			default:
				return taskcommon.Finished
			}
		}).Maybe()

		task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			stateCounter.Store(1) // CreateTaskOnWorker called
		}).Maybe()

		task.EXPECT().QueryTaskOnWorker(mock.Anything).Run(func(cluster session.Cluster) {
			stateCounter.Store(2) // QueryTaskOnWorker called
		}).Maybe()

		task.EXPECT().DropTaskOnWorker(mock.Anything).Run(func(cluster session.Cluster) {
			stateCounter.Store(3) // DropTaskOnWorker called
		}).Maybe()

		scheduler.Enqueue(task)
		assert.Eventually(t, func() bool {
			s := scheduler.(*globalTaskScheduler)
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			return task.GetTaskState() == taskcommon.Finished &&
				s.runningTasks.Len() == 0 && len(s.pendingTasks.TaskIDs()) == 0
		}, 10*time.Second, 10*time.Millisecond)
	})
}

func TestGlobalScheduler_RecordTaskFailureBackoff(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "1")
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffMaxInterval.Key, "4")
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffInterval.Key)
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffMaxInterval.Key)

	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)
	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(7).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Index).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()

	// exponential: 1s, 2s, 4s, then capped at the 4s max
	start := time.Now()
	scheduler.recordTaskFailure(task)
	bo, ok := scheduler.backoffs.Get(7)
	assert.True(t, ok)
	assert.Equal(t, 1, bo.failures)
	assert.InDelta(t, 1.0, bo.notBefore.Sub(start).Seconds(), 0.5)
	assert.True(t, scheduler.taskInBackoff(task))

	scheduler.recordTaskFailure(task)
	scheduler.recordTaskFailure(task)
	scheduler.recordTaskFailure(task)
	bo, _ = scheduler.backoffs.Get(7)
	assert.Equal(t, 4, bo.failures)
	assert.InDelta(t, 4.0, time.Until(bo.notBefore).Seconds(), 0.5)

	// clearing the entry ends the backoff
	scheduler.backoffs.Remove(7)
	assert.False(t, scheduler.taskInBackoff(task))

	// interval 0 disables the mechanism entirely
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "0")
	scheduler.recordTaskFailure(task)
	assert.False(t, scheduler.taskInBackoff(task))
}

func TestGlobalScheduler_FailedTaskBacksOffBeforeRedispatch(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "1")
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffInterval.Key)

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
	}).Maybe()

	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)
	scheduler.Start()
	defer scheduler.Stop()

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Index).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	task.EXPECT().GetTaskSlot().Return(1).Maybe()
	// CreateTaskOnWorker never flips the state away from Init: every dispatch fails
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
	var createCalls atomic.Int32
	task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
		createCalls.Add(1)
	}).Maybe()

	scheduler.Enqueue(task)

	// the first dispatch happens promptly
	assert.Eventually(t, func() bool { return createCalls.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	// during the 1s backoff the ~100ms scheduling tick must NOT re-dispatch
	// (without backoff this would already be ~5 more dispatches)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), createCalls.Load())
	// after the backoff elapses it is dispatched again
	assert.Eventually(t, func() bool { return createCalls.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
}

// TestGlobalScheduler_TerminalTaskClearsBackoff guards against a backoff-entry
// leak: when CreateTaskOnWorker drives a task straight to a terminal state it
// never enters runningTasks, so check()'s cleanup never runs. schedule() must
// drop the backoff entry itself, otherwise it lives until datacoord restarts.
func TestGlobalScheduler_TerminalTaskClearsBackoff(t *testing.T) {
	cluster := &exportSchedulingCluster{}
	defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(map[int64]*session.WorkerSlots{1: {AvailableSlots: 100}}).Build().UnPatch()
	pending := &exportSchedulingTask{}
	defer mockey.Mock((*exportSchedulingTask).GetTaskID).Return(int64(9)).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.Index).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskSlot).Return(int64(1)).Build().UnPatch()
	state := taskcommon.Init
	defer mockey.Mock((*exportSchedulingTask).GetTaskState).To(func() taskcommon.State { return state }).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).CreateTaskOnWorker).To(func(int64, session.Cluster) { state = taskcommon.None }).Build().UnPatch()
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	scheduler.Enqueue(pending)
	scheduler.backoffs.Insert(9, &taskBackoff{failures: 3, notBefore: time.Now().Add(-time.Second)})
	scheduler.schedule()
	assert.Zero(t, scheduler.backoffs.Len())
	assert.Zero(t, scheduler.tasks.Len())
	assert.Zero(t, scheduler.runningTasks.Len())
	assert.Zero(t, scheduler.pendingTasks.TaskCount())
}

type exportSchedulingTask struct{ Task }

type exportSchedulingCluster struct{ session.Cluster }

func TestGlobalScheduler_ExportUsesAvailableSlots(t *testing.T) {
	cluster := &exportSchedulingCluster{}
	// Shared-pool managers report capacity without Export-specific capabilities.
	slots := map[int64]*session.WorkerSlots{1: {AvailableSlots: 3}}
	defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(slots).Build().UnPatch()
	pending := &exportSchedulingTask{}
	defer mockey.Mock((*exportSchedulingTask).GetTaskID).Return(int64(99)).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.SnapshotExportCopy).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskSlot).Return(int64(2)).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
	state := taskcommon.Init
	defer mockey.Mock((*exportSchedulingTask).GetTaskState).To(func() taskcommon.State { return state }).Build().UnPatch()
	var assigned int64
	defer mockey.Mock((*exportSchedulingTask).CreateTaskOnWorker).To(func(nodeID int64, _ session.Cluster) {
		assigned = nodeID
		state = taskcommon.InProgress
	}).Build().UnPatch()
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	scheduler.Enqueue(pending)
	scheduler.schedule()
	assert.EqualValues(t, 1, assigned)
	assert.EqualValues(t, 1, slots[1].AvailableSlots)
	assert.Zero(t, scheduler.pendingTasks.TaskCount())
	assert.Equal(t, 1, scheduler.runningTasks.Len())
}

// Capacity exhaustion must stop dispatch, while explicit removal works without slots.
func TestGlobalScheduler_RemoveWithoutSlots(t *testing.T) {
	cluster := &exportSchedulingCluster{}
	defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(map[int64]*session.WorkerSlots{}).Build().UnPatch()
	pending, next := &exportSchedulingTask{}, &exportSchedulingTask{}
	defer mockey.Mock((*exportSchedulingTask).GetTaskID).To(func(t *exportSchedulingTask) int64 {
		if t == pending {
			return 99
		}
		return 100
	}).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.SnapshotExportCopy).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskState).Return(taskcommon.Init).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
	slot := mockey.Mock((*exportSchedulingTask).GetTaskSlot).Return(int64(1)).Build()
	defer slot.UnPatch()
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	scheduler.Enqueue(pending)
	scheduler.Enqueue(next)
	scheduler.schedule()
	assert.Equal(t, 1, slot.Times())
	assert.Equal(t, 2, scheduler.pendingTasks.TaskCount())
	scheduler.RemoveTask(99) // A Drop call would panic: the embedded dependency is nil.
	scheduler.RemoveTask(99)
	scheduler.RemoveTask(100)
	assert.Zero(t, scheduler.pendingTasks.TaskCount())
	assert.Zero(t, scheduler.tasks.Len())
}

func TestGlobalScheduler_RemoveDrainsCallback(t *testing.T) {
	cluster := &exportSchedulingCluster{}
	defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(map[int64]*session.WorkerSlots{1: {AvailableSlots: 1}}).Build().UnPatch()
	pending := &exportSchedulingTask{}
	defer mockey.Mock((*exportSchedulingTask).GetTaskID).Return(int64(99)).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.SnapshotExportCopy).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskState).Return(taskcommon.Init).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskSlot).Return(int64(1)).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
	entered, release, scheduled, removed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer mockey.Mock((*exportSchedulingTask).CreateTaskOnWorker).To(func(int64, session.Cluster) { close(entered); <-release }).Build().UnPatch()
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	scheduler.Enqueue(pending)
	go func() { scheduler.schedule(); close(scheduled) }()
	<-entered
	go func() { scheduler.RemoveTask(99); close(removed) }()
	select {
	case <-removed:
		t.Error("removal passed an active Create")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	<-scheduled
	<-removed
	assert.Zero(t, scheduler.tasks.Len())
	assert.Zero(t, scheduler.pendingTasks.TaskCount(), "failed Create must not resurrect removed work")
}

func TestGlobalScheduler_RemoveQueuedCallback(t *testing.T) {
	for _, checking := range []bool{false, true} {
		t.Run(fmt.Sprint(checking), func(t *testing.T) {
			cluster := &exportSchedulingCluster{}
			defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(map[int64]*session.WorkerSlots{1: {AvailableSlots: 1}}).Build().UnPatch()
			pending := &exportSchedulingTask{}
			defer mockey.Mock((*exportSchedulingTask).GetTaskID).Return(int64(99)).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.SnapshotExportCopy).Build().UnPatch()
			state := taskcommon.Init
			if checking {
				state = taskcommon.InProgress
			}
			defer mockey.Mock((*exportSchedulingTask).GetTaskState).To(func() taskcommon.State { return state }).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).GetTaskSlot).Return(int64(1)).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
			scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
			pool := conc.NewPool[struct{}](1)
			defer pool.Release()
			if checking {
				scheduler.checkPool = pool
			} else {
				scheduler.execPool = pool
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			blocker := pool.Submit(func() (struct{}, error) { close(entered); <-release; return struct{}{}, nil })
			<-entered
			scheduler.Enqueue(pending)
			go func() {
				if checking {
					scheduler.check()
				} else {
					scheduler.schedule()
				}
				close(done)
			}()
			// The pool is occupied: callbacks cannot run before removal. Re-registering
			// the SAME Task must not authorize the old callback under its new registration.
			assert.Eventually(t, func() bool { return pool.Waiting() == 1 }, time.Second, time.Millisecond)
			scheduler.RemoveTask(99)
			scheduler.Enqueue(pending)
			close(release)
			blocker.Await()
			<-done
			assert.Equal(t, 1, scheduler.tasks.Len())
			scheduler.RemoveTask(99)
		})
	}
}

func TestGlobalScheduler_RemoveDuringPop(t *testing.T) {
	cluster := &exportSchedulingCluster{}
	defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(map[int64]*session.WorkerSlots{1: {AvailableSlots: 1}}).Build().UnPatch()
	pending := &exportSchedulingTask{}
	defer mockey.Mock((*exportSchedulingTask).GetTaskID).Return(int64(99)).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.SnapshotExportCopy).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).GetTaskState).Return(taskcommon.Init).Build().UnPatch()
	defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	scheduler.Enqueue(pending)
	var pop func(*priorityQueuePolicy) Task
	defer mockey.Mock((*priorityQueuePolicy).Pop).Origin(&pop).To(func(q *priorityQueuePolicy) Task {
		picked := pop(q)
		if picked != nil {
			scheduler.RemoveTask(picked.GetTaskID())
		}
		return picked
	}).Build().UnPatch()
	scheduler.schedule()
	assert.Zero(t, scheduler.tasks.Len())
	assert.Zero(t, scheduler.pendingTasks.TaskCount())
}

func TestGlobalScheduler_StateChangesBeforeDispatchAndAfterQuery(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			cluster := &exportSchedulingCluster{}
			defer mockey.Mock((*exportSchedulingCluster).QuerySlot).Return(map[int64]*session.WorkerSlots{1: {AvailableSlots: 1}}).Build().UnPatch()
			pending := &exportSchedulingTask{}
			defer mockey.Mock((*exportSchedulingTask).GetTaskID).Return(int64(99)).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).GetTaskType).Return(taskcommon.SnapshotExportCopy).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).GetTaskSlot).Return(int64(1)).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).SetTaskTime).Return().Build().UnPatch()
			state := taskcommon.Init
			if running {
				state = taskcommon.InProgress
			}
			defer mockey.Mock((*exportSchedulingTask).GetTaskState).To(func() taskcommon.State { return state }).Build().UnPatch()
			defer mockey.Mock((*exportSchedulingTask).QueryTaskOnWorker).To(func(session.Cluster) { state = taskcommon.None }).Build().UnPatch()
			scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
			scheduler.Enqueue(pending)
			if running {
				scheduler.check()
			} else {
				state = taskcommon.Failed
				scheduler.schedule()
			}
			assert.Zero(t, scheduler.tasks.Len())
			assert.Zero(t, scheduler.pendingTasks.TaskCount())
			assert.Zero(t, scheduler.runningTasks.Len())
		})
	}
}
