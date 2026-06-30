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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
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
	scheduler.Enqueue(task)
	assert.Equal(t, 1, len(scheduler.(*globalTaskScheduler).pendingTasks.TaskIDs()))

	task = NewMockTask(t)
	task.EXPECT().GetTaskID().Return(2)
	task.EXPECT().GetTaskState().Return(taskcommon.InProgress)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).runningTasks.Len())
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).runningTasks.Len())
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
