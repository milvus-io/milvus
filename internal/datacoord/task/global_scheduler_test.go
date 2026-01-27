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
	taskcommon "github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

	t.Run("pick_from_multiple_idle_nodes", func(t *testing.T) {
		// Both nodes have same resources, should pick one based on memory priority
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 1, 4, true)
		assert.True(t, nodeID == int64(1) || nodeID == int64(2))
	})

	t.Run("pick_node_with_more_memory", func(t *testing.T) {
		// Node 2 has more memory, should be picked first
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 16,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 1, 4, true)
		assert.Equal(t, int64(2), nodeID)
	})

	t.Run("pick_idle_node_for_large_memory_task", func(t *testing.T) {
		// Task needs 100GB memory, only idle node should be picked
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    4,
				AvailableMemorySlot: 8,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 100, 100, true)
		assert.Equal(t, int64(2), nodeID)
	})

	t.Run("no_available_slots", func(t *testing.T) {
		// All nodes fully utilized
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    0,
				AvailableMemorySlot: 0,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    0,
				AvailableMemorySlot: 0,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 1, 1, false)
		assert.Equal(t, int64(NullNodeID), nodeID)
	})

	t.Run("empty_worker_slots", func(t *testing.T) {
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{}, 1, 1, true)
		assert.Equal(t, int64(NullNodeID), nodeID)
	})

	t.Run("task_with_cpu_oversubscription_allowed", func(t *testing.T) {
		// Task allows CPU over-subscription, can be scheduled even when CPU is insufficient
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    1,
				AvailableMemorySlot: 16,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 2, 4, true) // allowCpuOversubscription=true
		assert.Equal(t, int64(1), nodeID)
	})

	t.Run("vector_index_task_cannot_oversubscribe_cpu", func(t *testing.T) {
		// Vector index task doesn't allow CPU over-subscription
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    2,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 32, 8, false) // allowCpuOversubscription=false for vector index
		assert.Equal(t, int64(NullNodeID), nodeID)
	})

	t.Run("idle_node_accepts_any_task", func(t *testing.T) {
		// Completely idle node should accept any task
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 16, 8, false)
		assert.Equal(t, int64(1), nodeID)
	})

	t.Run("sufficient_cpu_and_memory", func(t *testing.T) {
		// Task fits within available resources
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    10,
				AvailableMemorySlot: 20,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 8, 16, false)
		assert.Equal(t, int64(1), nodeID)
	})

	t.Run("insufficient_memory", func(t *testing.T) {
		// Task needs more memory than available, and node is not idle
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    10,
				AvailableMemorySlot: 16,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 4, 20, false)
		assert.Equal(t, int64(NullNodeID), nodeID)
	})

	t.Run("resource_deduction", func(t *testing.T) {
		// Verify resources are deducted after scheduling
		workerSlots := map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}
		nodeID := scheduler.pickNode(workerSlots, 4, 8, false)
		assert.Equal(t, int64(1), nodeID)
		assert.Equal(t, 8.0, workerSlots[1].AvailableCpuSlot)
		assert.Equal(t, 24.0, workerSlots[1].AvailableMemorySlot)
	})

	t.Run("sort_by_memory_then_cpu", func(t *testing.T) {
		// Node 3 has most memory, should be picked
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    10,
				AvailableMemorySlot: 20,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    8,
				AvailableMemorySlot: 25,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			3: {
				NodeID:              3,
				AvailableCpuSlot:    6,
				AvailableMemorySlot: 30,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 2, 4, false)
		assert.Equal(t, int64(3), nodeID)
	})

	t.Run("same_memory_prefer_more_cpu", func(t *testing.T) {
		// Same memory, node 2 has more CPU
		nodeID := scheduler.pickNode(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    6,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    10,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}, 2, 4, false)
		assert.Equal(t, int64(2), nodeID)
	})

	t.Run("cpu_oversubscription_allows_negative", func(t *testing.T) {
		// When CPU over-subscription happens, slots can go negative to track actual usage
		workerSlots := map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    1,
				AvailableMemorySlot: 16,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}
		// Task with allowCpuOversubscription=true can over-subscribe CPU
		// Node has 1 CPU available, task needs 3, should allow scheduling
		nodeID := scheduler.pickNode(workerSlots, 3, 4, true)
		assert.Equal(t, int64(1), nodeID)
		// After scheduling, available CPU can be negative to track over-subscription
		assert.Equal(t, -2.0, workerSlots[1].AvailableCpuSlot)
		assert.Equal(t, 12.0, workerSlots[1].AvailableMemorySlot)
	})

	t.Run("lightweight_task_uses_memory_priority_sorting", func(t *testing.T) {
		// Lightweight tasks follow the same memory-priority sorting as other tasks
		// No additional CPU optimization to avoid O(N) overhead in high-concurrency scenarios
		workerSlots := map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    1,  // Less CPU available
				AvailableMemorySlot: 30, // More memory (will be picked)
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    5,  // More CPU available
				AvailableMemorySlot: 20, // Less memory
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}
		// Task allows CPU over-subscription, memorySlot=8
		// Should pick node 1 with more memory (follows sorting order)
		// This avoids extra O(N) traversal for better performance under high load
		nodeID := scheduler.pickNode(workerSlots, 2, 8, true)
		assert.Equal(t, int64(1), nodeID, "Should follow memory-priority sorting for efficiency")
		assert.Equal(t, -1.0, workerSlots[1].AvailableCpuSlot) // Negative after over-subscription (1-2=-1)
		assert.Equal(t, 22.0, workerSlots[1].AvailableMemorySlot)
	})

	t.Run("lightweight_task_respects_memory_constraint", func(t *testing.T) {
		// Even for lightweight tasks, memory constraint must be respected
		workerSlots := map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    1,
				AvailableMemorySlot: 20, // Sufficient memory
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    10, // Lots of CPU
				AvailableMemorySlot: 5,  // Insufficient memory
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}
		// Task allows CPU over-subscription, cpuSlot=2, memorySlot=15
		// Node 2 has more CPU but insufficient memory (5 < 15)
		// Should pick node 1 with sufficient memory despite less CPU
		nodeID := scheduler.pickNode(workerSlots, 2, 15, true)
		assert.Equal(t, int64(1), nodeID, "Should respect memory constraint even for lightweight tasks")
		assert.Equal(t, -1.0, workerSlots[1].AvailableCpuSlot)
		assert.Equal(t, 5.0, workerSlots[1].AvailableMemorySlot)
	})
}

func TestGlobalScheduler_TestSchedule(t *testing.T) {
	newCluster := func() session.Cluster {
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
			1: {
				NodeID:              1,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
			2: {
				NodeID:              2,
				AvailableCpuSlot:    12,
				AvailableMemorySlot: 32,
				TotalCpuSlot:        12,
				TotalMemorySlot:     32,
			},
		}).Maybe()
		return cluster
	}

	newTask := func() *MockTask {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(1).Maybe()
		task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
		task.EXPECT().GetTaskSlot().Return(int64(1)).Maybe()
		task.EXPECT().GetTaskSlotV2().Return(1.0, 1.0).Maybe()
		task.EXPECT().AllowCpuOversubscription().Return(true).Maybe()
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
