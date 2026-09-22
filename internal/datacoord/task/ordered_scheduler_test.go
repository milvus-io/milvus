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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

func TestGlobalScheduler_OrderedAdmissionAcrossRounds(t *testing.T) {
	cluster := &schedulerTestCluster{slots: map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 1},
	}}
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	defer scheduler.Stop()

	// A backlog is admitted out of order and drained one slot at a time.
	// Completion changes the existing wrapper, without re-enqueueing it.
	const taskCount = 67
	tasks := make([]*schedulerTestTask, taskCount)
	for i := range tasks {
		tasks[i] = newSchedulerTestTask(int64(i + 1))
	}
	for i := range tasks {
		scheduler.Enqueue(tasks[(i*17)%taskCount])
	}

	for i, next := range tasks {
		cluster.slots[1].AvailableSlots = 1
		runRoundSync(scheduler)

		require.Equal(t, taskcommon.InProgress, next.GetTaskState(),
			"task %d must receive this round's only slot", next.id)
		require.Equal(t, int64(1), next.nodeID.Load())
		require.Equal(t, taskCount-i-1, scheduler.GetPendingTaskCount(taskcommon.CopySegment))
		require.Equal(t, taskCount-i, scheduler.tasks.Len(), "completed owners must leave the queue")
		next.state.Store(int32(taskcommon.Finished))
	}
	runRoundSync(scheduler)
	assert.Zero(t, scheduler.tasks.Len())
}

type orderedSchedulerTestTask struct {
	*schedulerTestTask
	queued  atomic.Int32
	queries atomic.Int32
	drops   atomic.Int32
}

func (t *orderedSchedulerTestTask) SetTaskTime(kind taskcommon.TimeType, _ time.Time) {
	if kind == taskcommon.TimeQueue {
		t.queued.Add(1)
	}
}

func (t *orderedSchedulerTestTask) QueryTaskOnWorker(session.Cluster) { t.queries.Add(1) }
func (t *orderedSchedulerTestTask) DropTaskOnWorker(session.Cluster)  { t.drops.Add(1) }

func TestGlobalScheduler_OrderedSnapshotDoesNotAdoptReplacement(t *testing.T) {
	for _, state := range []taskcommon.State{taskcommon.Init, taskcommon.InProgress, taskcommon.Finished} {
		t.Run(state.String(), func(t *testing.T) {
			cluster := &schedulerTestCluster{slots: map[int64]*session.WorkerSlots{
				1: {NodeID: 1, AvailableSlots: 1},
			}}
			scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
			defer scheduler.Stop()
			stale := &orderedSchedulerTestTask{schedulerTestTask: newSchedulerTestTask(10)}
			scheduler.Enqueue(stale)
			stale.state.Store(int32(state))
			pending, running, ended := scheduler.partition()

			// An owner finalizes the old wrapper after the round has taken its
			// snapshot, then publishes another wrapper under the same ID.
			scheduler.Finalize(stale.id, func() {})
			later := newSchedulerTestTask(20)
			rebuilt := newSchedulerTestTask(stale.id)
			scheduler.Enqueue(later)
			scheduler.Enqueue(rebuilt)
			scheduler.releaseEndedTaskOwnership(ended)
			scheduler.schedule(pending)
			scheduler.check(running)

			assert.Equal(t, int64(NullNodeID), stale.nodeID.Load(), "stale pending snapshot must not dispatch")
			assert.Zero(t, stale.queries.Load(), "stale running snapshot must not poll")
			assert.Zero(t, stale.drops.Load(), "stale terminal snapshot must not clean up the replacement")
			owned, ok := scheduler.tasks.Get(stale.id)
			require.True(t, ok)
			require.Same(t, rebuilt, owned)
			require.Equal(t, taskcommon.Init, rebuilt.GetTaskState())

			cluster.slots[1].AvailableSlots = 1
			runRoundSync(scheduler)
			assert.Equal(t, taskcommon.InProgress, rebuilt.GetTaskState(), "a new round must see the new owner")
			assert.Equal(t, taskcommon.Init, later.GetTaskState(), "the rebuilt lower ID retains priority")
		})
	}
}

func TestGlobalScheduler_OrderedConcurrentEnqueueKeepsOneOwner(t *testing.T) {
	cluster := &schedulerTestCluster{slots: map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 1},
	}}
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	defer scheduler.Stop()

	const contenders = 32
	tasks := make([]*orderedSchedulerTestTask, contenders)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range tasks {
		task := &orderedSchedulerTestTask{schedulerTestTask: newSchedulerTestTask(10)}
		tasks[i] = task
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			scheduler.Enqueue(task)
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent enqueues did not complete")
	}

	require.Equal(t, 1, scheduler.tasks.Len())
	owned, ok := scheduler.tasks.Get(10)
	require.True(t, ok)
	var queued int32
	for _, task := range tasks {
		queued += task.queued.Load()
	}
	require.Equal(t, int32(1), queued, "exactly one contender must have been admitted")

	runRoundSync(scheduler)
	for _, task := range tasks {
		if Task(task) == owned {
			assert.Equal(t, int32(1), task.queued.Load(), "the admitted wrapper must still own the task")
			assert.Equal(t, taskcommon.InProgress, task.GetTaskState())
			assert.Equal(t, int64(1), task.nodeID.Load())
		} else {
			assert.Equal(t, taskcommon.Init, task.GetTaskState())
			assert.Equal(t, int64(NullNodeID), task.nodeID.Load())
		}
	}
}

func TestGlobalScheduler_OrderedBlockedTasksDoNotStopAdmission(t *testing.T) {
	cluster := &schedulerTestCluster{slots: map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 1},
	}}
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	defer scheduler.Stop()
	blocked := &statsCapabilityTestTask{newSchedulerTestTask(1)}
	ordinary := newSchedulerTestTask(2)
	waiting := newSchedulerTestTask(3)
	zeroCost := newSchedulerTestTask(4)
	zeroCost.slot = 0
	for _, task := range []Task{zeroCost, waiting, ordinary, blocked} {
		scheduler.Enqueue(task)
	}

	runRoundSync(scheduler)
	assert.Equal(t, taskcommon.Init, blocked.GetTaskState())
	assert.Equal(t, taskcommon.InProgress, ordinary.GetTaskState())
	assert.Equal(t, taskcommon.Init, waiting.GetTaskState())
	assert.Equal(t, taskcommon.InProgress, zeroCost.GetTaskState(), "zero-cost work can run after all slots are consumed")
	assert.Zero(t, cluster.slots[1].AvailableSlots)
	assert.Equal(t, 2, scheduler.GetPendingTaskCount(taskcommon.CopySegment))

	cluster.slots[1].SupportsV3StatsAttemptPath = true
	cluster.slots[1].AvailableSlots = 1
	runRoundSync(scheduler)
	assert.Equal(t, taskcommon.InProgress, blocked.GetTaskState(), "a blocked task keeps its priority when a compatible node appears")
	assert.Equal(t, taskcommon.Init, waiting.GetTaskState())
}
