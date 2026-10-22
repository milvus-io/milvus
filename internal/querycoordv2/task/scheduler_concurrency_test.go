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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Mockey's Origin callback stores call arguments in a shared closure. These
// hooks must be callable concurrently, so capture queue contents directly under
// the queue's own lock rather than forwarding through Origin.
func captureWaitingTasks(queue *taskQueue) []Task {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	var tasks []Task
	for priority := len(queue.buckets) - 1; priority >= 0; priority-- {
		for _, task := range queue.buckets[priority] {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func captureNodeTasks(queue *nodeTaskQueue, node int64) []Task {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	var tasks []Task
	buckets := queue.buckets[node]
	for priority := len(buckets) - 1; priority >= 0; priority-- {
		for _, task := range buckets[priority] {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func checkConcurrentSchedulerTask(scheduler *taskScheduler, task Task, checkDist bool) error {
	if err := task.Context().Err(); err != nil {
		return err
	}
	return scheduler.checkStale(task, checkDist)
}

func TestSchedulerDispatchAllowsOtherCollectionsToProgress(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{1, 14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	otherReplica := registrationOtherReplica(t, scheduler)
	first := registrationChannelTask(t, replica, 1, "busy-collection")
	require.NoError(t, scheduler.Add(first))
	scheduler.tryPromoteAll()
	scheduler.AddExecutor(1)
	scheduler.AddExecutor(14)

	checking, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	checkPatch := mockey.Mock((*taskScheduler).check).To(func(s *taskScheduler, task Task, checkDist bool) error {
		if s == scheduler && task == first && !checkDist {
			pauseOnce.Do(func() {
				close(checking)
				<-resume
			})
		}
		return checkConcurrentSchedulerTask(s, task, checkDist)
	}).Build()
	defer checkPatch.UnPatch()
	second := registrationChannelTask(t, otherReplica, 14, "independent-collection")
	committed := make(chan struct{}, 1)
	executePatch := mockey.Mock((*Executor).Execute).To(func(_ *Executor, task Task, _ int) bool {
		if task == second {
			committed <- struct{}{}
		}
		return true
	}).Build()
	defer executePatch.UnPatch()
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); scheduler.Dispatch(1) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, firstDone)
	}()
	waitRegistrationBarrier(t, checking)

	addDone := make(chan struct{})
	var addErr error
	go func() { defer close(addDone); addErr = scheduler.Add(second) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, addDone)
	}()
	// A slow operation in collection A must not hold a scheduler-wide writer
	// or dispatch lock that blocks collection B's registration and admission.
	waitRegistrationBarrier(t, addDone)
	require.NoError(t, addErr)
	secondDone := make(chan struct{})
	go func() { defer close(secondDone); scheduler.Dispatch(14) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, secondDone)
	}()
	waitRegistrationBarrier(t, committed)
	waitRegistrationBarrier(t, secondDone)
	requireRegisteredChannelTask(t, scheduler, second)
}

func TestSchedulerConcurrentWaitSnapshotsPromoteOnce(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	task := registrationChannelTask(t, replica, 14, "shared-wait-snapshot")
	require.NoError(t, scheduler.Add(task))

	snapshots, resume := make(chan struct{}, 2), make(chan struct{})
	var snapshotCount, promotionChecks atomic.Int32
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	snapshotPatch := mockey.Mock((*taskQueue).Snapshot).To(func(queue *taskQueue) []Task {
		result := captureWaitingTasks(queue)
		if queue == scheduler.waitQueue && snapshotCount.Add(1) <= 2 {
			snapshots <- struct{}{}
			<-resume // Both dispatchers have captured the same waiting task.
		}
		return result
	}).Build()
	defer snapshotPatch.UnPatch()
	checkPatch := mockey.Mock((*taskScheduler).check).To(func(s *taskScheduler, checked Task, checkDist bool) error {
		if s == scheduler && checked == task && checkDist {
			promotionChecks.Add(1)
		}
		return checkConcurrentSchedulerTask(s, checked, checkDist)
	}).Build()
	defer checkPatch.UnPatch()
	done := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() { defer workers.Done(); scheduler.Dispatch(14) }()
	}
	go func() { workers.Wait(); close(done) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, done)
	}()
	waitRegistrationBarrier(t, snapshots)
	waitRegistrationBarrier(t, snapshots)
	release()
	waitRegistrationBarrier(t, done)
	require.EqualValues(t, 1, promotionChecks.Load(), "a stale waiting snapshot must not promote an already-processing task")
	require.Zero(t, scheduler.waitQueue.Len())
	require.Equal(t, 1, scheduler.processQueue.LenByNode(14))
	requireRegisteredChannelTask(t, scheduler, task)
}

func TestSchedulerWaitingSnapshotPreservesReplacement(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	scheduler.AddExecutor(14)
	old := registrationChannelTask(t, replica, 14, "snapshot-replacement")
	require.NoError(t, scheduler.Add(old))
	replacement := registrationChannelTask(t, replica, 14, "snapshot-replacement")
	replacement.SetPriority(TaskPriorityHigh)

	snapshotted, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	snapshotPatch := mockey.Mock((*taskQueue).Snapshot).To(func(queue *taskQueue) []Task {
		result := captureWaitingTasks(queue)
		if queue == scheduler.waitQueue {
			pauseOnce.Do(func() {
				close(snapshotted)
				<-resume // No collection lock is held while a snapshot is consumed.
			})
		}
		return result
	}).Build()
	defer snapshotPatch.UnPatch()
	var oldExecutions, replacementExecutions atomic.Int32
	executePatch := mockey.Mock((*Executor).Execute).To(func(_ *Executor, task Task, _ int) bool {
		if task == old {
			oldExecutions.Add(1)
		}
		if task == replacement {
			replacementExecutions.Add(1)
		}
		return true
	}).Build()
	defer executePatch.UnPatch()
	dispatchDone := make(chan struct{})
	go func() { defer close(dispatchDone); scheduler.Dispatch(14) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, dispatchDone)
	}()
	waitRegistrationBarrier(t, snapshotted)
	addDone := make(chan struct{})
	var addErr error
	go func() { defer close(addDone); addErr = scheduler.Add(replacement) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, addDone)
	}()
	waitRegistrationBarrier(t, addDone)
	require.NoError(t, addErr)
	release()
	waitRegistrationBarrier(t, dispatchDone)
	requireRegisteredChannelTask(t, scheduler, replacement)
	require.Equal(t, TaskStatusCanceled, old.Status())
	require.Zero(t, oldExecutions.Load())
	// The next dispatch must find the replacement through its retained index
	// and queue entry, rather than resurrecting or cleaning the old snapshot.
	scheduler.Dispatch(14)
	require.EqualValues(t, 1, replacementExecutions.Load())
	requireRegisteredChannelTask(t, scheduler, replacement)
}

func TestSchedulerProcessingSnapshotPreservesReplacement(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	scheduler.AddExecutor(14)
	old := registrationChannelTask(t, replica, 14, "processing-snapshot-replacement")
	require.NoError(t, scheduler.Add(old))
	scheduler.tryPromoteAll()
	replacement := registrationChannelTask(t, replica, 14, "processing-snapshot-replacement")
	replacement.SetPriority(TaskPriorityHigh)

	snapshotted, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	snapshotPatch := mockey.Mock((*nodeTaskQueue).SnapshotByNode).To(func(queue *nodeTaskQueue, node int64) []Task {
		result := captureNodeTasks(queue, node)
		if queue == scheduler.processQueue && node == 14 {
			pauseOnce.Do(func() {
				close(snapshotted)
				<-resume
			})
		}
		return result
	}).Build()
	defer snapshotPatch.UnPatch()
	var oldExecutions, replacementExecutions atomic.Int32
	executePatch := mockey.Mock((*Executor).Execute).To(func(_ *Executor, task Task, _ int) bool {
		if task == old {
			oldExecutions.Add(1)
		}
		if task == replacement {
			replacementExecutions.Add(1)
		}
		return true
	}).Build()
	defer executePatch.UnPatch()
	dispatchDone := make(chan struct{})
	go func() { defer close(dispatchDone); scheduler.Dispatch(14) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, dispatchDone)
	}()
	waitRegistrationBarrier(t, snapshotted)
	addDone := make(chan struct{})
	var addErr error
	go func() {
		defer close(addDone)
		// Processing tasks cannot be preempted by priority. Retire the old
		// task explicitly to exercise a stale processing snapshot after removal.
		retireOwnershipTask(scheduler, old)
		addErr = scheduler.Add(replacement)
	}()
	defer func() {
		release()
		waitRegistrationBarrier(t, addDone)
	}()
	waitRegistrationBarrier(t, addDone)
	require.NoError(t, addErr)
	release()
	waitRegistrationBarrier(t, dispatchDone)
	require.Zero(t, oldExecutions.Load())
	requireRegisteredChannelTask(t, scheduler, replacement)
	scheduler.Dispatch(14)
	require.EqualValues(t, 1, replacementExecutions.Load())
	requireRegisteredChannelTask(t, scheduler, replacement)
}

func TestSchedulerConcurrentMoveDispatchAdvancesOnce(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{1}, []int64{12})
	scheduler := newChannelMembershipScheduler(t, replica)
	scheduler.distMgr.ChannelDistManager.Update(12, membershipChannelOn(12, 2))
	task := membershipTask(t, replica, 1, true)
	require.NoError(t, scheduler.Add(task))
	scheduler.AddExecutor(1)
	scheduler.AddExecutor(12)
	growExecutor, ok := scheduler.executors.Get(1)
	require.True(t, ok)
	reduceExecutor, ok := scheduler.executors.Get(12)
	require.True(t, ok)

	readerReady, stopReader, readerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var statsReads atomic.Int32
	go func() {
		defer close(readerDone)
		scheduler.GetTasksJSON()
		statsReads.Add(1)
		close(readerReady)
		for {
			select {
			case <-stopReader:
				return
			default:
			}
			scheduler.GetTasksJSON()
			statsReads.Add(1)
			runtime.Gosched()
		}
	}()
	defer func() {
		close(stopReader)
		waitRegistrationBarrier(t, readerDone)
	}()
	waitRegistrationBarrier(t, readerReady)

	growStarted, growDone := make(chan struct{}), make(chan struct{})
	reduceStarted, reduceDone := make(chan struct{}), make(chan struct{})
	resumeGrow, resumeReduce := make(chan struct{}), make(chan struct{})
	var growStartOnce, reduceStartOnce, releaseGrowOnce, releaseReduceOnce sync.Once
	var grows, reduces atomic.Int32
	release := func() {
		releaseGrowOnce.Do(func() { close(resumeGrow) })
		releaseReduceOnce.Do(func() { close(resumeReduce) })
	}
	// Keep the actual executor admission/deduplication and asynchronous step
	// capture. Replace only the remote side effect with controlled dist reports.
	patch := mockey.Mock((*Executor).executeDmChannelAction).To(func(executor *Executor, executed *ChannelTask, step int) {
		if step == 0 {
			defer close(growDone)
			defer executor.removeTask(executed, step)
			grows.Add(1)
			growStartOnce.Do(func() { close(growStarted) })
			<-resumeGrow
			scheduler.distMgr.ChannelDistManager.Update(1, membershipChannelOn(1, 3))
		} else {
			defer close(reduceDone)
			defer executor.removeTask(executed, step)
			reduces.Add(1)
			reduceStartOnce.Do(func() { close(reduceStarted) })
			<-resumeReduce
			scheduler.distMgr.ChannelDistManager.Update(12)
		}
	}).Build()
	defer patch.UnPatch()
	defer func() {
		release()
		// Dispatch only joins synchronous admission. A goroutine may have been
		// admitted without entering the hook yet, or have decremented its
		// counter without closing done yet. Join either case before unpatching.
		if growExecutor.channelTaskNum.Load() > 0 || grows.Load() > 0 {
			waitRegistrationBarrier(t, growDone)
		}
		if reduceExecutor.channelTaskNum.Load() > 0 || reduces.Load() > 0 {
			waitRegistrationBarrier(t, reduceDone)
		}
	}()

	dispatchBoth := func() <-chan struct{} {
		done, start := make(chan struct{}), make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		for _, node := range []int64{1, 12} {
			go func() {
				defer workers.Done()
				<-start
				scheduler.Dispatch(node)
			}()
		}
		go func() { workers.Wait(); close(done) }()
		close(start)
		return done
	}
	firstDispatches := dispatchBoth()
	defer func() {
		release()
		waitRegistrationBarrier(t, firstDispatches)
	}()
	waitRegistrationBarrier(t, growStarted)
	waitRegistrationBarrier(t, firstDispatches)
	require.EqualValues(t, 1, grows.Load())
	require.Zero(t, task.Step(), "an in-flight grow must not be stepped twice by two nodes")

	releaseGrowOnce.Do(func() { close(resumeGrow) })
	waitRegistrationBarrier(t, growDone)
	secondDispatches := dispatchBoth()
	defer func() {
		release()
		waitRegistrationBarrier(t, secondDispatches)
	}()
	waitRegistrationBarrier(t, reduceStarted)
	waitRegistrationBarrier(t, secondDispatches)
	require.EqualValues(t, 1, reduces.Load())
	require.Equal(t, 1, task.Step())

	releaseReduceOnce.Do(func() { close(resumeReduce) })
	waitRegistrationBarrier(t, reduceDone)
	scheduler.Dispatch(12)
	require.Equal(t, TaskStatusSucceeded, task.Status())
	require.Equal(t, 2, task.Step())
	require.Zero(t, scheduler.tasks.Len())
	require.Zero(t, scheduler.GetChannelTaskDelta(1, task.CollectionID()))
	require.Zero(t, scheduler.GetChannelTaskDelta(12, task.CollectionID()))
	require.EqualValues(t, 1, grows.Load())
	require.EqualValues(t, 1, reduces.Load())
	require.Greater(t, statsReads.Load(), int32(1), "task statistics must remain readable while the move advances")
}

func TestSchedulerRepeatedRemovalHasNoSideEffects(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	target := meta.NewMockTargetManager(t)
	target.EXPECT().UpdateCollectionNextTarget(mock.Anything, membershipCollectionID).Return(nil).Once()
	scheduler.targetMgr = target
	old := registrationChannelTask(t, replica, 14, "late-removal")
	require.NoError(t, scheduler.Add(old))
	// Recovery metadata can disappear while Watch is preparing its segments.
	// Retiring that failure triggers a target refresh, which must happen once
	// even if another dispatch still holds a snapshot of this task.
	old.Fail(merr.ErrSegmentNotFound)
	replacement := registrationChannelTask(t, replica, 14, "late-removal")
	replacement.SetPriority(TaskPriorityHigh)
	require.NoError(t, scheduler.Add(replacement))
	requireRegisteredChannelTask(t, scheduler, replacement)

	retireOldSnapshot := func() {
		scheduler.lifecycleMu.RLock()
		defer scheduler.lifecycleMu.RUnlock()
		scheduler.collKeyLock.Lock(old.CollectionID())
		defer scheduler.collKeyLock.Unlock(old.CollectionID())
		scheduler.remove(old)
	}
	retireOldSnapshot()
	retireOldSnapshot()
	target.AssertNumberOfCalls(t, "UpdateCollectionNextTarget", 1)
	requireRegisteredChannelTask(t, scheduler, replacement)
	require.False(t, scheduler.channelTaskDelta.taskIDRecords.Contain(old.ID()))
	require.Equal(t, 1, scheduler.tasks.Len())
	require.Equal(t, 1, scheduler.waitQueue.Len())
	require.Equal(t, 1, scheduler.GetChannelTaskNum())
	duplicate := registrationChannelTask(t, replica, 14, "late-removal")
	require.ErrorContains(t, scheduler.Add(duplicate), "same channel exists")
}

func TestSchedulerStopRejectsNewEntrypoints(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	scheduler.Stop()
	task := registrationChannelTask(t, replica, 14, "after-stop")
	require.Error(t, scheduler.Add(task))
	require.Equal(t, TaskStatusCanceled, task.Status())

	var dispatches atomic.Int32
	patch := mockey.Mock((*taskScheduler).schedule).To(func(_ *taskScheduler, _ int64) {
		dispatches.Add(1)
	}).Build()
	defer patch.UnPatch()
	scheduler.Dispatch(14)
	scheduler.AddExecutor(14)
	_, executorExists := scheduler.executors.Get(14)
	require.False(t, executorExists, "a stopped scheduler must not create new executors")
	require.Zero(t, dispatches.Load(), "a stopped scheduler must not enter another scheduling round")
	require.Zero(t, scheduler.tasks.Len())
	require.Zero(t, scheduler.waitQueue.Len())
	require.Zero(t, scheduler.processQueue.Len())
	require.Zero(t, scheduler.GetChannelTaskNum())
}
