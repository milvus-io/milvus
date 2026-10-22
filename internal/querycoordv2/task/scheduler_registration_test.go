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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func registrationChannelTask(t *testing.T, replica *meta.Replica, node int64, channel string) *ChannelTask {
	t.Helper()
	task, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1), replica.GetCollectionID(),
		replica, NewChannelAction(node, ActionTypeGrow, channel))
	require.NoError(t, err)
	t.Cleanup(func() { task.Cancel(nil) })
	return task
}

// publishRegistrationTask is used by admission hooks so that concurrent calls
// do not use mockey Origin, whose shared argument trampoline is not race safe.
func publishRegistrationTask(queue *taskQueue, task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.buckets[task.Priority()][task.ID()] = task
}

func waitRegistrationBarrier(t *testing.T, barrier <-chan struct{}) {
	t.Helper()
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler operation did not reach the barrier")
	}
}

// An unsafe implementation can finish the competing mutation while the first
// operation is paused. The fixed scheduler must defer it until the pause ends.
// Either way, assert the final bookkeeping after both operations have finished.
func allowRegistrationInterleaving(done <-chan struct{}) {
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
	}
}

func requireRegisteredChannelTask(t *testing.T, scheduler *taskScheduler, task *ChannelTask) {
	t.Helper()
	_, registered := scheduler.tasks.Get(task.ID())
	require.True(t, registered)
	indexed, found := scheduler.channelTasks.Get(NewReplicaChannelIndex(task))
	require.True(t, found, "old cleanup must not erase a replacement's deduplication index")
	require.Same(t, task, indexed)
	require.True(t, scheduler.channelTaskDelta.taskIDRecords.Contain(task.ID()))
	require.Equal(t, 1, scheduler.GetChannelTaskDelta(task.Actions()[0].Node(), task.CollectionID()),
		"last-task cleanup must not clear a concurrently registered task's delta")
}

func TestSchedulerRegistrationAndRemovalAreAtomic(t *testing.T) {
	for _, operation := range []string{"dispatch", "remove_by_node", "stop"} {
		t.Run(operation, func(t *testing.T) {
			replica := membershipReplica(nil, nil, []int64{14}, []int64{1})
			scheduler := newChannelMembershipScheduler(t, replica)
			// Dispatch skips all queues if tasks is empty. Keep an unrelated
			// registered task so it visits the partially published new task.
			sentinel := registrationChannelTask(t, replica, 14, "sentinel")
			require.NoError(t, scheduler.Add(sentinel))
			task := registrationChannelTask(t, replica, 1, "registration")
			published, resume := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(resume) }) }
			defer release()
			patch := mockey.Mock((*taskQueue).Add).To(func(queue *taskQueue, added Task) {
				publishRegistrationTask(queue, added)
				if queue == scheduler.waitQueue && added == task {
					close(published)
					<-resume // Published, with registration still owning the collection lock.
				}
			}).Build()
			defer patch.UnPatch()
			addDone := make(chan struct{})
			var addErr error
			go func() { defer close(addDone); addErr = scheduler.Add(task) }()
			defer func() { release(); waitRegistrationBarrier(t, addDone) }()
			waitRegistrationBarrier(t, published)
			removeStarted, removeDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(removeDone)
				close(removeStarted)
				switch operation {
				case "dispatch":
					scheduler.Dispatch(1)
				case "remove_by_node":
					scheduler.RemoveByNode(1)
				case "stop":
					scheduler.Stop()
				}
			}()
			defer func() { release(); waitRegistrationBarrier(t, removeDone) }()
			waitRegistrationBarrier(t, removeStarted)
			allowRegistrationInterleaving(removeDone)
			release()
			waitRegistrationBarrier(t, addDone)
			waitRegistrationBarrier(t, removeDone)
			require.NoError(t, addErr)
			require.Equal(t, TaskStatusCanceled, task.Status())
			_, registered := scheduler.tasks.Get(task.ID())
			require.False(t, registered, "removal must not leave an unqueued task registered after Add resumes")
			_, indexed := scheduler.channelTasks.Get(NewReplicaChannelIndex(task))
			require.False(t, indexed)
			require.False(t, scheduler.channelTaskDelta.taskIDRecords.Contain(task.ID()))
			require.Zero(t, scheduler.GetChannelTaskDelta(1, membershipCollectionID))
			if operation == "stop" {
				require.Zero(t, scheduler.tasks.Len())
			} else {
				requireRegisteredChannelTask(t, scheduler, sentinel)
			}
		})
	}
}

func TestSchedulerStaleRemovalPreservesHigherPriorityReplacement(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, []int64{1})
	scheduler := newChannelMembershipScheduler(t, replica)
	old := registrationChannelTask(t, replica, 1, "replacement")
	require.NoError(t, scheduler.Add(old))
	replacement := registrationChannelTask(t, replica, 14, "replacement")
	replacement.SetPriority(TaskPriorityHigh)
	removing, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	var paused atomic.Bool
	var original func(*taskScheduler, Task)
	patch := mockey.Mock((*taskScheduler).remove).Origin(&original).To(func(s *taskScheduler, task Task) {
		if s == scheduler && task == old && paused.CompareAndSwap(false, true) {
			close(removing)
			<-resume // Dispatch has canceled old and is about to clean its index.
		}
		original(s, task)
	}).Build()
	defer patch.UnPatch()
	dispatchDone := make(chan struct{})
	go func() { defer close(dispatchDone); scheduler.Dispatch(1) }()
	defer func() { release(); waitRegistrationBarrier(t, dispatchDone) }()
	waitRegistrationBarrier(t, removing)
	addStarted, addDone := make(chan struct{}), make(chan struct{})
	var addErr error
	go func() { defer close(addDone); close(addStarted); addErr = scheduler.Add(replacement) }()
	defer func() { release(); waitRegistrationBarrier(t, addDone) }()
	waitRegistrationBarrier(t, addStarted)
	allowRegistrationInterleaving(addDone)
	release()
	waitRegistrationBarrier(t, dispatchDone)
	waitRegistrationBarrier(t, addDone)
	require.NoError(t, addErr)
	require.Equal(t, TaskStatusCanceled, old.Status())
	requireRegisteredChannelTask(t, scheduler, replacement)
	require.Equal(t, 1, scheduler.tasks.Len())
	require.Equal(t, 1, scheduler.GetChannelTaskNum())
	duplicate := registrationChannelTask(t, replica, 14, "replacement")
	require.ErrorContains(t, scheduler.Add(duplicate), "same channel exists")
}

func TestSchedulerLastRemovalPreservesNewTaskDelta(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, []int64{1})
	scheduler := newChannelMembershipScheduler(t, replica)
	otherReplica := registrationOtherReplica(t, scheduler)
	old := registrationChannelTask(t, replica, 1, "old")
	require.NoError(t, scheduler.Add(old))
	replacement := registrationChannelTask(t, otherReplica, 14, "new")
	clearing, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	var paused atomic.Bool
	var original func(*ChannelTaskDelta, *ChannelTask)
	patch := mockey.Mock((*ChannelTaskDelta).Sub).Origin(&original).To(func(delta *ChannelTaskDelta, task *ChannelTask) {
		if delta == scheduler.channelTaskDelta && task == old && paused.CompareAndSwap(false, true) {
			close(clearing)
			<-resume // Retirement is about to subtract and reclaim under the delta lock.
		}
		original(delta, task)
	}).Build()
	defer patch.UnPatch()
	dispatchDone := make(chan struct{})
	go func() { defer close(dispatchDone); scheduler.Dispatch(1) }()
	defer func() { release(); waitRegistrationBarrier(t, dispatchDone) }()
	waitRegistrationBarrier(t, clearing)
	addStarted, addDone := make(chan struct{}), make(chan struct{})
	var addErr error
	go func() { defer close(addDone); close(addStarted); addErr = scheduler.Add(replacement) }()
	defer func() { release(); waitRegistrationBarrier(t, addDone) }()
	waitRegistrationBarrier(t, addStarted)
	// Admission in another collection completes before Sub takes the delta
	// lock. Removing the old reservation must preserve the new one.
	waitRegistrationBarrier(t, addDone)
	require.NoError(t, addErr)
	requireRegisteredChannelTask(t, scheduler, replacement)
	release()
	waitRegistrationBarrier(t, dispatchDone)
	waitRegistrationBarrier(t, addDone)
	require.NoError(t, addErr)
	requireRegisteredChannelTask(t, scheduler, replacement)
	require.Equal(t, 1, scheduler.tasks.Len())
	require.Equal(t, 1, scheduler.channelTaskDelta.taskIDRecords.Len())
}

func registrationOtherReplica(t *testing.T, scheduler *taskScheduler) *meta.Replica {
	t.Helper()
	replica := meta.NewReplica(&querypb.Replica{
		ID: membershipReplicaID + 1, CollectionID: membershipCollectionID + 1,
		ResourceGroup: meta.DefaultResourceGroupName, RwSqNodes: []int64{14},
	})
	require.NoError(t, scheduler.meta.Put(scheduler.ctx, replica))
	return replica
}

func TestSchedulerAddsInDifferentCollectionsRunConcurrently(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	otherReplica := registrationOtherReplica(t, scheduler)
	first := registrationChannelTask(t, replica, 14, "parallel-registration")
	second := registrationChannelTask(t, otherReplica, 14, "parallel-registration-other")
	published, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	patch := mockey.Mock((*taskQueue).Add).To(func(queue *taskQueue, added Task) {
		publishRegistrationTask(queue, added)
		if queue == scheduler.waitQueue && added == first {
			close(published)
			<-resume
		}
	}).Build()
	defer patch.UnPatch()
	firstDone := make(chan struct{})
	var firstErr error
	go func() { defer close(firstDone); firstErr = scheduler.Add(first) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, firstDone)
	}()
	waitRegistrationBarrier(t, published)
	secondDone := make(chan struct{})
	var secondErr error
	go func() { defer close(secondDone); secondErr = scheduler.Add(second) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, secondDone)
	}()
	// This must complete while the first collection is still publishing its
	// task. A scheduler-wide exclusive Add lock prevents this progress.
	waitRegistrationBarrier(t, secondDone)
	require.NoError(t, secondErr)
	requireRegisteredChannelTask(t, scheduler, second)
	release()
	waitRegistrationBarrier(t, firstDone)
	require.NoError(t, firstErr)
	requireRegisteredChannelTask(t, scheduler, first)
	require.Equal(t, 2, scheduler.tasks.Len())
}

func TestSchedulerAddsInSameCollectionPreserveDeduplication(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	first := registrationChannelTask(t, replica, 14, "duplicate-registration")
	duplicate := registrationChannelTask(t, replica, 14, "duplicate-registration")
	published, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	patch := mockey.Mock((*taskQueue).Add).To(func(queue *taskQueue, added Task) {
		publishRegistrationTask(queue, added)
		if queue == scheduler.waitQueue && added == first {
			close(published)
			<-resume // The first registration still owns the collection lock.
		}
	}).Build()
	defer patch.UnPatch()
	firstDone := make(chan struct{})
	var firstErr error
	go func() { defer close(firstDone); firstErr = scheduler.Add(first) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, firstDone)
	}()
	waitRegistrationBarrier(t, published)
	duplicateStarted, duplicateDone := make(chan struct{}), make(chan struct{})
	var duplicateErr error
	go func() {
		defer close(duplicateDone)
		close(duplicateStarted)
		duplicateErr = scheduler.Add(duplicate)
	}()
	defer func() {
		release()
		waitRegistrationBarrier(t, duplicateDone)
	}()
	waitRegistrationBarrier(t, duplicateStarted)
	allowRegistrationInterleaving(duplicateDone)
	release()
	waitRegistrationBarrier(t, firstDone)
	waitRegistrationBarrier(t, duplicateDone)
	require.NoError(t, firstErr)
	require.ErrorContains(t, duplicateErr, "same channel exists")
	requireRegisteredChannelTask(t, scheduler, first)
	require.Equal(t, 1, scheduler.tasks.Len())
	require.Equal(t, 1, scheduler.waitQueue.Len())
	require.Equal(t, TaskStatusCanceled, duplicate.Status())
}

func TestSchedulerReplacementPreservesConcurrentCollectionDelta(t *testing.T) {
	replica := membershipReplica(nil, nil, []int64{14}, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	otherReplica := registrationOtherReplica(t, scheduler)
	old := registrationChannelTask(t, replica, 14, "replacement-registration")
	require.NoError(t, scheduler.Add(old))
	replacement := registrationChannelTask(t, replica, 14, "replacement-registration")
	replacement.SetPriority(TaskPriorityHigh)
	other := registrationChannelTask(t, otherReplica, 14, "other-collection")
	paused, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer release()
	pause := func() {
		pauseOnce.Do(func() {
			close(paused)
			<-resume
		})
	}
	var originalSub func(*ChannelTaskDelta, *ChannelTask)
	subPatch := mockey.Mock((*ChannelTaskDelta).Sub).Origin(&originalSub).To(func(delta *ChannelTaskDelta, task *ChannelTask) {
		if delta == scheduler.channelTaskDelta && task == old {
			// Let another collection register before retirement takes the
			// delta lock to subtract and reclaim its reservation.
			pause()
		}
		originalSub(delta, task)
	}).Build()
	defer subPatch.UnPatch()
	replacementDone := make(chan struct{})
	var replacementErr error
	go func() { defer close(replacementDone); replacementErr = scheduler.Add(replacement) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, replacementDone)
	}()
	waitRegistrationBarrier(t, paused)
	otherDone := make(chan struct{})
	var otherErr error
	go func() { defer close(otherDone); otherErr = scheduler.Add(other) }()
	defer func() {
		release()
		waitRegistrationBarrier(t, otherDone)
	}()
	waitRegistrationBarrier(t, otherDone)
	require.NoError(t, otherErr)
	requireRegisteredChannelTask(t, scheduler, other)
	release()
	waitRegistrationBarrier(t, replacementDone)
	require.NoError(t, replacementErr)
	require.Equal(t, TaskStatusCanceled, old.Status())
	requireRegisteredChannelTask(t, scheduler, replacement)
	requireRegisteredChannelTask(t, scheduler, other)
	require.Equal(t, 2, scheduler.tasks.Len())
	require.Equal(t, 2, scheduler.channelTaskDelta.taskIDRecords.Len())
	require.False(t, scheduler.channelTaskDelta.taskIDRecords.Contain(old.ID()))
}
