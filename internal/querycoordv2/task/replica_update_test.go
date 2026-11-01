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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func markReplicaUpdateCollectionLoaded(t *testing.T, scheduler *taskScheduler) {
	t.Helper()
	require.NoError(t, scheduler.meta.PutCollectionWithoutSave(scheduler.ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: membershipCollectionID},
	}))
}

func requireReplicaUpdateTaskRetired(t *testing.T, scheduler *taskScheduler, task Task) {
	t.Helper()
	require.Equal(t, TaskStatusCanceled, task.Status())
	require.Error(t, task.Err())
	require.ErrorIs(t, task.Context().Err(), context.Canceled)
	require.Zero(t, scheduler.tasks.Len())
	require.Zero(t, scheduler.waitQueue.Len())
	for _, node := range []int64{1, 12, 14} {
		require.Zero(t, scheduler.processQueue.LenByNode(node))
		require.Zero(t, scheduler.GetChannelTaskDelta(node, membershipCollectionID))
	}
	require.Zero(t, scheduler.segmentTasks.Len())
	require.Zero(t, scheduler.channelTasks.Len())
	require.Zero(t, scheduler.segmentOwners.Len())
	require.Zero(t, scheduler.channelOwners.Len())
	require.Empty(t, scheduler.segmentTaskDelta.records)
	require.Zero(t, scheduler.channelTaskDelta.taskIDRecords.Len())
}

func TestReplicaUpdateRetiresSegmentLoadBeforeNextDispatch(t *testing.T) {
	for _, stage := range []string{"waiting", "processing"} {
		for _, update := range []string{"put", "remove_node"} {
			t.Run(stage+"/"+update, func(t *testing.T) {
				replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
				scheduler := newChannelMembershipScheduler(t, replica)
				old := ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
				late := ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
				require.NoError(t, scheduler.Add(old))
				if stage == "processing" {
					scheduler.tryPromoteAll()
					require.True(t, scheduler.isCurrent(old, taskProcessing))
				}
				if update == "put" {
					require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{14}, nil, nil, nil)))
				} else {
					require.NoError(t, scheduler.meta.RemoveNode(scheduler.ctx, membershipCollectionID, membershipReplicaID, 1))
				}
				// Publication itself must retire the task: no Dispatch or new dist
				// report is required to free its queues, indexes and reservations.
				requireReplicaUpdateTaskRetired(t, scheduler, old)
				require.Error(t, scheduler.Add(late), "an old checker snapshot must not revive a stale load")
				require.Equal(t, TaskStatusCanceled, late.Status())
				current := scheduler.meta.Get(scheduler.ctx, membershipReplicaID)
				replacement := ownershipSegmentTask(t, current, NewSegmentAction(14, ActionTypeGrow, membershipChannel, 10))
				require.NoError(t, scheduler.Add(replacement), "retirement must release the logical segment key")
			})
		}
	}
}

func TestReplicaUpdateRetiresCleanupWithoutWaitingForDistribution(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, membershipReplica([]int64{14}, nil, nil, nil))
			markReplicaUpdateCollectionLoaded(t, scheduler)
			var old, late Task
			if kind == "segment" {
				old = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				late = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				scheduler.distMgr.SegmentDistManager.Update(1, utils.CreateTestSegment(membershipCollectionID, 1, 10, 1, 1, membershipChannel))
			} else {
				old = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
				late = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
				scheduler.distMgr.ChannelDistManager.Update(1, membershipChannelOn(1, 1))
			}
			require.NoError(t, scheduler.Add(old))
			scheduler.tryPromoteAll()
			require.True(t, scheduler.isCurrent(old, taskProcessing))
			if action, ok := old.Actions()[0].(*SegmentAction); ok {
				action.rpcReturned.Store(true)
			}
			require.False(t, old.Actions()[0].IsFinished(scheduler.distMgr), "the old copy is still reported")
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{1, 14}, nil, nil, nil)))
			requireReplicaUpdateTaskRetired(t, scheduler, old)
			require.False(t, old.Actions()[0].IsFinished(scheduler.distMgr), "retirement must not depend on observing a disappearance")
			require.Error(t, scheduler.Add(late), "a stale cleanup snapshot must not be admitted after reassignment")
			require.Equal(t, TaskStatusCanceled, late.Status())
		})
	}
}

func TestReplicaUpdatePreservesReleasedCollectionCleanup(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, membershipReplica([]int64{14}, nil, nil, nil))
			setCollectionLoaded(t, scheduler, membershipCollectionID, false)
			var cleanup Task
			if kind == "segment" {
				cleanup = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
			} else {
				cleanup = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
			}
			require.NoError(t, scheduler.Add(cleanup))
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{1, 14}, nil, nil, nil)))
			require.Equal(t, TaskStatusStarted, cleanup.Status(), "retained replica metadata must not stop ReleaseCollection cleanup")
			require.True(t, scheduler.isCurrent(cleanup, taskWaiting))
		})
	}
}

func TestReplicaUpdatePreservesROCopyMaintenance(t *testing.T) {
	for _, typ := range []ActionType{ActionTypeGrow, ActionTypeReduce, ActionTypeUpdate, ActionTypeReopen} {
		t.Run(typ.String(), func(t *testing.T) {
			replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
			scheduler := newChannelMembershipScheduler(t, replica)
			task := ownershipSegmentTask(t, replica, NewSegmentAction(1, typ, membershipChannel, 10))
			require.NoError(t, scheduler.Add(task))
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{14}, []int64{1}, nil, nil)))
			if typ == ActionTypeGrow {
				requireReplicaUpdateTaskRetired(t, scheduler, task)
			} else {
				require.Equal(t, TaskStatusStarted, task.Status())
				require.True(t, scheduler.isCurrent(task, taskWaiting))
			}
		})
	}
	t.Run("leader_grow_with_ro_worker", func(t *testing.T) {
		replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
		scheduler := newChannelMembershipScheduler(t, replica)
		task := NewLeaderSegmentTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID,
			replica, 14, NewLeaderAction(14, 1, ActionTypeGrow, membershipChannel, 10, 1))
		defer task.Cancel(nil)
		require.NoError(t, scheduler.Add(task))
		require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{14}, []int64{1}, nil, nil)))
		require.Equal(t, TaskStatusStarted, task.Status(), "membership checks must use the delegator, not the RO worker")
		require.True(t, scheduler.isCurrent(task, taskWaiting))
	})
}

func TestReplicaUpdateRetiresLeaderGrowAfterWorkerLeavesReplica(t *testing.T) {
	replica := membershipReplica([]int64{14}, []int64{1}, nil, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	newTask := func() *LeaderTask {
		task := NewLeaderSegmentTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID,
			replica, 14, NewLeaderAction(14, 1, ActionTypeGrow, membershipChannel, 10, 1))
		t.Cleanup(func() { task.Cancel(nil) })
		return task
	}
	old, late := newTask(), newTask()
	require.NoError(t, scheduler.Add(old), "an RO worker still belonging to the replica may be routed")
	scheduler.tryPromoteAll()
	require.True(t, scheduler.isCurrent(old, taskProcessing))
	require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{14}, nil, nil, nil)))
	requireReplicaUpdateTaskRetired(t, scheduler, old)
	require.NotNil(t, scheduler.nodeMgr.Get(1), "the worker remains alive but has left this replica")
	require.Error(t, scheduler.Add(late), "an old checker snapshot must not restore a route to the departed worker")
	require.Equal(t, TaskStatusCanceled, late.Status())
	require.Zero(t, scheduler.tasks.Len())
}

func TestReplicaUpdateDoesNotReleaseInFlightExecutorOwnership(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, membershipReplica([]int64{14}, nil, nil, nil))
			markReplicaUpdateCollectionLoaded(t, scheduler)
			ex := NewExecutor(1, scheduler.meta, scheduler.distMgr, nil, nil, nil, scheduler.nodeMgr)
			scheduler.executors.Insert(1, ex)
			var old Task
			if kind == "segment" {
				old = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
			} else {
				old = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
			}
			require.NoError(t, scheduler.Add(old))
			started, resume, oldDone, newDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(resume) }) }
			run := func(ex *Executor, task Task, step int) {
				if task == old {
					defer close(oldDone)
				} else {
					defer close(newDone)
				}
				defer ex.removeTask(task, step)
				if task == old {
					close(started)
					<-resume
				}
			}
			var unpatch func()
			if kind == "segment" {
				patch := mockey.Mock((*Executor).executeSegmentAction).To(func(ex *Executor, task *SegmentTask, step int) { run(ex, task, step) }).Build()
				unpatch = func() { patch.UnPatch() }
			} else {
				patch := mockey.Mock((*Executor).executeDmChannelAction).To(func(ex *Executor, task *ChannelTask, step int) { run(ex, task, step) }).Build()
				unpatch = func() { patch.UnPatch() }
			}
			defer unpatch()
			require.True(t, ex.Execute(old, 0))
			defer func() { release(); waitRegistrationBarrier(t, oldDone) }()
			waitRegistrationBarrier(t, started)
			current := membershipReplica([]int64{1, 14}, nil, nil, nil)
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, current))
			require.Equal(t, TaskStatusCanceled, old.Status())
			require.Zero(t, scheduler.tasks.Len())
			require.Equal(t, 1, ownershipReservations(scheduler), "publication cannot withdraw an already issued RPC")
			newReplacement := func() Task {
				if kind == "segment" {
					return ownershipSegmentTask(t, current, NewSegmentActionWithScope(1, ActionTypeGrow, membershipChannel, 10, querypb.DataScope_Historical, 100))
				}
				return ownershipChannelTask(t, current, NewChannelAction(1, ActionTypeGrow, membershipChannel))
			}
			require.Error(t, scheduler.Add(newReplacement()), "the replacement must wait for the old callback")
			release()
			waitRegistrationBarrier(t, oldDone)
			require.Zero(t, ownershipReservations(scheduler))
			replacement := newReplacement()
			require.NoError(t, scheduler.Add(replacement))
			require.True(t, ex.Execute(replacement, 0))
			waitRegistrationBarrier(t, newDone)
			require.Empty(t, ex.executingTasks.Collect())
		})
	}
}

func TestReplicaUpdateSerializesSameCollectionAdmission(t *testing.T) {
	replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
	scheduler := newChannelMembershipScheduler(t, replica)
	otherReplica := meta.NewReplica(&querypb.Replica{
		ID: 872, CollectionID: 2000, ResourceGroup: meta.DefaultResourceGroupName,
		Nodes: []int64{14},
	})
	require.NoError(t, scheduler.meta.Put(scheduler.ctx, otherReplica))
	setCollectionLoaded(t, scheduler, 2000, true)
	old := ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
	late := ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 20))
	require.NoError(t, scheduler.Add(old))
	other, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1), 2000,
		otherReplica, NewChannelAction(14, ActionTypeGrow, "other-collection-channel"))
	require.NoError(t, err)
	defer other.Cancel(nil)

	published, resume, putDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	putErr := make(chan error, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	// Pause in the real publication hook, after indexes change but before
	// stale tasks retire, while applyReplicaUpdate still owns the collection gate.
	scheduler.meta.SetReplicaUpdateHook(func(collectionID int64, publish func()) {
		scheduler.applyReplicaUpdate(collectionID, func() {
			publish()
			if collectionID == membershipCollectionID {
				close(published)
				<-resume
			}
		})
	})
	go func() {
		defer close(putDone)
		putErr <- scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{14}, nil, nil, nil))
	}()
	defer func() { release(); waitRegistrationBarrier(t, putDone) }()
	waitRegistrationBarrier(t, published)
	require.False(t, scheduler.meta.Get(scheduler.ctx, membershipReplicaID).Contains(1))
	require.Equal(t, TaskStatusStarted, old.Status(), "the hook is paused before retirement")

	addStarted, addDone := make(chan struct{}), make(chan struct{})
	addErr := make(chan error, 1)
	go func() {
		defer close(addDone)
		close(addStarted)
		addErr <- scheduler.Add(late)
	}()
	defer func() { release(); waitRegistrationBarrier(t, addDone) }()
	waitRegistrationBarrier(t, addStarted)
	select {
	case <-addDone:
		t.Fatal("same-collection admission must wait until publication and retirement finish")
	case <-time.After(50 * time.Millisecond):
	}

	otherDone := make(chan struct{})
	otherErr := make(chan error, 1)
	go func() {
		defer close(otherDone)
		otherErr <- scheduler.Add(other)
	}()
	defer func() { release(); waitRegistrationBarrier(t, otherDone) }()
	waitRegistrationBarrier(t, otherDone)
	require.NoError(t, <-otherErr, "another collection must not wait for this publication")

	release()
	waitRegistrationBarrier(t, putDone)
	require.NoError(t, <-putErr)
	waitRegistrationBarrier(t, addDone)
	require.Error(t, <-addErr, "the delayed old snapshot must be checked against the published membership")
	require.Equal(t, TaskStatusCanceled, old.Status())
	require.Equal(t, TaskStatusCanceled, late.Status())
	require.Equal(t, 1, scheduler.tasks.Len())
	require.True(t, scheduler.isCurrent(other, taskWaiting))
	require.Zero(t, scheduler.segmentOwners.Len())
	require.Empty(t, scheduler.segmentTaskDelta.records)
}
