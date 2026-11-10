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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	require.Empty(t, scheduler.segmentTaskDelta.records)
	require.Zero(t, scheduler.channelTaskDelta.taskIDRecords.Len())
}

// ReplicaManager does not notify the scheduler. A plan made stale by a replica
// update is retired by the next gate it meets: a Dispatch of one of its nodes,
// or the admission of a task for the same key.
func TestReplicaUpdateRetiresSegmentLoad(t *testing.T) {
	for _, stage := range []string{"waiting", "processing"} {
		for _, update := range []string{"put", "remove_node"} {
			for _, gate := range []string{"dispatch", "admission"} {
				t.Run(stage+"/"+update+"/"+gate, func(t *testing.T) {
					replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
					scheduler := newChannelMembershipScheduler(t, replica)
					old := newTestSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
					late := newTestSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
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
					require.Error(t, scheduler.Add(late), "an old checker snapshot must not revive a stale load")
					require.Equal(t, TaskStatusCanceled, late.Status())
					current := scheduler.meta.Get(scheduler.ctx, membershipReplicaID)
					replacement := newTestSegmentTask(t, current, NewSegmentAction(14, ActionTypeGrow, membershipChannel, 10))
					if gate == "dispatch" {
						scheduler.Dispatch(1)
						requireReplicaUpdateTaskRetired(t, scheduler, old)
						require.NoError(t, scheduler.Add(replacement))
						return
					}
					// No Dispatch has run: admission itself must retire the stale
					// incumbent instead of rejecting the plan that replaces it.
					require.NoError(t, scheduler.Add(replacement), "a stale incumbent must not hold the logical segment key")
					require.Equal(t, TaskStatusCanceled, old.Status())
					require.Error(t, old.Err())
					require.Equal(t, 1, scheduler.tasks.Len())
					require.True(t, scheduler.isCurrent(replacement, taskWaiting))
				})
			}
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
				old = newTestSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				late = newTestSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				scheduler.distMgr.SegmentDistManager.Update(1, utils.CreateTestSegment(membershipCollectionID, 1, 10, 1, 1, membershipChannel))
			} else {
				old = newTestChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
				late = newTestChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
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
			scheduler.Dispatch(1)
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
				cleanup = newTestSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
			} else {
				cleanup = newTestChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
				scheduler.distMgr.ChannelDistManager.Update(1, membershipChannelOn(1, 1))
			}
			require.NoError(t, scheduler.Add(cleanup))
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{1, 14}, nil, nil, nil)))
			scheduler.Dispatch(1)
			require.Equal(t, TaskStatusStarted, cleanup.Status(), "retained replica metadata must not stop ReleaseCollection cleanup")
			require.True(t, scheduler.isCurrent(cleanup, taskProcessing))
		})
	}
}

func TestReplicaUpdatePreservesROCopyMaintenance(t *testing.T) {
	for _, typ := range []ActionType{ActionTypeGrow, ActionTypeReduce, ActionTypeUpdate, ActionTypeReopen} {
		t.Run(typ.String(), func(t *testing.T) {
			replica := membershipReplica([]int64{1, 14}, nil, nil, nil)
			scheduler := newChannelMembershipScheduler(t, replica)
			task := newTestSegmentTask(t, replica, NewSegmentAction(1, typ, membershipChannel, 10))
			require.NoError(t, scheduler.Add(task))
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, membershipReplica([]int64{14}, []int64{1}, nil, nil)))
			scheduler.Dispatch(1)
			if typ == ActionTypeGrow {
				requireReplicaUpdateTaskRetired(t, scheduler, task)
			} else {
				require.Equal(t, TaskStatusStarted, task.Status())
				require.True(t, scheduler.isCurrent(task, taskProcessing))
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
		scheduler.Dispatch(14)
		require.Equal(t, TaskStatusStarted, task.Status(), "membership checks must use the delegator, not the RO worker")
		require.True(t, scheduler.isCurrent(task, taskProcessing))
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
	scheduler.Dispatch(14)
	requireReplicaUpdateTaskRetired(t, scheduler, old)
	require.NotNil(t, scheduler.nodeMgr.Get(1), "the worker remains alive but has left this replica")
	require.Error(t, scheduler.Add(late), "an old checker snapshot must not restore a route to the departed worker")
	require.Equal(t, TaskStatusCanceled, late.Status())
	require.Zero(t, scheduler.tasks.Len())
}

// ReplicaObserver removes an RO node from its replica as soon as the node's
// distribution is empty. That can happen before the Dispatch that reports the
// balance finished, and must not turn a completed move into a canceled one.
func TestReplicaUpdateKeepsMoveWhoseSourceAlreadyLeft(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		for _, released := range []bool{true, false} {
			name := kind + "/source_still_reported"
			if released {
				name = kind + "/source_released"
			}
			t.Run(name, func(t *testing.T) {
				replica := membershipReplica([]int64{14}, []int64{1}, nil, nil)
				scheduler := newChannelMembershipScheduler(t, replica)
				segment := func(node int64) *meta.Segment {
					return utils.CreateTestSegment(membershipCollectionID, 1, 10, node, 1, membershipChannel)
				}
				var task Task
				if kind == "segment" {
					scheduler.distMgr.ChannelDistManager.Update(14, membershipChannelOn(14, 1))
					scheduler.distMgr.SegmentDistManager.Update(1, segment(1))
					task = newTestSegmentTask(t, replica,
						NewSegmentAction(14, ActionTypeGrow, membershipChannel, 10),
						NewSegmentAction(1, ActionTypeReduce, membershipChannel, 10))
				} else {
					scheduler.distMgr.ChannelDistManager.Update(1, membershipChannelOn(1, 1))
					task = newTestChannelTask(t, replica,
						NewChannelAction(14, ActionTypeGrow, membershipChannel),
						NewChannelAction(1, ActionTypeReduce, membershipChannel))
				}
				require.NoError(t, scheduler.Add(task))
				scheduler.tryPromoteAll()
				require.True(t, scheduler.isCurrent(task, taskProcessing))

				// Both RPCs returned and the destination reports its copy.
				for _, action := range task.Actions() {
					if action, ok := action.(*SegmentAction); ok {
						action.rpcReturned.Store(true)
					}
				}
				if kind == "segment" {
					scheduler.distMgr.SegmentDistManager.Update(14, segment(14))
					if released {
						scheduler.distMgr.SegmentDistManager.Update(1)
					}
				} else {
					scheduler.distMgr.ChannelDistManager.Update(14, membershipChannelOn(14, 2))
					if released {
						scheduler.distMgr.ChannelDistManager.Update(1)
					}
				}
				require.NoError(t, scheduler.meta.RemoveNode(scheduler.ctx, membershipCollectionID, membershipReplicaID, 1))
				require.False(t, scheduler.meta.Get(scheduler.ctx, membershipReplicaID).Contains(1))

				scheduler.Dispatch(1)
				require.Zero(t, scheduler.tasks.Len())
				if released {
					require.Equal(t, TaskStatusSucceeded, task.Status())
					require.NoError(t, task.Err())
				} else {
					// The copy now belongs to whoever owns node 1, not to this plan.
					require.Equal(t, TaskStatusCanceled, task.Status())
					require.ErrorIs(t, task.Err(), merr.ErrNodeStateUnexpected)
				}
			})
		}
	}
}
