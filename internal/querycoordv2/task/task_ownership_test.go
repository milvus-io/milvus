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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

func ownershipReplica(id int64) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{ID: id, CollectionID: membershipCollectionID,
		ResourceGroup: meta.DefaultResourceGroupName, Nodes: []int64{1, 12, 14}, RwSqNodes: []int64{1, 12, 14}})
}

func ownershipSegmentTask(t *testing.T, replica *meta.Replica, actions ...Action) *SegmentTask {
	t.Helper()
	task, err := NewSegmentTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID,
		replica, commonpb.LoadPriority_LOW, actions...)
	require.NoError(t, err)
	t.Cleanup(func() { task.Cancel(nil) })
	return task
}

func ownershipChannelTask(t *testing.T, replica *meta.Replica, actions ...Action) *ChannelTask {
	t.Helper()
	task, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID, replica, actions...)
	require.NoError(t, err)
	t.Cleanup(func() { task.Cancel(nil) })
	return task
}

func retireOwnershipTask(scheduler *taskScheduler, task Task) {
	scheduler.lifecycleMu.RLock()
	defer scheduler.lifecycleMu.RUnlock()
	scheduler.collKeyLock.Lock(task.CollectionID())
	defer scheduler.collKeyLock.Unlock(task.CollectionID())
	scheduler.remove(task)
}

func TestOrphanTaskKeysAreNodeScoped(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, ownershipReplica(1))
			for _, node := range []int64{1, 12} {
				var task Task
				if kind == "segment" {
					task = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(node, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
				} else {
					task = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(node, ActionTypeReduce, membershipChannel))
				}
				require.NoError(t, scheduler.Add(task), "orphan cleanup on another node must remain independent")
			}
			require.Equal(t, 2, scheduler.tasks.Len())
			if kind == "segment" {
				require.Equal(t, 2, scheduler.segmentTasks.Len())
			} else {
				require.Equal(t, 2, scheduler.channelTasks.Len())
			}
		})
	}
}

func TestSchedulerPhysicalSegmentOwnershipAcrossReplicas(t *testing.T) {
	for _, oldType := range []ActionType{ActionTypeGrow, ActionTypeReduce} {
		t.Run(oldType.String(), func(t *testing.T) {
			first, second := ownershipReplica(1), ownershipReplica(2)
			scheduler := newChannelMembershipScheduler(t, first)
			old := ownershipSegmentTask(t, first, NewSegmentActionWithScope(1, oldType, membershipChannel, 10, querypb.DataScope_Historical, 100))
			require.NoError(t, scheduler.Add(old))
			collision := ownershipSegmentTask(t, second, NewSegmentActionWithScope(1, ActionTypeGrow, membershipChannel, 10, querypb.DataScope_Historical, 100))
			collision.SetPriority(TaskPriorityHigh)
			require.ErrorContains(t, scheduler.Add(collision), "same segment on this node")
			otherNode := ownershipSegmentTask(t, second, NewSegmentActionWithScope(12, ActionTypeGrow, membershipChannel, 10, querypb.DataScope_Historical, 100))
			require.NoError(t, scheduler.Add(otherNode))
			retireOwnershipTask(scheduler, old)
			replacement := ownershipSegmentTask(t, first, NewSegmentActionWithScope(1, ActionTypeGrow, membershipChannel, 10, querypb.DataScope_Historical, 100))
			require.NoError(t, scheduler.Add(replacement))
		})
	}
}

func TestSchedulerPhysicalChannelOwnershipAcrossReplicas(t *testing.T) {
	scheduler := newChannelMembershipScheduler(t, ownershipReplica(1))
	old := ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
	require.NoError(t, scheduler.Add(old))
	newTask := ownershipChannelTask(t, ownershipReplica(2), NewChannelAction(1, ActionTypeGrow, membershipChannel))
	require.ErrorContains(t, scheduler.Add(newTask), "same channel on this node")
	otherNode := ownershipChannelTask(t, ownershipReplica(2), NewChannelAction(12, ActionTypeGrow, membershipChannel))
	require.NoError(t, scheduler.Add(otherNode))
}

func TestSchedulerMoveReservesBothEndpoints(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			replica := ownershipReplica(1)
			scheduler := newChannelMembershipScheduler(t, replica)
			scheduler.distMgr.ChannelDistManager.Update(1, membershipChannelOn(1, 1))
			scheduler.distMgr.ChannelDistManager.Update(12, membershipChannelOn(12, 2))
			scheduler.distMgr.SegmentDistManager.Update(12, utils.CreateTestSegment(membershipCollectionID, 1, 10, 12, 1, membershipChannel))
			var move Task
			if kind == "segment" {
				move = ownershipSegmentTask(t, replica, NewSegmentAction(14, ActionTypeGrow, membershipChannel, 10), NewSegmentAction(12, ActionTypeReduce, membershipChannel, 10))
			} else {
				move = ownershipChannelTask(t, replica, NewChannelAction(14, ActionTypeGrow, membershipChannel), NewChannelAction(12, ActionTypeReduce, membershipChannel))
			}
			require.NoError(t, scheduler.Add(move))
			for _, node := range []int64{12, 14} {
				var collision Task
				if kind == "segment" {
					collision = ownershipSegmentTask(t, ownershipReplica(2), NewSegmentAction(node, ActionTypeGrow, membershipChannel, 10))
				} else {
					collision = ownershipChannelTask(t, ownershipReplica(2), NewChannelAction(node, ActionTypeReduce, membershipChannel))
				}
				require.ErrorContains(t, scheduler.Add(collision), "on this node exists")
			}
		})
	}
}

func TestSchedulerPhysicalScopeOverlap(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second querypb.DataScope
		conflict      bool
	}{
		{"all blocks streaming", querypb.DataScope_All, querypb.DataScope_Streaming, true},
		{"streaming blocks all", querypb.DataScope_Streaming, querypb.DataScope_All, true},
		{"all blocks historical", querypb.DataScope_All, querypb.DataScope_Historical, true},
		{"historical blocks all", querypb.DataScope_Historical, querypb.DataScope_All, true},
		{"copies independent", querypb.DataScope_Historical, querypb.DataScope_Streaming, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, ownershipReplica(1))
			first := ownershipSegmentTask(t, ownershipReplica(1), NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, tc.first, 100))
			second := ownershipSegmentTask(t, ownershipReplica(2), NewSegmentActionWithScope(1, ActionTypeGrow, membershipChannel, 10, tc.second, 100))
			require.NoError(t, scheduler.Add(first))
			err := scheduler.Add(second)
			if tc.conflict {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchedulerPriorityReplacementOnlyBeforeProcessing(t *testing.T) {
	for _, kind := range []string{"segment", "channel", "leader", "drop index"} {
		for _, stage := range []taskStage{taskWaiting, taskProcessing} {
			t.Run(kind+"/"+map[taskStage]string{taskWaiting: "waiting", taskProcessing: "processing"}[stage], func(t *testing.T) {
				replica := ownershipReplica(1)
				scheduler := newChannelMembershipScheduler(t, replica)
				var old, replacement Task
				if kind == "segment" {
					old = ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeReduce, membershipChannel, 10))
					replacement = ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
				} else if kind == "drop index" {
					old = NewDropIndexTask(context.Background(), WrapIDSource(1), membershipCollectionID, replica, 10,
						NewDropIndexAction(1, ActionTypeDropIndex, membershipChannel, []int64{1000}))
					t.Cleanup(func() { old.Cancel(nil) })
					replacement = ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
				} else if kind == "leader" {
					old = NewLeaderSegmentTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID, replica, 1,
						NewLeaderAction(1, 1, ActionTypeGrow, membershipChannel, 10, 1))
					t.Cleanup(func() { old.Cancel(nil) })
					replacement = ownershipSegmentTask(t, replica, NewSegmentAction(1, ActionTypeGrow, membershipChannel, 10))
				} else {
					old = ownershipChannelTask(t, replica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
					replacement = ownershipChannelTask(t, replica, NewChannelAction(1, ActionTypeGrow, membershipChannel))
				}
				require.NoError(t, scheduler.Add(old))
				scheduler.tasks.Insert(old.ID(), scheduledTask{task: old, stage: stage})
				replacement.SetPriority(TaskPriorityHigh)
				if stage == taskProcessing {
					require.Error(t, scheduler.Add(replacement))
					require.Equal(t, TaskStatusStarted, old.Status(), "processing ownership cannot be preempted by priority")
					require.True(t, scheduler.isCurrent(old, stage))
					return
				}
				require.NoError(t, scheduler.Add(replacement))
				require.Equal(t, TaskStatusCanceled, old.Status())
				retireOwnershipTask(scheduler, old) // Stale snapshot retries retirement after replacement.
				require.True(t, scheduler.isCurrent(replacement, taskWaiting))
				if _, ok := replacement.(*SegmentTask); ok {
					require.Len(t, scheduler.segmentTaskDelta.records, 1)
					require.Contains(t, scheduler.segmentTaskDelta.records, replacement.ID())
				} else {
					require.Equal(t, 1, scheduler.channelTaskDelta.taskIDRecords.Len())
					require.True(t, scheduler.channelTaskDelta.taskIDRecords.Contain(replacement.ID()))
				}
				segments, channels := taskResourceKeys(replacement)
				for _, key := range segments {
					owner, ok := scheduler.segmentOwners.Get(key)
					require.True(t, ok)
					require.Same(t, replacement, owner)
				}
				for _, key := range channels {
					owner, ok := scheduler.channelOwners.Get(key)
					require.True(t, ok)
					require.Same(t, replacement, owner)
				}
			})
		}
	}
}

func TestOrphanCleanupRechecksMembershipBeforeRPC(t *testing.T) {
	for _, kind := range []string{"segment", "channel"} {
		t.Run(kind, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, ownershipReplica(1))
			loaded := true
			patch := mockey.Mock((*meta.CollectionManager).Exist).To(func(_ *meta.CollectionManager, _ context.Context, _ int64) bool { return loaded }).Build()
			defer patch.UnPatch()
			ex := NewExecutor(1, scheduler.meta, scheduler.distMgr, nil, nil, scheduler.cluster, scheduler.nodeMgr)
			var task Task
			if kind == "segment" {
				task = ownershipSegmentTask(t, meta.NilReplica, NewSegmentActionWithScope(1, ActionTypeReduce, membershipChannel, 10, querypb.DataScope_Historical, 100))
			} else {
				task = ownershipChannelTask(t, meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
			}
			require.ErrorContains(t, scheduler.checkStale(task, true), "reassigned")
			loaded = false
			require.NoError(t, scheduler.checkStale(task, true), "ReleaseCollection must still clean retained replica metadata")
			loaded = true
			// Invoke the real callback after reassignment. An unexpected cluster RPC
			// fails this test; nil target metadata also proves the check runs first.
			ex.executingTasks.Insert(task.Index())
			if kind == "segment" {
				ex.nonChannelTaskNum.Inc()
				ex.releaseSegment(task.(*SegmentTask), 0)
			} else {
				ex.channelTaskNum.Inc()
				require.ErrorContains(t, ex.unsubscribeChannel(task.(*ChannelTask), 0), "reassigned")
			}
			require.Equal(t, TaskStatusCanceled, task.Status())
			require.Empty(t, ex.executingTasks.Collect())
		})
	}
}
