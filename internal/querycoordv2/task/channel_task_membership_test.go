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

	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	membershipCollectionID int64 = 1000
	membershipReplicaID    int64 = 871
	membershipChannel            = "membership-channel"
)

func newChannelMembershipScheduler(t *testing.T, replica *meta.Replica) *taskScheduler {
	t.Helper()
	paramtable.Init()
	ctx := context.Background()
	nodes := session.NewNodeManager()
	for _, id := range []int64{1, 12, 14} {
		nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id}))
	}
	m := meta.NewMeta(func() (int64, error) { return 1, nil }, versionProbeCatalog{}, nodes, metacache.NewMetaStore(nil))
	require.NoError(t, m.Put(ctx, replica))
	cluster := session.NewMockCluster(t)
	scheduler := NewScheduler(ctx, m, meta.NewDistributionManager(nodes), nil, nil, cluster, nodes)
	t.Cleanup(scheduler.Stop)
	return scheduler
}

func membershipReplica(rw, ro, sqrw, sqro []int64) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID: membershipReplicaID, CollectionID: membershipCollectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         rw, RoNodes: ro, RwSqNodes: sqrw, RoSqNodes: sqro,
	})
}

func membershipChannelOn(node, version int64) *meta.DmChannel {
	return &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: membershipCollectionID, ChannelName: membershipChannel},
		Node:         node, Version: version,
		View: &meta.LeaderView{
			ID: node, CollectionID: membershipCollectionID, Channel: membershipChannel,
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	}
}

func membershipTask(t *testing.T, replica *meta.Replica, destination int64, move bool) *ChannelTask {
	t.Helper()
	actions := []Action{NewChannelAction(destination, ActionTypeGrow, membershipChannel)}
	if move {
		actions = append(actions, NewChannelAction(12, ActionTypeReduce, membershipChannel))
	}
	task, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID, replica, actions...)
	require.NoError(t, err)
	t.Cleanup(func() { task.Cancel(nil) })
	return task
}

func requireMembershipTaskRemoved(t *testing.T, scheduler *taskScheduler, task *ChannelTask) {
	t.Helper()
	require.Equal(t, TaskStatusCanceled, task.Status())
	require.ErrorIs(t, task.Err(), merr.ErrNodeStateUnexpected)
	require.ErrorContains(t, task.Err(), "channel destination no longer belongs to replica")
	require.ErrorIs(t, task.Context().Err(), context.Canceled)
	require.Zero(t, scheduler.tasks.Len())
	require.Zero(t, scheduler.waitQueue.Len())
	require.Zero(t, scheduler.processQueue.LenByNode(1))
	require.Zero(t, scheduler.processQueue.LenByNode(12))
	require.Zero(t, scheduler.GetChannelTaskNum())
	_, exists := scheduler.channelTasks.Get(replicaChannelIndex{membershipReplicaID, membershipChannel})
	require.False(t, exists)
	require.Zero(t, scheduler.GetChannelTaskDelta(1, membershipCollectionID))
	require.Zero(t, scheduler.GetChannelTaskDelta(12, membershipCollectionID))
	require.Zero(t, scheduler.channelTaskDelta.taskIDRecords.Len())
}

func TestChannelTaskQueuedDestinationRemoved(t *testing.T) {
	for _, move := range []bool{false, true} {
		name := "grow"
		if move {
			name = "move"
		}
		t.Run(name, func(t *testing.T) {
			replica := membershipReplica(nil, nil, []int64{1, 14}, []int64{12})
			scheduler := newChannelMembershipScheduler(t, replica)
			if move {
				scheduler.distMgr.ChannelDistManager.Update(12, membershipChannelOn(12, 2))
			}
			task := membershipTask(t, replica, 1, move)
			require.NoError(t, scheduler.Add(task))
			require.Equal(t, 1, scheduler.GetChannelTaskDelta(1, membershipCollectionID))
			// The task retains its original snapshot; the node stays alive and
			// disappears entirely from current membership, including both RO sets.
			current := membershipReplica(nil, nil, []int64{14}, []int64{12})
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, current))
			require.True(t, replica.Contains(1))
			require.False(t, current.Contains(1))
			require.NotNil(t, scheduler.nodeMgr.Get(1))
			scheduler.Dispatch(1)
			requireMembershipTaskRemoved(t, scheduler, task)
			require.Zero(t, task.Step())
			replacement := membershipTask(t, current, 14, move)
			require.NoError(t, scheduler.Add(replacement), "the canceled task must release its deduplication slot")
		})
	}
}

func TestChannelMoveReportedDestinationRemoved(t *testing.T) {
	for _, reassigned := range []bool{false, true} {
		name := "removed"
		if reassigned {
			name = "reassigned_to_other_replica"
		}
		t.Run(name, func(t *testing.T) {
			replica := membershipReplica(nil, nil, []int64{1, 14}, []int64{12})
			scheduler := newChannelMembershipScheduler(t, replica)
			dist := scheduler.distMgr.ChannelDistManager
			dist.Update(12, membershipChannelOn(12, 2))
			task := membershipTask(t, replica, 1, true)
			require.NoError(t, scheduler.Add(task))
			scheduler.tryPromoteAll()
			// Watch succeeded and is reported, but the old source is still the
			// leader, so the move cannot advance to its Reduce action.
			dist.Update(1, membershipChannelOn(1, 1))
			scheduler.Dispatch(1)
			require.True(t, task.Actions()[0].IsFinished(scheduler.distMgr))
			require.Zero(t, task.Step())
			require.Equal(t, TaskStatusStarted, task.Status())

			current := membershipReplica(nil, nil, []int64{14}, []int64{12})
			require.NoError(t, scheduler.meta.Put(scheduler.ctx, current))
			if reassigned {
				other := meta.NewReplica(&querypb.Replica{
					ID: 872, CollectionID: membershipCollectionID,
					ResourceGroup: meta.DefaultResourceGroupName, RwSqNodes: []int64{1},
				})
				require.NoError(t, scheduler.meta.Put(scheduler.ctx, other))
				require.Equal(t, int64(1), dist.GetShardLeader(membershipChannel, other).Node)
			}
			scheduler.Dispatch(12)
			requireMembershipTaskRemoved(t, scheduler, task)
			require.Zero(t, task.Step(), "stale move must not advance to Reduce")
			require.Len(t, dist.GetByFilter(meta.WithChannelName2Channel(membershipChannel)), 2,
				"canceling a task must not delete subscriptions, including a target now owned by another replica")

			replacement := membershipTask(t, current, 14, true)
			require.NoError(t, scheduler.Add(replacement))
			scheduler.tryPromoteAll()
			dist.Update(14, membershipChannelOn(14, 1))
			scheduler.Dispatch(14)
			require.Zero(t, replacement.Step(), "source 12 is still leader: Reduce must remain blocked")
			require.Equal(t, TaskStatusStarted, replacement.Status())
			dist.Update(14, membershipChannelOn(14, 3))
			scheduler.Dispatch(14)
			require.Equal(t, 1, replacement.Step(), "only after 14 becomes leader may Reduce 12 proceed")
			require.Equal(t, int64(12), replacement.Actions()[replacement.Step()].Node())
			dist.Update(12)
			scheduler.Dispatch(12)
			require.Equal(t, TaskStatusSucceeded, replacement.Status())
			require.Zero(t, scheduler.GetChannelTaskNum())
			require.Zero(t, scheduler.GetChannelTaskDelta(14, membershipCollectionID))
			require.Zero(t, scheduler.GetChannelTaskDelta(12, membershipCollectionID))
		})
	}
}

func TestChannelTaskMembershipPreservesExistingStates(t *testing.T) {
	cases := []struct {
		name     string
		replica  *meta.Replica
		stopping bool
		err      error
	}{
		{name: "rw", replica: membershipReplica([]int64{1}, nil, nil, nil)},
		{name: "sq_rw", replica: membershipReplica(nil, nil, []int64{1}, nil)},
		{name: "ro", replica: membershipReplica(nil, []int64{1}, nil, nil), err: merr.ErrNodeStateUnexpected},
		{name: "sq_ro", replica: membershipReplica(nil, nil, nil, []int64{1}), err: merr.ErrNodeStateUnexpected},
		{name: "stopping", replica: membershipReplica(nil, nil, []int64{1}, nil), stopping: true, err: merr.ErrNodeOffline},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := newChannelMembershipScheduler(t, tc.replica)
			if tc.stopping {
				scheduler.nodeMgr.Get(1).SetState(session.NodeStateStopping)
			}
			task := membershipTask(t, tc.replica, 1, false)
			for _, promote := range []bool{false, true} {
				err := scheduler.checkStale(task, promote)
				if tc.err == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.err)
				}
			}
		})
	}
	t.Run("nil_replica_reduce", func(t *testing.T) {
		scheduler := newChannelMembershipScheduler(t, membershipReplica(nil, nil, []int64{14}, nil))
		task, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID,
			meta.NilReplica, NewChannelAction(1, ActionTypeReduce, membershipChannel))
		require.NoError(t, err)
		defer task.Cancel(nil)
		require.NoError(t, scheduler.checkStale(task, true))
	})
	t.Run("leader_grow_uses_leader_not_ro_worker", func(t *testing.T) {
		replica := membershipReplica([]int64{14}, []int64{1}, nil, nil)
		scheduler := newChannelMembershipScheduler(t, replica)
		task := NewLeaderSegmentTask(context.Background(), time.Minute, WrapIDSource(1), membershipCollectionID,
			replica, 14, NewLeaderAction(14, 1, ActionTypeGrow, membershipChannel, 99, 1))
		defer task.Cancel(nil)
		require.NoError(t, scheduler.checkStale(task, true))
	})
}
