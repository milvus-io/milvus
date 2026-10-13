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

package checkers

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Keep queue construction, Check, admission, and epoch reconciliation real;
// the balancer supplies deterministic work without requiring native loading.
type epochQueueBalancer struct {
	balance.Balance
	plannedReplicas []int64
	noPlans         map[int64]bool
}

func (b *epochQueueBalancer) BalanceReplica(_ context.Context, replica *meta.Replica) ([]assign.SegmentAssignPlan, []assign.ChannelAssignPlan) {
	b.plannedReplicas = append(b.plannedReplicas, replica.GetID())
	if b.noPlans[replica.GetID()] {
		return nil, nil
	}
	nodes := replica.GetRWNodes()
	return []assign.SegmentAssignPlan{{
		Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
			ID: int64(len(b.plannedReplicas)), CollectionID: replica.GetCollectionID(),
			InsertChannel: "ch", NumOfRows: 1,
		}},
		Replica: replica, From: nodes[0], To: nodes[1],
	}}, nil
}

type epochQueueFixture struct {
	checker  *BalanceChecker
	balancer *epochQueueBalancer
	accepted []task.Task
}

func newEpochQueueFixture(t *testing.T, batch, maxChecks int, rows map[int64]int64, replicas ...*meta.Replica) *epochQueueFixture {
	t.Helper()
	paramtable.Init()
	params := paramtable.Get()
	config := &params.QueryCoordCfg
	for key, value := range map[string]string{
		config.EnableStoppingBalance.Key:              "false",
		config.AutoBalance.Key:                        "true",
		config.BalanceTriggerOrder.Key:                "ByRowCount",
		config.BalanceSegmentBatchSize.Key:            strconv.Itoa(batch),
		config.BalanceChannelBatchSize.Key:            "1",
		config.BalanceCheckCollectionMaxCount.Key:     strconv.Itoa(maxChecks),
		config.EnableBalanceOnMultipleCollections.Key: "true",
	} {
		previous := paramtable.GetBaseTable().Get(key)
		require.NoError(t, params.Save(key, value))
		t.Cleanup(func() { require.NoError(t, params.Save(key, previous)) })
	}

	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil)
	metadata := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
		ReplicaManager:    meta.NewReplicaManager(nil, catalog),
	}
	for _, replica := range replicas {
		require.NoError(t, metadata.ReplicaManager.Put(context.Background(), replica))
	}
	nodes := session.NewNodeManager()
	dist := meta.NewDistributionManager(nodes)
	for collectionID := range rows {
		require.NoError(t, metadata.PutCollectionWithoutSave(context.Background(), &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: collectionID, Status: querypb.LoadStatus_Loaded},
		}))
		dist.ChannelDistManager.Update(collectionID, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: fmt.Sprintf("ch-%d", collectionID)},
			View:         &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})
	}
	targets := meta.NewMockTargetManager(t)
	targets.On("IsNextTargetExist", mock.Anything, mock.Anything).Return(true)
	targets.On("IsCurrentTargetReady", mock.Anything, mock.Anything).Return(true)
	targets.On("GetCollectionRowCount", mock.Anything, mock.Anything, meta.CurrentTargetFirst).
		Return(func(_ context.Context, collectionID int64, _ int32) int64 { return rows[collectionID] })
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).
		Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	fixture := &epochQueueFixture{balancer: &epochQueueBalancer{}}
	scheduler.EXPECT().Add(mock.Anything).Run(func(work task.Task) {
		fixture.accepted = append(fixture.accepted, work)
	}).Return(nil)
	fixture.checker = NewBalanceChecker(metadata, dist, targets, nodes, scheduler)
	balance.ResetGlobalBalancerFactoryForTest()
	balance.InitGlobalBalancerFactory(scheduler, nodes, dist, targets)
	t.Cleanup(balance.ResetGlobalBalancerFactoryForTest)
	patch := mockey.Mock((*balance.BalancerFactory).GetBalancer).Return(fixture.balancer).Build()
	t.Cleanup(func() { patch.UnPatch() })
	t.Cleanup(func() {
		for _, work := range fixture.accepted {
			work.Cancel(nil)
		}
	})
	return fixture
}

func epochQueueReplica(id, collectionID int64, rg string) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID: id, CollectionID: collectionID, ResourceGroup: rg,
		Nodes: []int64{id*10 + 1, id*10 + 2},
	})
}

func (f *epochQueueFixture) tick() {
	// Advance through real checker invocations without waiting for the existing
	// auto-balance interval or changing task deadlines.
	f.checker.autoBalanceTs = time.Time{}
	f.checker.Check(context.Background())
}

func TestBalanceEpochQueueRetainsBlockedCollectionAndAllowsOtherRG(t *testing.T) {
	for _, maxChecks := range []int{100, 1} {
		t.Run(fmt.Sprintf("maxChecks=%d", maxChecks), func(t *testing.T) {
			fixture := newEpochQueueFixture(t, 1, maxChecks, map[int64]int64{1: 1000, 2: 100, 3: 10},
				epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"), epochQueueReplica(31, 3, "other"))
			fixture.tick()
			require.Len(t, fixture.accepted, 1)
			require.EqualValues(t, 1, fixture.accepted[0].CollectionID())

			fixture.tick()
			require.Len(t, fixture.accepted, 2, "an unrelated RG must progress while the first epoch is running")
			require.EqualValues(t, 3, fixture.accepted[1].CollectionID())
			require.Equal(t, 1, fixture.checker.normalBalanceQueue.Len(), "the blocked collection must keep its queue position")

			fixture.tick()
			require.Len(t, fixture.accepted, 2, "an entirely blocked round must neither spin nor reconstruct the queue")
			fixture.accepted[0].SetStatus(task.TaskStatusSucceeded)
			fixture.tick()
			require.Len(t, fixture.accepted, 3)
			require.EqualValues(t, 2, fixture.accepted[2].CollectionID(), "the waiting smaller collection must precede another round for the larger collection")
			require.Equal(t, []int64{11, 31, 21}, fixture.balancer.plannedReplicas)
		})
	}
}

func TestBalanceEpochQueueDefersOnlyBlockedReplicas(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 100, map[int64]int64{1: 1000, 2: 100},
		epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"), epochQueueReplica(22, 2, "other"))
	fixture.tick()
	fixture.tick()
	require.Len(t, fixture.accepted, 2)
	require.EqualValues(t, 22, fixture.accepted[1].ReplicaID(), "an eligible replica in the same collection must proceed in another RG")

	fixture.accepted[1].SetStatus(task.TaskStatusSucceeded)
	fixture.tick()
	require.Len(t, fixture.accepted, 2, "a replica already checked in this queue round must wait for the next round")
	fixture.tick()
	require.Len(t, fixture.accepted, 3, "another RG must start its next batch without waiting for the blocked RG")
	require.EqualValues(t, 22, fixture.accepted[2].ReplicaID())
	fixture.accepted[0].SetStatus(task.TaskStatusSucceeded)
	fixture.tick()
	require.Len(t, fixture.accepted, 4)
	require.EqualValues(t, 21, fixture.accepted[3].ReplicaID())
	require.Equal(t, []int64{11, 22, 22, 21}, fixture.balancer.plannedReplicas)
}

func TestBalanceEpochQueuePartialNoPlanDoesNotStarveOtherRG(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 1, map[int64]int64{1: 1000, 2: 100, 3: 10},
		epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"),
		epochQueueReplica(22, 2, "balanced"), epochQueueReplica(31, 3, "other"))
	fixture.balancer.noPlans = map[int64]bool{22: true}
	fixture.tick()
	fixture.tick()
	require.Len(t, fixture.accepted, 1, "checking the balanced replica consumes the first round's check budget")
	fixture.tick()
	require.Len(t, fixture.accepted, 2, "the already checked replica must leave the next tick's budget for another RG")
	require.EqualValues(t, 3, fixture.accepted[1].CollectionID())
	require.Equal(t, []int64{11, 22, 31}, fixture.balancer.plannedReplicas)

	fixture.accepted[1].SetStatus(task.TaskStatusSucceeded)
	fixture.tick()
	require.Len(t, fixture.accepted, 3, "a consumed RG must enter a fresh round even while the old RG remains blocked")
	require.EqualValues(t, 3, fixture.accepted[2].CollectionID())
}

func TestBalanceEpochQueueAdmitsNewReplicaWhileSiblingWaits(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 1, map[int64]int64{1: 1000, 2: 100},
		epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"), epochQueueReplica(22, 2, "other"))
	fixture.tick()
	fixture.tick()
	require.NoError(t, fixture.checker.meta.ReplicaManager.Put(context.Background(), epochQueueReplica(23, 2, "new")))
	fixture.tick()
	require.Len(t, fixture.accepted, 3)
	require.EqualValues(t, 23, fixture.accepted[2].ReplicaID(), "a live replica addition must not be hidden by the deferred item's checked set")
}

func TestBalanceEpochQueueAllowsJoiningCurrentInvocation(t *testing.T) {
	fixture := newEpochQueueFixture(t, 2, 100, map[int64]int64{1: 1000, 2: 100},
		epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"))
	fixture.tick()
	require.Len(t, fixture.accepted, 2, "a new epoch must accept other collections in the same planning invocation")
	require.Equal(t, []int64{11, 21}, fixture.balancer.plannedReplicas)
}
