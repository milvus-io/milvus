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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type pendingGateScheduler struct {
	task.Scheduler
	pending map[int64][]*querypb.LoadingSegmentInfo
}

func (scheduler *pendingGateScheduler) PendingSegmentLoads() map[int64][]*querypb.LoadingSegmentInfo {
	return scheduler.pending
}

func TestBalanceRecoveryGateUsesExplicitDiffAndKeepsLegacyFallback(t *testing.T) {
	paramtable.Init()
	replica := epochQueueReplica(11, 1, "shared")
	checker := &BalanceChecker{
		nodeManager: session.NewNodeManager(),
		scheduler: &pendingGateScheduler{pending: map[int64][]*querypb.LoadingSegmentInfo{
			111: {{CollectionId: 1, SegmentId: 7, NumRows: 10}},
		}},
	}
	ctx := context.Background()
	require.True(t, checker.planningBlocks(ctx, replica, false), "legacy callers retain their conservative pending-load guard")
	for _, blocked := range []map[string]struct{}{nil, {}, {"other": {}}} {
		require.False(t, checker.planningBlocks(WithRecoveryBlockedRGs(ctx, blocked), replica, false), "an explicit diff must not classify any pending Grow as recovery")
	}
	blocked := map[string]struct{}{"shared": {}}
	recoveryCtx := WithRecoveryBlockedRGs(ctx, blocked)
	delete(blocked, "shared")
	require.True(t, recoveryBlockedRG(recoveryCtx, "shared"), "the cycle's recovery result must not change when the caller updates its map")
	require.True(t, checker.planningBlocks(recoveryCtx, replica, false))
	require.False(t, checker.planningBlocks(recoveryCtx, replica, true), "stopping balance is a separate draining phase")

	work, err := task.NewSegmentTask(ctx, time.Minute, utils.BalanceChecker, 1, replica, commonpb.LoadPriority_LOW,
		task.NewSegmentAction(112, task.ActionTypeGrow, "ch", 7))
	require.NoError(t, err)
	t.Cleanup(func() { work.Cancel(nil) })
	checker.trackEpochWork(work)
	checker.epochSequence++
	require.True(t, checker.planningBlocks(WithRecoveryBlockedRGs(ctx, nil), replica, false), "an existing balance epoch still blocks a new ordinary epoch")
	require.True(t, checker.planningBlocks(recoveryCtx, replica, true), "stopping retains its existing epoch gate")
}

func tickBalanceWithRecovery(fixture *epochQueueFixture, blocked map[string]struct{}) {
	fixture.checker.autoBalanceTs = time.Time{}
	fixture.checker.Check(WithRecoveryBlockedRGs(context.Background(), blocked))
}

func TestBalanceRecoveryGateDefersWholeRGWithoutAcceptedRecoveryTasks(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 1, map[int64]int64{1: 1000, 2: 100, 3: 10},
		epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"), epochQueueReplica(31, 3, "other"))
	blocked := map[string]struct{}{"shared": {}}
	// The preceding channel/segment checks found a gap but could not admit a
	// task. The explicit RG result must protect every collection sharing it.
	tickBalanceWithRecovery(fixture, blocked)
	require.Len(t, fixture.accepted, 1)
	require.EqualValues(t, 3, fixture.accepted[0].CollectionID())
	require.Equal(t, 2, fixture.checker.normalBalanceQueue.Len())
	tickBalanceWithRecovery(fixture, blocked)
	require.Len(t, fixture.accepted, 1)
	fixture.accepted[0].SetStatus(task.TaskStatusSucceeded)
	tickBalanceWithRecovery(fixture, blocked)
	require.Len(t, fixture.accepted, 2, "a free RG must enter another round while recovery still blocks its peers")
	require.Equal(t, []int64{31, 31}, fixture.balancer.plannedReplicas)

	tickBalanceWithRecovery(fixture, nil)
	require.Len(t, fixture.accepted, 3)
	require.EqualValues(t, 1, fixture.accepted[2].CollectionID(), "the deferred queue becomes eligible when recovery reports no gaps")
}

func TestBalanceRecoveryGateDefersOnlyAffectedReplicaRG(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 1, map[int64]int64{1: 1000},
		epochQueueReplica(11, 1, "recovering"), epochQueueReplica(12, 1, "other"))
	tickBalanceWithRecovery(fixture, map[string]struct{}{"recovering": {}})
	require.Len(t, fixture.accepted, 1)
	require.EqualValues(t, 12, fixture.accepted[0].ReplicaID())
	require.Equal(t, 1, fixture.checker.normalBalanceQueue.Len())
	tickBalanceWithRecovery(fixture, nil)
	require.Len(t, fixture.accepted, 2)
	require.EqualValues(t, 11, fixture.accepted[1].ReplicaID())
}

func TestBalanceRecoveryGateRetainsAcceptedRecoveryWork(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 1, map[int64]int64{1: 1000, 2: 100, 3: 10},
		epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"), epochQueueReplica(31, 3, "other"))
	recovery, err := task.NewSegmentTask(context.Background(), time.Minute, utils.SegmentChecker, 1,
		fixture.checker.meta.Get(context.Background(), 11), commonpb.LoadPriority_HIGH,
		task.NewSegmentActionWithScope(112, task.ActionTypeGrow, "ch", 7, querypb.DataScope_Historical, 10))
	require.NoError(t, err)
	require.NoError(t, fixture.checker.scheduler.Add(recovery))
	tickBalanceWithRecovery(fixture, map[string]struct{}{"shared": {}})
	require.Len(t, fixture.accepted, 2)
	require.Equal(t, task.TaskStatusStarted, recovery.Status(), "the ordinary gate must not cancel admitted recovery")
	require.EqualValues(t, 3, fixture.accepted[1].CollectionID())
	require.Equal(t, []int64{31}, fixture.balancer.plannedReplicas, "in-flight recovery protects every collection in its RG")
}

func TestStoppingBalanceRetainsDeferredQueueAcrossOrdinaryWork(t *testing.T) {
	fixture := newEpochQueueFixture(t, 1, 1, map[int64]int64{1: 1000, 2: 100, 3: 10},
		epochQueueReplica(11, 1, "draining"), epochQueueReplica(21, 2, "draining"), epochQueueReplica(31, 3, "other"))
	fixture.tick() // An older ordinary epoch is still running in the draining RG.
	for _, pair := range [][2]int64{{11, 1}, {21, 2}} {
		require.NoError(t, fixture.checker.meta.ReplicaManager.Put(context.Background(), meta.NewReplica(&querypb.Replica{
			ID: pair[0], CollectionID: pair[1], ResourceGroup: "draining",
			Nodes: []int64{pair[0]*10 + 1, pair[0]*10 + 2}, RoNodes: []int64{pair[0]*10 + 3},
		})))
	}
	require.NoError(t, paramtable.Get().Save(paramtable.Get().QueryCoordCfg.EnableStoppingBalance.Key, "true"))
	stopFactory := mockey.Mock((*balance.BalancerFactory).GetStoppingBalancer).Return(&balance.StoppingBalancer{}).Build()
	t.Cleanup(func() { stopFactory.UnPatch() })
	stopPlan := mockey.Mock((*balance.StoppingBalancer).BalanceReplica).To(
		func(_ *balance.StoppingBalancer, ctx context.Context, replica *meta.Replica) ([]assign.SegmentAssignPlan, []assign.ChannelAssignPlan) {
			plans, channels := fixture.balancer.BalanceReplica(ctx, replica)
			plans[0].From = replica.GetRONodes()[0]
			return plans, channels
		}).Build()
	t.Cleanup(func() { stopPlan.UnPatch() })
	blocked := map[string]struct{}{"draining": {}}
	tickBalanceWithRecovery(fixture, blocked)
	require.Len(t, fixture.accepted, 2)
	require.EqualValues(t, 3, fixture.accepted[1].CollectionID(), "ordinary work in another RG may progress while draining waits for its old epoch")
	require.NotNil(t, fixture.checker.stoppingBalanceQueue)
	require.Equal(t, 2, fixture.checker.stoppingBalanceQueue.Len(), "ordinary admission must retain deferred draining collections")

	fixture.accepted[0].SetStatus(task.TaskStatusSucceeded)
	tickBalanceWithRecovery(fixture, blocked)
	require.Len(t, fixture.accepted, 3)
	require.EqualValues(t, 1, fixture.accepted[2].CollectionID())
	require.Equal(t, commonpb.LoadPriority_HIGH, fixture.accepted[2].(*task.SegmentTask).LoadPriority(), "draining ignores the ordinary recovery gate and keeps HIGH load priority")
	fixture.accepted[2].SetStatus(task.TaskStatusSucceeded)
	tickBalanceWithRecovery(fixture, blocked)
	require.Len(t, fixture.accepted, 4)
	require.EqualValues(t, 2, fixture.accepted[3].CollectionID(), "the smaller deferred collection must run before another draining round for the larger one")
	require.Equal(t, commonpb.LoadPriority_HIGH, fixture.accepted[3].(*task.SegmentTask).LoadPriority())
}
