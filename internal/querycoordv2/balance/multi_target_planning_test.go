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

package balance

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newMultiTargetPlanningFixture(t *testing.T, counts []int, batch int) (*MultiTargetBalancer, *meta.Replica, map[int64]assign.PlanningNode, []assign.PlanningLoadKey) {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().QueryCoordCfg
	for _, setting := range []struct {
		item  *paramtable.ParamItem
		value string
	}{
		{&cfg.AutoBalanceChannel, "false"},
		{&cfg.BalanceSegmentBatchSize, strconv.Itoa(batch)},
		{&cfg.RowCountMaxSteps, "50"},
		{&cfg.SegmentCountMaxSteps, "50"},
		{&cfg.RandomMaxSteps, "10"},
		{&cfg.RowCountFactor, "1"},
		{&cfg.GlobalRowCountFactor, "1"},
		{&cfg.SegmentCountFactor, "1"},
		{&cfg.GlobalSegmentCountFactor, "1"},
		{&cfg.BalanceCostThreshold, "0.001"},
	} {
		key, previous := setting.item.Key, setting.item.GetValue()
		require.NoError(t, paramtable.Get().Save(key, setting.value))
		t.Cleanup(func() { paramtable.Get().Save(key, previous) })
	}

	nodes := session.NewNodeManager()
	dist := meta.NewDistributionManager(nodes)
	states := make(map[int64]assign.PlanningNode)
	var nodeIDs []int64
	var loaded []assign.PlanningLoadKey
	var segmentID int64
	for idx, count := range counts {
		nodeID := int64(idx + 1)
		nodeIDs = append(nodeIDs, nodeID)
		node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID})
		node.SetState(session.NodeStateNormal)
		nodes.Add(node)
		states[nodeID] = assign.PlanningNode{
			Ready: true, CapacityBytes: 100000, MemoryUsageBytes: int64(count * 100),
			Rows: int64(count * 10), CollectionRows: map[int64]int64{1: int64(count * 10)},
		}
		var segments []*meta.Segment
		for i := 0; i < count; i++ {
			segmentID++
			segments = append(segments, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
				ID: segmentID, CollectionID: 1, NumOfRows: 10, InsertChannel: "shard",
				Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 100, LogSize: 100}}}},
			}})
			loaded = append(loaded, assign.PlanningLoadKey{NodeID: nodeID, CollectionID: 1, SegmentID: segmentID})
		}
		dist.SegmentDistManager.Update(nodeID, segments...)
	}
	targets := meta.NewMockTargetManager(t)
	targets.EXPECT().CanSegmentBeMoved(mock.Anything, int64(1), mock.Anything).Return(true)
	scheduler := task.NewMockScheduler(t)
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.InitGlobalAssignPolicyFactory(scheduler, nodes, dist, nil, targets)
	t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, Nodes: nodeIDs, ResourceGroup: "rg"})
	return NewMultiTargetBalancer(scheduler, nodes, dist, targets), replica, states, loaded
}

func TestMultiTargetPlanningAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		batch, cap, want        int
		noLedger, unready, full bool
		pending, reported       bool
	}{
		{name: "legacy-without-ledger", batch: 1, cap: 1, want: 2, noLedger: true},
		{name: "per-node-cap", batch: 5, cap: 1, want: 1},
		{name: "batch-bound", batch: 1, cap: 8, want: 1},
		{name: "zero-batch", batch: 0, cap: 8},
		{name: "unready-target", batch: 5, cap: 8, unready: true},
		{name: "physical-envelope", batch: 5, cap: 8, full: true},
		{name: "pending-grow-fills-cap", batch: 5, cap: 1, pending: true},
		{name: "reported-grow-fills-cap", batch: 5, cap: 1, reported: true},
		{name: "same-grow-local-and-reported-counts-once", batch: 5, cap: 2, want: 1, pending: true, reported: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, replica, states, loaded := newMultiTargetPlanningFixture(t, []int{4, 0}, tc.batch)
			target := states[2]
			target.Ready = !tc.unready
			if tc.full {
				target.CapacityBytes, target.MemoryUsageBytes = 100, 100
			}
			load := assign.PlanningLoad{Key: assign.PlanningLoadKey{NodeID: 2, CollectionID: 1, SegmentID: 99},
				From: -1, Rows: 10, MemoryBytes: 100, DiskBytes: 100}
			var pending, reported []assign.PlanningLoad
			if tc.pending {
				pending = []assign.PlanningLoad{load}
				target.PendingRows = load.Rows
				target.PendingCollectionRows = map[int64]int64{1: load.Rows}
			}
			if tc.reported {
				reported = []assign.PlanningLoad{load}
			}
			states[2] = target
			ledger := assign.NewPlanningLedgerState(states, loaded, pending, reported, tc.cap)
			ctx := context.Background()
			if !tc.noLedger {
				ctx = assign.WithPlanningLedger(ctx, ledger)
			}
			rowsBefore := ledger.Rows(2, 1)
			memoryBefore, diskBefore := ledger.PhysicalUsage(2)
			plans, channels := b.BalanceReplica(ctx, replica)
			require.Empty(t, channels)
			require.Len(t, plans, tc.want)
			for _, plan := range plans {
				require.EqualValues(t, 1, plan.From)
				require.EqualValues(t, 2, plan.To)
				require.Same(t, replica, plan.Replica)
			}
			if tc.noLedger {
				return
			}
			require.EqualValues(t, 40-10*len(plans), ledger.Rows(1, 1))
			require.EqualValues(t, rowsBefore+int64(10*len(plans)), ledger.Rows(2, 1))
			memory, disk := ledger.PhysicalUsage(2)
			require.EqualValues(t, memoryBefore+int64(100*len(plans)), memory)
			require.EqualValues(t, diskBefore+int64(100*len(plans)), disk)
			for _, plan := range plans {
				require.True(t, ledger.UndoSegment(plan))
			}
			require.EqualValues(t, 40, ledger.Rows(1, 1))
			require.Equal(t, rowsBefore, ledger.Rows(2, 1), "rejected and batch-truncated plans leave no reservation")
			memory, disk = ledger.PhysicalUsage(2)
			require.Equal(t, memoryBefore, memory)
			require.Equal(t, diskBefore, disk)
		})
	}
}

func TestMultiTargetPlanningContinuesAfterRejectedTarget(t *testing.T) {
	b, replica, states, loaded := newMultiTargetPlanningFixture(t, []int{6, 0, 0}, 2)
	full := states[2]
	full.CapacityBytes, full.MemoryUsageBytes = 100, 100
	states[2] = full
	ledger := assign.NewPlanningLedgerState(states, loaded, nil, nil, 8)
	plans, channels := b.BalanceReplica(assign.WithPlanningLedger(context.Background(), ledger), replica)
	require.Empty(t, channels)
	// The optimizer alternates the two initially empty destinations. Rejecting
	// node 2 must neither stop at its first plan nor consume the successful batch.
	require.Len(t, plans, 2)
	for _, plan := range plans {
		require.EqualValues(t, 3, plan.To)
	}
	require.EqualValues(t, 40, ledger.Rows(1, 1))
	require.Zero(t, ledger.Rows(2, 1))
	require.EqualValues(t, 20, ledger.Rows(3, 1))
}

func TestMultiTargetPlanningDeduplicatesAcrossCandidates(t *testing.T) {
	b, replica, states, loaded := newMultiTargetPlanningFixture(t, []int{6, 0, 0}, 5)
	var reported []assign.PlanningLoad
	for _, key := range loaded {
		key.NodeID = 3
		reported = append(reported, assign.PlanningLoad{Key: key, From: -1, Rows: 10})
	}
	ledger := assign.NewPlanningLedgerState(states, loaded, nil, reported, 8)
	plans, _ := b.BalanceReplica(assign.WithPlanningLedger(context.Background(), ledger), replica)
	require.Empty(t, plans, "loading on node 3 also prevents another Grow of that segment to node 2")
	require.EqualValues(t, 60, ledger.Rows(1, 1))
	require.Zero(t, ledger.Rows(2, 1))
}

func TestMultiTargetPlanningReservationLifecycle(t *testing.T) {
	b, replica, states, loaded := newMultiTargetPlanningFixture(t, []int{4, 0}, 5)
	ledger := assign.NewPlanningLedgerState(states, loaded, nil, nil, 1)
	ctx := assign.WithPlanningLedger(context.Background(), ledger)
	plans, _ := b.BalanceReplica(ctx, replica)
	require.Len(t, plans, 1)
	next, _ := b.BalanceReplica(ctx, replica)
	require.Empty(t, next, "later calls share the same occupied slot")
	require.True(t, ledger.UndoSegment(plans[0]))
	retry, _ := b.BalanceReplica(ctx, replica)
	require.Len(t, retry, 1, "rejected tasks release the provisional slot")
	ledger.CommitSegment(retry[0])
	require.False(t, ledger.UndoSegment(retry[0]))
	next, _ = b.BalanceReplica(ctx, replica)
	require.Empty(t, next, "accepted work stays reserved for the rest of this planning round")
}
