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

package assign

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type planningInspectorScheduler struct {
	*task.MockScheduler
	loads map[int64][]*querypb.LoadingSegmentInfo
}

func (s *planningInspectorScheduler) PendingSegmentLoads() map[int64][]*querypb.LoadingSegmentInfo {
	return s.loads
}

func TestPlanningAdapterLoadedPendingGrowDoesNotDoubleCount(t *testing.T) {
	for _, loaded := range []bool{false, true} {
		t.Run(map[bool]string{false: "loading", true: "distributed-with-pending-Grow"}[loaded], func(t *testing.T) {
			nodes := session.NewNodeManager()
			node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
			node.SetState(session.NodeStateNormal)
			node.UpdateStats(session.WithMemCapacity(1))
			info := &querypb.LoadingSegmentInfo{SegmentId: 7, CollectionId: 1, NumRows: 20, EstimatedMemoryBytes: 100}
			node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{MemoryUsageBytes: 100, LoadingMemoryBytes: 100,
				LoadingSegments: []*querypb.LoadingSegmentInfo{info}}, true, true, time.Now())
			nodes.Add(node)
			dist := meta.NewDistributionManager(nodes)
			delta := 20
			if loaded {
				dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, NumOfRows: 20}})
				// GetSegmentTaskDeltaSnapshot skips Grow when its object is in dist.
				delta = 0
			}
			scheduler := &planningInspectorScheduler{MockScheduler: task.NewMockScheduler(t),
				loads: map[int64][]*querypb.LoadingSegmentInfo{1: {info}}}
			scheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0)
			scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).
				Return(task.NewSegmentTaskDeltaSnapshot(map[int64]int{1: delta}, map[int64]int{1: delta}))
			ledger := NewPlanningLedger(nodes, dist, scheduler, 2)
			require.EqualValues(t, 20, ledger.Rows(1, -1))
			require.EqualValues(t, 20, ledger.Rows(1, 1))
			memory, _ := ledger.PhysicalUsage(1)
			require.EqualValues(t, 200, memory)
			require.True(t, ledger.Reserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 8}, From: -1, Rows: 5}))
			require.False(t, ledger.Reserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 9}, From: -1, Rows: 5}))
		})
	}
}

func TestPlanningAdapterTaskAdmissionRollbackAndSourceIdentity(t *testing.T) {
	ledger := NewPlanningLedgerState(map[int64]PlanningNode{29: ledgerTestNode(20, 1000), 31: ledgerTestNode(0, 1000)}, nil, nil, nil, 2)
	ctx := WithPlanningLedger(context.Background(), ledger)
	plan := SegmentAssignPlan{Segment: &meta.Segment{Node: 29,
		SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, NumOfRows: 20}}, From: -1, To: 31}
	require.True(t, ledger.ReserveSegment(plan))
	// StoppingBalancer attaches From after assigning to the RW-only node set.
	plan.From = 29
	require.EqualValues(t, 0, ledger.Rows(29, -1))
	grow, err := task.NewSegmentTask(ctx, time.Minute, task.WrapIDSource(0), 1, nil, commonpb.LoadPriority_LOW,
		task.NewSegmentAction(31, task.ActionTypeGrow, "shard", 7),
		task.NewSegmentAction(29, task.ActionTypeReduce, "shard", 7))
	require.NoError(t, err)
	defer grow.Cancel(nil)
	UndoTask(ctx, grow)
	require.EqualValues(t, 20, ledger.Rows(29, -1))
	require.EqualValues(t, 0, ledger.Rows(31, -1))
	require.True(t, ledger.ReserveSegment(plan))
	CommitTask(ctx, grow)
	UndoTask(ctx, grow)
	require.EqualValues(t, 20, ledger.Rows(31, -1), "accepted work stays in this Check's projection")
}

func TestPlanningAdapterChannelProjectionMatchesPolicyWeight(t *testing.T) {
	first, second := ledgerTestNode(0, 1000), ledgerTestNode(10, 1000)
	first.Channels, first.CollectionChannels[1] = 2, 1
	second.Channels, second.CollectionChannels[1] = 1, 1
	ledger := NewPlanningLedgerState(map[int64]PlanningNode{1: first, 2: second}, nil, nil, nil, 8)
	plan := ChannelAssignPlan{From: -1, To: 1, Channel: &meta.DmChannel{Node: 2,
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "shard"},
		View:         &meta.LeaderView{NumOfGrowingRows: 10}}}
	require.True(t, ledger.ReserveChannel(plan))
	require.EqualValues(t, 7, ledger.ChannelScore(1, 1, 3))
	require.EqualValues(t, 0, ledger.ChannelScore(2, 1, 3))
	require.EqualValues(t, 3, ledger.ChannelScore(1, -1, 1))
	require.EqualValues(t, 10, ledger.Rows(1, -1))
	require.EqualValues(t, 0, ledger.Rows(2, -1))
	require.True(t, ledger.UndoChannel(plan))
	require.EqualValues(t, 4, ledger.ChannelScore(1, 1, 3))
	require.EqualValues(t, 10, ledger.Rows(2, -1))
}

func TestPlanningAdapterExistingCandidateLoadPreventsAlternateTarget(t *testing.T) {
	pending := PlanningLoad{Key: PlanningLoadKey{31, 1, 7}, From: -1, Rows: 20}
	ledger := NewPlanningLedgerState(map[int64]PlanningNode{31: ledgerTestNode(0, 1000), 32: ledgerTestNode(0, 1000)}, nil, nil, []PlanningLoad{pending}, 8)
	queue := NewPriorityQueue()
	for _, nodeID := range []int64{31, 32} {
		item := NewNodeItem(0, nodeID)
		queue.Push(&item)
	}
	segment := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, NumOfRows: 20}}
	require.Nil(t, popPlanningSegmentTarget(WithPlanningLedger(context.Background(), ledger), &queue, segment))
	require.Equal(t, 2, queue.Len(), "existing loading work must not be retargeted to another empty node")
}

func TestPlanningAdapterChannelRecoveryReservationsAndMoveRollback(t *testing.T) {
	ledger := NewPlanningLedgerState(map[int64]PlanningNode{
		29: ledgerTestNode(10, 1000), 31: ledgerTestNode(0, 1000), 32: ledgerTestNode(0, 1000),
	}, nil, nil, nil, 8)
	ctx := WithPlanningLedger(context.Background(), ledger)
	channel := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "recovery"}}
	first := ChannelAssignPlan{From: -1, To: 31, Channel: channel}
	second := ChannelAssignPlan{From: -1, To: 32, Channel: channel}
	require.True(t, ledger.ReserveChannel(first))
	require.True(t, ledger.ReserveChannel(second), "different replicas need separate copies of the same missing channel")
	require.False(t, ledger.ReserveChannel(first), "the same destination still has only one reservation")

	firstTask, err := task.NewChannelTask(ctx, time.Minute, task.WrapIDSource(0), 1, nil,
		task.NewChannelAction(31, task.ActionTypeGrow, "recovery"))
	require.NoError(t, err)
	defer firstTask.Cancel(nil)
	secondTask, err := task.NewChannelTask(ctx, time.Minute, task.WrapIDSource(0), 1, nil,
		task.NewChannelAction(32, task.ActionTypeGrow, "recovery"))
	require.NoError(t, err)
	defer secondTask.Cancel(nil)
	UndoTask(ctx, firstTask)
	require.EqualValues(t, 0, ledger.ChannelScore(31, -1, 1))
	require.EqualValues(t, 1, ledger.ChannelScore(32, -1, 1), "one replica's rejection must retain the other reservation")
	CommitTask(ctx, secondTask)
	UndoTask(ctx, secondTask)
	require.False(t, ledger.ReserveChannel(second), "accepted work must remain reserved")
	require.True(t, ledger.ReserveChannel(first), "rejected work can be planned again")

	move := ChannelAssignPlan{From: -1, To: 31, Channel: &meta.DmChannel{
		Node: 29, VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "move"},
		View: &meta.LeaderView{NumOfGrowingRows: 10},
	}}
	alternate := move
	alternate.To = 32
	require.True(t, ledger.ReserveChannel(move))
	require.False(t, ledger.ReserveChannel(alternate), "a real source cannot move the same channel to two targets")
	require.True(t, ledger.UndoChannel(move))
	require.EqualValues(t, 10, ledger.Rows(29, -1))
	require.True(t, ledger.ReserveChannel(alternate))
	ledger.CommitChannel(alternate)
	require.False(t, ledger.UndoChannel(alternate))
	require.False(t, ledger.ReserveChannel(move), "a committed Move must retain its source claim")
}
