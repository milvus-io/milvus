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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func recoveryPlanningFixture(t *testing.T, slotLimit int, nodeIDs ...int64) (*SegmentChecker, *meta.Replica, context.Context) {
	t.Helper()
	paramtable.Init()
	manager := session.NewNodeManager()
	states := make(map[int64]assign.PlanningNode)
	for _, id := range nodeIDs {
		manager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id}))
		states[id] = assign.PlanningNode{Ready: true, CapacityBytes: 10000}
	}
	dist := meta.NewDistributionManager(manager)
	dist.ChannelDistManager.Update(nodeIDs[0],
		&meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "a"}},
		&meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "b"}})
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	scheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	assign.ResetGlobalAssignPolicyFactoryForTest()
	t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)
	assign.InitGlobalAssignPolicyFactory(scheduler, manager, dist, nil, nil)
	checker := &SegmentChecker{dist: dist, nodeMgr: manager, scheduler: scheduler, assignPolicy: assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeRoundRobin)}
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, Nodes: nodeIDs})
	ctx := assign.WithPlanningLedger(context.Background(), assign.NewPlanningLedgerState(states, nil, nil, nil, slotLimit))
	return checker, replica, ctx
}

func TestRecoveryPlanningPrioritizesMissingCurrentTargetBeforeLargeHandoff(t *testing.T) {
	checker, replica, ctx := recoveryPlanningFixture(t, 1, 1)
	segments := []*datapb.SegmentInfo{
		{ID: 1, CollectionID: 1, InsertChannel: "a", NumOfRows: 1000},
		{ID: 2, CollectionID: 1, InsertChannel: "b", NumOfRows: 1},
	}
	tasks := checker.createSegmentLoadTasks(ctx, segments, []commonpb.LoadPriority{commonpb.LoadPriority_LOW, commonpb.LoadPriority_HIGH}, replica)
	require.Len(t, tasks, 1)
	require.EqualValues(t, 2, tasks[0].(*task.SegmentTask).SegmentID())
	require.Equal(t, commonpb.LoadPriority_HIGH, tasks[0].(*task.SegmentTask).LoadPriority())
	require.True(t, checker.deferredLoads[1], "slot exhaustion must not cache the collection as synchronized")
}

func TestRecoveryPlanningSharesPlacementAcrossShards(t *testing.T) {
	checker, replica, ctx := recoveryPlanningFixture(t, 2, 1, 2)
	segments := []*datapb.SegmentInfo{{ID: 1, CollectionID: 1, InsertChannel: "a", NumOfRows: 100}, {ID: 2, CollectionID: 1, InsertChannel: "b", NumOfRows: 100}}
	tasks := checker.createSegmentLoadTasks(ctx, segments, []commonpb.LoadPriority{commonpb.LoadPriority_HIGH, commonpb.LoadPriority_HIGH}, replica)
	require.Len(t, tasks, 2)
	require.NotEqual(t, tasks[0].Actions()[0].Node(), tasks[1].Actions()[0].Node())
}

func TestRecoveryPlanningPreservesExclusiveReplicaNodeList(t *testing.T) {
	checker, _, ctx := recoveryPlanningFixture(t, 2, 1, 2)
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, Nodes: []int64{1, 2},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{"a": {RwNodes: []int64{2, 1}}},
	})
	before := append([]int64(nil), replica.GetChannelRWNodes("a")...)
	require.Equal(t, []int64{2, 1}, before)
	tasks := checker.createSegmentLoadTasks(ctx, []*datapb.SegmentInfo{{ID: 1, CollectionID: 1, InsertChannel: "a", NumOfRows: 100}}, []commonpb.LoadPriority{commonpb.LoadPriority_HIGH}, replica)
	require.Len(t, tasks, 1)
	require.Equal(t, before, replica.GetChannelRWNodes("a"), "planning must not mutate the immutable metadata snapshot")
}
