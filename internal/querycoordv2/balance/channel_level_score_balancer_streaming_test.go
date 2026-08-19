// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package balance

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestChannelLevelScoreBalancer_StreamingRWSQDoesNotBlockSegmentBalance(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{344, 345, 346, 347},
		RwSqNodes:     []int64{339, 333},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{344, 345}},
			"channel2": {RwNodes: []int64{346, 347}},
		},
	})
	targetChannels := map[string]*meta.DmChannel{
		"channel1": newTestChannel(collectionID, "channel1", 0),
		"channel2": newTestChannel(collectionID, "channel2", 0),
	}

	balancer, dist := newStreamingChannelLevelBalancer(t, replica)
	mockGetChannels := mockey.Mock((*meta.TargetManager).GetDmChannelsByCollection).Return(targetChannels).Build()
	defer mockGetChannels.UnPatch()
	mockCanMove := mockey.Mock((*meta.TargetManager).CanSegmentBeMoved).Return(true).Build()
	defer mockCanMove.UnPatch()

	dist.ChannelDistManager.Update(339, newTestChannel(collectionID, "channel1", 339))
	dist.ChannelDistManager.Update(333, newTestChannel(collectionID, "channel2", 333))
	dist.SegmentDistManager.Update(344,
		newTestSegment(collectionID, 1, "channel1", 344),
		newTestSegment(collectionID, 2, "channel1", 344),
	)
	dist.SegmentDistManager.Update(346,
		newTestSegment(collectionID, 3, "channel2", 346),
		newTestSegment(collectionID, 4, "channel2", 346),
	)

	segmentPlans, channelPlans := balancer.BalanceReplica(ctx, replica)

	require.Empty(t, channelPlans)
	require.NotEmpty(t, segmentPlans)
	rwSQNodes := typeutil.NewUniqueSet(replica.GetRWSQNodes()...)
	for _, plan := range segmentPlans {
		require.False(t, rwSQNodes.Contain(plan.From))
		require.False(t, rwSQNodes.Contain(plan.To))
		channelRWNodes := typeutil.NewUniqueSet(replica.GetChannelRWNodes(plan.Segment.GetInsertChannel())...)
		require.True(t, channelRWNodes.Contain(plan.From))
		require.True(t, channelRWNodes.Contain(plan.To))
	}
}

func TestChannelLevelScoreBalancer_StreamingTrueSegmentOutboundFirst(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{344, 345},
		RwSqNodes:     []int64{339},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{344, 345}},
		},
	})
	targetChannels := map[string]*meta.DmChannel{
		"channel1": newTestChannel(collectionID, "channel1", 0),
	}

	balancer, dist := newStreamingChannelLevelBalancer(t, replica)
	mockGetChannels := mockey.Mock((*meta.TargetManager).GetDmChannelsByCollection).Return(targetChannels).Build()
	defer mockGetChannels.UnPatch()
	mockCanMove := mockey.Mock((*meta.TargetManager).CanSegmentBeMoved).Return(true).Build()
	defer mockCanMove.UnPatch()

	dist.ChannelDistManager.Update(339, newTestChannel(collectionID, "channel1", 339))
	dist.SegmentDistManager.Update(339, newTestSegment(collectionID, 1, "channel1", 339))
	dist.SegmentDistManager.Update(344,
		newTestSegment(collectionID, 2, "channel1", 344),
		newTestSegment(collectionID, 3, "channel1", 344),
	)

	segmentPlans, channelPlans := balancer.BalanceReplica(ctx, replica)

	require.Empty(t, channelPlans)
	require.Len(t, segmentPlans, 1)
	require.Equal(t, int64(1), segmentPlans[0].Segment.GetID())
	require.Equal(t, int64(339), segmentPlans[0].From)
	require.Contains(t, replica.GetChannelRWNodes("channel1"), segmentPlans[0].To)
}

func TestChannelLevelScoreBalancer_StreamingEmptyOutboundPlanFallsBackToSegmentBalance(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{344, 345},
		RwSqNodes:     []int64{339},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{344, 345}},
		},
	})
	targetChannels := map[string]*meta.DmChannel{
		"channel1": newTestChannel(collectionID, "channel1", 0),
	}

	balancer, dist := newStreamingChannelLevelBalancer(t, replica)
	mockGetChannels := mockey.Mock((*meta.TargetManager).GetDmChannelsByCollection).Return(targetChannels).Build()
	defer mockGetChannels.UnPatch()
	mockCanMove := mockey.Mock((*meta.TargetManager).CanSegmentBeMoved).
		To(func(_ *meta.TargetManager, _ context.Context, _, segmentID int64) bool {
			return segmentID != 1
		}).Build()
	defer mockCanMove.UnPatch()

	dist.ChannelDistManager.Update(339, newTestChannel(collectionID, "channel1", 339))
	dist.SegmentDistManager.Update(339, newTestSegment(collectionID, 1, "channel1", 339))
	dist.SegmentDistManager.Update(344,
		newTestSegment(collectionID, 2, "channel1", 344),
		newTestSegment(collectionID, 3, "channel1", 344),
	)

	segmentPlans, channelPlans := balancer.BalanceReplica(ctx, replica)

	require.Empty(t, channelPlans)
	require.Len(t, segmentPlans, 1)
	require.NotEqual(t, int64(1), segmentPlans[0].Segment.GetID())
	require.Equal(t, int64(344), segmentPlans[0].From)
	require.Equal(t, int64(345), segmentPlans[0].To)
}

func TestChannelLevelScoreBalancer_StreamingChannelPlanTakesPriority(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{344, 345},
		RwSqNodes:     []int64{339, 333},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{344}},
			"channel2": {RwNodes: []int64{345}},
		},
	})

	balancer, dist := newStreamingChannelLevelBalancer(t, replica)
	mockSQNodes := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDs).
		Return(typeutil.NewUniqueSet(339, 333)).Build()
	defer mockSQNodes.UnPatch()
	// Channel assignment asks which streaming nodes may carry a delegator, and
	// that question is answered per resource group. Left unmocked, the real
	// manager waits on a streaming coord balancer that a unit test never
	// starts, and the wait has no deadline.
	mockSQNodesByRG := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDsByResourceGroup).
		Return(map[string]typeutil.UniqueSet{
			meta.DefaultResourceGroupName: typeutil.NewUniqueSet(339, 333),
		}).Build()
	defer mockSQNodesByRG.UnPatch()
	mockWALLocated := mockey.Mock((*snmanager.StreamingNodeManager).GetWALLocated).Return(int64(333)).Build()
	defer mockWALLocated.UnPatch()

	dist.ChannelDistManager.Update(339,
		newTestChannel(collectionID, "channel1", 339),
		newTestChannel(collectionID, "channel2", 339),
	)

	segmentPlans, channelPlans := balancer.BalanceReplica(ctx, replica)

	require.Empty(t, segmentPlans)
	require.NotEmpty(t, channelPlans)
	for _, plan := range channelPlans {
		require.Equal(t, int64(339), plan.From)
		require.Equal(t, int64(333), plan.To)
	}
}

func TestChannelLevelScoreBalancer_NoNormalNodes(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(1)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{344}},
		},
	})
	targetChannels := map[string]*meta.DmChannel{
		"channel1": newTestChannel(collectionID, "channel1", 0),
	}

	balancer, _ := newStreamingChannelLevelBalancer(t, replica)
	mockGetChannels := mockey.Mock((*meta.TargetManager).GetDmChannelsByCollection).Return(targetChannels).Build()
	defer mockGetChannels.UnPatch()

	segmentPlans, channelPlans := balancer.BalanceReplica(ctx, replica)

	require.Empty(t, segmentPlans)
	require.Empty(t, channelPlans)
}

func newStreamingChannelLevelBalancer(t *testing.T, replica *meta.Replica) (*ChannelLevelScoreBalancer, *meta.DistributionManager) {
	t.Helper()
	paramtable.Init()

	streamingutil.SetStreamingServiceEnabled()
	t.Cleanup(streamingutil.UnsetStreamingServiceEnabled)
	assign.ResetGlobalAssignPolicyFactoryForTest()
	t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)

	nodeManager := session.NewNodeManager()
	for _, nodeID := range replica.GetNodes() {
		node := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		node.SetState(session.NodeStateNormal)
		nodeManager.Add(node)
	}

	var nextID int64
	idAllocator := func() (int64, error) {
		nextID++
		return nextID, nil
	}
	testMeta := meta.NewMeta(idAllocator, nil, nodeManager)
	targetMgr := meta.NewTargetManager(nil, testMeta)
	dist := meta.NewDistributionManager(nodeManager)
	scheduler := task.NewScheduler(context.Background(), testMeta, dist, targetMgr, nil, nil, nodeManager)
	assign.InitGlobalAssignPolicyFactory(scheduler, nodeManager, dist, testMeta, targetMgr)

	return NewChannelLevelScoreBalancer(scheduler, nodeManager, dist, targetMgr), dist
}

func newTestChannel(collectionID int64, channelName string, nodeID int64) *meta.DmChannel {
	return &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channelName,
		},
		Node: nodeID,
		View: &meta.LeaderView{
			ID:           nodeID,
			CollectionID: collectionID,
		},
	}
}

func newTestSegment(collectionID, segmentID int64, channelName string, nodeID int64) *meta.Segment {
	return &meta.Segment{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collectionID,
			PartitionID:   1,
			InsertChannel: channelName,
			NumOfRows:     100,
		},
		Node: nodeID,
	}
}
