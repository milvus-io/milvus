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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BalanceTestSuite struct {
	suite.Suite
	mockScheduler      *task.MockScheduler
	nodeManager        *session.NodeManager
	dist               *meta.DistributionManager
	targetMgr          *meta.MockTargetManager
	roundRobinBalancer *RoundRobinBalancer
}

func (suite *BalanceTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *BalanceTestSuite) SetupTest() {
	// Reset factory first to ensure clean state for each test
	assign.ResetGlobalAssignPolicyFactoryForTest()

	suite.nodeManager = session.NewNodeManager()
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.dist = meta.NewDistributionManager(suite.nodeManager)
	suite.targetMgr = meta.NewMockTargetManager(suite.T())

	// Initialize global assign policy factory before creating balancer
	assign.InitGlobalAssignPolicyFactory(suite.mockScheduler, suite.nodeManager, suite.dist, nil, suite.targetMgr)

	suite.roundRobinBalancer = NewRoundRobinBalancer(suite.mockScheduler, suite.nodeManager, suite.dist, nil, suite.targetMgr)

	suite.mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
}

func (suite *BalanceTestSuite) TearDownTest() {
	assign.ResetGlobalAssignPolicyFactoryForTest()
}

func (suite *BalanceTestSuite) TestAssignBalance() {
	ctx := context.Background()
	cases := []struct {
		name        string
		nodeIDs     []int64
		segmentCnts []int
		states      []session.State
		deltaCnts   []int
		assignments []*meta.Segment
		expectPlans []assign.SegmentAssignPlan
	}{
		{
			name:        "normal assignment",
			nodeIDs:     []int64{1, 2, 3},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateStopping},
			segmentCnts: []int{100, 200, 0},
			deltaCnts:   []int{0, -200, 0},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 1}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 2}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 3}},
			},
			expectPlans: []assign.SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 1}}, From: -1, To: 2},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 2}}, From: -1, To: 1},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 3}}, From: -1, To: 2},
			},
		},
		{
			name:        "empty assignment",
			nodeIDs:     []int64{},
			segmentCnts: []int{},
			states:      []session.State{},
			deltaCnts:   []int{},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 1}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 2}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 3}},
			},
			expectPlans: []assign.SegmentAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupTest()
			suite.mockScheduler.ExpectedCalls = nil

			// Setup mock scheduler to return the correct delta for each node
			for i := range c.nodeIDs {
				nodeID := c.nodeIDs[i]
				delta := c.deltaCnts[i]
				suite.mockScheduler.EXPECT().GetSegmentTaskDelta(nodeID, int64(-1)).Return(delta).Maybe()
			}
			suite.mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

			for i := range c.nodeIDs {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodeIDs[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
					Version:  common.Version,
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.SetState(c.states[i])
				suite.roundRobinBalancer.nodeManager.Add(nodeInfo)
			}
			plans := suite.roundRobinBalancer.GetAssignPolicy().AssignSegment(ctx, 0, c.assignments, c.nodeIDs, false)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func (suite *BalanceTestSuite) TestAssignChannel() {
	ctx := context.Background()
	cases := []struct {
		name        string
		nodeIDs     []int64
		channelCnts []int
		states      []session.State
		deltaCnts   []int
		assignments []*meta.DmChannel
		expectPlans []assign.ChannelAssignPlan
	}{
		{
			name:        "normal assignment",
			nodeIDs:     []int64{1, 2, 3},
			channelCnts: []int{100, 200, 0},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateStopping},
			deltaCnts:   []int{0, -200, 0},
			assignments: []*meta.DmChannel{
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-1"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-2"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-3"}},
			},
			expectPlans: []assign.ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-1"}}, From: -1, To: 2},
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-2"}}, From: -1, To: 1},
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-3"}}, From: -1, To: 2},
			},
		},
		{
			name:        "empty assignment",
			nodeIDs:     []int64{},
			channelCnts: []int{},
			states:      []session.State{},
			deltaCnts:   []int{},
			assignments: []*meta.DmChannel{
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-1"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-2"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-3"}},
			},
			expectPlans: []assign.ChannelAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupTest()
			suite.mockScheduler.ExpectedCalls = nil

			// Setup mock scheduler to return the correct delta for each node
			for i := range c.nodeIDs {
				nodeID := c.nodeIDs[i]
				delta := c.deltaCnts[i]
				suite.mockScheduler.EXPECT().GetChannelTaskDelta(nodeID, int64(-1)).Return(delta).Maybe()
			}
			suite.mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

			for i := range c.nodeIDs {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodeIDs[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
					Version:  common.Version,
				})
				nodeInfo.UpdateStats(session.WithChannelCnt(c.channelCnts[i]))
				nodeInfo.SetState(c.states[i])
				suite.roundRobinBalancer.nodeManager.Add(nodeInfo)
			}
			plans := suite.roundRobinBalancer.GetAssignPolicy().AssignChannel(ctx, 1, c.assignments, c.nodeIDs, false)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func (suite *BalanceTestSuite) TestBalanceReplica_LessThan2Nodes() {
	ctx := context.Background()

	// Create a replica with only 1 node
	replica := meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         []int64{1},
		},
		typeutil.NewUniqueSet(1),
	)

	segmentPlans, channelPlans := suite.roundRobinBalancer.BalanceReplica(ctx, replica)
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
}

func (suite *BalanceTestSuite) TestBalanceReplica_BalanceSegments() {
	ctx := context.Background()

	// Setup target manager mock
	suite.targetMgr.EXPECT().CanSegmentBeMoved(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Create nodes
	for _, nodeID := range []int64{1, 2} {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		nodeInfo.SetState(session.NodeStateNormal)
		suite.nodeManager.Add(nodeInfo)
	}

	// Create segments distribution - node 1 has 3 segments, node 2 has 0 segments
	segments1 := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 100}, Node: 1},
		{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 200}, Node: 1},
		{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 300}, Node: 1},
	}
	suite.dist.SegmentDistManager.Update(1, segments1...)
	suite.dist.SegmentDistManager.Update(2)

	// Create replica with 2 nodes
	replica := meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         []int64{1, 2},
		},
		typeutil.NewUniqueSet(1, 2),
	)

	segmentPlans, channelPlans := suite.roundRobinBalancer.BalanceReplica(ctx, replica)

	// Should generate segment plans to move segments from node 1 to node 2
	// Average = ceil(3/2) = 2, so node 1 should keep 2 segments and move 1 segment
	suite.Len(segmentPlans, 1)
	suite.Empty(channelPlans)
	suite.Equal(int64(1), segmentPlans[0].From)
	suite.Equal(int64(2), segmentPlans[0].To)
}

func (suite *BalanceTestSuite) TestBalanceReplica_BalanceChannels() {
	ctx := context.Background()

	// Enable auto balance channel
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.AutoBalanceChannel.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.AutoBalanceChannel.Key)

	// Setup target manager mock
	suite.targetMgr.EXPECT().CanSegmentBeMoved(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Create nodes
	for _, nodeID := range []int64{1, 2} {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		nodeInfo.SetState(session.NodeStateNormal)
		suite.nodeManager.Add(nodeInfo)
	}

	// Create channels distribution - node 1 has 3 channels, node 2 has 0 channels
	channels1 := []*meta.DmChannel{
		{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "ch1"}, Node: 1},
		{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "ch2"}, Node: 1},
		{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "ch3"}, Node: 1},
	}
	suite.dist.ChannelDistManager.Update(1, channels1...)
	suite.dist.ChannelDistManager.Update(2)

	// Create replica with 2 nodes
	replica := meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         []int64{1, 2},
		},
		typeutil.NewUniqueSet(1, 2),
	)

	segmentPlans, channelPlans := suite.roundRobinBalancer.BalanceReplica(ctx, replica)

	// Should generate channel plans to move channels from node 1 to node 2
	// Average = ceil(3/2) = 2, so node 1 should keep 2 channels and move 1 channel
	suite.Empty(segmentPlans)
	suite.Len(channelPlans, 1)
	suite.Equal(int64(1), channelPlans[0].From)
	suite.Equal(int64(2), channelPlans[0].To)
}

func (suite *BalanceTestSuite) TestBalanceReplica_AlreadyBalanced() {
	ctx := context.Background()

	// Setup target manager mock
	suite.targetMgr.EXPECT().CanSegmentBeMoved(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Create nodes
	for _, nodeID := range []int64{1, 2} {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		nodeInfo.SetState(session.NodeStateNormal)
		suite.nodeManager.Add(nodeInfo)
	}

	// Create balanced segments distribution - node 1 has 2 segments, node 2 has 2 segments
	segments1 := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 100}, Node: 1},
		{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 200}, Node: 1},
	}
	segments2 := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 300}, Node: 2},
		{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 400}, Node: 2},
	}
	suite.dist.SegmentDistManager.Update(1, segments1...)
	suite.dist.SegmentDistManager.Update(2, segments2...)

	// Create replica with 2 nodes
	replica := meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         []int64{1, 2},
		},
		typeutil.NewUniqueSet(1, 2),
	)

	segmentPlans, channelPlans := suite.roundRobinBalancer.BalanceReplica(ctx, replica)

	// Should not generate any plans since already balanced
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
}

func (suite *BalanceTestSuite) TestBalanceReplica_NoSegments() {
	ctx := context.Background()

	// Setup target manager mock
	suite.targetMgr.EXPECT().CanSegmentBeMoved(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Create nodes
	for _, nodeID := range []int64{1, 2} {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		nodeInfo.SetState(session.NodeStateNormal)
		suite.nodeManager.Add(nodeInfo)
	}

	// No segments
	suite.dist.SegmentDistManager.Update(1)
	suite.dist.SegmentDistManager.Update(2)

	// Create replica with 2 nodes
	replica := meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         []int64{1, 2},
		},
		typeutil.NewUniqueSet(1, 2),
	)

	segmentPlans, channelPlans := suite.roundRobinBalancer.BalanceReplica(ctx, replica)

	// Should not generate any plans since no segments
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
}

func (suite *BalanceTestSuite) TestBalanceReplica_SkipRedundantSegments() {
	ctx := context.Background()

	// Setup target manager mock
	suite.targetMgr.EXPECT().CanSegmentBeMoved(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Create nodes
	for _, nodeID := range []int64{1, 2} {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		nodeInfo.SetState(session.NodeStateNormal)
		suite.nodeManager.Add(nodeInfo)
	}

	// Create redundant segment distribution - same segment on both nodes
	segments1 := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 100}, Node: 1},
		{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 200}, Node: 1},
		{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 300}, Node: 1},
	}
	segments2 := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 100}, Node: 2}, // Redundant segment
	}
	suite.dist.SegmentDistManager.Update(1, segments1...)
	suite.dist.SegmentDistManager.Update(2, segments2...)

	// Create replica with 2 nodes
	replica := meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			ResourceGroup: meta.DefaultResourceGroupName,
			Nodes:         []int64{1, 2},
		},
		typeutil.NewUniqueSet(1, 2),
	)

	segmentPlans, channelPlans := suite.roundRobinBalancer.BalanceReplica(ctx, replica)

	// Should generate plans for non-redundant segments only
	// node 1 has 3, node 2 has 1, average = ceil(4/2) = 2
	// segment 1 is redundant, so only segment 2 and 3 are considered for move
	suite.Empty(channelPlans)
	// Only non-redundant segments should be moved
	for _, plan := range segmentPlans {
		suite.NotEqual(int64(1), plan.Segment.GetID(), "Redundant segment should not be moved")
	}
}

func TestBalanceSuite(t *testing.T) {
	suite.Run(t, new(BalanceTestSuite))
}
