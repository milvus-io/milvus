package balance

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/stretchr/testify/suite"
)

type BalanceTestSuite struct {
	suite.Suite
	mockScheduler      *task.MockScheduler
	roundRobinBalancer *RoundRobinBalancer
}

func (suite *BalanceTestSuite) SetupTest() {
	nodeManager := session.NewNodeManager()
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.roundRobinBalancer = NewRoundRobinBalancer(suite.mockScheduler, nodeManager)
}

func (suite *BalanceTestSuite) TestAssignBalance() {
	cases := []struct {
		name        string
		nodeIDs     []int64
		segmentCnts []int
		deltaCnts   []int
		assignments []*meta.Segment
		expectPlans []SegmentAssignPlan
	}{
		{
			name:        "normal assignment",
			nodeIDs:     []int64{1, 2},
			segmentCnts: []int{100, 200},
			deltaCnts:   []int{0, -200},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 1}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 2}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 3}},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 1}}, From: -1, To: 2},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 2}}, From: -1, To: 1},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 3}}, From: -1, To: 2},
			},
		},
		{
			name:        "empty assignment",
			nodeIDs:     []int64{},
			segmentCnts: []int{},
			deltaCnts:   []int{},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 1}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 2}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 3}},
			},
			expectPlans: []SegmentAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupTest()
			for i := range c.nodeIDs {
				nodeInfo := session.NewNodeInfo(c.nodeIDs[i], "127.0.0.1:0")
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				suite.roundRobinBalancer.nodeManager.Add(nodeInfo)
				suite.mockScheduler.EXPECT().GetNodeSegmentDelta(c.nodeIDs[i]).Return(c.deltaCnts[i])
			}
			plans := suite.roundRobinBalancer.AssignSegment(c.assignments, c.nodeIDs)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func (suite *BalanceTestSuite) TestAssignChannel() {
	cases := []struct {
		name        string
		nodeIDs     []int64
		channelCnts []int
		deltaCnts   []int
		assignments []*meta.DmChannel
		expectPlans []ChannelAssignPlan
	}{
		{
			name:        "normal assignment",
			nodeIDs:     []int64{1, 2},
			channelCnts: []int{100, 200},
			deltaCnts:   []int{0, -200},
			assignments: []*meta.DmChannel{
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-1"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-2"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-3"}},
			},
			expectPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-1"}}, From: -1, To: 2},
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-2"}}, From: -1, To: 1},
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-3"}}, From: -1, To: 2},
			},
		},
		{
			name:        "empty assignment",
			nodeIDs:     []int64{},
			channelCnts: []int{},
			deltaCnts:   []int{},
			assignments: []*meta.DmChannel{
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-1"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-2"}},
				{VchannelInfo: &datapb.VchannelInfo{ChannelName: "channel-3"}},
			},
			expectPlans: []ChannelAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupTest()
			for i := range c.nodeIDs {
				nodeInfo := session.NewNodeInfo(c.nodeIDs[i], "127.0.0.1:0")
				nodeInfo.UpdateStats(session.WithChannelCnt(c.channelCnts[i]))
				suite.roundRobinBalancer.nodeManager.Add(nodeInfo)
				suite.mockScheduler.EXPECT().GetNodeChannelDelta(c.nodeIDs[i]).Return(c.deltaCnts[i])
			}
			plans := suite.roundRobinBalancer.AssignChannel(c.assignments, c.nodeIDs)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func TestBalanceSuite(t *testing.T) {
	suite.Run(t, new(BalanceTestSuite))
}
