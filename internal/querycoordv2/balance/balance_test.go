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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type BalanceTestSuite struct {
	suite.Suite
	mockScheduler      *task.MockScheduler
	roundRobinBalancer *RoundRobinBalancer
}

func (suite *BalanceTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *BalanceTestSuite) SetupTest() {
	nodeManager := session.NewNodeManager()
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.roundRobinBalancer = NewRoundRobinBalancer(suite.mockScheduler, nodeManager)

	suite.mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
}

func (suite *BalanceTestSuite) TestAssignBalance() {
	cases := []struct {
		name        string
		nodeIDs     []int64
		segmentCnts []int
		states      []session.State
		deltaCnts   []int
		assignments []*meta.Segment
		expectPlans []SegmentAssignPlan
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
			states:      []session.State{},
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
			suite.mockScheduler.ExpectedCalls = nil
			for i := range c.nodeIDs {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodeIDs[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.SetState(c.states[i])
				suite.roundRobinBalancer.nodeManager.Add(nodeInfo)
				if !nodeInfo.IsStoppingState() {
					suite.mockScheduler.EXPECT().GetSegmentTaskDelta(c.nodeIDs[i], int64(-1)).Return(c.deltaCnts[i])
				}
			}
			plans := suite.roundRobinBalancer.AssignSegment(0, c.assignments, c.nodeIDs, false)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func (suite *BalanceTestSuite) TestAssignChannel() {
	cases := []struct {
		name        string
		nodeIDs     []int64
		channelCnts []int
		states      []session.State
		deltaCnts   []int
		assignments []*meta.DmChannel
		expectPlans []ChannelAssignPlan
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
			states:      []session.State{},
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
			suite.mockScheduler.ExpectedCalls = nil
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
				if !nodeInfo.IsStoppingState() {
					suite.mockScheduler.EXPECT().GetChannelTaskDelta(c.nodeIDs[i], int64(-1)).Return(c.deltaCnts[i])
				}
			}
			plans := suite.roundRobinBalancer.AssignChannel(1, c.assignments, c.nodeIDs, false)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func TestBalanceSuite(t *testing.T) {
	suite.Run(t, new(BalanceTestSuite))
}
