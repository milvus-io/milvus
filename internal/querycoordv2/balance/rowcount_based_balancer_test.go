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

	"github.com/milvus-io/milvus/internal/querycoordv2/task"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type RowCountBasedBalancerTestSuite struct {
	suite.Suite
	balancer      *RowCountBasedBalancer
	kv            *etcdkv.EtcdKV
	broker        *meta.MockBroker
	mockScheduler *task.MockScheduler
}

func (suite *RowCountBasedBalancerTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *RowCountBasedBalancerTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.broker = meta.NewMockBroker(suite.T())

	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	nodeManager := session.NewNodeManager()
	testMeta := meta.NewMeta(idAllocator, store, nodeManager)
	testTarget := meta.NewTargetManager(suite.broker, testMeta)

	distManager := meta.NewDistributionManager()
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.balancer = NewRowCountBasedBalancer(suite.mockScheduler, nodeManager, distManager, testMeta, testTarget)

	suite.broker.EXPECT().GetPartitions(mock.Anything, int64(1)).Return([]int64{1}, nil).Maybe()
}

func (suite *RowCountBasedBalancerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *RowCountBasedBalancerTestSuite) TestAssignSegment() {
	cases := []struct {
		name          string
		distributions map[int64][]*meta.Segment
		assignments   []*meta.Segment
		nodes         []int64
		segmentCnts   []int
		states        []session.State
		expectPlans   []SegmentAssignPlan
	}{
		{
			name: "test normal assignment",
			distributions: map[int64][]*meta.Segment{
				2: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20}, Node: 2}},
				3: {{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 30}, Node: 3}},
			},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 15}},
			},
			nodes:       []int64{1, 2, 3, 4},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal, session.NodeStateStopping},
			segmentCnts: []int{0, 1, 1, 0},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5}}, From: -1, To: 2},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10}}, From: -1, To: 1},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 15}}, From: -1, To: 1},
			},
		},
		// TODO: add more cases
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			// I do not find a better way to do the setup and teardown work for subtests yet.
			// If you do, please replace with it.
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(c.nodes[i], "127.0.0.1:0")
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
			}
			plans := balancer.AssignSegment(c.assignments, c.nodes)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestBalance() {
	cases := []struct {
		name                 string
		nodes                []int64
		notExistedNodes      []int64
		segmentCnts          []int
		states               []session.State
		shouldMock           bool
		distributions        map[int64][]*meta.Segment
		distributionChannels map[int64][]*meta.DmChannel
		expectPlans          []SegmentAssignPlan
		expectChannelPlans   []ChannelAssignPlan
	}{
		{
			name:        "normal balance",
			nodes:       []int64{1, 2},
			segmentCnts: []int{1, 2},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2}, From: 2, To: 1, ReplicaID: 1},
			},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:        "all stopping balance",
			nodes:       []int64{1, 2},
			segmentCnts: []int{1, 2},
			states:      []session.State{session.NodeStateStopping, session.NodeStateStopping},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:        "part stopping balance",
			nodes:       []int64{1, 2, 3},
			segmentCnts: []int{1, 2, 2},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateStopping},
			shouldMock:  true,
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3},
					{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3},
				},
			},
			distributionChannels: map[int64][]*meta.DmChannel{
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 2},
				},
				3: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, ReplicaID: 1, Weight: weightHigh},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, ReplicaID: 1, Weight: weightHigh},
			},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3}, From: 3, To: 1, ReplicaID: 1, Weight: weightHigh},
			},
		},
		{
			name:            "already balanced",
			nodes:           []int64{1, 2},
			notExistedNodes: []int64{10},
			segmentCnts:     []int{1, 2},
			states:          []session.State{session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 30}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
				10: {{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 30}, Node: 10}},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
	}

	suite.mockScheduler.Mock.On("GetNodeChannelDelta", mock.Anything).Return(0)
	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			collection := utils.CreateTestCollection(1, 1)
			segments := []*datapb.SegmentBinlogs{
				{
					SegmentID: 1,
				},
				{
					SegmentID: 2,
				},
				{
					SegmentID: 3,
				},
				{
					SegmentID: 4,
				},
				{
					SegmentID: 5,
				},
			}
			suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, int64(1), int64(1)).Return(
				nil, segments, nil)
			balancer.targetMgr.UpdateCollectionNextTargetWithPartitions(int64(1), int64(1))
			balancer.targetMgr.UpdateCollectionCurrentTarget(1, 1)
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			collection.LoadType = querypb.LoadType_LoadCollection
			balancer.meta.CollectionManager.PutCollection(collection)
			balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, append(c.nodes, c.notExistedNodes...)))
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(c.nodes[i], "127.0.0.1:0")
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
				suite.balancer.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, c.nodes[i])
			}
			segmentPlans, channelPlans := balancer.Balance()
			suite.ElementsMatch(c.expectChannelPlans, channelPlans)
			suite.ElementsMatch(c.expectPlans, segmentPlans)
		})
	}

}

func (suite *RowCountBasedBalancerTestSuite) TestBalanceOutboundNodes() {
	cases := []struct {
		name                 string
		nodes                []int64
		notExistedNodes      []int64
		segmentCnts          []int
		states               []session.State
		shouldMock           bool
		distributions        map[int64][]*meta.Segment
		distributionChannels map[int64][]*meta.DmChannel
		expectPlans          []SegmentAssignPlan
		expectChannelPlans   []ChannelAssignPlan
	}{
		{
			name:        "balance out bound nodes",
			nodes:       []int64{1, 2, 3},
			segmentCnts: []int{1, 2, 2},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			shouldMock:  true,
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3},
					{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3},
				},
			},
			distributionChannels: map[int64][]*meta.DmChannel{
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 2},
				},
				3: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, ReplicaID: 1, Weight: weightHigh},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, ReplicaID: 1, Weight: weightHigh},
			},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3}, From: 3, To: 1, ReplicaID: 1, Weight: weightHigh},
			},
		},
	}

	suite.mockScheduler.Mock.On("GetNodeChannelDelta", mock.Anything).Return(0)
	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			collection := utils.CreateTestCollection(1, 1)
			segments := []*datapb.SegmentBinlogs{
				{
					SegmentID: 1,
				},
				{
					SegmentID: 2,
				},
				{
					SegmentID: 3,
				},
				{
					SegmentID: 4,
				},
				{
					SegmentID: 5,
				},
			}
			suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, int64(1), int64(1)).Return(
				nil, segments, nil)
			balancer.targetMgr.UpdateCollectionNextTargetWithPartitions(int64(1), int64(1))
			balancer.targetMgr.UpdateCollectionCurrentTarget(1, 1)
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			collection.LoadType = querypb.LoadType_LoadCollection
			balancer.meta.CollectionManager.PutCollection(collection)
			balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, append(c.nodes, c.notExistedNodes...)))
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(c.nodes[i], "127.0.0.1:0")
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
			}
			// make node-3 outbound
			err := balancer.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 1)
			suite.NoError(err)
			err = balancer.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 2)
			suite.NoError(err)
			segmentPlans, channelPlans := balancer.Balance()
			suite.ElementsMatch(c.expectChannelPlans, channelPlans)
			suite.ElementsMatch(c.expectPlans, segmentPlans)
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestBalanceOnLoadingCollection() {
	cases := []struct {
		name          string
		nodes         []int64
		distributions map[int64][]*meta.Segment
		expectPlans   []SegmentAssignPlan
	}{
		{
			name:  "normal balance",
			nodes: []int64{1, 2},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
			},
			expectPlans: []SegmentAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			collection := utils.CreateTestCollection(1, 1)
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loading
			collection.LoadType = querypb.LoadType_LoadCollection
			balancer.meta.CollectionManager.PutCollection(collection)
			balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, c.nodes))
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			segmentPlans, channelPlans := balancer.Balance()
			suite.Empty(channelPlans)
			suite.ElementsMatch(c.expectPlans, segmentPlans)
		})
	}

}

func TestRowCountBasedBalancerSuite(t *testing.T) {
	suite.Run(t, new(RowCountBasedBalancerTestSuite))
}
