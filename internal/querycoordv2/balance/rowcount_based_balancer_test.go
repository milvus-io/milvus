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
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type RowCountBasedBalancerTestSuite struct {
	suite.Suite
	balancer      *RowCountBasedBalancer
	kv            kv.MetaKv
	broker        *meta.MockBroker
	mockScheduler *task.MockScheduler
}

func (suite *RowCountBasedBalancerTestSuite) SetupSuite() {
	paramtable.Init()
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

	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	nodeManager := session.NewNodeManager()
	testMeta := meta.NewMeta(idAllocator, store, nodeManager)
	testTarget := meta.NewTargetManager(suite.broker, testMeta)

	distManager := meta.NewDistributionManager()
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.balancer = NewRowCountBasedBalancer(suite.mockScheduler, nodeManager, distManager, testMeta, testTarget)

	suite.broker.EXPECT().GetPartitions(mock.Anything, int64(1)).Return([]int64{1}, nil).Maybe()

	suite.mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.mockScheduler.EXPECT().GetSegmentTaskNum(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.mockScheduler.EXPECT().GetChannelTaskNum(mock.Anything, mock.Anything).Return(0).Maybe()
}

func (suite *RowCountBasedBalancerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *RowCountBasedBalancerTestSuite) TestAssignSegment() {
	ctx := context.Background()
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
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
			}
			plans := balancer.AssignSegment(ctx, 0, c.assignments, c.nodes, false)
			assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, plans)
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestBalance() {
	ctx := context.Background()
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
		multiple             bool
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
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 2}, From: 2, To: 1, Replica: newReplicaDefaultRG(1)},
			},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:        "skip balance for redundant segment",
			nodes:       []int64{1, 2},
			segmentCnts: []int{1, 2},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 20}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 40}, Node: 2},
				},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:        "balance won't trigger",
			nodes:       []int64{1, 2, 3},
			segmentCnts: []int{1, 2, 2},
			states:      []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 40}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 10}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 40}, Node: 2},
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 10}, Node: 3},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 40}, Node: 3},
				},
			},
			expectPlans:        []SegmentAssignPlan{},
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
			name:        "part stopping balance channel",
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
			expectPlans: []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
			},
		},
		{
			name:        "part stopping balance segment",
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
				1: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 1},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
			},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:          "balance channel",
			nodes:         []int64{2, 3},
			segmentCnts:   []int{2, 2},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal},
			shouldMock:    true,
			distributions: map[int64][]*meta.Segment{},
			distributionChannels: map[int64][]*meta.DmChannel{
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 2},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 2},
				},
				3: {},
			},
			expectPlans: []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 2}, From: 2, To: 3, Replica: newReplicaDefaultRG(1)},
			},
		},
		{
			name:          "unbalance stable view",
			nodes:         []int64{1, 2, 3},
			segmentCnts:   []int{0, 0, 0},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			shouldMock:    true,
			distributions: map[int64][]*meta.Segment{},
			distributionChannels: map[int64][]*meta.DmChannel{
				1: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v1"}, Node: 1},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 1},
				},
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 2},
				},
				3: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v4"}, Node: 3},
				},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:          "balance unstable view",
			nodes:         []int64{1, 2, 3},
			segmentCnts:   []int{0, 0, 0},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			shouldMock:    true,
			distributions: map[int64][]*meta.Segment{},
			distributionChannels: map[int64][]*meta.DmChannel{
				1: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v1"}, Node: 1},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 1},
				},
				2: {},
				3: {},
			},
			expectPlans: []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 1}, From: 1, To: 2, Replica: newReplicaDefaultRG(1)},
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 1}, From: 1, To: 3, Replica: newReplicaDefaultRG(1)},
			},
			multiple: true,
		},
		{
			name:            "already balanced",
			nodes:           []int64{11, 22},
			notExistedNodes: []int64{10},
			segmentCnts:     []int{1, 2},
			states:          []session.State{session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				11: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 30}, Node: 11}},
				22: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 22},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 22},
				},
				10: {{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 30}, Node: 10}},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			segments := []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
				},
				{
					ID:          3,
					PartitionID: 1,
				},
				{
					ID:          4,
					PartitionID: 1,
				},
				{
					ID:          5,
					PartitionID: 1,
				},
			}
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, segments, nil)
			collection := utils.CreateTestCollection(1, 1)
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			collection.LoadType = querypb.LoadType_LoadCollection
			balancer.meta.CollectionManager.PutCollection(ctx, collection)
			balancer.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
			balancer.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, c.nodes))
			suite.broker.ExpectedCalls = nil
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, segments, nil)
			balancer.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
			balancer.targetMgr.UpdateCollectionCurrentTarget(ctx, 1)
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
					Version:  common.Version,
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
				suite.balancer.meta.ResourceManager.HandleNodeUp(ctx, c.nodes[i])
			}
			utils.RecoverAllCollection(balancer.meta)

			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, 1)
			if !c.multiple {
				assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)
				assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans)
			} else {
				assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans, true)
				assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans, true)
			}

			// clear distribution

			for _, node := range c.nodes {
				balancer.meta.ResourceManager.HandleNodeDown(ctx, node)
				balancer.nodeManager.Remove(node)
				balancer.dist.SegmentDistManager.Update(node)
				balancer.dist.ChannelDistManager.Update(node)
			}
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestBalanceOnPartStopping() {
	ctx := context.Background()
	cases := []struct {
		name                 string
		nodes                []int64
		notExistedNodes      []int64
		segmentCnts          []int
		states               []session.State
		shouldMock           bool
		distributions        map[int64][]*meta.Segment
		distributionChannels map[int64][]*meta.DmChannel
		segmentInCurrent     []*datapb.SegmentInfo
		segmentInNext        []*datapb.SegmentInfo
		expectPlans          []SegmentAssignPlan
		expectChannelPlans   []ChannelAssignPlan
	}{
		{
			name:        "exist in next target",
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
			segmentInCurrent: []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
				},
				{
					ID:          3,
					PartitionID: 1,
				},
				{
					ID:          4,
					PartitionID: 1,
				},
				{
					ID:          5,
					PartitionID: 1,
				},
			},

			segmentInNext: []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
				},
				{
					ID:          3,
					PartitionID: 1,
				},
				{
					ID:          4,
					PartitionID: 1,
				},
				{
					ID:          5,
					PartitionID: 1,
				},
			},
			distributionChannels: map[int64][]*meta.DmChannel{
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 2},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
			},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:        "not exist in next target",
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
			segmentInCurrent: []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
				},
				{
					ID:          3,
					PartitionID: 1,
				},
				{
					ID:          4,
					PartitionID: 1,
				},
				{
					ID:          5,
					PartitionID: 1,
				},
			},
			segmentInNext: []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
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
			expectPlans: []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
			},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			collection := utils.CreateTestCollection(1, 1)

			collection.LoadPercentage = 100
			collection.LoadType = querypb.LoadType_LoadCollection
			collection.Status = querypb.LoadStatus_Loaded
			balancer.meta.CollectionManager.PutCollection(ctx, collection)
			balancer.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
			balancer.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, append(c.nodes, c.notExistedNodes...)))
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, c.segmentInCurrent, nil)
			balancer.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
			balancer.targetMgr.UpdateCollectionCurrentTarget(ctx, 1)
			suite.broker.ExpectedCalls = nil
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, c.segmentInNext, nil)
			balancer.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
					Version:  common.Version,
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
				suite.balancer.meta.ResourceManager.HandleNodeUp(ctx, c.nodes[i])
			}
			utils.RecoverAllCollection(balancer.meta)

			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, 1)
			assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)
			assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans)
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestBalanceOutboundNodes() {
	ctx := context.Background()
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
			name:        "balance channel with outbound nodes",
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
			expectPlans: []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
			},
		},
		{
			name:        "balance segment with outbound node",
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
				1: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 1},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 1, NumOfRows: 10}, Node: 3}, From: 3, To: 1, Replica: newReplicaDefaultRG(1)},
			},
			expectChannelPlans: []ChannelAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			collection := utils.CreateTestCollection(1, 1)
			segments := []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
				},
				{
					ID:          3,
					PartitionID: 1,
				},
				{
					ID:          4,
					PartitionID: 1,
				},
				{
					ID:          5,
					PartitionID: 1,
				},
			}

			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			collection.LoadType = querypb.LoadType_LoadCollection
			balancer.meta.CollectionManager.PutCollection(ctx, collection)
			balancer.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
			balancer.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, append(c.nodes, c.notExistedNodes...)))
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, segments, nil)
			balancer.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
			balancer.targetMgr.UpdateCollectionCurrentTarget(ctx, 1)
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
					Version:  common.Version,
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
			}
			// make node-3 outbound
			balancer.meta.ResourceManager.HandleNodeUp(ctx, 1)
			balancer.meta.ResourceManager.HandleNodeUp(ctx, 2)
			utils.RecoverAllCollection(balancer.meta)
			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, 1)
			assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans)
			assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)

			// clean up distribution for next test
			for node := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node)
				balancer.dist.ChannelDistManager.Update(node)
			}
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestBalanceOnLoadingCollection() {
	ctx := context.Background()
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
			balancer.meta.CollectionManager.PutCollection(ctx, collection)
			balancer.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, c.nodes))
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, 1)
			suite.Empty(channelPlans)
			assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) getCollectionBalancePlans(balancer *RowCountBasedBalancer,
	collectionID int64,
) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	ctx := context.Background()
	replicas := balancer.meta.ReplicaManager.GetByCollection(ctx, collectionID)
	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	for _, replica := range replicas {
		sPlans, cPlans := balancer.BalanceReplica(ctx, replica)
		segmentPlans = append(segmentPlans, sPlans...)
		channelPlans = append(channelPlans, cPlans...)
	}
	return segmentPlans, channelPlans
}

func (suite *RowCountBasedBalancerTestSuite) TestAssignSegmentWithGrowing() {
	suite.SetupSuite()
	defer suite.TearDownTest()
	balancer := suite.balancer
	ctx := context.Background()

	distributions := map[int64][]*meta.Segment{
		1: {
			{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20, CollectionID: 1}, Node: 1},
		},
		2: {
			{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 20, CollectionID: 1}, Node: 2},
		},
	}
	for node, s := range distributions {
		balancer.dist.SegmentDistManager.Update(node, s...)
	}

	for _, node := range lo.Keys(distributions) {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
		})
		nodeInfo.UpdateStats(session.WithSegmentCnt(20))
		nodeInfo.SetState(session.NodeStateNormal)
		suite.balancer.nodeManager.Add(nodeInfo)
	}

	toAssign := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 10, CollectionID: 1}, Node: 3},
		{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, CollectionID: 1}, Node: 3},
	}

	// mock 50 growing row count in node 1, which is delegator, expect all segment assign to node 2
	leaderView := &meta.LeaderView{
		ID:               1,
		CollectionID:     1,
		NumOfGrowingRows: 50,
	}
	suite.balancer.dist.LeaderViewManager.Update(1, leaderView)
	plans := balancer.AssignSegment(ctx, 1, toAssign, lo.Keys(distributions), false)
	for _, p := range plans {
		suite.Equal(int64(2), p.To)
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestDisableBalanceChannel() {
	ctx := context.Background()
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
		multiple             bool
		enableBalanceChannel bool
	}{
		{
			name:          "balance channel",
			nodes:         []int64{2, 3},
			segmentCnts:   []int{2, 2},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal},
			shouldMock:    true,
			distributions: map[int64][]*meta.Segment{},
			distributionChannels: map[int64][]*meta.DmChannel{
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 2},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 2},
				},
				3: {},
			},
			expectPlans: []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{
				{Channel: &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 2}, From: 2, To: 3, Replica: newReplicaDefaultRG(1)},
			},
			enableBalanceChannel: true,
		},

		{
			name:          "disable balance channel",
			nodes:         []int64{2, 3},
			segmentCnts:   []int{2, 2},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal},
			shouldMock:    true,
			distributions: map[int64][]*meta.Segment{},
			distributionChannels: map[int64][]*meta.DmChannel{
				2: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v2"}, Node: 2},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v3"}, Node: 2},
				},
				3: {},
			},
			expectPlans:          []SegmentAssignPlan{},
			expectChannelPlans:   []ChannelAssignPlan{},
			enableBalanceChannel: false,
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer
			segments := []*datapb.SegmentInfo{
				{
					ID:          1,
					PartitionID: 1,
				},
				{
					ID:          2,
					PartitionID: 1,
				},
				{
					ID:          3,
					PartitionID: 1,
				},
				{
					ID:          4,
					PartitionID: 1,
				},
				{
					ID:          5,
					PartitionID: 1,
				},
			}
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, segments, nil)
			collection := utils.CreateTestCollection(1, 1)
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			collection.LoadType = querypb.LoadType_LoadCollection
			balancer.meta.CollectionManager.PutCollection(ctx, collection)
			balancer.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
			balancer.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, append(c.nodes, c.notExistedNodes...)))
			suite.broker.ExpectedCalls = nil
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(nil, segments, nil)
			balancer.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
			balancer.targetMgr.UpdateCollectionCurrentTarget(ctx, 1)
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
					Version:  common.Version,
				})
				nodeInfo.UpdateStats(session.WithSegmentCnt(c.segmentCnts[i]))
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
				suite.balancer.meta.ResourceManager.HandleNodeUp(ctx, c.nodes[i])
			}

			Params.Save(Params.QueryCoordCfg.AutoBalanceChannel.Key, fmt.Sprint(c.enableBalanceChannel))
			defer Params.Reset(Params.QueryCoordCfg.AutoBalanceChannel.Key)
			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, 1)
			if !c.multiple {
				assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans)
				assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)
			} else {
				assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans, true)
				assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans, true)
			}

			// clear distribution
			for node := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node)
			}
			for node := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node)
			}
		})
	}
}

func (suite *RowCountBasedBalancerTestSuite) TestMultiReplicaBalance() {
	ctx := context.Background()
	cases := []struct {
		name               string
		collectionID       int64
		replicaWithNodes   map[int64][]int64
		segments           []*datapb.SegmentInfo
		channels           []*datapb.VchannelInfo
		states             []session.State
		shouldMock         bool
		segmentDist        map[int64][]*meta.Segment
		channelDist        map[int64][]*meta.DmChannel
		expectPlans        []SegmentAssignPlan
		expectChannelPlans []ChannelAssignPlan
	}{
		{
			name:             "balance on multi replica",
			collectionID:     1,
			replicaWithNodes: map[int64][]int64{1: {1, 2}, 2: {3, 4}},
			segments: []*datapb.SegmentInfo{
				{ID: 1, CollectionID: 1, PartitionID: 1},
				{ID: 2, CollectionID: 1, PartitionID: 1},
				{ID: 3, CollectionID: 1, PartitionID: 1},
				{ID: 4, CollectionID: 1, PartitionID: 1},
			},
			channels: []*datapb.VchannelInfo{
				{
					CollectionID: 1, ChannelName: "channel1", FlushedSegmentIds: []int64{1},
				},
				{
					CollectionID: 1, ChannelName: "channel2", FlushedSegmentIds: []int64{2},
				},
				{
					CollectionID: 1, ChannelName: "channel3", FlushedSegmentIds: []int64{3},
				},
				{
					CollectionID: 1, ChannelName: "channel4", FlushedSegmentIds: []int64{4},
				},
			},
			states: []session.State{session.NodeStateNormal, session.NodeStateNormal},
			segmentDist: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 30}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 30}, Node: 1},
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 3},
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, NumOfRows: 30}, Node: 3},
				},
			},
			channelDist: map[int64][]*meta.DmChannel{
				1: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel1"}, Node: 1},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel2"}, Node: 1},
				},
				3: {
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel3"}, Node: 3},
					{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel4"}, Node: 3},
				},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			suite.SetupSuite()
			defer suite.TearDownTest()
			balancer := suite.balancer

			// 1. set up target for multi collections
			collection := utils.CreateTestCollection(c.collectionID, int32(len(c.replicaWithNodes)))
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, c.collectionID).Return(
				c.channels, c.segments, nil)
			suite.broker.EXPECT().GetPartitions(mock.Anything, c.collectionID).Return([]int64{c.collectionID}, nil).Maybe()
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			balancer.meta.CollectionManager.PutCollection(ctx, collection)
			balancer.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(c.collectionID, c.collectionID))
			for replicaID, nodes := range c.replicaWithNodes {
				balancer.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(replicaID, c.collectionID, nodes))
			}
			balancer.targetMgr.UpdateCollectionNextTarget(ctx, c.collectionID)
			balancer.targetMgr.UpdateCollectionCurrentTarget(ctx, c.collectionID)

			// 2. set up target for distribution for multi collections
			for node, s := range c.segmentDist {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.channelDist {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}

			// 3. set up nodes info and resourceManager for balancer
			for _, nodes := range c.replicaWithNodes {
				for i := range nodes {
					nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
						NodeID:  nodes[i],
						Address: "127.0.0.1:0",
						Version: common.Version,
					})
					nodeInfo.UpdateStats(session.WithChannelCnt(len(c.channelDist[nodes[i]])))
					nodeInfo.SetState(c.states[i])
					suite.balancer.nodeManager.Add(nodeInfo)
					suite.balancer.meta.ResourceManager.HandleNodeUp(ctx, nodes[i])
				}
			}

			// expected to balance channel first
			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, c.collectionID)
			suite.Len(segmentPlans, 0)
			suite.Len(channelPlans, 2)

			// mock new distribution after channel balance
			balancer.dist.ChannelDistManager.Update(1, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel1"}, Node: 1})
			balancer.dist.ChannelDistManager.Update(2, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel2"}, Node: 2})
			balancer.dist.ChannelDistManager.Update(3, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel3"}, Node: 3})
			balancer.dist.ChannelDistManager.Update(4, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel4"}, Node: 4})

			// expected to balance segment
			segmentPlans, channelPlans = suite.getCollectionBalancePlans(balancer, c.collectionID)
			suite.Len(segmentPlans, 2)
			suite.Len(channelPlans, 0)
		})
	}
}

func TestRowCountBasedBalancerSuite(t *testing.T) {
	suite.Run(t, new(RowCountBasedBalancerTestSuite))
}

func newReplicaDefaultRG(replicaID int64) *meta.Replica {
	return meta.NewReplica(
		&querypb.Replica{
			ID:            replicaID,
			ResourceGroup: meta.DefaultResourceGroupName,
		},
		typeutil.NewUniqueSet(),
	)
}

// remove it after resource group enhancement.
func assertSegmentAssignPlanElementMatch(suite *suite.Suite, left []SegmentAssignPlan, right []SegmentAssignPlan, subset ...bool) {
	suite.Equal(len(left), len(right))

	type comparablePlan struct {
		Segment   int64
		ReplicaID int64
		From      int64
		To        int64
	}

	leftPlan := make([]comparablePlan, 0)
	for _, p := range left {
		replicaID := int64(-1)
		if p.Replica != nil {
			replicaID = p.Replica.GetID()
		}
		leftPlan = append(leftPlan, comparablePlan{
			Segment:   p.Segment.ID,
			ReplicaID: replicaID,
			From:      p.From,
			To:        p.To,
		})
	}

	rightPlan := make([]comparablePlan, 0)
	for _, p := range right {
		replicaID := int64(-1)
		if p.Replica != nil {
			replicaID = p.Replica.GetID()
		}
		rightPlan = append(rightPlan, comparablePlan{
			Segment:   p.Segment.ID,
			ReplicaID: replicaID,
			From:      p.From,
			To:        p.To,
		})
	}
	if len(subset) > 0 && subset[0] {
		suite.Subset(leftPlan, rightPlan)
	} else {
		suite.ElementsMatch(leftPlan, rightPlan)
	}
}

// remove it after resource group enhancement.
func assertChannelAssignPlanElementMatch(suite *suite.Suite, left []ChannelAssignPlan, right []ChannelAssignPlan, subset ...bool) {
	type comparablePlan struct {
		Channel   string
		ReplicaID int64
		From      int64
		To        int64
	}

	leftPlan := make([]comparablePlan, 0)
	for _, p := range left {
		replicaID := int64(-1)
		if p.Replica != nil {
			replicaID = p.Replica.GetID()
		}
		leftPlan = append(leftPlan, comparablePlan{
			Channel:   p.Channel.GetChannelName(),
			ReplicaID: replicaID,
			From:      p.From,
			To:        p.To,
		})
	}

	rightPlan := make([]comparablePlan, 0)
	for _, p := range right {
		replicaID := int64(-1)
		if p.Replica != nil {
			replicaID = p.Replica.GetID()
		}
		rightPlan = append(rightPlan, comparablePlan{
			Channel:   p.Channel.GetChannelName(),
			ReplicaID: replicaID,
			From:      p.From,
			To:        p.To,
		})
	}
	if len(subset) > 0 && subset[0] {
		suite.Subset(leftPlan, rightPlan)
	} else {
		suite.ElementsMatch(leftPlan, rightPlan)
	}
}
