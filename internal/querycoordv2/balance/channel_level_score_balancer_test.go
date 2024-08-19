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
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ChannelLevelScoreBalancerTestSuite struct {
	suite.Suite
	balancer      *ChannelLevelScoreBalancer
	kv            kv.MetaKv
	broker        *meta.MockBroker
	mockScheduler *task.MockScheduler
}

func (suite *ChannelLevelScoreBalancerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ChannelLevelScoreBalancerTestSuite) SetupTest() {
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
	suite.balancer = NewChannelLevelScoreBalancer(suite.mockScheduler, nodeManager, distManager, testMeta, testTarget)

	suite.mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
}

func (suite *ChannelLevelScoreBalancerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestAssignSegment() {
	cases := []struct {
		name               string
		comment            string
		distributions      map[int64][]*meta.Segment
		assignments        [][]*meta.Segment
		nodes              []int64
		collectionIDs      []int64
		segmentCnts        []int
		states             []session.State
		expectPlans        [][]SegmentAssignPlan
		unstableAssignment bool
	}{
		{
			name:          "test empty cluster assigning one collection",
			comment:       "this is most simple case in which global row count is zero for all nodes",
			distributions: map[int64][]*meta.Segment{},
			assignments: [][]*meta.Segment{
				{
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 5, CollectionID: 1}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 10, CollectionID: 1}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 15, CollectionID: 1}},
				},
			},
			nodes:              []int64{1, 2, 3},
			collectionIDs:      []int64{0},
			states:             []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			segmentCnts:        []int{0, 0, 0},
			unstableAssignment: true,
			expectPlans: [][]SegmentAssignPlan{
				{
					// as assign segments is used while loading collection,
					// all assignPlan should have weight equal to 1(HIGH PRIORITY)
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
						ID: 3, NumOfRows: 15,
						CollectionID: 1,
					}}, From: -1, To: 1},
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
						ID: 2, NumOfRows: 10,
						CollectionID: 1,
					}}, From: -1, To: 3},
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
						ID: 1, NumOfRows: 5,
						CollectionID: 1,
					}}, From: -1, To: 2},
				},
			},
		},
		{
			name: "test non-empty cluster assigning one collection",
			comment: "this case will verify the effect of global row for loading segments process, although node1" +
				"has only 10 rows at the beginning, but it has so many rows on global view, resulting in a lower priority",
			distributions: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 10, CollectionID: 1}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 300, CollectionID: 2}, Node: 1},
					// base: collection1-node1-priority is 10 + 0.1 * 310 = 41
					// assign3: collection1-node1-priority is 15 + 0.1 * 315 = 46.5
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 20, CollectionID: 1}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 180, CollectionID: 2}, Node: 2},
					// base: collection1-node2-priority is 20 + 0.1 * 200 = 40
					// assign2: collection1-node2-priority is 30 + 0.1 * 210 = 51
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 30, CollectionID: 1}, Node: 3},
					{SegmentInfo: &datapb.SegmentInfo{ID: 6, NumOfRows: 20, CollectionID: 2}, Node: 3},
					// base: collection1-node2-priority is 30 + 0.1 * 50 = 35
					// assign1: collection1-node2-priority is 45 + 0.1 * 65 = 51.5
				},
			},
			assignments: [][]*meta.Segment{
				{
					{SegmentInfo: &datapb.SegmentInfo{ID: 7, NumOfRows: 5, CollectionID: 1}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 8, NumOfRows: 10, CollectionID: 1}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 9, NumOfRows: 15, CollectionID: 1}},
				},
			},
			nodes:         []int64{1, 2, 3},
			collectionIDs: []int64{1},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			segmentCnts:   []int{0, 0, 0},
			expectPlans: [][]SegmentAssignPlan{
				{
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 9, NumOfRows: 15, CollectionID: 1}}, From: -1, To: 3},
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 8, NumOfRows: 10, CollectionID: 1}}, From: -1, To: 2},
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, NumOfRows: 5, CollectionID: 1}}, From: -1, To: 1},
				},
			},
		},
		{
			name: "test non-empty cluster assigning two collections at one round segment checking",
			comment: "this case is used to demonstrate the existing assign mechanism having flaws when assigning " +
				"multi collections at one round by using the only segment distribution",
			distributions: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 10, CollectionID: 1}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 20, CollectionID: 1}, Node: 2},
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 40, CollectionID: 1}, Node: 3},
				},
			},
			assignments: [][]*meta.Segment{
				{
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 60, CollectionID: 1}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 50, CollectionID: 1}},
				},
				{
					{SegmentInfo: &datapb.SegmentInfo{ID: 6, NumOfRows: 15, CollectionID: 2}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 7, NumOfRows: 10, CollectionID: 2}},
				},
			},
			nodes:         []int64{1, 2, 3},
			collectionIDs: []int64{1, 2},
			states:        []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			segmentCnts:   []int{0, 0, 0},
			expectPlans: [][]SegmentAssignPlan{
				// note that these two segments plans are absolutely unbalanced globally,
				// as if the assignment for collection1 could succeed, node1 and node2 will both have 70 rows
				// much more than node3, but following assignment will still assign segment based on [10,20,40]
				// rather than [70,70,40], this flaw will be mitigated by balance process and maybe fixed in the later versions
				{
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 60, CollectionID: 1}}, From: -1, To: 1},
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 50, CollectionID: 1}}, From: -1, To: 2},
				},
				{
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 6, NumOfRows: 15, CollectionID: 2}}, From: -1, To: 1},
					{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, NumOfRows: 10, CollectionID: 2}}, From: -1, To: 2},
				},
			},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
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
			for i := range c.collectionIDs {
				plans := balancer.AssignSegment(c.collectionIDs[i], c.assignments[i], c.nodes, false)
				if c.unstableAssignment {
					suite.Equal(len(plans), len(c.expectPlans[i]))
				} else {
					assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans[i], plans)
				}
			}
		})
	}
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestAssignSegmentWithGrowing() {
	suite.SetupSuite()
	defer suite.TearDownTest()
	balancer := suite.balancer

	distributions := map[int64][]*meta.Segment{
		1: {
			{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 100, CollectionID: 1}, Node: 1},
		},
		2: {
			{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 100, CollectionID: 1}, Node: 2},
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

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.DelegatorMemoryOverloadFactor.Key, "0.3")
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.DelegatorMemoryOverloadFactor.Key)

	// mock 50 growing row count in node 1, which is delegator, expect all segment assign to node 2
	leaderView := &meta.LeaderView{
		ID:           1,
		CollectionID: 1,
	}
	suite.balancer.dist.LeaderViewManager.Update(1, leaderView)
	plans := balancer.AssignSegment(1, toAssign, lo.Keys(distributions), false)
	for _, p := range plans {
		suite.Equal(int64(2), p.To)
	}
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestBalanceOneRound() {
	cases := []struct {
		name                 string
		nodes                []int64
		collectionID         int64
		replicaID            int64
		segments             []*datapb.SegmentInfo
		channels             []*datapb.VchannelInfo
		states               []session.State
		shouldMock           bool
		distributions        map[int64][]*meta.Segment
		distributionChannels map[int64][]*meta.DmChannel
		expectPlans          []SegmentAssignPlan
		expectChannelPlans   []ChannelAssignPlan
	}{
		{
			name:         "normal balance for one collection only",
			nodes:        []int64{1, 2},
			collectionID: 1,
			replicaID:    1,
			segments: []*datapb.SegmentInfo{
				{ID: 1, PartitionID: 1}, {ID: 2, PartitionID: 1}, {ID: 3, PartitionID: 1},
			},
			channels: []*datapb.VchannelInfo{
				{
					CollectionID: 1, ChannelName: "channel1",
				},
			},
			states: []session.State{session.NodeStateNormal, session.NodeStateNormal},
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
			name:         "already balanced for one collection only",
			nodes:        []int64{1, 2},
			collectionID: 1,
			replicaID:    1,
			segments: []*datapb.SegmentInfo{
				{ID: 1, PartitionID: 1}, {ID: 2, PartitionID: 1}, {ID: 3, PartitionID: 1},
			},
			states: []session.State{session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
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
			collection := utils.CreateTestCollection(c.collectionID, int32(c.replicaID))
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, c.collectionID).Return(
				c.channels, c.segments, nil)
			suite.broker.EXPECT().GetPartitions(mock.Anything, c.collectionID).Return([]int64{c.collectionID}, nil).Maybe()
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			balancer.meta.CollectionManager.PutCollection(collection)
			balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(c.collectionID, c.collectionID))
			balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(c.replicaID, c.collectionID, c.nodes))
			balancer.targetMgr.UpdateCollectionNextTarget(c.collectionID)
			balancer.targetMgr.UpdateCollectionCurrentTarget(c.collectionID)

			// 2. set up target for distribution for multi collections
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}

			// 3. set up nodes info and resourceManager for balancer
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
				})
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
				suite.balancer.meta.ResourceManager.HandleNodeUp(c.nodes[i])
			}

			// 4. balance and verify result
			segmentPlans, channelPlans := suite.getCollectionBalancePlans(balancer, c.collectionID)
			assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans)
			assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)
		})
	}
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestBalanceMultiRound() {
	balanceCase := struct {
		name            string
		nodes           []int64
		notExistedNodes []int64
		collectionIDs   []int64
		replicaIDs      []int64
		segments        [][]*datapb.SegmentInfo
		channels        []*datapb.VchannelInfo
		states          []session.State
		shouldMock      bool
		distributions   []map[int64][]*meta.Segment
		expectPlans     [][]SegmentAssignPlan
	}{
		name:          "balance considering both global rowCounts and collection rowCounts",
		nodes:         []int64{1, 2, 3},
		collectionIDs: []int64{1, 2},
		replicaIDs:    []int64{1, 2},
		segments: [][]*datapb.SegmentInfo{
			{
				{ID: 1, PartitionID: 1},
				{ID: 3, PartitionID: 1},
			},
			{
				{ID: 2, PartitionID: 2},
				{ID: 4, PartitionID: 2},
			},
		},
		channels: []*datapb.VchannelInfo{
			{
				CollectionID: 1, ChannelName: "channel1",
			},
		},
		states: []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
		distributions: []map[int64][]*meta.Segment{
			{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 20}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 2, NumOfRows: 20}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 20}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 2, NumOfRows: 30}, Node: 2},
				},
			},
			{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 20}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 2, NumOfRows: 20}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 2, NumOfRows: 30}, Node: 2},
				},
				3: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 20}, Node: 3},
				},
			},
		},
		expectPlans: [][]SegmentAssignPlan{
			{
				{
					Segment: &meta.Segment{
						SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 20},
						Node:        2,
					}, From: 2, To: 3, Replica: newReplicaDefaultRG(1),
				},
			},
			{},
		},
	}

	suite.SetupSuite()
	defer suite.TearDownTest()
	balancer := suite.balancer

	// 1. set up target for multi collections
	for i := range balanceCase.collectionIDs {
		collection := utils.CreateTestCollection(balanceCase.collectionIDs[i], int32(balanceCase.replicaIDs[i]))
		suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, balanceCase.collectionIDs[i]).Return(
			balanceCase.channels, balanceCase.segments[i], nil)

		collection.LoadPercentage = 100
		collection.Status = querypb.LoadStatus_Loaded
		collection.LoadType = querypb.LoadType_LoadCollection
		balancer.meta.CollectionManager.PutCollection(collection)
		balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(balanceCase.collectionIDs[i], balanceCase.collectionIDs[i]))
		balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(balanceCase.replicaIDs[i], balanceCase.collectionIDs[i],
			append(balanceCase.nodes, balanceCase.notExistedNodes...)))
		balancer.targetMgr.UpdateCollectionNextTarget(balanceCase.collectionIDs[i])
		balancer.targetMgr.UpdateCollectionCurrentTarget(balanceCase.collectionIDs[i])
	}

	// 2. set up target for distribution for multi collections
	for node, s := range balanceCase.distributions[0] {
		balancer.dist.SegmentDistManager.Update(node, s...)
	}

	// 3. set up nodes info and resourceManager for balancer
	for i := range balanceCase.nodes {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   balanceCase.nodes[i],
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
		})
		nodeInfo.SetState(balanceCase.states[i])
		suite.balancer.nodeManager.Add(nodeInfo)
		suite.balancer.meta.ResourceManager.HandleNodeUp(balanceCase.nodes[i])
	}

	// 4. first round balance
	segmentPlans, _ := suite.getCollectionBalancePlans(balancer, balanceCase.collectionIDs[0])
	assertSegmentAssignPlanElementMatch(&suite.Suite, balanceCase.expectPlans[0], segmentPlans)

	// 5. update segment distribution to simulate balance effect
	for node, s := range balanceCase.distributions[1] {
		balancer.dist.SegmentDistManager.Update(node, s...)
	}

	// 6. balance again
	segmentPlans, _ = suite.getCollectionBalancePlans(balancer, balanceCase.collectionIDs[1])
	assertSegmentAssignPlanElementMatch(&suite.Suite, balanceCase.expectPlans[1], segmentPlans)
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestStoppedBalance() {
	cases := []struct {
		name                 string
		nodes                []int64
		outBoundNodes        []int64
		collectionID         int64
		replicaID            int64
		segments             []*datapb.SegmentInfo
		channels             []*datapb.VchannelInfo
		states               []session.State
		shouldMock           bool
		distributions        map[int64][]*meta.Segment
		distributionChannels map[int64][]*meta.DmChannel
		expectPlans          []SegmentAssignPlan
		expectChannelPlans   []ChannelAssignPlan
	}{
		{
			name:          "stopped balance for one collection",
			nodes:         []int64{1, 2, 3},
			outBoundNodes: []int64{},
			collectionID:  1,
			replicaID:     1,
			segments: []*datapb.SegmentInfo{
				{ID: 1, PartitionID: 1}, {ID: 2, PartitionID: 1}, {ID: 3, PartitionID: 1},
			},
			channels: []*datapb.VchannelInfo{
				{
					CollectionID: 1, ChannelName: "channel1",
				},
			},
			states: []session.State{session.NodeStateStopping, session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{
					SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20},
					Node:        1,
				}, From: 1, To: 3, Replica: newReplicaDefaultRG(1)},
				{Segment: &meta.Segment{
					SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10},
					Node:        1,
				}, From: 1, To: 3, Replica: newReplicaDefaultRG(1)},
			},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:          "all nodes stopping",
			nodes:         []int64{1, 2, 3},
			outBoundNodes: []int64{},
			collectionID:  1,
			replicaID:     1,
			segments: []*datapb.SegmentInfo{
				{ID: 1}, {ID: 2}, {ID: 3},
			},
			channels: []*datapb.VchannelInfo{
				{
					CollectionID: 1, ChannelName: "channel1",
				},
			},
			states: []session.State{session.NodeStateStopping, session.NodeStateStopping, session.NodeStateStopping},
			distributions: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
				},
			},
			expectPlans:        []SegmentAssignPlan{},
			expectChannelPlans: []ChannelAssignPlan{},
		},
		{
			name:          "all nodes outbound",
			nodes:         []int64{1, 2, 3},
			outBoundNodes: []int64{1, 2, 3},
			collectionID:  1,
			replicaID:     1,
			segments: []*datapb.SegmentInfo{
				{ID: 1}, {ID: 2}, {ID: 3},
			},
			channels: []*datapb.VchannelInfo{
				{
					CollectionID: 1, ChannelName: "channel1",
				},
			},
			states: []session.State{session.NodeStateNormal, session.NodeStateNormal, session.NodeStateNormal},
			distributions: map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10}, Node: 1},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20}, Node: 1},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30}, Node: 2},
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
			collection := utils.CreateTestCollection(c.collectionID, int32(c.replicaID))
			suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, c.collectionID).Return(
				c.channels, c.segments, nil)
			suite.broker.EXPECT().GetPartitions(mock.Anything, c.collectionID).Return([]int64{c.collectionID}, nil).Maybe()
			collection.LoadPercentage = 100
			collection.Status = querypb.LoadStatus_Loaded
			balancer.meta.CollectionManager.PutCollection(collection)
			balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(c.collectionID, c.collectionID))
			balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(c.replicaID, c.collectionID, c.nodes))
			balancer.targetMgr.UpdateCollectionNextTarget(c.collectionID)
			balancer.targetMgr.UpdateCollectionCurrentTarget(c.collectionID)

			// 2. set up target for distribution for multi collections
			for node, s := range c.distributions {
				balancer.dist.SegmentDistManager.Update(node, s...)
			}
			for node, v := range c.distributionChannels {
				balancer.dist.ChannelDistManager.Update(node, v...)
			}

			// 3. set up nodes info and resourceManager for balancer
			for i := range c.nodes {
				nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
					NodeID:   c.nodes[i],
					Address:  "127.0.0.1:0",
					Hostname: "localhost",
				})
				nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
				nodeInfo.SetState(c.states[i])
				suite.balancer.nodeManager.Add(nodeInfo)
				suite.balancer.meta.ResourceManager.HandleNodeUp(c.nodes[i])
			}

			for i := range c.outBoundNodes {
				suite.balancer.meta.ResourceManager.HandleNodeDown(c.outBoundNodes[i])
			}
			utils.RecoverAllCollection(balancer.meta)

			// 4. balance and verify result
			segmentPlans, channelPlans := suite.getCollectionBalancePlans(suite.balancer, c.collectionID)
			assertChannelAssignPlanElementMatch(&suite.Suite, c.expectChannelPlans, channelPlans)
			assertSegmentAssignPlanElementMatch(&suite.Suite, c.expectPlans, segmentPlans)
		})
	}
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestMultiReplicaBalance() {
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
			name:             "normal balance for one collection only",
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
					CollectionID: 1, ChannelName: "channel1",
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
			balancer.meta.CollectionManager.PutCollection(collection)
			balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(c.collectionID, c.collectionID))
			for replicaID, nodes := range c.replicaWithNodes {
				balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(replicaID, c.collectionID, nodes))
			}
			balancer.targetMgr.UpdateCollectionNextTarget(c.collectionID)
			balancer.targetMgr.UpdateCollectionCurrentTarget(c.collectionID)

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
					suite.balancer.meta.ResourceManager.HandleNodeUp(nodes[i])
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

func (suite *ChannelLevelScoreBalancerTestSuite) getCollectionBalancePlans(balancer *ChannelLevelScoreBalancer,
	collectionID int64,
) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	replicas := balancer.meta.ReplicaManager.GetByCollection(collectionID)
	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	for _, replica := range replicas {
		sPlans, cPlans := balancer.BalanceReplica(replica)
		segmentPlans = append(segmentPlans, sPlans...)
		channelPlans = append(channelPlans, cPlans...)
	}
	return segmentPlans, channelPlans
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestExclusiveChannelBalance_ChannelOutBound() {
	Params.Save(Params.QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	defer Params.Reset(Params.QueryCoordCfg.Balancer.Key)
	Params.Save(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")
	defer Params.Reset(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key)

	balancer := suite.balancer

	collectionID := int64(1)
	partitionID := int64(1)

	// 1. set up target for multi collections
	segments := []*datapb.SegmentInfo{
		{ID: 1, PartitionID: partitionID}, {ID: 2, PartitionID: partitionID},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1, ChannelName: "channel1",
		},
		{
			CollectionID: 1, ChannelName: "channel2",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		channels, segments, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{collectionID}, nil).Maybe()

	collection := utils.CreateTestCollection(collectionID, int32(1))
	collection.LoadPercentage = 100
	collection.Status = querypb.LoadStatus_Loaded
	balancer.meta.CollectionManager.PutCollection(collection)
	balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(collectionID, partitionID))
	balancer.meta.ReplicaManager.Spawn(1, map[string]int{meta.DefaultResourceGroupName: 1}, []string{"channel1", "channel2"})
	balancer.targetMgr.UpdateCollectionNextTarget(collectionID)
	balancer.targetMgr.UpdateCollectionCurrentTarget(collectionID)

	// 3. set up nodes info and resourceManager for balancer
	nodeCount := 4
	for i := 0; i < nodeCount; i++ {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		// nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
		nodeInfo.SetState(session.NodeStateNormal)
		suite.balancer.nodeManager.Add(nodeInfo)
		suite.balancer.meta.ResourceManager.HandleNodeUp(nodeInfo.ID())
	}
	utils.RecoverAllCollection(balancer.meta)

	replica := balancer.meta.ReplicaManager.GetByCollection(collectionID)[0]
	ch1Nodes := replica.GetChannelRWNodes("channel1")
	ch2Nodes := replica.GetChannelRWNodes("channel2")
	suite.Len(ch1Nodes, 2)
	suite.Len(ch2Nodes, 2)

	balancer.dist.ChannelDistManager.Update(ch1Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel2",
			},
			Node: ch1Nodes[0],
		},
	}...)

	sPlans, cPlans := balancer.BalanceReplica(replica)
	suite.Len(sPlans, 0)
	suite.Len(cPlans, 1)
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestExclusiveChannelBalance_SegmentOutbound() {
	Params.Save(Params.QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	defer Params.Reset(Params.QueryCoordCfg.Balancer.Key)
	Params.Save(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")
	defer Params.Reset(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key)

	balancer := suite.balancer

	collectionID := int64(1)
	partitionID := int64(1)

	// 1. set up target for multi collections
	segments := []*datapb.SegmentInfo{
		{ID: 1, PartitionID: partitionID}, {ID: 2, PartitionID: partitionID}, {ID: 3, PartitionID: partitionID},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1, ChannelName: "channel1",
		},
		{
			CollectionID: 1, ChannelName: "channel2",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		channels, segments, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{collectionID}, nil).Maybe()

	collection := utils.CreateTestCollection(collectionID, int32(1))
	collection.LoadPercentage = 100
	collection.Status = querypb.LoadStatus_Loaded
	balancer.meta.CollectionManager.PutCollection(collection)
	balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(collectionID, partitionID))
	balancer.meta.ReplicaManager.Spawn(1, map[string]int{meta.DefaultResourceGroupName: 1}, []string{"channel1", "channel2"})
	balancer.targetMgr.UpdateCollectionNextTarget(collectionID)
	balancer.targetMgr.UpdateCollectionCurrentTarget(collectionID)

	// 3. set up nodes info and resourceManager for balancer
	nodeCount := 4
	for i := 0; i < nodeCount; i++ {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		// nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
		nodeInfo.SetState(session.NodeStateNormal)
		suite.balancer.nodeManager.Add(nodeInfo)
		suite.balancer.meta.ResourceManager.HandleNodeUp(nodeInfo.ID())
	}
	utils.RecoverAllCollection(balancer.meta)

	replica := balancer.meta.ReplicaManager.GetByCollection(collectionID)[0]
	ch1Nodes := replica.GetChannelRWNodes("channel1")
	ch2Nodes := replica.GetChannelRWNodes("channel2")
	suite.Len(ch1Nodes, 2)
	suite.Len(ch2Nodes, 2)

	balancer.dist.ChannelDistManager.Update(ch1Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel1",
			},
			Node: ch1Nodes[0],
		},
	}...)

	balancer.dist.ChannelDistManager.Update(ch2Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel2",
			},
			Node: ch2Nodes[0],
		},
	}...)

	balancer.dist.SegmentDistManager.Update(ch1Nodes[0], []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[0].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel2",
			},
			Node: ch1Nodes[0],
		},
	}...)

	sPlans, cPlans := balancer.BalanceReplica(replica)
	suite.Len(sPlans, 1)
	suite.Len(cPlans, 0)
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestExclusiveChannelBalance_NodeStopping() {
	Params.Save(Params.QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	defer Params.Reset(Params.QueryCoordCfg.Balancer.Key)
	Params.Save(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")
	defer Params.Reset(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key)

	balancer := suite.balancer

	collectionID := int64(1)
	partitionID := int64(1)

	// 1. set up target for multi collections
	segments := []*datapb.SegmentInfo{
		{ID: 1, PartitionID: partitionID}, {ID: 2, PartitionID: partitionID}, {ID: 3, PartitionID: partitionID},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1, ChannelName: "channel1",
		},
		{
			CollectionID: 1, ChannelName: "channel2",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		channels, segments, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{collectionID}, nil).Maybe()

	collection := utils.CreateTestCollection(collectionID, int32(1))
	collection.LoadPercentage = 100
	collection.Status = querypb.LoadStatus_Loaded
	balancer.meta.CollectionManager.PutCollection(collection)
	balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(collectionID, partitionID))
	balancer.meta.ReplicaManager.Spawn(1, map[string]int{meta.DefaultResourceGroupName: 1}, []string{"channel1", "channel2"})
	balancer.targetMgr.UpdateCollectionNextTarget(collectionID)
	balancer.targetMgr.UpdateCollectionCurrentTarget(collectionID)

	// 3. set up nodes info and resourceManager for balancer
	nodeCount := 4
	for i := 0; i < nodeCount; i++ {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		// nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
		nodeInfo.SetState(session.NodeStateNormal)
		suite.balancer.nodeManager.Add(nodeInfo)
		suite.balancer.meta.ResourceManager.HandleNodeUp(nodeInfo.ID())
	}
	utils.RecoverAllCollection(balancer.meta)

	replica := balancer.meta.ReplicaManager.GetByCollection(collectionID)[0]
	ch1Nodes := replica.GetChannelRWNodes("channel1")
	ch2Nodes := replica.GetChannelRWNodes("channel2")
	suite.Len(ch1Nodes, 2)
	suite.Len(ch2Nodes, 2)

	balancer.dist.ChannelDistManager.Update(ch1Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel1",
			},
			Node: ch1Nodes[0],
		},
	}...)

	balancer.dist.ChannelDistManager.Update(ch2Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel2",
			},
			Node: ch2Nodes[0],
		},
	}...)

	balancer.dist.SegmentDistManager.Update(ch1Nodes[0], []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[0].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel1",
			},
			Node: ch1Nodes[0],
		},
	}...)

	balancer.dist.SegmentDistManager.Update(ch2Nodes[0], []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[1].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel2",
			},
			Node: ch2Nodes[0],
		},
	}...)

	balancer.nodeManager.Stopping(ch1Nodes[0])
	balancer.nodeManager.Stopping(ch2Nodes[0])
	suite.balancer.meta.ResourceManager.HandleNodeStopping(ch1Nodes[0])
	suite.balancer.meta.ResourceManager.HandleNodeStopping(ch2Nodes[0])
	utils.RecoverAllCollection(balancer.meta)

	replica = balancer.meta.ReplicaManager.Get(replica.GetID())
	sPlans, cPlans := balancer.BalanceReplica(replica)
	suite.Len(sPlans, 0)
	suite.Len(cPlans, 2)

	balancer.dist.ChannelDistManager.Update(ch1Nodes[0])
	balancer.dist.ChannelDistManager.Update(ch2Nodes[0])

	sPlans, cPlans = balancer.BalanceReplica(replica)
	suite.Len(sPlans, 2)
	suite.Len(cPlans, 0)
}

func (suite *ChannelLevelScoreBalancerTestSuite) TestExclusiveChannelBalance_SegmentUnbalance() {
	Params.Save(Params.QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	defer Params.Reset(Params.QueryCoordCfg.Balancer.Key)
	Params.Save(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")
	defer Params.Reset(Params.QueryCoordCfg.ChannelExclusiveNodeFactor.Key)

	balancer := suite.balancer

	collectionID := int64(1)
	partitionID := int64(1)

	// 1. set up target for multi collections
	segments := []*datapb.SegmentInfo{
		{ID: 1, PartitionID: partitionID}, {ID: 2, PartitionID: partitionID}, {ID: 3, PartitionID: partitionID}, {ID: 4, PartitionID: partitionID},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1, ChannelName: "channel1",
		},
		{
			CollectionID: 1, ChannelName: "channel2",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		channels, segments, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{collectionID}, nil).Maybe()

	collection := utils.CreateTestCollection(collectionID, int32(1))
	collection.LoadPercentage = 100
	collection.Status = querypb.LoadStatus_Loaded
	balancer.meta.CollectionManager.PutCollection(collection)
	balancer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(collectionID, partitionID))
	balancer.meta.ReplicaManager.Spawn(1, map[string]int{meta.DefaultResourceGroupName: 1}, []string{"channel1", "channel2"})
	balancer.targetMgr.UpdateCollectionNextTarget(collectionID)
	balancer.targetMgr.UpdateCollectionCurrentTarget(collectionID)

	// 3. set up nodes info and resourceManager for balancer
	nodeCount := 4
	for i := 0; i < nodeCount; i++ {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		// nodeInfo.UpdateStats(session.WithChannelCnt(len(c.distributionChannels[c.nodes[i]])))
		nodeInfo.SetState(session.NodeStateNormal)
		suite.balancer.nodeManager.Add(nodeInfo)
		suite.balancer.meta.ResourceManager.HandleNodeUp(nodeInfo.ID())
	}
	utils.RecoverAllCollection(balancer.meta)

	replica := balancer.meta.ReplicaManager.GetByCollection(collectionID)[0]
	ch1Nodes := replica.GetChannelRWNodes("channel1")
	ch2Nodes := replica.GetChannelRWNodes("channel2")
	suite.Len(ch1Nodes, 2)
	suite.Len(ch2Nodes, 2)

	balancer.dist.ChannelDistManager.Update(ch1Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel1",
			},
			Node: ch1Nodes[0],
		},
	}...)

	balancer.dist.ChannelDistManager.Update(ch2Nodes[0], []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  "channel2",
			},
			Node: ch2Nodes[0],
		},
	}...)

	balancer.dist.SegmentDistManager.Update(ch1Nodes[0], []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[0].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel1",
			},
			Node: ch1Nodes[0],
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[1].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel1",
			},
			Node: ch1Nodes[0],
		},
	}...)

	balancer.dist.SegmentDistManager.Update(ch2Nodes[0], []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[2].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel2",
			},
			Node: ch2Nodes[0],
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segments[3].ID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				NumOfRows:     10,
				InsertChannel: "channel2",
			},
			Node: ch2Nodes[0],
		},
	}...)

	sPlans, cPlans := balancer.BalanceReplica(replica)
	suite.Len(sPlans, 2)
	suite.Len(cPlans, 0)
}

func TestChannelLevelScoreBalancerSuite(t *testing.T) {
	suite.Run(t, new(ChannelLevelScoreBalancerTestSuite))
}
