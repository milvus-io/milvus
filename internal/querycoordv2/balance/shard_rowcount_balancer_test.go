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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/mock"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"

	"github.com/stretchr/testify/suite"
)

type ShardRowCountBalancerTestSuite struct {
	suite.Suite
	balancer *ShardRowCountBasedBalancer
	kv       *etcdkv.EtcdKV
	broker   *meta.MockBroker
}

func (suite *ShardRowCountBalancerTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *ShardRowCountBalancerTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd,
		config.EtcdUseSSL,
		config.Endpoints,
		config.EtcdTLSCert,
		config.EtcdTLSKey,
		config.EtcdTLSCACert,
		config.EtcdTLSMinVersion)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)
	suite.broker = meta.NewMockBroker(suite.T())

	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	testMeta := meta.NewMeta(idAllocator, store)
	testTarget := meta.NewTargetManager(suite.broker, testMeta)

	distManager := meta.NewDistributionManager()
	nodeManager := session.NewNodeManager()
	suite.balancer = NewShardRowCountBasedBalancer(nil, nodeManager, distManager, testMeta, testTarget)
}

func (suite *ShardRowCountBalancerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ShardRowCountBalancerTestSuite) TestAssignSegment() {
	cases := []struct {
		name          string
		distributions map[int64][]*meta.Segment
		assignments   []*meta.Segment
		nodes         []int64
		expectPlans   []SegmentAssignPlan
	}{
		{
			name: "test normal assignment",
			distributions: map[int64][]*meta.Segment{
				2: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 2}},
				3: {{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 30, InsertChannel: "channel2"}, Node: 3}},
			},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5, InsertChannel: "channel1"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, InsertChannel: "channel2"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 16, InsertChannel: "channel3"}},
			},
			nodes: []int64{1, 2, 3},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5, InsertChannel: "channel1"}}, From: -1, To: 2},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, InsertChannel: "channel2"}}, From: -1, To: 1},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 16, InsertChannel: "channel3"}}, From: -1, To: 1},
			},
		},
		{
			name: "test normal assignment",
			distributions: map[int64][]*meta.Segment{
				2: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 2}},
				3: {{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 30, InsertChannel: "channel2"}, Node: 3}},
			},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5, InsertChannel: "channel1"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, InsertChannel: "channel1"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 16, InsertChannel: "channel2"}},
			},
			nodes: []int64{1, 2, 3},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5, InsertChannel: "channel1"}}, From: -1, To: 2},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, InsertChannel: "channel1"}}, From: -1, To: 1},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 16, InsertChannel: "channel2"}}, From: -1, To: 1},
			},
		},

		{
			name: "test normal assignment",
			distributions: map[int64][]*meta.Segment{
				2: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1, InsertChannel: "channel1"}, Node: 2}},
				3: {{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 4, InsertChannel: "channel2"}, Node: 3}},
			},
			assignments: []*meta.Segment{
				{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5, InsertChannel: "channel1"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, InsertChannel: "channel1"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 7, InsertChannel: "channel2"}},
				{SegmentInfo: &datapb.SegmentInfo{ID: 6, NumOfRows: 8, InsertChannel: "channel2"}},
			},
			nodes: []int64{1, 2, 3},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 5, InsertChannel: "channel1"}}, From: -1, To: 1},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10, InsertChannel: "channel1"}}, From: -1, To: 2},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 5, NumOfRows: 7, InsertChannel: "channel2"}}, From: -1, To: 3},
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 6, NumOfRows: 8, InsertChannel: "channel2"}}, From: -1, To: 1},
			},
		},
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
			plans := balancer.AssignSegment(c.assignments, c.nodes, nil)
			fmt.Println("expect", c.expectPlans)
			fmt.Println("plans:", plans)
			suite.ElementsMatch(c.expectPlans, plans)
		})
	}
}

func (suite *ShardRowCountBalancerTestSuite) TestAssignSegmentShardsNumSmallThanNode() {
	type TestCase struct {
		name          string
		distributions map[int64][]*meta.Segment
		assignments   []*meta.Segment
		nodes         []int64
		expectPlans   []SegmentAssignPlan
	}

	c := TestCase{
		name: "test assignment: ShardsNumSmallThanNode",
		distributions: map[int64][]*meta.Segment{
			2: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 1}},
			3: {{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 30, InsertChannel: "channel2"}, Node: 2}},
		},
		assignments: []*meta.Segment{
			{SegmentInfo: &datapb.SegmentInfo{ID: -1, NumOfRows: 5, InsertChannel: "channel1"}},
			{SegmentInfo: &datapb.SegmentInfo{ID: -1, NumOfRows: 10, InsertChannel: "channel2"}},
		},
		nodes: []int64{},
	}
	numNode := 30
	segNum := 100

	for i := 0; i < numNode; i++ {
		c.nodes = append(c.nodes, int64(i+1))
	}

	var numRows []int64
	for i := 0; i < segNum; i++ {
		numRow := rand.Int63n(1000000)
		channelName := "channel1"
		c.assignments = append(c.assignments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            int64(i),
				InsertChannel: channelName,
				NumOfRows:     numRow,
			},
			Node: 0,
		})
		numRows = append(numRows, numRow)
	}

	for i, numRow := range numRows {
		channelName := "channel2"
		c.assignments = append(c.assignments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            int64(segNum + i),
				InsertChannel: channelName,
				NumOfRows:     numRow + rand.Int63n(500000),
			},
			Node: 0,
		})
	}

	suite.Run(c.name, func() {
		suite.SetupSuite()
		defer suite.TearDownTest()
		balancer := suite.balancer
		for node, s := range c.distributions {
			balancer.dist.SegmentDistManager.Update(node, s...)
		}
		plans := balancer.AssignSegment(c.assignments, c.nodes, nil)
		nInfos := balancer.convertToNodeInfos(c.nodes)
		for _, plan := range plans {
			nInfos.removeNode(plan.From, plan.Segment.NumOfRows, plan.Segment.InsertChannel)
			nInfos.addNode(plan.To, plan.Segment.NumOfRows, plan.Segment.InsertChannel, true)
		}
		fmt.Println(nInfos.getStatsStr())
	})
}

func (suite *ShardRowCountBalancerTestSuite) TestAssignSegmentShardsNumBigThanNode() {
	type TestCase struct {
		name          string
		distributions map[int64][]*meta.Segment
		assignments   []*meta.Segment
		nodes         []int64
		expectPlans   []SegmentAssignPlan
	}

	c := TestCase{
		name: "test assignment: ShardsNumSmallThanNode",
		distributions: map[int64][]*meta.Segment{
			2: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 1}},
			3: {{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 30, InsertChannel: "channel2"}, Node: 2}},
		},
		assignments: []*meta.Segment{
			{SegmentInfo: &datapb.SegmentInfo{ID: -1, NumOfRows: 5, InsertChannel: "channel1"}},
			{SegmentInfo: &datapb.SegmentInfo{ID: -1, NumOfRows: 10, InsertChannel: "channel2"}},
		},
		nodes: []int64{},
	}
	numNode := 30
	segNum := 1000

	for i := 0; i < numNode; i++ {
		c.nodes = append(c.nodes, int64(i+1))
	}

	channelNum := 50

	var numRows []int64

	for i := 0; i < segNum/channelNum; i++ {
		numRow := rand.Int63n(1000000)
		numRows = append(numRows, numRow)
	}

	for i := 1; i <= channelNum; i++ {
		cName := fmt.Sprintf("channel%d", i)
		for j := 0; j < segNum/channelNum; j++ {
			numRow := numRows[j]
			c.assignments = append(c.assignments, &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            -1,
					InsertChannel: cName,
					NumOfRows:     numRow + rand.Int63n(50000),
				},
			})
		}
	}
	suite.Run(c.name, func() {
		suite.SetupSuite()
		defer suite.TearDownTest()
		balancer := suite.balancer
		for node, s := range c.distributions {
			balancer.dist.SegmentDistManager.Update(node, s...)
		}
		plans := balancer.AssignSegment(c.assignments, c.nodes, nil)
		nInfos := balancer.convertToNodeInfos(c.nodes)
		for _, plan := range plans {
			nInfos.removeNode(plan.From, plan.Segment.NumOfRows, plan.Segment.InsertChannel)
			nInfos.addNode(plan.To, plan.Segment.NumOfRows, plan.Segment.InsertChannel, true)
		}
		fmt.Println(nInfos.getStatsStr())
	})
}

func (suite *ShardRowCountBalancerTestSuite) TestBalance() {
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
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 10, InsertChannel: "channel1"}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30, InsertChannel: "channel2"}, Node: 2},
				},
			},
			expectPlans: []SegmentAssignPlan{
				{Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 2}, From: 2, To: 1, ReplicaID: 1},
			},
		},
		{
			name:  "already balanced",
			nodes: []int64{1, 2},
			distributions: map[int64][]*meta.Segment{
				1: {{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, NumOfRows: 30, InsertChannel: "channel1"}, Node: 1}},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, NumOfRows: 20, InsertChannel: "channel1"}, Node: 2},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, NumOfRows: 30, InsertChannel: "channel2"}, Node: 2},
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

func prepareBalanceSegments(nodeID int64, segmentCnt int, segmentMaxSize int64, channelNum int, segmentIDStart int64) ([]*meta.Segment, []*datapb.SegmentBinlogs) {
	var nodeSegmentBinlogs []*datapb.SegmentBinlogs
	var nodeMetaSegments []*meta.Segment
	for i := 0; i < segmentCnt; i++ {
		channel := 0
		for channel == 0 {
			channel = int(rand.Int31n(int32(channelNum)))
		}
		insertChannel := fmt.Sprintf("channel%d", channel)
		binLogInfo := &datapb.SegmentBinlogs{
			SegmentID:     segmentIDStart + int64(i),
			NumOfRows:     rand.Int63n(segmentMaxSize),
			InsertChannel: insertChannel,
		}
		nodeSegmentBinlogs = append(nodeSegmentBinlogs, binLogInfo)
		nodeMetaSegments = append(nodeMetaSegments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            binLogInfo.SegmentID,
				CollectionID:  1,
				InsertChannel: insertChannel,
				NumOfRows:     binLogInfo.NumOfRows,
			},
			Node:    nodeID,
			Version: 1,
		})
	}
	return nodeMetaSegments, nodeSegmentBinlogs
}

func (suite *ShardRowCountBalancerTestSuite) TestBalanceBigShardSmallNode() {
	suite.testBalanceShardNodeNode(2, 30, "big shard small node")
}

func (suite *ShardRowCountBalancerTestSuite) TestBalanceSmallShardBigNode() {
	suite.testBalanceShardNodeNode(30, 2, "small shard big node")
}

func (suite *ShardRowCountBalancerTestSuite) TestBalanceBigShardBigNode() {
	suite.testBalanceShardNodeNode(30, 50, "big shard big node")
}

func (suite *ShardRowCountBalancerTestSuite) TestBalanceSmallShardSmallNode() {
	suite.testBalanceShardNodeNode(2, 2, "small shard small node")
}

func (suite *ShardRowCountBalancerTestSuite) testBalanceShardNodeNode(nodeNum int, shardNum int, testName string) {
	type TestCase struct {
		name          string
		nodes         []int64
		distributions map[int64][]*meta.Segment
		expectPlans   []SegmentAssignPlan
	}
	//nodeNum := 30

	c := TestCase{
		name:          testName,
		nodes:         []int64{},
		distributions: make(map[int64][]*meta.Segment),
	}
	for i := 0; i < nodeNum; i++ {
		c.nodes = append(c.nodes, int64(i+1))
	}

	suite.Run(c.name, func() {
		suite.SetupSuite()
		defer suite.TearDownTest()
		balancer := suite.balancer
		collection := utils.CreateTestCollection(1, 1)
		nodeMaxSegmentCnt := 10
		segmentCnt := int64(0)
		segmentMaxSize := int64(1000000)
		var allSegmentBinlogs []*datapb.SegmentBinlogs

		for _, nodeID := range c.nodes {
			cnt := 0
			for cnt == 0 {
				cnt = int(rand.Int31n(int32(nodeMaxSegmentCnt)))
			}
			nodeMetaSegments, deltaSegmentBinlogs := prepareBalanceSegments(nodeID, cnt, segmentMaxSize, shardNum, segmentCnt+1)
			allSegmentBinlogs = append(allSegmentBinlogs, deltaSegmentBinlogs...)
			segmentCnt += int64(cnt)
			c.distributions[nodeID] = nodeMetaSegments
		}
		suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, int64(1), int64(1)).Return(
			nil, allSegmentBinlogs, nil)
		balancer.targetMgr.UpdateCollectionNextTargetWithPartitions(int64(1), int64(1))
		balancer.targetMgr.UpdateCollectionCurrentTarget(1, 1)
		collection.LoadPercentage = 100
		collection.Status = querypb.LoadStatus_Loaded
		balancer.meta.CollectionManager.PutCollection(collection)
		balancer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, c.nodes))
		for node, s := range c.distributions {
			balancer.dist.SegmentDistManager.Update(node, s...)
		}

		nInfosOld := balancer.convertToNodeInfos(c.nodes)
		curSD := nInfosOld.calculateSD()
		fmt.Println(nInfosOld.getStatsStr())
		segmentPlans, channelPlans := balancer.Balance()
		suite.Empty(channelPlans)
		for _, plan := range segmentPlans {
			fmtStr := fmt.Sprintf("segment%d (%d rows, channel:%s) node%d --> node%d", plan.Segment.GetID(), plan.Segment.GetNumOfRows(), plan.Segment.GetInsertChannel(), plan.From, plan.To)
			fmt.Println(fmtStr)
			//fmt.Println("plan:", plan)
		}
		nInfosNew := balancer.convertToNodeInfos(c.nodes)
		for _, plan := range segmentPlans {
			nInfosNew.removeNode(plan.From, plan.Segment.NumOfRows, plan.Segment.InsertChannel)
			nInfosNew.addNode(plan.To, plan.Segment.NumOfRows, plan.Segment.InsertChannel, true)
		}
		newSD := nInfosNew.calculateSD()
		suite.Equal(true, newSD <= curSD)
		fmt.Println(nInfosNew.getStatsStr())
	})
}

func (suite *ShardRowCountBalancerTestSuite) TestBalanceOnLoadingCollection() {
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

func TestShardRowCountBasedBalancerSuite(t *testing.T) {
	suite.Run(t, new(ShardRowCountBalancerTestSuite))
}
