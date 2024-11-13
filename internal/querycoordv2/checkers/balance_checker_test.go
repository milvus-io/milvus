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
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type BalanceCheckerTestSuite struct {
	suite.Suite
	kv        kv.MetaKv
	checker   *BalanceChecker
	balancer  *balance.MockBalancer
	meta      *meta.Meta
	broker    *meta.MockBroker
	nodeMgr   *session.NodeManager
	scheduler *task.MockScheduler
	targetMgr *meta.TargetManager
	dist      *meta.DistributionManager
}

func (suite *BalanceCheckerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *BalanceCheckerTestSuite) SetupTest() {
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

	// meta
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.scheduler = task.NewMockScheduler(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)

	suite.balancer = balance.NewMockBalancer(suite.T())
	suite.dist = meta.NewDistributionManager()
	suite.checker = NewBalanceChecker(suite.meta, suite.dist, suite.targetMgr, suite.nodeMgr, suite.scheduler, func() balance.Balance { return suite.balancer })
}

func (suite *BalanceCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *BalanceCheckerTestSuite) TestAutoBalanceConf() {
	// set up nodes info
	nodeID1, nodeID2 := 1, 2
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(nodeID1),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(nodeID2),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.checker.meta.ResourceManager.HandleNodeUp(int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(int64(nodeID2))

	// set collections meta
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(channels, segments, nil)

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{int64(nodeID1), int64(nodeID2)})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(replica1)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(replica2)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid2))

	// test disable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
	suite.scheduler.EXPECT().GetSegmentTaskNum().Maybe().Return(func() int {
		return 0
	})
	replicasToBalance := suite.checker.replicasToBalance()
	suite.Empty(replicasToBalance)
	segPlans, _ := suite.checker.balanceReplicas(replicasToBalance)
	suite.Empty(segPlans)

	// test enable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	idsToBalance := []int64{int64(replicaID1)}
	replicasToBalance = suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)
	// next round
	idsToBalance = []int64{int64(replicaID2)}
	replicasToBalance = suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)
}

func (suite *BalanceCheckerTestSuite) TestBusyScheduler() {
	// set up nodes info
	nodeID1, nodeID2 := 1, 2
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(nodeID1),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(nodeID2),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.checker.meta.ResourceManager.HandleNodeUp(int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(int64(nodeID2))

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(channels, segments, nil)

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{int64(nodeID1), int64(nodeID2)})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(replica1)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(replica2)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid2))

	// test scheduler busy
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	suite.scheduler.EXPECT().GetSegmentTaskNum().Maybe().Return(func() int {
		return 1
	})
	replicasToBalance := suite.checker.replicasToBalance()
	suite.Len(replicasToBalance, 1)
}

func (suite *BalanceCheckerTestSuite) TestStoppingBalance() {
	// set up nodes info, stopping node1
	nodeID1, nodeID2 := 1, 2
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(nodeID1),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(nodeID2),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Stopping(int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(int64(nodeID2))

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(channels, segments, nil)

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{int64(nodeID1), int64(nodeID2)})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(replica1)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(replica2)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid2))

	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(mr1.IntoReplica())

	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(mr2.IntoReplica())

	// test stopping balance
	idsToBalance := []int64{int64(replicaID1), int64(replicaID2)}
	replicasToBalance := suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)

	// checker check
	segPlans, chanPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	mockPlan := balance.SegmentAssignPlan{
		Segment: utils.CreateTestSegment(1, 1, 1, 1, 1, "1"),
		Replica: meta.NilReplica,
		From:    1,
		To:      2,
	}
	segPlans = append(segPlans, mockPlan)
	suite.balancer.EXPECT().BalanceReplica(mock.Anything).Return(segPlans, chanPlans)
	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 2)
}

func (suite *BalanceCheckerTestSuite) TestTargetNotReady() {
	// set up nodes info, stopping node1
	nodeID1, nodeID2 := int64(1), int64(2)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Stopping(nodeID1)
	suite.checker.meta.ResourceManager.HandleNodeUp(nodeID1)
	suite.checker.meta.ResourceManager.HandleNodeUp(nodeID2)

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(channels, segments, nil)

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{nodeID1, nodeID2})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(replica1)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{nodeID1, nodeID2})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(replica2)

	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(mr1.IntoReplica())

	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(mr2.IntoReplica())

	// test stopping balance
	idsToBalance := []int64{int64(replicaID1)}
	replicasToBalance := suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)
}

func (suite *BalanceCheckerTestSuite) TestMultiCollections() {
	nodeID1, nodeID2 := int64(1), int64(2)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Stopping(nodeID1)
	suite.checker.meta.ResourceManager.HandleNodeUp(nodeID1)
	suite.checker.meta.ResourceManager.HandleNodeUp(nodeID2)

	c1Segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
			NumOfRows:     100,
		},
		{
			ID:            2,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
			NumOfRows:     100,
		},
	}
	c1Channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}

	c2Segments := []*datapb.SegmentInfo{
		{
			ID:            3,
			PartitionID:   2,
			InsertChannel: "test-insert-channel",
			NumOfRows:     10000,
		},
		{
			ID:            4,
			PartitionID:   2,
			InsertChannel: "test-insert-channel",
			NumOfRows:     10000,
		},
	}
	c2Channels := []*datapb.VchannelInfo{
		{
			CollectionID: 2,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, collectionID int64, i2 ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
			if collectionID == 1 {
				return c1Channels, c1Segments, nil
			}
			return c2Channels, c2Segments, nil
		})

	segmentsOnDist := []*meta.Segment{
		{
			SegmentInfo: c1Segments[0],
			Node:        1,
		},
		{
			SegmentInfo: c1Segments[1],
			Node:        1,
		},
		{
			SegmentInfo: c2Segments[0],
			Node:        1,
		},
		{
			SegmentInfo: c2Segments[1],
			Node:        1,
		},
	}
	suite.dist.SegmentDistManager.Update(1, segmentsOnDist...)

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{nodeID1, nodeID2})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(replica1)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{nodeID1, nodeID2})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(replica2)
	suite.targetMgr.UpdateCollectionNextTarget(int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(int64(cid2))

	// test balance, expect balance collection 2
	replicas := suite.checker.replicasToBalance()
	suite.Len(replicas, 1)
	suite.Equal(replicas[0], replica1.GetID())

	// mock distribution
	segmentsOnDist1 := []*meta.Segment{
		{
			SegmentInfo: c1Segments[0],
			Node:        1,
		},
		{
			SegmentInfo: c1Segments[1],
			Node:        1,
		},
		{
			SegmentInfo: c2Segments[0],
			Node:        1,
		},
	}

	segmentsOnDist2 := []*meta.Segment{
		{
			SegmentInfo: c2Segments[1],
			Node:        1,
		},
	}
	suite.dist.SegmentDistManager.Update(1, segmentsOnDist1...)
	suite.dist.SegmentDistManager.Update(2, segmentsOnDist2...)

	// test balance, expect balance collection 2
	replicas = suite.checker.replicasToBalance()
	suite.Len(replicas, 1)
	suite.Equal(replicas[0], replica2.GetID())

	// test balance, expect balance collection 2
	replicas = suite.checker.replicasToBalance()
	suite.Len(replicas, 1)
	suite.Equal(replicas[0], replica1.GetID())
}

func TestBalanceCheckerSuite(t *testing.T) {
	suite.Run(t, new(BalanceCheckerTestSuite))
}
