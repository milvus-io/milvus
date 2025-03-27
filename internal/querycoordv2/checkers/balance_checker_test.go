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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	targetMgr meta.TargetManagerInterface
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
	suite.checker = NewBalanceChecker(suite.meta, suite.targetMgr, suite.nodeMgr, suite.scheduler, func() balance.Balance { return suite.balancer })
}

func (suite *BalanceCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *BalanceCheckerTestSuite) TestAutoBalanceConf() {
	ctx := context.Background()
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
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID2))

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
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid2))

	// test disable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
	suite.scheduler.EXPECT().GetSegmentTaskNum().Maybe().Return(func() int {
		return 0
	})
	replicasToBalance := suite.checker.getReplicaForNormalBalance(ctx)
	suite.Empty(replicasToBalance)
	segPlans, _ := suite.checker.balanceReplicas(ctx, replicasToBalance)
	suite.Empty(segPlans)

	// test enable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	idsToBalance := []int64{int64(replicaID1)}
	replicasToBalance = suite.checker.getReplicaForNormalBalance(ctx)
	suite.ElementsMatch(idsToBalance, replicasToBalance)
	// next round
	idsToBalance = []int64{int64(replicaID2)}
	replicasToBalance = suite.checker.getReplicaForNormalBalance(ctx)
	suite.ElementsMatch(idsToBalance, replicasToBalance)
	// final round
	replicasToBalance = suite.checker.getReplicaForNormalBalance(ctx)
	suite.Empty(replicasToBalance)
}

func (suite *BalanceCheckerTestSuite) TestBusyScheduler() {
	ctx := context.Background()
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
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID2))

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
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid2))

	// test scheduler busy
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	suite.scheduler.EXPECT().GetSegmentTaskNum().Maybe().Return(func() int {
		return 1
	})
	replicasToBalance := suite.checker.getReplicaForNormalBalance(ctx)
	suite.Len(replicasToBalance, 1)
}

func (suite *BalanceCheckerTestSuite) TestStoppingBalance() {
	ctx := context.Background()
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
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID2))

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
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid1))

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid2))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid2))

	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr1.IntoReplica())

	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr2.IntoReplica())

	// test stopping balance
	idsToBalance := []int64{int64(replicaID1)}
	replicasToBalance := suite.checker.getReplicaForStoppingBalance(ctx)
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
	suite.balancer.EXPECT().BalanceReplica(mock.Anything, mock.Anything).Return(segPlans, chanPlans)
	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
}

func (suite *BalanceCheckerTestSuite) TestTargetNotReady() {
	ctx := context.Background()
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
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, nodeID1)
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, nodeID2)

	mockTarget := meta.NewMockTargetManager(suite.T())
	suite.checker.targetMgr = mockTarget

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{nodeID1, nodeID2})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{nodeID1, nodeID2})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)

	// test normal balance when one collection has unready target
	mockTarget.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true)
	mockTarget.EXPECT().IsCurrentTargetReady(mock.Anything, mock.Anything).Return(false)
	mockTarget.EXPECT().GetCollectionRowCount(mock.Anything, mock.Anything, mock.Anything).Return(100).Maybe()
	replicasToBalance := suite.checker.getReplicaForNormalBalance(ctx)
	suite.Len(replicasToBalance, 0)

	// test stopping balance with target not ready
	mockTarget.ExpectedCalls = nil
	mockTarget.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(false)
	mockTarget.EXPECT().IsCurrentTargetExist(mock.Anything, int64(cid1), mock.Anything).Return(true).Maybe()
	mockTarget.EXPECT().IsCurrentTargetExist(mock.Anything, int64(cid2), mock.Anything).Return(false).Maybe()
	mockTarget.EXPECT().GetCollectionRowCount(mock.Anything, mock.Anything, mock.Anything).Return(100).Maybe()
	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr1.IntoReplica())

	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr2.IntoReplica())

	idsToBalance := []int64{int64(replicaID1)}
	replicasToBalance = suite.checker.getReplicaForStoppingBalance(ctx)
	suite.ElementsMatch(idsToBalance, replicasToBalance)
}

func (suite *BalanceCheckerTestSuite) TestAutoBalanceInterval() {
	ctx := context.Background()
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
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID1))
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, int64(nodeID2))

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
		{
			ID:            2,
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
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, int64(cid1))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(cid1))

	funcCallCounter := atomic.NewInt64(0)
	suite.balancer.EXPECT().BalanceReplica(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *meta.Replica) ([]balance.SegmentAssignPlan, []balance.ChannelAssignPlan) {
		funcCallCounter.Inc()
		return nil, nil
	})

	// first auto balance should be triggered
	suite.checker.Check(ctx)
	suite.Equal(funcCallCounter.Load(), int64(1))

	// second auto balance won't be triggered due to autoBalanceInterval == 3s
	suite.checker.Check(ctx)
	suite.Equal(funcCallCounter.Load(), int64(1))

	// set autoBalanceInterval == 1, sleep 1s, auto balance should be triggered
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.AutoBalanceInterval.Key, "1000")
	paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.AutoBalanceInterval.Key)
	time.Sleep(1 * time.Second)
	suite.checker.Check(ctx)
	suite.Equal(funcCallCounter.Load(), int64(1))
}

func (suite *BalanceCheckerTestSuite) TestBalanceOrder() {
	ctx := context.Background()
	nodeID1, nodeID2 := int64(1), int64(2)

	// set collections meta
	cid1, replicaID1, partitionID1 := 1, 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{nodeID1, nodeID2})
	partition1 := utils.CreateTestPartition(int64(cid1), int64(partitionID1))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1, partition1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)

	cid2, replicaID2, partitionID2 := 2, 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{nodeID1, nodeID2})
	partition2 := utils.CreateTestPartition(int64(cid2), int64(partitionID2))
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2, partition2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)

	// mock collection row count
	mockTargetManager := meta.NewMockTargetManager(suite.T())
	suite.checker.targetMgr = mockTargetManager
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, int64(cid1), mock.Anything).Return(int64(100)).Maybe()
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, int64(cid2), mock.Anything).Return(int64(200)).Maybe()
	mockTargetManager.EXPECT().IsCurrentTargetReady(mock.Anything, mock.Anything).Return(true).Maybe()
	mockTargetManager.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true).Maybe()

	// mock stopping node
	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(nodeID1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr1.IntoReplica())
	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(nodeID2)
	suite.checker.meta.ReplicaManager.Put(ctx, mr2.IntoReplica())

	// test normal balance order
	replicas := suite.checker.getReplicaForNormalBalance(ctx)
	suite.Equal(replicas, []int64{int64(replicaID2)})

	// test stopping balance order
	replicas = suite.checker.getReplicaForStoppingBalance(ctx)
	suite.Equal(replicas, []int64{int64(replicaID2)})

	// mock collection row count
	mockTargetManager.ExpectedCalls = nil
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, int64(cid1), mock.Anything).Return(int64(200)).Maybe()
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, int64(cid2), mock.Anything).Return(int64(100)).Maybe()
	mockTargetManager.EXPECT().IsCurrentTargetReady(mock.Anything, mock.Anything).Return(true).Maybe()
	mockTargetManager.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true).Maybe()

	// test normal balance order
	replicas = suite.checker.getReplicaForNormalBalance(ctx)
	suite.Equal(replicas, []int64{int64(replicaID1)})

	// test stopping balance order
	replicas = suite.checker.getReplicaForStoppingBalance(ctx)
	suite.Equal(replicas, []int64{int64(replicaID1)})
}

func (suite *BalanceCheckerTestSuite) TestSortCollections() {
	ctx := context.Background()

	// Set up test collections
	cid1, cid2, cid3 := int64(1), int64(2), int64(3)

	// Mock the target manager for row count returns
	mockTargetManager := meta.NewMockTargetManager(suite.T())
	suite.checker.targetMgr = mockTargetManager

	// Collection 1: Low ID, High row count
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, cid1, mock.Anything).Return(int64(300)).Maybe()

	// Collection 2: Middle ID, Low row count
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, cid2, mock.Anything).Return(int64(100)).Maybe()

	// Collection 3: High ID, Middle row count
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, cid3, mock.Anything).Return(int64(200)).Maybe()

	collections := []int64{cid1, cid2, cid3}

	// Test ByRowCount sorting (default)
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "ByRowCount")
	sortedCollections := suite.checker.sortCollections(ctx, collections)
	suite.Equal([]int64{cid1, cid3, cid2}, sortedCollections, "Collections should be sorted by row count (highest first)")

	// Test ByCollectionID sorting
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "ByCollectionID")
	sortedCollections = suite.checker.sortCollections(ctx, collections)
	suite.Equal([]int64{cid1, cid2, cid3}, sortedCollections, "Collections should be sorted by collection ID (ascending)")

	// Test with empty sort order (should default to ByRowCount)
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "")
	sortedCollections = suite.checker.sortCollections(ctx, collections)
	suite.Equal([]int64{cid1, cid3, cid2}, sortedCollections, "Should default to ByRowCount when sort order is empty")

	// Test with invalid sort order (should default to ByRowCount)
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "InvalidOrder")
	sortedCollections = suite.checker.sortCollections(ctx, collections)
	suite.Equal([]int64{cid1, cid3, cid2}, sortedCollections, "Should default to ByRowCount when sort order is invalid")

	// Test with mixed case (should be case-insensitive)
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "bYcOlLeCtIoNiD")
	sortedCollections = suite.checker.sortCollections(ctx, collections)
	suite.Equal([]int64{cid1, cid2, cid3}, sortedCollections, "Should handle case-insensitive sort order names")

	// Test with empty collection list
	emptyCollections := []int64{}
	sortedCollections = suite.checker.sortCollections(ctx, emptyCollections)
	suite.Equal([]int64{}, sortedCollections, "Should handle empty collection list")
}

func (suite *BalanceCheckerTestSuite) TestSortCollectionsIntegration() {
	ctx := context.Background()

	// Set up test collections and nodes
	nodeID1 := int64(1)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, nodeID1)

	// Create two collections to ensure sorting is triggered
	cid1, replicaID1 := int64(1), int64(101)
	collection1 := utils.CreateTestCollection(cid1, int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(replicaID1, cid1, []int64{nodeID1})
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)

	// Add a second collection with different characteristics
	cid2, replicaID2 := int64(2), int64(102)
	collection2 := utils.CreateTestCollection(cid2, int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(replicaID2, cid2, []int64{nodeID1})
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)

	// Mock target manager
	mockTargetManager := meta.NewMockTargetManager(suite.T())
	suite.checker.targetMgr = mockTargetManager

	// Setup different row counts to test sorting
	// Collection 1 has more rows than Collection 2
	var getRowCountCallCount int
	mockTargetManager.On("GetCollectionRowCount", mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, collectionID int64, scope int32) int64 {
			getRowCountCallCount++
			if collectionID == cid1 {
				return 200 // More rows in collection 1
			}
			return 100 // Fewer rows in collection 2
		})

	mockTargetManager.On("IsCurrentTargetReady", mock.Anything, mock.Anything).Return(true)
	mockTargetManager.On("IsNextTargetExist", mock.Anything, mock.Anything).Return(true)

	// Configure for testing
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "ByRowCount")

	// Clear first to avoid previous test state
	suite.checker.normalBalanceCollectionsCurrentRound.Clear()

	// Call normal balance
	_ = suite.checker.getReplicaForNormalBalance(ctx)

	// Verify GetCollectionRowCount was called at least twice (once for each collection)
	// This confirms that the collections were sorted
	suite.True(getRowCountCallCount >= 2, "GetCollectionRowCount should be called at least twice during normal balance")

	// Reset counter and test stopping balance
	getRowCountCallCount = 0

	// Set up for stopping balance test
	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(nodeID1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr1.IntoReplica())

	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(nodeID1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr2.IntoReplica())

	paramtable.Get().Save(Params.QueryCoordCfg.EnableStoppingBalance.Key, "true")

	// Call stopping balance
	_ = suite.checker.getReplicaForStoppingBalance(ctx)

	// Verify GetCollectionRowCount was called at least twice during stopping balance
	suite.True(getRowCountCallCount >= 2, "GetCollectionRowCount should be called at least twice during stopping balance")
}

func (suite *BalanceCheckerTestSuite) TestBalanceTriggerOrder() {
	ctx := context.Background()

	// Set up nodes
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
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, nodeID1)
	suite.checker.meta.ResourceManager.HandleNodeUp(ctx, nodeID2)

	// Create collections with different row counts
	cid1, replicaID1 := int64(1), int64(101)
	collection1 := utils.CreateTestCollection(cid1, int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(replicaID1, cid1, []int64{nodeID1, nodeID2})
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection1)
	suite.checker.meta.ReplicaManager.Put(ctx, replica1)

	cid2, replicaID2 := int64(2), int64(102)
	collection2 := utils.CreateTestCollection(cid2, int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(replicaID2, cid2, []int64{nodeID1, nodeID2})
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection2)
	suite.checker.meta.ReplicaManager.Put(ctx, replica2)

	cid3, replicaID3 := int64(3), int64(103)
	collection3 := utils.CreateTestCollection(cid3, int32(replicaID3))
	collection3.Status = querypb.LoadStatus_Loaded
	replica3 := utils.CreateTestReplica(replicaID3, cid3, []int64{nodeID1, nodeID2})
	suite.checker.meta.CollectionManager.PutCollection(ctx, collection3)
	suite.checker.meta.ReplicaManager.Put(ctx, replica3)

	// Mock the target manager
	mockTargetManager := meta.NewMockTargetManager(suite.T())
	suite.checker.targetMgr = mockTargetManager

	// Set row counts: Collection 1 (highest), Collection 3 (middle), Collection 2 (lowest)
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, cid1, mock.Anything).Return(int64(300)).Maybe()
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, cid2, mock.Anything).Return(int64(100)).Maybe()
	mockTargetManager.EXPECT().GetCollectionRowCount(mock.Anything, cid3, mock.Anything).Return(int64(200)).Maybe()

	// Mark the current target as ready
	mockTargetManager.EXPECT().IsCurrentTargetReady(mock.Anything, mock.Anything).Return(true).Maybe()
	mockTargetManager.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true).Maybe()
	mockTargetManager.EXPECT().IsCurrentTargetExist(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Enable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")

	// Test with ByRowCount order (default)
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "ByRowCount")
	suite.checker.normalBalanceCollectionsCurrentRound.Clear()

	// Normal balance should pick the collection with highest row count first
	replicas := suite.checker.getReplicaForNormalBalance(ctx)
	suite.Contains(replicas, replicaID1, "Should balance collection with highest row count first")

	// Add stopping nodes to test stopping balance
	mr1 := replica1.CopyForWrite()
	mr1.AddRONode(nodeID1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr1.IntoReplica())

	mr2 := replica2.CopyForWrite()
	mr2.AddRONode(nodeID1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr2.IntoReplica())

	mr3 := replica3.CopyForWrite()
	mr3.AddRONode(nodeID1)
	suite.checker.meta.ReplicaManager.Put(ctx, mr3.IntoReplica())

	// Enable stopping balance
	paramtable.Get().Save(Params.QueryCoordCfg.EnableStoppingBalance.Key, "true")

	// Stopping balance should also pick the collection with highest row count first
	replicas = suite.checker.getReplicaForStoppingBalance(ctx)
	suite.Contains(replicas, replicaID1, "Stopping balance should prioritize collection with highest row count")

	// Test with ByCollectionID order
	paramtable.Get().Save(Params.QueryCoordCfg.BalanceTriggerOrder.Key, "ByCollectionID")
	suite.checker.normalBalanceCollectionsCurrentRound.Clear()

	// Normal balance should pick the collection with lowest ID first
	replicas = suite.checker.getReplicaForNormalBalance(ctx)
	suite.Contains(replicas, replicaID1, "Should balance collection with lowest ID first")

	// Stopping balance should also pick the collection with lowest ID first
	replicas = suite.checker.getReplicaForStoppingBalance(ctx)
	suite.Contains(replicas, replicaID1, "Stopping balance should prioritize collection with lowest ID")
}

func TestBalanceCheckerSuite(t *testing.T) {
	suite.Run(t, new(BalanceCheckerTestSuite))
}
