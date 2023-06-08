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

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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
}

func (suite *BalanceCheckerTestSuite) SetupSuite() {
	Params.Init()
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
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.scheduler = task.NewMockScheduler(suite.T())

	suite.balancer = balance.NewMockBalancer(suite.T())
	suite.checker = NewBalanceChecker(suite.meta, suite.balancer, suite.nodeMgr, suite.scheduler)
}

func (suite *BalanceCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *BalanceCheckerTestSuite) TestAutoBalanceConf() {
	//set up nodes info
	nodeID1, nodeID2 := 1, 2
	suite.nodeMgr.Add(session.NewNodeInfo(int64(nodeID1), "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(int64(nodeID2), "localhost"))
	suite.checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(nodeID1))
	suite.checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(nodeID2))

	// set collections meta
	cid1, replicaID1 := 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{int64(nodeID1), int64(nodeID2)})
	suite.checker.meta.CollectionManager.PutCollection(collection1)
	suite.checker.meta.ReplicaManager.Put(replica1)

	cid2, replicaID2 := 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	suite.checker.meta.CollectionManager.PutCollection(collection2)
	suite.checker.meta.ReplicaManager.Put(replica2)

	//test disable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
	suite.scheduler.EXPECT().GetSegmentTaskNum().Maybe().Return(func() int {
		return 0
	})
	replicasToBalance := suite.checker.replicasToBalance()
	suite.Empty(replicasToBalance)
	segPlans, _ := suite.checker.balanceReplicas(replicasToBalance)
	suite.Empty(segPlans)

	//test enable auto balance
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	idsToBalance := []int64{int64(replicaID1)}
	replicasToBalance = suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)
	//next round
	idsToBalance = []int64{int64(replicaID2)}
	replicasToBalance = suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)
	//final round
	replicasToBalance = suite.checker.replicasToBalance()
	suite.Empty(replicasToBalance)
}

func (suite *BalanceCheckerTestSuite) TestBusyScheduler() {
	//set up nodes info
	nodeID1, nodeID2 := 1, 2
	suite.nodeMgr.Add(session.NewNodeInfo(int64(nodeID1), "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(int64(nodeID2), "localhost"))
	suite.checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(nodeID1))
	suite.checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(nodeID2))

	// set collections meta
	cid1, replicaID1 := 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{int64(nodeID1), int64(nodeID2)})
	suite.checker.meta.CollectionManager.PutCollection(collection1)
	suite.checker.meta.ReplicaManager.Put(replica1)

	cid2, replicaID2 := 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	suite.checker.meta.CollectionManager.PutCollection(collection2)
	suite.checker.meta.ReplicaManager.Put(replica2)

	//test scheduler busy
	paramtable.Get().Save(Params.QueryCoordCfg.AutoBalance.Key, "true")
	suite.scheduler.EXPECT().GetSegmentTaskNum().Maybe().Return(func() int {
		return 1
	})
	replicasToBalance := suite.checker.replicasToBalance()
	suite.Empty(replicasToBalance)
	segPlans, _ := suite.checker.balanceReplicas(replicasToBalance)
	suite.Empty(segPlans)
}

func (suite *BalanceCheckerTestSuite) TestStoppingBalance() {
	//set up nodes info, stopping node1
	nodeID1, nodeID2 := 1, 2
	suite.nodeMgr.Add(session.NewNodeInfo(int64(nodeID1), "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(int64(nodeID2), "localhost"))
	suite.nodeMgr.Stopping(int64(nodeID1))
	suite.checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(nodeID1))
	suite.checker.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(nodeID2))

	// set collections meta
	cid1, replicaID1 := 1, 1
	collection1 := utils.CreateTestCollection(int64(cid1), int32(replicaID1))
	collection1.Status = querypb.LoadStatus_Loaded
	replica1 := utils.CreateTestReplica(int64(replicaID1), int64(cid1), []int64{int64(nodeID1), int64(nodeID2)})
	suite.checker.meta.CollectionManager.PutCollection(collection1)
	suite.checker.meta.ReplicaManager.Put(replica1)

	cid2, replicaID2 := 2, 2
	collection2 := utils.CreateTestCollection(int64(cid2), int32(replicaID2))
	collection2.Status = querypb.LoadStatus_Loaded
	replica2 := utils.CreateTestReplica(int64(replicaID2), int64(cid2), []int64{int64(nodeID1), int64(nodeID2)})
	suite.checker.meta.CollectionManager.PutCollection(collection2)
	suite.checker.meta.ReplicaManager.Put(replica2)

	//test stopping balance
	idsToBalance := []int64{int64(replicaID1), int64(replicaID2)}
	replicasToBalance := suite.checker.replicasToBalance()
	suite.ElementsMatch(idsToBalance, replicasToBalance)

	//checker check
	segPlans, chanPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	mockPlan := balance.SegmentAssignPlan{
		Segment:   utils.CreateTestSegment(1, 1, 1, 1, 1, "1"),
		ReplicaID: 1,
		From:      1,
		To:        2,
	}
	segPlans = append(segPlans, mockPlan)
	suite.balancer.EXPECT().BalanceReplica(mock.Anything).Return(segPlans, chanPlans)
	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 2)
}

func TestBalanceCheckerSuite(t *testing.T) {
	suite.Run(t, new(BalanceCheckerTestSuite))
}
