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
package observers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReplicaObserverSuite struct {
	suite.Suite

	kv kv.MetaKv
	//dependency
	meta    *meta.Meta
	distMgr *meta.DistributionManager

	nodeMgr  *session.NodeManager
	observer *ReplicaObserver

	collectionID int64
	partitionID  int64
}

func (suite *ReplicaObserverSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.CheckNodeInReplicaInterval.Key, "1")
}

func (suite *ReplicaObserverSuite) SetupTest() {
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

	suite.distMgr = meta.NewDistributionManager()
	suite.observer = NewReplicaObserver(suite.meta, suite.distMgr)
	suite.observer.Start(context.TODO())
	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)
}

func (suite *ReplicaObserverSuite) TestCheckNodesInReplica() {
	suite.meta.ResourceManager.AddResourceGroup("rg1")
	suite.meta.ResourceManager.AddResourceGroup("rg2")
	suite.nodeMgr.Add(session.NewNodeInfo(1, "localhost:8080"))
	suite.nodeMgr.Add(session.NewNodeInfo(2, "localhost:8080"))
	suite.nodeMgr.Add(session.NewNodeInfo(3, "localhost:8080"))
	suite.nodeMgr.Add(session.NewNodeInfo(4, "localhost:8080"))
	suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 1)
	suite.meta.ResourceManager.TransferNode(meta.DefaultResourceGroupName, "rg1", 1)
	suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 2)
	suite.meta.ResourceManager.TransferNode(meta.DefaultResourceGroupName, "rg1", 1)
	suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 3)
	suite.meta.ResourceManager.TransferNode(meta.DefaultResourceGroupName, "rg2", 1)
	suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 4)
	suite.meta.ResourceManager.TransferNode(meta.DefaultResourceGroupName, "rg2", 1)

	err := suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(suite.collectionID, 1))
	suite.NoError(err)
	replicas := make([]*meta.Replica, 2)
	replicas[0] = meta.NewReplica(
		&querypb.Replica{
			ID:            10000,
			CollectionID:  suite.collectionID,
			ResourceGroup: "rg1",
			Nodes:         []int64{1, 2, 3},
		},
		typeutil.NewUniqueSet(1, 2, 3),
	)

	replicas[1] = meta.NewReplica(
		&querypb.Replica{
			ID:            10001,
			CollectionID:  suite.collectionID,
			ResourceGroup: "rg2",
			Nodes:         []int64{4},
		},
		typeutil.NewUniqueSet(4),
	)
	err = suite.meta.ReplicaManager.Put(replicas...)
	suite.NoError(err)
	suite.distMgr.ChannelDistManager.Update(1, utils.CreateTestChannel(suite.collectionID, 1, 1, "test-insert-channel1"))
	suite.distMgr.SegmentDistManager.Update(1, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 1, 1, 1, "test-insert-channel1"))
	suite.distMgr.ChannelDistManager.Update(2, utils.CreateTestChannel(suite.collectionID, 2, 1, "test-insert-channel2"))
	suite.distMgr.SegmentDistManager.Update(2, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 2, 2, 1, "test-insert-channel2"))
	suite.distMgr.ChannelDistManager.Update(3, utils.CreateTestChannel(suite.collectionID, 3, 1, "test-insert-channel3"))
	suite.distMgr.SegmentDistManager.Update(3, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 2, 3, 1, "test-insert-channel3"))

	suite.Eventually(func() bool {
		replica0 := suite.meta.ReplicaManager.Get(10000)
		replica1 := suite.meta.ReplicaManager.Get(10001)
		return suite.Contains(replica0.GetNodes(), int64(3)) && suite.NotContains(replica1.GetNodes(), int64(3)) && suite.Len(replica1.GetNodes(), 1)
	}, 6*time.Second, 2*time.Second)

	suite.distMgr.ChannelDistManager.Update(3)
	suite.distMgr.SegmentDistManager.Update(3)

	suite.Eventually(func() bool {
		replica0 := suite.meta.ReplicaManager.Get(10000)
		replica1 := suite.meta.ReplicaManager.Get(10001)
		return suite.NotContains(replica0.GetNodes(), int64(3)) && suite.Contains(replica1.GetNodes(), int64(3)) && suite.Len(replica1.GetNodes(), 2)
	}, 6*time.Second, 2*time.Second)
}

func (suite *ReplicaObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
}

func TestReplicaObserver(t *testing.T) {
	suite.Run(t, new(ReplicaObserverSuite))
}
