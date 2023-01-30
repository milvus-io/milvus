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
package observers

import (
	"context"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/suite"
)

type ReplicaObserverSuite struct {
	suite.Suite

	kv *etcdkv.EtcdKV
	//dependency
	meta    *meta.Meta
	distMgr *meta.DistributionManager

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
	suite.meta = meta.NewMeta(idAllocator, store, session.NewNodeManager())

	suite.distMgr = meta.NewDistributionManager()
	suite.observer = NewReplicaObserver(suite.meta, suite.distMgr)
	suite.observer.Start(context.TODO())
	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)

	suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, 1)
	err = suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(suite.collectionID, 1))
	suite.NoError(err)
	replicas, err := suite.meta.ReplicaManager.Spawn(suite.collectionID, 1, meta.DefaultResourceGroupName)
	suite.NoError(err)
	err = suite.meta.ReplicaManager.Put(replicas...)
	suite.NoError(err)
}

func (suite *ReplicaObserverSuite) TestCheckNodesInReplica() {
	replicas := suite.meta.ReplicaManager.GetByCollection(suite.collectionID)

	suite.distMgr.ChannelDistManager.Update(1, utils.CreateTestChannel(suite.collectionID, 2, 1, "test-insert-channel1"))
	suite.distMgr.SegmentDistManager.Update(1, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 1, 100, 1, "test-insert-channel1"))
	replicas[0].AddNode(1)
	suite.distMgr.ChannelDistManager.Update(100, utils.CreateTestChannel(suite.collectionID, 100, 1, "test-insert-channel2"))
	suite.distMgr.SegmentDistManager.Update(100, utils.CreateTestSegment(suite.collectionID, suite.partitionID, 2, 100, 1, "test-insert-channel2"))
	replicas[0].AddNode(100)

	suite.Eventually(func() bool {
		// node 100 should be kept
		replicas := suite.meta.ReplicaManager.GetByCollection(suite.collectionID)

		for _, node := range replicas[0].GetNodes() {
			if node == 100 {
				return true
			}
		}
		return false
	}, 6*time.Second, 2*time.Second)
	suite.Len(replicas[0].GetNodes(), 2)

	suite.distMgr.ChannelDistManager.Update(100)
	suite.distMgr.SegmentDistManager.Update(100)

	suite.Eventually(func() bool {
		// node 100 should be removed
		replicas := suite.meta.ReplicaManager.GetByCollection(suite.collectionID)

		for _, node := range replicas[0].GetNodes() {
			if node == 100 {
				return false
			}
		}
		return true
	}, 5*time.Second, 1*time.Second)
	suite.Len(replicas[0].GetNodes(), 1)
	suite.Equal([]int64{1}, replicas[0].GetNodes())
}

func (suite *ReplicaObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
}

func TestReplicaObserver(t *testing.T) {
	suite.Run(t, new(ReplicaObserverSuite))
}
