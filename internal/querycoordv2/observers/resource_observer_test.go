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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdKV "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ResourceObserverSuite struct {
	suite.Suite

	kv kv.MetaKv
	// dependency
	store    *mocks.QueryCoordCatalog
	meta     *meta.Meta
	observer *ResourceObserver
	nodeMgr  *session.NodeManager

	collectionID int64
	partitionID  int64
}

func (suite *ResourceObserverSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.CheckResourceGroupInterval.Key, "3")
}

func (suite *ResourceObserverSuite) SetupTest() {
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
	suite.kv = etcdKV.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	// meta
	suite.store = mocks.NewQueryCoordCatalog(suite.T())
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, suite.store, suite.nodeMgr)

	suite.observer = NewResourceObserver(suite.meta)
	suite.observer.Start()

	suite.store.EXPECT().SaveResourceGroup(mock.Anything).Return(nil)
	suite.store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	for i := 0; i < 10; i++ {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.meta.ResourceManager.HandleNodeUp(int64(i))
	}
}

func (suite *ResourceObserverSuite) TearDownTest() {
	suite.observer.Stop()
	suite.store.ExpectedCalls = nil
}

func (suite *ResourceObserverSuite) TestCheckNodesInReplica() {
	suite.store.EXPECT().SaveCollection(mock.Anything).Return(nil)
	suite.store.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 2))
	suite.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{100, 101},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(100, 101),
	))

	// hack all node down from replica
	suite.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            2,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))
	suite.meta.ResourceManager.AddResourceGroup("rg", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 4},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 4},
	})
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(100),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(101),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(102),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(103),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(100)
	suite.meta.ResourceManager.HandleNodeUp(101)
	suite.meta.ResourceManager.HandleNodeUp(102)
	suite.meta.ResourceManager.HandleNodeUp(103)
	suite.meta.ResourceManager.HandleNodeDown(100)
	suite.meta.ResourceManager.HandleNodeDown(101)

	rg := suite.meta.ResourceManager.GetResourceGroup("rg")
	suite.NotNil(rg)

	suite.False(rg.ContainNode(100))
	suite.False(rg.ContainNode(101))
	suite.True(rg.ContainNode(102))
	suite.True(rg.ContainNode(103))
}

func (suite *ResourceObserverSuite) TestRecoverResourceGroupFailed() {
	suite.meta.ResourceManager.AddResourceGroup("rg", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 50},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 50},
	})
	for i := 100; i < 200; i++ {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.meta.ResourceManager.HandleNodeUp(int64(i))
		suite.meta.ResourceManager.HandleNodeDown(int64(i))
	}

	suite.Error(suite.meta.ResourceManager.MeetRequirement("rg"))
}

func (suite *ResourceObserverSuite) TestRecoverReplicaFailed() {
	suite.store.EXPECT().SaveCollection(mock.Anything).Return(nil)
	suite.store.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 2))
	suite.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{100, 101},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(100, 101),
	))

	// hack all node down from replica
	suite.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            2,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	suite.store.EXPECT().SaveReplica(mock.Anything).Return(errors.New("store error"))
	suite.meta.ResourceManager.AddResourceGroup("rg", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 4},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 4},
	})
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(100),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(101),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(102),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(103),
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(100)
	suite.meta.ResourceManager.HandleNodeUp(101)
	suite.meta.ResourceManager.HandleNodeUp(102)
	suite.meta.ResourceManager.HandleNodeUp(103)
	suite.meta.ResourceManager.HandleNodeDown(100)
	suite.meta.ResourceManager.HandleNodeDown(101)

	suite.Error(suite.meta.ResourceManager.MeetRequirement("rg"))
}

func (suite *ResourceObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
}

func TestResourceObserver(t *testing.T) {
	suite.Run(t, new(ResourceObserverSuite))
}
