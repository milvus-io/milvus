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

	etcdKV "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/suite"
)

type ResourceObserverSuite struct {
	suite.Suite

	kv *etcdKV.EtcdKV
	//dependency
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
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)

	suite.observer = NewResourceObserver(suite.meta)
	suite.observer.Start(context.TODO())

	for i := 1; i < 10; i++ {
		suite.nodeMgr.Add(session.NewNodeInfo(int64(i), "localhost"))
		suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, int64(i))
	}
}

func (suite *ResourceObserverSuite) TestCheckNodesInReplica() {
	suite.meta.ResourceManager.AddResourceGroup("rg")
	suite.nodeMgr.Add(session.NewNodeInfo(int64(100), "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(int64(101), "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(int64(102), "localhost"))
	suite.meta.ResourceManager.AssignNode("rg", 100)
	suite.meta.ResourceManager.AssignNode("rg", 101)
	suite.meta.ResourceManager.AssignNode("rg", 102)
	suite.meta.ResourceManager.HandleNodeDown(100)
	suite.meta.ResourceManager.HandleNodeDown(101)

	//before auto recover rg
	suite.Eventually(func() bool {
		lackNodesNum := suite.meta.ResourceManager.CheckLackOfNode("rg")
		return lackNodesNum == 2
	}, 5*time.Second, 1*time.Second)

	// after auto recover rg
	suite.Eventually(func() bool {
		lackNodesNum := suite.meta.ResourceManager.CheckLackOfNode("rg")
		return lackNodesNum == 0
	}, 5*time.Second, 1*time.Second)

}

func (suite *ResourceObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
}

func TestResourceObserver(t *testing.T) {
	suite.Run(t, new(ResourceObserverSuite))
}
