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
package meta

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ResourceManagerSuite struct {
	suite.Suite

	kv      kv.MetaKv
	manager *ResourceManager
}

func (suite *ResourceManagerSuite) SetupSuite() {
	Params.Init()
}

func (suite *ResourceManagerSuite) SetupTest() {
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

	store := NewMetaStore(suite.kv)
	suite.manager = NewResourceManager(store, session.NewNodeManager())
}

func (suite *ResourceManagerSuite) TestManipulateResourceGroup() {
	// test add rg
	err := suite.manager.AddResourceGroup("rg1")
	suite.NoError(err)
	suite.True(suite.manager.ContainResourceGroup("rg1"))
	suite.Len(suite.manager.ListResourceGroups(), 2)

	// test add duplicate rg
	err = suite.manager.AddResourceGroup("rg1")
	suite.Error(err)
	// test delete rg
	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)

	// test delete rg which doesn't exist
	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)
	// test delete default rg
	err = suite.manager.RemoveResourceGroup(DefaultResourceGroupName)
	suite.ErrorIs(ErrDeleteDefaultRG, err)
}

func (suite *ResourceManagerSuite) TestManipulateNode() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	err := suite.manager.AddResourceGroup("rg1")
	suite.NoError(err)
	// test add node to rg
	err = suite.manager.AssignNode("rg1", 1)
	suite.NoError(err)

	// test add non-exist node to rg
	err = suite.manager.AssignNode("rg1", 2)
	suite.ErrorIs(err, ErrNodeNotExist)

	// test add node to non-exist rg
	err = suite.manager.AssignNode("rg2", 1)
	suite.ErrorIs(err, ErrRGNotExist)

	// test remove node from rg
	err = suite.manager.UnassignNode("rg1", 1)
	suite.NoError(err)

	// test remove non-exist node from rg
	err = suite.manager.UnassignNode("rg1", 2)
	suite.NoError(err)

	// test remove node from non-exist rg
	err = suite.manager.UnassignNode("rg2", 1)
	suite.ErrorIs(err, ErrRGNotExist)

	// add node which already assign to rg  to another rg
	err = suite.manager.AddResourceGroup("rg2")
	suite.NoError(err)
	err = suite.manager.AssignNode("rg1", 1)
	suite.NoError(err)
	err = suite.manager.AssignNode("rg2", 1)
	suite.ErrorIs(err, ErrNodeAlreadyAssign)

	// transfer node between rgs
	_, err = suite.manager.TransferNode("rg1", "rg2", 1)
	suite.NoError(err)

	// transfer meet non exist rg
	_, err = suite.manager.TransferNode("rgggg", "rg2", 1)
	suite.ErrorIs(err, ErrRGNotExist)

	_, err = suite.manager.TransferNode("rg1", "rg2", 5)
	suite.ErrorIs(err, ErrNodeNotEnough)

	suite.manager.nodeMgr.Add(session.NewNodeInfo(11, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(12, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(13, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(14, "localhost"))
	suite.manager.AssignNode("rg1", 11)
	suite.manager.AssignNode("rg1", 12)
	suite.manager.AssignNode("rg1", 13)
	suite.manager.AssignNode("rg1", 14)

	rg1, err := suite.manager.GetResourceGroup("rg1")
	suite.NoError(err)
	rg2, err := suite.manager.GetResourceGroup("rg2")
	suite.NoError(err)
	suite.Equal(rg1.GetCapacity(), 4)
	suite.Equal(rg2.GetCapacity(), 1)
	suite.manager.TransferNode("rg1", "rg2", 3)
	suite.Equal(rg1.GetCapacity(), 1)
	suite.Equal(rg2.GetCapacity(), 4)
}

func (suite *ResourceManagerSuite) TestHandleNodeUp() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(100, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(101, "localhost"))
	err := suite.manager.AddResourceGroup("rg1")
	suite.NoError(err)

	suite.manager.AssignNode("rg1", 1)
	suite.manager.AssignNode("rg1", 2)
	suite.manager.AssignNode("rg1", 3)

	// test query node id not change, expect assign back to origin rg
	rg, err := suite.manager.GetResourceGroup("rg1")
	suite.NoError(err)
	suite.Equal(rg.GetCapacity(), 3)
	suite.Equal(len(rg.GetNodes()), 3)
	suite.manager.HandleNodeUp(1)
	suite.Equal(rg.GetCapacity(), 3)
	suite.Equal(len(rg.GetNodes()), 3)

	suite.manager.HandleNodeDown(2)
	rg, err = suite.manager.GetResourceGroup("rg1")
	suite.NoError(err)
	suite.Equal(rg.GetCapacity(), 3)
	suite.Equal(len(rg.GetNodes()), 2)
	suite.NoError(err)
	defaultRG, err := suite.manager.GetResourceGroup(DefaultResourceGroupName)
	suite.NoError(err)
	suite.Equal(DefaultResourceGroupCapacity, defaultRG.GetCapacity())
	suite.manager.HandleNodeUp(101)
	rg, err = suite.manager.GetResourceGroup("rg1")
	suite.NoError(err)
	suite.Equal(rg.GetCapacity(), 3)
	suite.Equal(len(rg.GetNodes()), 2)
	suite.False(suite.manager.ContainsNode("rg1", 101))
	suite.Equal(DefaultResourceGroupCapacity, defaultRG.GetCapacity())
}

func (suite *ResourceManagerSuite) TestRecover() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(4, "localhost"))
	err := suite.manager.AddResourceGroup("rg1")
	suite.NoError(err)
	err = suite.manager.AddResourceGroup("rg2")
	suite.NoError(err)

	suite.manager.AssignNode(DefaultResourceGroupName, 1)
	suite.manager.TransferNode(DefaultResourceGroupName, "rg1", 1)
	suite.manager.AssignNode(DefaultResourceGroupName, 2)
	suite.manager.TransferNode(DefaultResourceGroupName, "rg2", 1)
	suite.manager.AssignNode(DefaultResourceGroupName, 3)
	suite.manager.AssignNode(DefaultResourceGroupName, 4)

	suite.manager.HandleNodeDown(2)
	suite.manager.HandleNodeDown(3)

	// clear resource manager in hack way
	delete(suite.manager.groups, "rg1")
	delete(suite.manager.groups, "rg2")
	delete(suite.manager.groups, DefaultResourceGroupName)
	suite.manager.Recover()

	rg, err := suite.manager.GetResourceGroup("rg1")
	suite.NoError(err)
	suite.Equal(1, rg.GetCapacity())
	suite.True(suite.manager.ContainsNode("rg1", 1))

	rg, err = suite.manager.GetResourceGroup("rg2")
	suite.NoError(err)
	suite.Equal(1, rg.GetCapacity())
	suite.False(suite.manager.ContainsNode("rg2", 2))

	rg, err = suite.manager.GetResourceGroup(DefaultResourceGroupName)
	suite.NoError(err)
	suite.Equal(DefaultResourceGroupCapacity, rg.GetCapacity())
	suite.False(suite.manager.ContainsNode(DefaultResourceGroupName, 3))
	suite.True(suite.manager.ContainsNode(DefaultResourceGroupName, 4))
}

func (suite *ResourceManagerSuite) TestCheckOutboundNodes() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	err := suite.manager.AddResourceGroup("rg")
	suite.NoError(err)
	suite.manager.AssignNode("rg", 1)
	suite.manager.AssignNode("rg", 2)
	suite.manager.AssignNode("rg", 3)

	replica := NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{1, 2, 3, 4},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(1, 2, 3, 4),
	)

	outboundNodes := suite.manager.CheckOutboundNodes(replica)
	suite.Len(outboundNodes, 1)
	suite.True(outboundNodes.Contain(4))
}

func (suite *ResourceManagerSuite) TestCheckResourceGroup() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	err := suite.manager.AddResourceGroup("rg")
	suite.NoError(err)
	suite.manager.AssignNode("rg", 1)
	suite.manager.AssignNode("rg", 2)
	suite.manager.AssignNode("rg", 3)

	suite.manager.HandleNodeDown(1)
	lackNodes := suite.manager.CheckLackOfNode("rg")
	suite.Equal(lackNodes, 1)

	suite.manager.nodeMgr.Remove(2)
	suite.manager.checkRGNodeStatus("rg")
	lackNodes = suite.manager.CheckLackOfNode("rg")
	suite.Equal(lackNodes, 2)

	rg, err := suite.manager.FindResourceGroupByNode(3)
	suite.NoError(err)
	suite.Equal(rg, "rg")
}

func (suite *ResourceManagerSuite) TestGetOutboundNode() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	suite.manager.AddResourceGroup("rg")
	suite.manager.AddResourceGroup("rg1")
	suite.manager.AssignNode("rg", 1)
	suite.manager.AssignNode("rg", 2)
	suite.manager.AssignNode("rg1", 3)

	replica := NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  100,
			ResourceGroup: "rg",
			Nodes:         []int64{1, 2, 3},
		},
		typeutil.NewUniqueSet(1, 2, 3),
	)

	outgoingNodes := suite.manager.GetOutgoingNodeNumByReplica(replica)
	suite.NotNil(outgoingNodes)
	suite.Len(outgoingNodes, 1)
	suite.NotNil(outgoingNodes["rg1"])
	suite.Equal(outgoingNodes["rg1"], int32(1))
}

func (suite *ResourceManagerSuite) TestAutoRecover() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	err := suite.manager.AddResourceGroup("rg")
	suite.NoError(err)
	suite.manager.AssignNode(DefaultResourceGroupName, 1)
	suite.manager.AssignNode(DefaultResourceGroupName, 2)
	suite.manager.AssignNode("rg", 3)

	suite.manager.HandleNodeDown(3)
	lackNodes := suite.manager.CheckLackOfNode("rg")
	suite.Equal(lackNodes, 1)
	suite.manager.AutoRecoverResourceGroup("rg")
	lackNodes = suite.manager.CheckLackOfNode("rg")
	suite.Equal(lackNodes, 0)

	// test auto recover behavior when all node down
	suite.manager.nodeMgr.Remove(1)
	suite.manager.nodeMgr.Remove(2)
	suite.manager.AutoRecoverResourceGroup("rg")
	nodes, _ := suite.manager.GetNodes("rg")
	suite.Len(nodes, 0)
	nodes, _ = suite.manager.GetNodes(DefaultResourceGroupName)
	suite.Len(nodes, 0)

	suite.manager.nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	suite.manager.HandleNodeUp(1)
	suite.manager.AutoRecoverResourceGroup("rg")
	nodes, _ = suite.manager.GetNodes("rg")
	suite.Len(nodes, 1)
	nodes, _ = suite.manager.GetNodes(DefaultResourceGroupName)
	suite.Len(nodes, 0)
}

func (suite *ResourceManagerSuite) TestDefaultResourceGroup() {
	for i := 0; i < 10; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(int64(i), "localhost"))
	}
	defaultRG, err := suite.manager.GetResourceGroup(DefaultResourceGroupName)
	suite.NoError(err)
	suite.Equal(defaultRG.GetCapacity(), DefaultResourceGroupCapacity)
	suite.Len(defaultRG.GetNodes(), 0)

	suite.manager.HandleNodeUp(1)
	suite.manager.HandleNodeUp(2)
	suite.manager.HandleNodeUp(3)
	suite.Equal(defaultRG.GetCapacity(), DefaultResourceGroupCapacity)
	suite.Len(defaultRG.GetNodes(), 3)

	// shutdown node 1 and 2
	suite.manager.nodeMgr.Remove(1)
	suite.manager.nodeMgr.Remove(2)

	defaultRG, err = suite.manager.GetResourceGroup(DefaultResourceGroupName)
	suite.NoError(err)
	suite.Equal(defaultRG.GetCapacity(), DefaultResourceGroupCapacity)
	suite.Len(defaultRG.GetNodes(), 1)

	suite.manager.HandleNodeUp(4)
	suite.manager.HandleNodeUp(5)
	suite.Equal(defaultRG.GetCapacity(), DefaultResourceGroupCapacity)
	suite.Len(defaultRG.GetNodes(), 3)

	suite.manager.HandleNodeUp(7)
	suite.manager.HandleNodeUp(8)
	suite.manager.HandleNodeUp(9)
	suite.Equal(defaultRG.GetCapacity(), DefaultResourceGroupCapacity)
	suite.Len(defaultRG.GetNodes(), 6)
}

func (suite *ResourceManagerSuite) TestStoreFailed() {
	store := NewMockStore(suite.T())
	nodeMgr := session.NewNodeManager()
	manager := NewResourceManager(store, nodeMgr)

	nodeMgr.Add(session.NewNodeInfo(1, "localhost"))
	nodeMgr.Add(session.NewNodeInfo(2, "localhost"))
	nodeMgr.Add(session.NewNodeInfo(3, "localhost"))
	storeErr := errors.New("store error")
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(storeErr)
	store.EXPECT().RemoveResourceGroup(mock.Anything).Return(storeErr)

	err := manager.AddResourceGroup("rg")
	suite.ErrorIs(err, storeErr)

	manager.groups["rg"] = &ResourceGroup{
		nodes:    typeutil.NewUniqueSet(),
		capacity: 0,
	}

	err = manager.RemoveResourceGroup("rg")
	suite.ErrorIs(err, storeErr)

	err = manager.AssignNode("rg", 1)
	suite.ErrorIs(err, storeErr)

	manager.groups["rg"].assignNode(1, 1)
	err = manager.UnassignNode("rg", 1)
	suite.ErrorIs(err, storeErr)

	_, err = manager.TransferNode("rg", DefaultResourceGroupName, 1)
	suite.ErrorIs(err, storeErr)

	_, err = manager.HandleNodeUp(2)
	suite.ErrorIs(err, storeErr)

	_, err = manager.HandleNodeDown(1)
	suite.ErrorIs(err, storeErr)
}

func (suite *ResourceManagerSuite) TearDownSuite() {
	suite.kv.Close()
}

func TestResourceManager(t *testing.T) {
	suite.Run(t, new(ResourceManagerSuite))
}
