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
package meta

import (
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
)

type ResourceManagerSuite struct {
	suite.Suite

	kv      *etcdkv.EtcdKV
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

	// test add duplicate rg
	err = suite.manager.AddResourceGroup("rg1")
	suite.ErrorIs(err, ErrRGAlreadyExist)
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
	println(err.Error())
	suite.ErrorIs(err, ErrNodeAlreadyAssign)

	// trasfer node between rgs
	err = suite.manager.TransferNode("rg1", "rg2", 1)
	suite.NoError(err)

	// transfer non-exist node between rgs
	err = suite.manager.TransferNode("rg1", "rg2", 2)
	suite.ErrorIs(err, ErrNodeNotExist)

	// transfer meet non exist rg
	err = suite.manager.TransferNode("rggg", "rg2", 1)
	suite.ErrorIs(err, ErrRGNotExist)
	err = suite.manager.TransferNode("rg1", "rgggg", 1)
	suite.ErrorIs(err, ErrRGNotExist)
}

func (suite *ResourceManagerSuite) TearDownSuite() {
	suite.kv.Close()
}

func TestResourceManager(t *testing.T) {
	suite.Run(t, new(ResourceManagerSuite))
}
