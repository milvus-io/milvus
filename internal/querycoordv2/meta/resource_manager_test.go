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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ResourceManagerSuite struct {
	suite.Suite

	kv      kv.MetaKv
	manager *ResourceManager
}

func (suite *ResourceManagerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ResourceManagerSuite) SetupTest() {
	config := params.GenerateEtcdConfig()
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

	store := querycoord.NewCatalog(suite.kv)
	suite.manager = NewResourceManager(store, session.NewNodeManager())
}

func (suite *ResourceManagerSuite) TearDownSuite() {
	suite.kv.Close()
}

func TestResourceManager(t *testing.T) {
	suite.Run(t, new(ResourceManagerSuite))
}

func (suite *ResourceManagerSuite) TestValidateConfiguration() {
	err := suite.manager.validateResourceGroupConfig("rg1", createResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.validateResourceGroupConfig("rg1", &rgpb.ResourceGroupConfig{})
	suite.ErrorIs(err, ErrIllegalRGConfig)

	err = suite.manager.validateResourceGroupConfig("rg1", createResourceGroupConfig(-1, 2))
	suite.ErrorIs(err, ErrIllegalRGConfig)

	err = suite.manager.validateResourceGroupConfig("rg1", createResourceGroupConfig(2, -1))
	suite.ErrorIs(err, ErrIllegalRGConfig)

	err = suite.manager.validateResourceGroupConfig("rg1", createResourceGroupConfig(3, 2))
	suite.ErrorIs(err, ErrIllegalRGConfig)

	cfg := createResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, ErrIllegalRGConfig)

	cfg = createResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg2"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, ErrIllegalRGConfig)

	cfg = createResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, ErrIllegalRGConfig)

	cfg = createResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg2"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, ErrIllegalRGConfig)

	err = suite.manager.AddResourceGroup("rg2", createResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.RemoveResourceGroup("rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestValidateDelete() {
	// Non empty resource group can not be removed.
	err := suite.manager.AddResourceGroup("rg1", createResourceGroupConfig(1, 1))
	suite.NoError(err)

	err = suite.manager.validateResourceGroupIsDeletable(DefaultResourceGroupName)
	suite.ErrorIs(err, ErrDeleteDefaultRG)

	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, ErrDeleteNonEmptyRG)

	cfg := createResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	suite.manager.AddResourceGroup("rg2", cfg)
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": createResourceGroupConfig(0, 0),
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, ErrDeleteInUsedRG)

	cfg = createResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": cfg,
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, ErrDeleteInUsedRG)

	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": createResourceGroupConfig(0, 0),
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.NoError(err)

	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestManipulateResourceGroup() {
	// test add rg
	err := suite.manager.AddResourceGroup("rg1", createResourceGroupConfig(0, 0))
	suite.NoError(err)
	suite.True(suite.manager.ContainResourceGroup("rg1"))
	suite.Len(suite.manager.ListResourceGroups(), 2)

	// test add duplicate rg but same configuration is ok
	err = suite.manager.AddResourceGroup("rg1", createResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.AddResourceGroup("rg1", createResourceGroupConfig(1, 1))
	suite.Error(err)

	// test delete rg
	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)

	// test delete rg which doesn't exist
	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)
	// test delete default rg
	err = suite.manager.RemoveResourceGroup(DefaultResourceGroupName)
	suite.ErrorIs(err, ErrDeleteDefaultRG)

	// test delete a rg not empty.
	err = suite.manager.AddResourceGroup("rg2", createResourceGroupConfig(1, 1))
	suite.NoError(err)
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.ErrorIs(err, ErrDeleteNonEmptyRG)

	// test delete a rg after update
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": createResourceGroupConfig(0, 0),
	})
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.NoError(err)

	// assign a node to rg.
	err = suite.manager.AddResourceGroup("rg2", createResourceGroupConfig(1, 1))
	suite.NoError(err)
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	defer suite.manager.nodeMgr.Remove(1)
	suite.manager.HandleNodeUp(1)
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.ErrorIs(err, ErrDeleteNonEmptyRG)
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": createResourceGroupConfig(0, 0),
	})
	// RemoveResourceGroup will remove all nodes from the resource group.
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestNodeUpAndDown() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	err := suite.manager.AddResourceGroup("rg1", createResourceGroupConfig(1, 1))
	suite.NoError(err)
	// test add node to rg
	suite.manager.HandleNodeUp(1)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())

	// test add non-exist node to rg
	err = suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": createResourceGroupConfig(2, 3),
	})
	suite.NoError(err)
	suite.manager.HandleNodeUp(2)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// teardown a non-exist node from rg.
	suite.manager.HandleNodeDown(2)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// test add exist node to rg
	suite.manager.HandleNodeUp(1)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// teardown a exist node from rg.
	suite.manager.HandleNodeDown(1)
	suite.Zero(suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// teardown a exist node from rg.
	suite.manager.HandleNodeDown(1)
	suite.Zero(suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	suite.manager.HandleNodeUp(1)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	err = suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": createResourceGroupConfig(4, 4),
	})
	suite.NoError(err)
	suite.manager.AddResourceGroup("rg2", createResourceGroupConfig(1, 1))
	suite.NoError(err)

	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   11,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   12,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   13,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   14,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.HandleNodeUp(11)
	suite.manager.HandleNodeUp(12)
	suite.manager.HandleNodeUp(13)
	suite.manager.HandleNodeUp(14)

	suite.Equal(4, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(1, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	suite.manager.HandleNodeDown(11)
	suite.manager.HandleNodeDown(12)
	suite.manager.HandleNodeDown(13)
	suite.manager.HandleNodeDown(14)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	suite.manager.HandleNodeDown(1)
	suite.Zero(suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": createResourceGroupConfig(20, 30),
		"rg2": createResourceGroupConfig(30, 40),
	})
	for i := 1; i <= 100; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.manager.HandleNodeUp(int64(i))
	}

	suite.Equal(20, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(30, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(50, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// down all nodes
	for i := 1; i <= 100; i++ {
		suite.manager.HandleNodeDown(int64(i))
		suite.Equal(100-i, suite.manager.GetResourceGroup("rg1").NodeNum()+
			suite.manager.GetResourceGroup("rg2").NodeNum()+
			suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
	}

	// if there are all rgs reach limit, should be fall back to default rg.
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1":                    createResourceGroupConfig(0, 0),
		"rg2":                    createResourceGroupConfig(0, 0),
		DefaultResourceGroupName: createResourceGroupConfig(0, 0),
	})

	for i := 1; i <= 100; i++ {
		suite.manager.HandleNodeUp(int64(i))
		suite.Equal(i, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
		suite.Equal(0, suite.manager.GetResourceGroup("rg1").NodeNum())
		suite.Equal(0, suite.manager.GetResourceGroup("rg2").NodeNum())
	}
}

func (suite *ResourceManagerSuite) TestAutoRecover() {
	for i := 1; i <= 100; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.manager.HandleNodeUp(int64(i))
	}
	suite.Equal(100, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Recover 10 nodes from default resource group
	suite.manager.AddResourceGroup("rg1", createResourceGroupConfig(10, 30))
	suite.Zero(suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").MissingNumOfNodes())
	suite.Equal(100, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup("rg1").MissingNumOfNodes())
	suite.Equal(90, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Recover 20 nodes from default resource group
	suite.manager.AddResourceGroup("rg2", createResourceGroupConfig(20, 30))
	suite.Zero(suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(20, suite.manager.GetResourceGroup("rg2").MissingNumOfNodes())
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(90, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.Equal(20, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(70, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Recover 5 redundant nodes from resource group
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": createResourceGroupConfig(5, 5),
	})
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.Equal(20, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(75, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Recover 10 redundant nodes from resource group 2 to resource group 1 and default resource group.
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": createResourceGroupConfig(10, 20),
		"rg2": createResourceGroupConfig(5, 10),
	})

	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// recover redundant nodes from default resource group
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1":                    createResourceGroupConfig(10, 20),
		"rg2":                    createResourceGroupConfig(20, 30),
		DefaultResourceGroupName: createResourceGroupConfig(10, 20),
	})
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)

	// Even though the default resource group has 20 nodes limits,
	// all redundant nodes will be assign to default resource group.
	suite.Equal(20, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(30, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(50, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Test recover missing from high priority resource group by set `from`.
	suite.manager.AddResourceGroup("rg3", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 15,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 15,
		},
		TransferFrom: []*rgpb.ResourceGroupTransfer{{
			ResourceGroup: "rg1",
		}},
	})
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: createResourceGroupConfig(30, 40),
	})

	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup("rg3")

	// Get 10 from default group for redundant nodes, get 5 from rg1 for rg3 at high priority.
	suite.Equal(15, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(30, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(15, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Test recover redundant to high priority resource group by set `to`.
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg3": {
			Requests: &rgpb.ResourceGroupLimit{
				NodeNum: 0,
			},
			Limits: &rgpb.ResourceGroupLimit{
				NodeNum: 0,
			},
			TransferTo: []*rgpb.ResourceGroupTransfer{{
				ResourceGroup: "rg2",
			}},
		},
		"rg1": createResourceGroupConfig(15, 100),
		"rg2": createResourceGroupConfig(15, 40),
	})

	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup("rg3")

	// Recover rg3 by transfer 10 nodes to rg2 with high priority, 5 to rg1.
	suite.Equal(20, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Test down all nodes.
	for i := 1; i <= 100; i++ {
		suite.manager.nodeMgr.Remove(int64(i))
	}
	suite.manager.RemoveAllDownNode()
	suite.Zero(suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
}

func createResourceGroupConfig(requestNode int, limitNode int) *rgpb.ResourceGroupConfig {
	return &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: int32(requestNode),
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: int32(limitNode),
		},
	}
}
