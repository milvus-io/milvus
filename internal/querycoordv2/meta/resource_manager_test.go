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
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	err := suite.manager.validateResourceGroupConfig("rg1", newResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.validateResourceGroupConfig("rg1", &rgpb.ResourceGroupConfig{})
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	err = suite.manager.validateResourceGroupConfig("rg1", newResourceGroupConfig(-1, 2))
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	err = suite.manager.validateResourceGroupConfig("rg1", newResourceGroupConfig(2, -1))
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	err = suite.manager.validateResourceGroupConfig("rg1", newResourceGroupConfig(3, 2))
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	cfg := newResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	cfg = newResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg2"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	cfg = newResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	cfg = newResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg2"}}
	err = suite.manager.validateResourceGroupConfig("rg1", cfg)
	suite.ErrorIs(err, merr.ErrResourceGroupIllegalConfig)

	err = suite.manager.AddResourceGroup("rg2", newResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.RemoveResourceGroup("rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestValidateDelete() {
	// Non empty resource group can not be removed.
	err := suite.manager.AddResourceGroup("rg1", newResourceGroupConfig(1, 1))
	suite.NoError(err)

	err = suite.manager.validateResourceGroupIsDeletable(DefaultResourceGroupName)
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	cfg := newResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	suite.manager.AddResourceGroup("rg2", cfg)
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(0, 0),
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	cfg = newResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": cfg,
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": newResourceGroupConfig(0, 0),
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
	err := suite.manager.AddResourceGroup("rg1", newResourceGroupConfig(0, 0))
	suite.NoError(err)
	suite.True(suite.manager.ContainResourceGroup("rg1"))
	suite.Len(suite.manager.ListResourceGroups(), 2)

	// test add duplicate rg but same configuration is ok
	err = suite.manager.AddResourceGroup("rg1", newResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.AddResourceGroup("rg1", newResourceGroupConfig(1, 1))
	suite.Error(err)

	// test delete rg
	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)

	// test delete rg which doesn't exist
	err = suite.manager.RemoveResourceGroup("rg1")
	suite.NoError(err)
	// test delete default rg
	err = suite.manager.RemoveResourceGroup(DefaultResourceGroupName)
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	// test delete a rg not empty.
	err = suite.manager.AddResourceGroup("rg2", newResourceGroupConfig(1, 1))
	suite.NoError(err)
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	// test delete a rg after update
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": newResourceGroupConfig(0, 0),
	})
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.NoError(err)

	// assign a node to rg.
	err = suite.manager.AddResourceGroup("rg2", newResourceGroupConfig(1, 1))
	suite.NoError(err)
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	defer suite.manager.nodeMgr.Remove(1)
	suite.manager.HandleNodeUp(1)
	err = suite.manager.RemoveResourceGroup("rg2")
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg2": newResourceGroupConfig(0, 0),
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
	err := suite.manager.AddResourceGroup("rg1", newResourceGroupConfig(1, 1))
	suite.NoError(err)
	// test add node to rg
	suite.manager.HandleNodeUp(1)
	suite.Equal(1, suite.manager.GetResourceGroup("rg1").NodeNum())

	// test add non-exist node to rg
	err = suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(2, 3),
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
		"rg1": newResourceGroupConfig(4, 4),
	})
	suite.NoError(err)
	suite.manager.AddResourceGroup("rg2", newResourceGroupConfig(1, 1))
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
		"rg1": newResourceGroupConfig(20, 30),
		"rg2": newResourceGroupConfig(30, 40),
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
		"rg1":                    newResourceGroupConfig(0, 0),
		"rg2":                    newResourceGroupConfig(0, 0),
		DefaultResourceGroupName: newResourceGroupConfig(0, 0),
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
	suite.manager.AddResourceGroup("rg1", newResourceGroupConfig(10, 30))
	suite.Zero(suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").MissingNumOfNodes())
	suite.Equal(100, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup("rg1").MissingNumOfNodes())
	suite.Equal(90, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Recover 20 nodes from default resource group
	suite.manager.AddResourceGroup("rg2", newResourceGroupConfig(20, 30))
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
		"rg1": newResourceGroupConfig(5, 5),
	})
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.Equal(20, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(75, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Recover 10 redundant nodes from resource group 2 to resource group 1 and default resource group.
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(10, 20),
		"rg2": newResourceGroupConfig(5, 10),
	})

	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// recover redundant nodes from default resource group
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		"rg1":                    newResourceGroupConfig(10, 20),
		"rg2":                    newResourceGroupConfig(20, 30),
		DefaultResourceGroupName: newResourceGroupConfig(10, 20),
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
		DefaultResourceGroupName: newResourceGroupConfig(30, 40),
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
		"rg1": newResourceGroupConfig(15, 100),
		"rg2": newResourceGroupConfig(15, 40),
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

	suite.testTransferNode()

	// Test redundant nodes recover to default resource group.
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(1, 1),
		"rg3":                    newResourceGroupConfig(0, 0),
		"rg2":                    newResourceGroupConfig(0, 0),
		"rg1":                    newResourceGroupConfig(0, 0),
	})
	// Even default resource group has 1 node limit,
	// all redundant nodes will be assign to default resource group if there's no resource group can hold.
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup("rg3")
	suite.Equal(0, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(100, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Test redundant recover to missing nodes and missing nodes from redundant nodes.
	// Initialize
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(0, 0),
		"rg3":                    newResourceGroupConfig(10, 10),
		"rg2":                    newResourceGroupConfig(80, 80),
		"rg1":                    newResourceGroupConfig(10, 10),
	})
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup("rg3")
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(0, 5),
		"rg3":                    newResourceGroupConfig(5, 5),
		"rg2":                    newResourceGroupConfig(80, 80),
		"rg1":                    newResourceGroupConfig(20, 30),
	})
	suite.manager.AutoRecoverResourceGroup("rg3") // recover redundant to missing rg.
	suite.Equal(15, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
	suite.manager.updateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(5, 5),
		"rg3":                    newResourceGroupConfig(5, 10),
		"rg2":                    newResourceGroupConfig(80, 80),
		"rg1":                    newResourceGroupConfig(10, 10),
	})
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName) // recover missing from redundant rg.
	suite.Equal(10, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
}

func (suite *ResourceManagerSuite) testTransferNode() {
	// Test redundant nodes recover to default resource group.
	suite.manager.UpdateResourceGroups(map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(40, 40),
		"rg3":                    newResourceGroupConfig(0, 0),
		"rg2":                    newResourceGroupConfig(40, 40),
		"rg1":                    newResourceGroupConfig(20, 20),
	})
	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup("rg3")

	suite.Equal(20, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())

	// Test TransferNode.
	// param error.
	err := suite.manager.TransferNode("rg1", "rg1", 1)
	suite.Error(err)

	err = suite.manager.TransferNode("rg1", "rg2", 0)
	suite.Error(err)

	err = suite.manager.TransferNode("rg3", "rg2", 1)
	suite.Error(err)

	err = suite.manager.TransferNode("rg1", "rg10086", 1)
	suite.Error(err)

	err = suite.manager.TransferNode("rg10086", "rg2", 1)
	suite.Error(err)

	// success
	err = suite.manager.TransferNode("rg1", "rg3", 5)
	suite.NoError(err)

	suite.manager.AutoRecoverResourceGroup("rg1")
	suite.manager.AutoRecoverResourceGroup("rg2")
	suite.manager.AutoRecoverResourceGroup(DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup("rg3")

	suite.Equal(15, suite.manager.GetResourceGroup("rg1").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup("rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup("rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(DefaultResourceGroupName).NodeNum())
}

func (suite *ResourceManagerSuite) TestIncomingNode() {
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.incomingNode.Insert(1)

	suite.Equal(1, suite.manager.CheckIncomingNodeNum())
	suite.manager.AssignPendingIncomingNode()
	suite.Equal(0, suite.manager.CheckIncomingNodeNum())
	nodes, err := suite.manager.GetNodes(DefaultResourceGroupName)
	suite.NoError(err)
	suite.Len(nodes, 1)
}
