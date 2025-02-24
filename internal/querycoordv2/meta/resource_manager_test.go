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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ResourceManagerSuite struct {
	suite.Suite

	kv      kv.MetaKv
	manager *ResourceManager
	ctx     context.Context
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
	suite.ctx = context.Background()
}

func (suite *ResourceManagerSuite) TearDownSuite() {
	suite.kv.Close()
}

func TestResourceManager(t *testing.T) {
	suite.Run(t, new(ResourceManagerSuite))
}

func (suite *ResourceManagerSuite) TestValidateConfiguration() {
	ctx := suite.ctx
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

	err = suite.manager.AddResourceGroup(ctx, "rg2", newResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.RemoveResourceGroup(ctx, "rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestValidateDelete() {
	ctx := suite.ctx
	// Non empty resource group can not be removed.
	err := suite.manager.AddResourceGroup(ctx, "rg1", newResourceGroupConfig(1, 1))
	suite.NoError(err)

	err = suite.manager.validateResourceGroupIsDeletable(DefaultResourceGroupName)
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	cfg := newResourceGroupConfig(0, 0)
	cfg.TransferFrom = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	suite.manager.AddResourceGroup(ctx, "rg2", cfg)
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(0, 0),
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	cfg = newResourceGroupConfig(0, 0)
	cfg.TransferTo = []*rgpb.ResourceGroupTransfer{{ResourceGroup: "rg1"}}
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg2": cfg,
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg2": newResourceGroupConfig(0, 0),
	})
	err = suite.manager.validateResourceGroupIsDeletable("rg1")
	suite.NoError(err)

	err = suite.manager.RemoveResourceGroup(ctx, "rg1")
	suite.NoError(err)
	err = suite.manager.RemoveResourceGroup(ctx, "rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestManipulateResourceGroup() {
	ctx := suite.ctx
	// test add rg
	err := suite.manager.AddResourceGroup(ctx, "rg1", newResourceGroupConfig(0, 0))
	suite.NoError(err)
	suite.True(suite.manager.ContainResourceGroup(ctx, "rg1"))
	suite.Len(suite.manager.ListResourceGroups(ctx), 2)

	// test add duplicate rg but same configuration is ok
	err = suite.manager.AddResourceGroup(ctx, "rg1", newResourceGroupConfig(0, 0))
	suite.NoError(err)

	err = suite.manager.AddResourceGroup(ctx, "rg1", newResourceGroupConfig(1, 1))
	suite.Error(err)

	// test delete rg
	err = suite.manager.RemoveResourceGroup(ctx, "rg1")
	suite.NoError(err)

	// test delete rg which doesn't exist
	err = suite.manager.RemoveResourceGroup(ctx, "rg1")
	suite.NoError(err)
	// test delete default rg
	err = suite.manager.RemoveResourceGroup(ctx, DefaultResourceGroupName)
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	// test delete a rg not empty.
	err = suite.manager.AddResourceGroup(ctx, "rg2", newResourceGroupConfig(1, 1))
	suite.NoError(err)
	err = suite.manager.RemoveResourceGroup(ctx, "rg2")
	suite.ErrorIs(err, merr.ErrParameterInvalid)

	// test delete a rg after update
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg2": newResourceGroupConfig(0, 0),
	})
	err = suite.manager.RemoveResourceGroup(ctx, "rg2")
	suite.NoError(err)

	// assign a node to rg.
	err = suite.manager.AddResourceGroup(ctx, "rg2", newResourceGroupConfig(1, 1))
	suite.NoError(err)
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	defer suite.manager.nodeMgr.Remove(1)
	suite.manager.HandleNodeUp(ctx, 1)
	err = suite.manager.RemoveResourceGroup(ctx, "rg2")
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg2": newResourceGroupConfig(0, 0),
	})
	log.Info("xxxxx")
	// RemoveResourceGroup will remove all nodes from the resource group.
	err = suite.manager.RemoveResourceGroup(ctx, "rg2")
	suite.NoError(err)
}

func (suite *ResourceManagerSuite) TestNodeUpAndDown() {
	ctx := suite.ctx
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	err := suite.manager.AddResourceGroup(ctx, "rg1", newResourceGroupConfig(1, 1))
	suite.NoError(err)
	// test add node to rg
	suite.manager.HandleNodeUp(ctx, 1)
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())

	// test add non-exist node to rg
	err = suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(2, 3),
	})
	suite.NoError(err)
	suite.manager.HandleNodeUp(ctx, 2)
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// teardown a non-exist node from rg.
	suite.manager.HandleNodeDown(ctx, 2)
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// test add exist node to rg
	suite.manager.HandleNodeUp(ctx, 1)
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// teardown a exist node from rg.
	suite.manager.HandleNodeDown(ctx, 1)
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// teardown a exist node from rg.
	suite.manager.HandleNodeDown(ctx, 1)
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	suite.manager.HandleNodeUp(ctx, 1)
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	err = suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(4, 4),
	})
	suite.NoError(err)
	suite.manager.AddResourceGroup(ctx, "rg2", newResourceGroupConfig(1, 1))
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
	suite.manager.HandleNodeUp(ctx, 11)
	suite.manager.HandleNodeUp(ctx, 12)
	suite.manager.HandleNodeUp(ctx, 13)
	suite.manager.HandleNodeUp(ctx, 14)

	suite.Equal(4, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	suite.manager.HandleNodeDown(ctx, 11)
	suite.manager.HandleNodeDown(ctx, 12)
	suite.manager.HandleNodeDown(ctx, 13)
	suite.manager.HandleNodeDown(ctx, 14)
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	suite.manager.HandleNodeDown(ctx, 1)
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Zero(suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(20, 30),
		"rg2": newResourceGroupConfig(30, 40),
	})
	for i := 1; i <= 100; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}

	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(30, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(50, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// down all nodes
	for i := 1; i <= 100; i++ {
		suite.manager.HandleNodeDown(ctx, int64(i))
		suite.Equal(100-i, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum()+
			suite.manager.GetResourceGroup(ctx, "rg2").NodeNum()+
			suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	}

	// if there are all rgs reach limit, should be fall back to default rg.
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1":                    newResourceGroupConfig(0, 0),
		"rg2":                    newResourceGroupConfig(0, 0),
		DefaultResourceGroupName: newResourceGroupConfig(0, 0),
	})

	for i := 1; i <= 100; i++ {
		suite.manager.HandleNodeUp(ctx, int64(i))
		suite.Equal(i, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
		suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
		suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	}
}

func (suite *ResourceManagerSuite) TestAutoRecover() {
	ctx := suite.ctx
	for i := 1; i <= 100; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}
	suite.Equal(100, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Recover 10 nodes from default resource group
	suite.manager.AddResourceGroup(ctx, "rg1", newResourceGroupConfig(10, 30))
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").MissingNumOfNodes())
	suite.Equal(100, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg1").MissingNumOfNodes())
	suite.Equal(90, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Recover 20 nodes from default resource group
	suite.manager.AddResourceGroup(ctx, "rg2", newResourceGroupConfig(20, 30))
	suite.Zero(suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg2").MissingNumOfNodes())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(90, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(70, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Recover 5 redundant nodes from resource group
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(5, 5),
	})
	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(75, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Recover 10 redundant nodes from resource group 2 to resource group 1 and default resource group.
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(10, 20),
		"rg2": newResourceGroupConfig(5, 10),
	})

	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// recover redundant nodes from default resource group
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1":                    newResourceGroupConfig(10, 20),
		"rg2":                    newResourceGroupConfig(20, 30),
		DefaultResourceGroupName: newResourceGroupConfig(10, 20),
	})
	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)

	// Even though the default resource group has 20 nodes limits,
	// all redundant nodes will be assign to default resource group.
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(30, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(50, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Test recover missing from high priority resource group by set `from`.
	suite.manager.AddResourceGroup(ctx, "rg3", &rgpb.ResourceGroupConfig{
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
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(30, 40),
	})

	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")

	// Get 10 from default group for redundant nodes, get 5 from rg1 for rg3 at high priority.
	suite.Equal(15, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(30, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(15, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Test recover redundant to high priority resource group by set `to`.
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
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

	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")

	// Recover rg3 by transfer 10 nodes to rg2 with high priority, 5 to rg1.
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	suite.testTransferNode()

	// Test redundant nodes recover to default resource group.
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(1, 1),
		"rg3":                    newResourceGroupConfig(0, 0),
		"rg2":                    newResourceGroupConfig(0, 0),
		"rg1":                    newResourceGroupConfig(0, 0),
	})
	// Even default resource group has 1 node limit,
	// all redundant nodes will be assign to default resource group if there's no resource group can hold.
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(100, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Test redundant recover to missing nodes and missing nodes from redundant nodes.
	// Initialize
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(0, 0),
		"rg3":                    newResourceGroupConfig(10, 10),
		"rg2":                    newResourceGroupConfig(80, 80),
		"rg1":                    newResourceGroupConfig(10, 10),
	})
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(0, 5),
		"rg3":                    newResourceGroupConfig(5, 5),
		"rg2":                    newResourceGroupConfig(80, 80),
		"rg1":                    newResourceGroupConfig(20, 30),
	})
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3") // recover redundant to missing rg.
	suite.Equal(15, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	suite.manager.updateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(5, 5),
		"rg3":                    newResourceGroupConfig(5, 10),
		"rg2":                    newResourceGroupConfig(80, 80),
		"rg1":                    newResourceGroupConfig(10, 10),
	})
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName) // recover missing from redundant rg.
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(80, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
}

func (suite *ResourceManagerSuite) testTransferNode() {
	ctx := suite.ctx
	// Test redundant nodes recover to default resource group.
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		DefaultResourceGroupName: newResourceGroupConfig(40, 40),
		"rg3":                    newResourceGroupConfig(0, 0),
		"rg2":                    newResourceGroupConfig(40, 40),
		"rg1":                    newResourceGroupConfig(20, 20),
	})
	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")

	suite.Equal(20, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// Test TransferNode.
	// param error.
	err := suite.manager.TransferNode(ctx, "rg1", "rg1", 1)
	suite.Error(err)

	err = suite.manager.TransferNode(ctx, "rg1", "rg2", 0)
	suite.Error(err)

	err = suite.manager.TransferNode(ctx, "rg3", "rg2", 1)
	suite.Error(err)

	err = suite.manager.TransferNode(ctx, "rg1", "rg10086", 1)
	suite.Error(err)

	err = suite.manager.TransferNode(ctx, "rg10086", "rg2", 1)
	suite.Error(err)

	// success
	err = suite.manager.TransferNode(ctx, "rg1", "rg3", 5)
	suite.NoError(err)

	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")

	suite.Equal(15, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(5, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(40, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
}

func (suite *ResourceManagerSuite) TestIncomingNode() {
	ctx := suite.ctx
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.incomingNode.Insert(1)

	suite.Equal(1, suite.manager.CheckIncomingNodeNum(ctx))
	suite.manager.AssignPendingIncomingNode(ctx)
	suite.Equal(0, suite.manager.CheckIncomingNodeNum(ctx))
	nodes, err := suite.manager.GetNodes(ctx, DefaultResourceGroupName)
	suite.NoError(err)
	suite.Len(nodes, 1)
}

func (suite *ResourceManagerSuite) TestUnassignFail() {
	ctx := suite.ctx
	// suite.man
	mockKV := mocks.NewMetaKv(suite.T())
	mockKV.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil).Once()

	store := querycoord.NewCatalog(mockKV)
	suite.manager = NewResourceManager(store, session.NewNodeManager())

	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": newResourceGroupConfig(20, 30),
	})

	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.manager.HandleNodeUp(ctx, 1)

	mockKV.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(merr.WrapErrServiceInternal("mocked")).Once()

	suite.Panics(func() {
		suite.manager.HandleNodeDown(ctx, 1)
	})
}

func TestGetResourceGroupsJSON(t *testing.T) {
	ctx := context.Background()
	nodeManager := session.NewNodeManager()
	manager := &ResourceManager{groups: make(map[string]*ResourceGroup)}
	rg1 := NewResourceGroup("rg1", newResourceGroupConfig(0, 10), nodeManager)
	rg1.nodes = typeutil.NewUniqueSet(1, 2)
	rg2 := NewResourceGroup("rg2", newResourceGroupConfig(0, 20), nodeManager)
	rg2.nodes = typeutil.NewUniqueSet(3, 4)
	manager.groups["rg1"] = rg1
	manager.groups["rg2"] = rg2

	jsonOutput := manager.GetResourceGroupsJSON(ctx)
	var resourceGroups []*metricsinfo.ResourceGroup
	err := json.Unmarshal([]byte(jsonOutput), &resourceGroups)
	assert.NoError(t, err)
	assert.Len(t, resourceGroups, 2)

	checkResult := func(rg *metricsinfo.ResourceGroup) {
		if rg.Name == "rg1" {
			assert.ElementsMatch(t, []int64{1, 2}, rg.Nodes)
		} else if rg.Name == "rg2" {
			assert.ElementsMatch(t, []int64{3, 4}, rg.Nodes)
		} else {
			assert.Failf(t, "unexpected resource group name", "unexpected resource group name %s", rg.Name)
		}
	}

	for _, rg := range resourceGroups {
		checkResult(rg)
	}
}

func (suite *ResourceManagerSuite) TestNodeLabels_NodeAssign() {
	ctx := suite.ctx
	suite.manager.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		NodeFilter: &rgpb.ResourceGroupNodeFilter{
			NodeLabels: []*commonpb.KeyValuePair{
				{
					Key:   "dc_name",
					Value: "label1",
				},
			},
		},
	})

	suite.manager.AddResourceGroup(ctx, "rg2", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		NodeFilter: &rgpb.ResourceGroupNodeFilter{
			NodeLabels: []*commonpb.KeyValuePair{
				{
					Key:   "dc_name",
					Value: "label2",
				},
			},
		},
	})

	suite.manager.AddResourceGroup(ctx, "rg3", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		NodeFilter: &rgpb.ResourceGroupNodeFilter{
			NodeLabels: []*commonpb.KeyValuePair{
				{
					Key:   "dc_name",
					Value: "label3",
				},
			},
		},
	})

	// test that all query nodes has been marked label1
	for i := 1; i <= 30; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
			Labels: map[string]string{
				"dc_name": "label1",
			},
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// test new querynode with label2
	for i := 31; i <= 40; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
			Labels: map[string]string{
				"dc_name": "label2",
			},
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(0, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	nodesInRG, _ := suite.manager.GetNodes(ctx, "rg2")
	for _, node := range nodesInRG {
		suite.Equal("label2", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}

	// test new querynode with label3
	for i := 41; i <= 50; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
			Labels: map[string]string{
				"dc_name": "label3",
			},
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	nodesInRG, _ = suite.manager.GetNodes(ctx, "rg3")
	for _, node := range nodesInRG {
		suite.Equal("label3", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}

	// test swap rg's label
	suite.manager.UpdateResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{
		"rg1": {
			Requests: &rgpb.ResourceGroupLimit{
				NodeNum: 10,
			},
			Limits: &rgpb.ResourceGroupLimit{
				NodeNum: 10,
			},
			NodeFilter: &rgpb.ResourceGroupNodeFilter{
				NodeLabels: []*commonpb.KeyValuePair{
					{
						Key:   "dc_name",
						Value: "label2",
					},
				},
			},
		},
		"rg2": {
			Requests: &rgpb.ResourceGroupLimit{
				NodeNum: 10,
			},
			Limits: &rgpb.ResourceGroupLimit{
				NodeNum: 10,
			},
			NodeFilter: &rgpb.ResourceGroupNodeFilter{
				NodeLabels: []*commonpb.KeyValuePair{
					{
						Key:   "dc_name",
						Value: "label3",
					},
				},
			},
		},
		"rg3": {
			Requests: &rgpb.ResourceGroupLimit{
				NodeNum: 10,
			},
			Limits: &rgpb.ResourceGroupLimit{
				NodeNum: 10,
			},
			NodeFilter: &rgpb.ResourceGroupNodeFilter{
				NodeLabels: []*commonpb.KeyValuePair{
					{
						Key:   "dc_name",
						Value: "label1",
					},
				},
			},
		},
	})

	log.Info("test swap rg's label")
	for i := 0; i < 4; i++ {
		suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
		suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
		suite.manager.AutoRecoverResourceGroup(ctx, "rg3")
		suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	}
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(20, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	nodesInRG, _ = suite.manager.GetNodes(ctx, "rg1")
	for _, node := range nodesInRG {
		suite.Equal("label2", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}

	nodesInRG, _ = suite.manager.GetNodes(ctx, "rg2")
	for _, node := range nodesInRG {
		suite.Equal("label3", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}

	nodesInRG, _ = suite.manager.GetNodes(ctx, "rg3")
	for _, node := range nodesInRG {
		suite.Equal("label1", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}
}

func (suite *ResourceManagerSuite) TestNodeLabels_NodeDown() {
	ctx := suite.ctx
	suite.manager.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		NodeFilter: &rgpb.ResourceGroupNodeFilter{
			NodeLabels: []*commonpb.KeyValuePair{
				{
					Key:   "dc_name",
					Value: "label1",
				},
			},
		},
	})

	suite.manager.AddResourceGroup(ctx, "rg2", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		NodeFilter: &rgpb.ResourceGroupNodeFilter{
			NodeLabels: []*commonpb.KeyValuePair{
				{
					Key:   "dc_name",
					Value: "label2",
				},
			},
		},
	})

	suite.manager.AddResourceGroup(ctx, "rg3", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 10,
		},
		NodeFilter: &rgpb.ResourceGroupNodeFilter{
			NodeLabels: []*commonpb.KeyValuePair{
				{
					Key:   "dc_name",
					Value: "label3",
				},
			},
		},
	})

	// test that all query nodes has been marked label1
	for i := 1; i <= 10; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
			Labels: map[string]string{
				"dc_name": "label1",
			},
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}

	// test new querynode with label2
	for i := 31; i <= 40; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
			Labels: map[string]string{
				"dc_name": "label2",
			},
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}
	// test new querynode with label3
	for i := 41; i <= 50; i++ {
		suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
			Labels: map[string]string{
				"dc_name": "label3",
			},
		}))
		suite.manager.HandleNodeUp(ctx, int64(i))
	}
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())

	// test node down with label1
	suite.manager.HandleNodeDown(ctx, int64(1))
	suite.manager.nodeMgr.Remove(int64(1))
	suite.Equal(9, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())

	// test node up with label2
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(101),
		Address:  "localhost",
		Hostname: "localhost",
		Labels: map[string]string{
			"dc_name": "label2",
		},
	}))
	suite.manager.HandleNodeUp(ctx, int64(101))
	suite.Equal(9, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())

	// test node up with label1
	suite.manager.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   int64(102),
		Address:  "localhost",
		Hostname: "localhost",
		Labels: map[string]string{
			"dc_name": "label1",
		},
	}))
	suite.manager.HandleNodeUp(ctx, int64(102))
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg1").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg2").NodeNum())
	suite.Equal(10, suite.manager.GetResourceGroup(ctx, "rg3").NodeNum())
	suite.Equal(1, suite.manager.GetResourceGroup(ctx, DefaultResourceGroupName).NodeNum())
	nodesInRG, _ := suite.manager.GetNodes(ctx, "rg1")
	for _, node := range nodesInRG {
		suite.Equal("label1", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}

	suite.manager.AutoRecoverResourceGroup(ctx, "rg1")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg2")
	suite.manager.AutoRecoverResourceGroup(ctx, "rg3")
	suite.manager.AutoRecoverResourceGroup(ctx, DefaultResourceGroupName)
	nodesInRG, _ = suite.manager.GetNodes(ctx, DefaultResourceGroupName)
	for _, node := range nodesInRG {
		suite.Equal("label2", suite.manager.nodeMgr.Get(node).Labels()["dc_name"])
	}
}
