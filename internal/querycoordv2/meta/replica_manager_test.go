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
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// newTestResourceGroup creates a ResourceGroup for testing with given nodes.
func newTestResourceGroup(name string, nodes typeutil.UniqueSet) *ResourceGroup {
	return &ResourceGroup{
		name:  name,
		nodes: nodes,
		cfg:   newResourceGroupConfig(int32(nodes.Len()), int32(nodes.Len())),
	}
}

type collectionLoadConfig struct {
	spawnConfig map[string]int
}

func (c *collectionLoadConfig) getTotalSpawn() int {
	totalSpawn := 0
	for _, spawnNum := range c.spawnConfig {
		totalSpawn += spawnNum
	}
	return totalSpawn
}

// Old replica manager test suite.
type ReplicaManagerSuite struct {
	suite.Suite

	rgNodes     map[string]typeutil.UniqueSet
	collections map[int64]collectionLoadConfig
	idAllocator func() (int64, error)
	kv          kv.MetaKv
	catalog     metastore.QueryCoordCatalog
	mgr         *ReplicaManager
	ctx         context.Context
}

func (suite *ReplicaManagerSuite) SetupSuite() {
	paramtable.Init()

	suite.rgNodes = map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(1),
		"RG2": typeutil.NewUniqueSet(2, 3),
		"RG3": typeutil.NewUniqueSet(4, 5, 6),
	}
	suite.collections = map[int64]collectionLoadConfig{
		100: {
			spawnConfig: map[string]int{"RG1": 1},
		},
		101: {
			spawnConfig: map[string]int{"RG2": 2},
		},
		102: {
			spawnConfig: map[string]int{"RG3": 2},
		},
		103: {
			spawnConfig: map[string]int{"RG1": 1, "RG2": 1, "RG3": 1},
		},
	}
	suite.ctx = context.Background()
}

func (suite *ReplicaManagerSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue(),
	)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.catalog = querycoord.NewCatalog(suite.kv)

	suite.idAllocator = RandomIncrementIDAllocator()
	suite.mgr = NewReplicaManager(suite.idAllocator, suite.catalog)
	suite.spawnAll()
}

func (suite *ReplicaManagerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ReplicaManagerSuite) TestSpawnWithReplicaConfig() {
	mgr := suite.mgr
	ctx := suite.ctx

	replicas, err := mgr.SpawnWithReplicaConfig(ctx, SpawnWithReplicaConfigParams{
		CollectionID: 100,
		Channels:     []string{"channel1", "channel2"},
		Configs: []*messagespb.LoadReplicaConfig{
			{ReplicaId: 1000, ResourceGroupName: "RG1", Priority: commonpb.LoadPriority_HIGH},
		},
	})
	suite.NoError(err)
	suite.Len(replicas, 1)

	replicas, err = mgr.SpawnWithReplicaConfig(ctx, SpawnWithReplicaConfigParams{
		CollectionID: 100,
		Channels:     []string{"channel1", "channel2"},
		Configs: []*messagespb.LoadReplicaConfig{
			{ReplicaId: 1000, ResourceGroupName: "RG1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaId: 1001, ResourceGroupName: "RG1", Priority: commonpb.LoadPriority_HIGH},
		},
	})
	suite.NoError(err)
	suite.Len(replicas, 2)

	replicas, err = mgr.SpawnWithReplicaConfig(ctx, SpawnWithReplicaConfigParams{
		CollectionID: 100,
		Channels:     []string{"channel1", "channel2"},
		Configs: []*messagespb.LoadReplicaConfig{
			{ReplicaId: 1000, ResourceGroupName: "RG1", Priority: commonpb.LoadPriority_HIGH},
		},
	})
	suite.NoError(err)
	suite.Len(replicas, 1)
}

func (suite *ReplicaManagerSuite) TestSpawn() {
	mgr := suite.mgr
	ctx := suite.ctx

	mgr.idAllocator = ErrorIDAllocator()
	_, err := mgr.Spawn(ctx, 1, map[string]int{DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.Error(err)

	replicas := mgr.GetByCollection(ctx, 1)
	suite.Len(replicas, 0)

	mgr.idAllocator = suite.idAllocator
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ScoreBasedBalancerName)
	replicas, err = mgr.Spawn(ctx, 1, map[string]int{DefaultResourceGroupName: 1}, []string{"channel1", "channel2"}, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	for _, replica := range replicas {
		suite.Len(replica.replicaPB.GetChannelNodeInfos(), 0)
	}

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
	replicas, err = mgr.Spawn(ctx, 2, map[string]int{DefaultResourceGroupName: 1}, []string{"channel1", "channel2"}, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	for _, replica := range replicas {
		suite.Len(replica.replicaPB.GetChannelNodeInfos(), 2)
	}
}

func (suite *ReplicaManagerSuite) TestGet() {
	mgr := suite.mgr
	ctx := suite.ctx

	for collectionID, collectionCfg := range suite.collections {
		replicas := mgr.GetByCollection(ctx, collectionID)
		replicaNodes := make(map[int64][]int64)
		nodes := make([]int64, 0)
		for _, replica := range replicas {
			suite.Equal(collectionID, replica.GetCollectionID())
			suite.Equal(replica, mgr.Get(ctx, replica.GetID()))
			suite.Equal(len(replica.replicaPB.GetNodes()), replica.RWNodesCount())
			suite.ElementsMatch(replica.replicaPB.GetNodes(), replica.GetNodes())
			replicaNodes[replica.GetID()] = replica.GetNodes()
			nodes = append(nodes, replica.GetNodes()...)
		}

		expectedNodes := make([]int64, 0)
		for rg := range collectionCfg.spawnConfig {
			expectedNodes = append(expectedNodes, suite.rgNodes[rg].Collect()...)
		}
		suite.ElementsMatch(nodes, expectedNodes)

		for replicaID, nodes := range replicaNodes {
			for _, node := range nodes {
				replica := mgr.GetByCollectionAndNode(ctx, collectionID, node)
				suite.Equal(replicaID, replica.GetID())
			}
		}
	}
}

func (suite *ReplicaManagerSuite) TestGetByNode() {
	mgr := suite.mgr
	ctx := suite.ctx

	randomNodeID := int64(11111)
	testReplica1 := newReplica(&querypb.Replica{
		CollectionID:  3002,
		ID:            10086,
		Nodes:         []int64{randomNodeID},
		ResourceGroup: DefaultResourceGroupName,
	})
	testReplica2 := newReplica(&querypb.Replica{
		CollectionID:  3002,
		ID:            10087,
		Nodes:         []int64{randomNodeID},
		ResourceGroup: DefaultResourceGroupName,
	})
	mgr.Put(ctx, testReplica1, testReplica2)

	replicas := mgr.GetByNode(ctx, randomNodeID)
	suite.Len(replicas, 2)
}

func (suite *ReplicaManagerSuite) TestRecover() {
	mgr := suite.mgr
	ctx := suite.ctx

	// Clear data in memory, and then recover from meta store
	suite.clearMemory()
	mgr.Recover(ctx, lo.Keys(suite.collections))
	suite.TestGet()

	// Test recover from 2.1 meta store
	replicaInfo := milvuspb.ReplicaInfo{
		ReplicaID:    2100,
		CollectionID: 1000,
		NodeIds:      []int64{1, 2, 3},
	}
	value, err := proto.Marshal(&replicaInfo)
	suite.NoError(err)
	suite.kv.Save(ctx, querycoord.ReplicaMetaPrefixV1+"/2100", string(value))

	suite.clearMemory()
	mgr.Recover(ctx, append(lo.Keys(suite.collections), 1000))
	replica := mgr.Get(ctx, 2100)
	suite.NotNil(replica)
	suite.EqualValues(1000, replica.GetCollectionID())
	suite.ElementsMatch([]int64{1, 2, 3}, replica.GetNodes())
	suite.Len(replica.GetNodes(), len(replica.GetNodes()))
	for _, node := range replica.GetNodes() {
		suite.True(replica.Contains(node))
	}
}

func (suite *ReplicaManagerSuite) TestRemove() {
	mgr := suite.mgr
	ctx := suite.ctx

	for collection := range suite.collections {
		err := mgr.RemoveCollection(ctx, collection)
		suite.NoError(err)

		replicas := mgr.GetByCollection(ctx, collection)
		suite.Empty(replicas)
	}

	// Check whether the replicas are also removed from meta store
	mgr.Recover(ctx, lo.Keys(suite.collections))
	for collection := range suite.collections {
		replicas := mgr.GetByCollection(ctx, collection)
		suite.Empty(replicas)
	}
}

func (suite *ReplicaManagerSuite) TestNodeManipulate() {
	mgr := suite.mgr
	ctx := suite.ctx

	// add node into rg.
	rgNodes := map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(1, 7),
		"RG2": typeutil.NewUniqueSet(2, 3, 8),
		"RG3": typeutil.NewUniqueSet(4, 5, 6, 9),
	}

	// Add node into rg.
	for collectionID, cfg := range suite.collections {
		rgsOfCollection := make(map[string]*ResourceGroup)
		for rg := range cfg.spawnConfig {
			rgsOfCollection[rg] = newTestResourceGroup(rg, rgNodes[rg])
		}
		mgr.RecoverNodesInCollection(ctx, collectionID, rgsOfCollection)
		for rg := range cfg.spawnConfig {
			for _, node := range rgNodes[rg].Collect() {
				replica := mgr.GetByCollectionAndNode(ctx, collectionID, node)
				suite.Contains(replica.GetNodes(), node)
			}
		}
	}

	// Check these modifications are applied to meta store
	suite.clearMemory()
	mgr.Recover(ctx, lo.Keys(suite.collections))
	for collectionID, cfg := range suite.collections {
		for rg := range cfg.spawnConfig {
			for _, node := range rgNodes[rg].Collect() {
				replica := mgr.GetByCollectionAndNode(ctx, collectionID, node)
				suite.Contains(replica.GetNodes(), node)
			}
		}
	}
}

func (suite *ReplicaManagerSuite) spawnAll() {
	mgr := suite.mgr
	ctx := suite.ctx

	for id, cfg := range suite.collections {
		replicas, err := mgr.Spawn(ctx, id, cfg.spawnConfig, nil, commonpb.LoadPriority_LOW)
		suite.NoError(err)
		totalSpawn := 0
		rgsOfCollection := make(map[string]*ResourceGroup)
		for rg, spawnNum := range cfg.spawnConfig {
			totalSpawn += spawnNum
			rgsOfCollection[rg] = newTestResourceGroup(rg, suite.rgNodes[rg])
		}
		mgr.RecoverNodesInCollection(ctx, id, rgsOfCollection)
		suite.Len(replicas, totalSpawn)
	}
}

func (suite *ReplicaManagerSuite) TestResourceGroup() {
	mgr := NewReplicaManager(suite.idAllocator, suite.catalog)
	ctx := suite.ctx
	replicas1, err := mgr.Spawn(ctx, int64(1000), map[string]int{DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	suite.NotNil(replicas1)
	suite.Len(replicas1, 1)

	replica2, err := mgr.Spawn(ctx, int64(2000), map[string]int{DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	suite.NotNil(replica2)
	suite.Len(replica2, 1)

	replicas := mgr.GetByResourceGroup(ctx, DefaultResourceGroupName)
	suite.Len(replicas, 2)
	rgNames := mgr.GetResourceGroupByCollection(ctx, int64(1000))
	suite.Len(rgNames, 1)
	suite.True(rgNames.Contain(DefaultResourceGroupName))
}

func (suite *ReplicaManagerSuite) clearMemory() {
	suite.mgr.coll2Replicas.Range(func(collID int64, _ []*Replica) bool {
		suite.mgr.coll2Replicas.Remove(collID)
		return true
	})
	suite.mgr.flatReplicas.Range(func(replicaID int64, _ *Replica) bool {
		suite.mgr.flatReplicas.Remove(replicaID)
		return true
	})
	for _, replicaID := range suite.mgr.queryInvisibleReplicas.Collect() {
		suite.mgr.queryInvisibleReplicas.Remove(replicaID)
	}
}

type ReplicaManagerV2Suite struct {
	suite.Suite

	rgNodes         map[string]typeutil.UniqueSet
	sqNodesByRG     map[string]typeutil.UniqueSet // streaming query nodes grouped by resource group
	outboundSQNodes []int64
	collections     map[int64]collectionLoadConfig
	kv              kv.MetaKv
	catalog         metastore.QueryCoordCatalog
	mgr             *ReplicaManager
	ctx             context.Context
}

func (suite *ReplicaManagerV2Suite) SetupSuite() {
	paramtable.Init()

	suite.rgNodes = map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(1),
		"RG2": typeutil.NewUniqueSet(2, 3),
		"RG3": typeutil.NewUniqueSet(4, 5, 6),
		"RG4": typeutil.NewUniqueSet(7, 8, 9, 10),
		"RG5": typeutil.NewUniqueSet(11, 12, 13, 14, 15),
	}
	// Streaming query nodes grouped by resource group for resource group isolation testing.
	// Use a default empty resource group to test fallback mode initially.
	suite.sqNodesByRG = map[string]typeutil.UniqueSet{
		"": typeutil.NewUniqueSet(16, 17, 18, 19, 20),
	}
	suite.outboundSQNodes = []int64{}
	suite.collections = map[int64]collectionLoadConfig{
		1000: {
			spawnConfig: map[string]int{"RG1": 1},
		},
		1001: {
			spawnConfig: map[string]int{"RG2": 2},
		},
		1002: {
			spawnConfig: map[string]int{"RG3": 2},
		},
		1003: {
			spawnConfig: map[string]int{"RG1": 1, "RG2": 1, "RG3": 1},
		},
		1004: {
			spawnConfig: map[string]int{"RG4": 2, "RG5": 3},
		},
		1005: {
			spawnConfig: map[string]int{"RG4": 3, "RG5": 2},
		},
	}

	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue(),
	)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.catalog = querycoord.NewCatalog(suite.kv)

	idAllocator := RandomIncrementIDAllocator()
	suite.mgr = NewReplicaManager(idAllocator, suite.catalog)
	suite.ctx = context.Background()
}

func (suite *ReplicaManagerV2Suite) TearDownSuite() {
	suite.kv.Close()
}

func (suite *ReplicaManagerV2Suite) TestSpawn() {
	mgr := suite.mgr
	ctx := suite.ctx

	for id, cfg := range suite.collections {
		replicas, err := mgr.Spawn(ctx, id, cfg.spawnConfig, nil, commonpb.LoadPriority_LOW)
		suite.NoError(err)
		rgsOfCollection := make(map[string]*ResourceGroup)
		for rg := range cfg.spawnConfig {
			rgsOfCollection[rg] = newTestResourceGroup(rg, suite.rgNodes[rg])
		}
		mgr.RecoverNodesInCollection(ctx, id, rgsOfCollection)
		mgr.RecoverSQNodesInCollections(ctx, []int64{id}, suite.sqNodesByRG)
		for rg := range cfg.spawnConfig {
			for _, node := range suite.rgNodes[rg].Collect() {
				replica := mgr.GetByCollectionAndNode(ctx, id, node)
				suite.Contains(replica.GetNodes(), node)
			}
		}
		suite.Len(replicas, cfg.getTotalSpawn())
		replicas = mgr.GetByCollection(ctx, id)
		suite.Len(replicas, cfg.getTotalSpawn())
	}
	suite.testIfBalanced()
}

func (suite *ReplicaManagerV2Suite) testIfBalanced() {
	ctx := suite.ctx
	// If balanced
	for id := range suite.collections {
		replicas := suite.mgr.GetByCollection(ctx, id)
		rgToReplica := make(map[string][]*Replica, 0)
		for _, r := range replicas {
			rgToReplica[r.GetResourceGroup()] = append(rgToReplica[r.GetResourceGroup()], r)
		}

		maximumSQNodes := -1
		minimumSQNodes := -1
		sqNodes := make([]int64, 0)
		for _, replicas := range rgToReplica {
			maximumNodes := -1
			minimumNodes := -1
			nodes := make([]int64, 0)
			for _, r := range replicas {
				availableNodes := suite.rgNodes[r.GetResourceGroup()]
				if maximumNodes == -1 || r.RWNodesCount() > maximumNodes {
					maximumNodes = r.RWNodesCount()
				}
				if minimumNodes == -1 || r.RWNodesCount() < minimumNodes {
					minimumNodes = r.RWNodesCount()
				}
				if maximumSQNodes == -1 || r.RWSQNodesCount() > maximumSQNodes {
					maximumSQNodes = r.RWSQNodesCount()
				}
				if minimumSQNodes == -1 || r.RWSQNodesCount() < minimumSQNodes {
					minimumSQNodes = r.RWSQNodesCount()
				}
				nodes = append(nodes, r.GetRWNodes()...)
				nodes = append(nodes, r.GetRONodes()...)
				sqNodes = append(sqNodes, r.GetRWSQNodes()...)
				r.RangeOverRONodes(func(node int64) bool {
					if availableNodes.Contain(node) {
						nodes = append(nodes, node)
					}
					return true
				})
			}
			suite.ElementsMatch(nodes, suite.rgNodes[replicas[0].GetResourceGroup()].Collect())
			suite.True(maximumNodes-minimumNodes <= 1)
		}
		availableSQNodes := suite.getAllSQNodes()
		availableSQNodes.Remove(suite.outboundSQNodes...)
		suite.ElementsMatch(availableSQNodes.Collect(), sqNodes)
		suite.True(maximumSQNodes-minimumSQNodes <= 1)
	}
}

// getAllSQNodes returns all streaming query nodes from all resource groups.
func (suite *ReplicaManagerV2Suite) getAllSQNodes() typeutil.UniqueSet {
	allNodes := typeutil.NewUniqueSet()
	for _, nodes := range suite.sqNodesByRG {
		for node := range nodes {
			allNodes.Insert(node)
		}
	}
	return allNodes
}

func (suite *ReplicaManagerV2Suite) TestTransferReplica() {
	ctx := suite.ctx
	// param error
	err := suite.mgr.TransferReplica(ctx, 10086, "RG4", "RG5", 1)
	suite.Error(err)
	err = suite.mgr.TransferReplica(ctx, 1005, "RG4", "RG5", 0)
	suite.Error(err)
	err = suite.mgr.TransferReplica(ctx, 1005, "RG4", "RG4", 1)
	suite.Error(err)

	err = suite.mgr.TransferReplica(ctx, 1005, "RG4", "RG5", 1)
	suite.NoError(err)
	suite.recoverReplica(2, true)
	suite.testIfBalanced()
}

func (suite *ReplicaManagerV2Suite) TestTransferReplicaAndAddNode() {
	ctx := suite.ctx
	suite.mgr.TransferReplica(ctx, 1005, "RG4", "RG5", 1)
	suite.recoverReplica(1, false)
	suite.rgNodes["RG5"].Insert(16, 17, 18)
	// Add new streaming query nodes to the default resource group.
	suite.sqNodesByRG[""].Insert(20, 21, 22)
	suite.recoverReplica(2, true)
	suite.testIfBalanced()
}

func (suite *ReplicaManagerV2Suite) TestTransferNode() {
	suite.rgNodes["RG4"].Remove(7)
	suite.rgNodes["RG5"].Insert(7)
	suite.outboundSQNodes = []int64{16, 17, 18}
	suite.recoverReplica(2, true)
	suite.testIfBalanced()
}

func (suite *ReplicaManagerV2Suite) recoverReplica(k int, clearOutbound bool) {
	ctx := suite.ctx
	// need at least two times to recover the replicas.
	// transfer node between replicas need set to outbound and then set to incoming.
	for i := 0; i < k; i++ {
		// do a recover
		for id, cfg := range suite.collections {
			rgsOfCollection := make(map[string]*ResourceGroup)
			for rg := range cfg.spawnConfig {
				rgsOfCollection[rg] = newTestResourceGroup(rg, suite.rgNodes[rg])
			}
			// Build sqNodes map with outbound nodes removed.
			sqNodesByRG := make(map[string]typeutil.UniqueSet)
			for rg, nodes := range suite.sqNodesByRG {
				sqNodesByRG[rg] = nodes.Clone()
				sqNodesByRG[rg].Remove(suite.outboundSQNodes...)
			}
			suite.mgr.RecoverNodesInCollection(ctx, id, rgsOfCollection)
			suite.mgr.RecoverSQNodesInCollections(ctx, []int64{id}, sqNodesByRG)
		}

		// clear all outbound nodes
		if clearOutbound {
			for id := range suite.collections {
				replicas := suite.mgr.GetByCollection(ctx, id)
				for _, r := range replicas {
					outboundNodes := r.GetRONodes()
					suite.mgr.RemoveNode(ctx, id, r.GetID(), outboundNodes...)
					suite.mgr.RemoveSQNodesInCollections(ctx, []SQNodeRemoval{{
						CollectionID: id,
						ReplicaID:    r.GetID(),
						Nodes:        r.GetROSQNodes(),
					}})
				}
			}
		}
	}
}

// TestSQNodeResourceGroupIsolation tests that streaming query nodes are assigned
// by resource group isolation when the streaming node resource groups cover all
// replica resource groups.
func TestSQNodeResourceGroupIsolation(t *testing.T) {
	paramtable.Init()

	catalog := mocks.NewQueryCoordCatalog(t)
	// Use On directly to handle variadic arguments.
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	idAllocator := RandomIncrementIDAllocator()
	mgr := NewReplicaManager(idAllocator, catalog)
	ctx := context.Background()

	// Create replicas in different resource groups.
	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "RG1",
		Nodes:         []int64{},
	})
	replica2 := newReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  100,
		ResourceGroup: "RG2",
		Nodes:         []int64{},
	})
	replica3 := newReplica(&querypb.Replica{
		ID:            3,
		CollectionID:  100,
		ResourceGroup: "RG3",
		Nodes:         []int64{},
	})

	err := mgr.Put(ctx, replica1, replica2, replica3)
	assert.NoError(t, err)

	// Test case 1: Resource group isolation mode.
	// Streaming nodes by RG that covers all replica RGs.
	sqNodesByRG := map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(101, 102),
		"RG2": typeutil.NewUniqueSet(201, 202, 203),
		"RG3": typeutil.NewUniqueSet(301),
	}

	err = mgr.RecoverSQNodesInCollections(ctx, []int64{100}, sqNodesByRG)
	assert.NoError(t, err)

	// Verify that each replica only gets streaming nodes from its own resource group.
	updatedReplica1 := mgr.Get(ctx, 1)
	updatedReplica2 := mgr.Get(ctx, 2)
	updatedReplica3 := mgr.Get(ctx, 3)

	// RG1 has 2 nodes, replica1 should get both.
	assert.ElementsMatch(t, []int64{101, 102}, updatedReplica1.GetRWSQNodes())
	// RG2 has 3 nodes, replica2 should get all 3.
	assert.ElementsMatch(t, []int64{201, 202, 203}, updatedReplica2.GetRWSQNodes())
	// RG3 has 1 node, replica3 should get it.
	assert.ElementsMatch(t, []int64{301}, updatedReplica3.GetRWSQNodes())

	// Test case 2: Fallback mode.
	// Create a new collection with a replica in a resource group not covered by streaming nodes.
	replica4 := newReplica(&querypb.Replica{
		ID:            4,
		CollectionID:  200,
		ResourceGroup: "RG_UNKNOWN", // This RG is not in sqNodesByRG.
		Nodes:         []int64{},
	})
	replica5 := newReplica(&querypb.Replica{
		ID:            5,
		CollectionID:  200,
		ResourceGroup: "RG1",
		Nodes:         []int64{},
	})

	err = mgr.Put(ctx, replica4, replica5)
	assert.NoError(t, err)

	err = mgr.RecoverSQNodesInCollections(ctx, []int64{200}, sqNodesByRG)
	assert.NoError(t, err)

	// In fallback mode, all streaming nodes are pooled together.
	// Total 6 nodes (2 + 3 + 1), 2 replicas, each should get 3 nodes.
	updatedReplica4 := mgr.Get(ctx, 4)
	updatedReplica5 := mgr.Get(ctx, 5)

	// Each replica should have 3 streaming query nodes (6 total / 2 replicas = 3 each).
	assert.Equal(t, 3, len(updatedReplica4.GetRWSQNodes()))
	assert.Equal(t, 3, len(updatedReplica5.GetRWSQNodes()))

	// Verify that nodes are from all resource groups combined.
	allNodes := append(updatedReplica4.GetRWSQNodes(), updatedReplica5.GetRWSQNodes()...)
	allExpectedNodes := []int64{101, 102, 201, 202, 203, 301}
	assert.ElementsMatch(t, allExpectedNodes, allNodes)

	// Test case 3: Rolling upgrade compatibility mode.
	// Old StreamingNodes without RG labels are reported under the default RG. Replicas
	// in default or old/uncovered RGs should use only that legacy default pool, while
	// replicas in newly covered RGs should keep strict RG isolation.
	replica8 := newReplica(&querypb.Replica{
		ID:            8,
		CollectionID:  250,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{},
	})
	replica9 := newReplica(&querypb.Replica{
		ID:            9,
		CollectionID:  250,
		ResourceGroup: "RG1",
		Nodes:         []int64{},
	})
	replica10 := newReplica(&querypb.Replica{
		ID:            10,
		CollectionID:  250,
		ResourceGroup: "RG_OLD_REPLICA", // Not covered by labeled streaming nodes.
		Nodes:         []int64{},
	})

	err = mgr.Put(ctx, replica8, replica9, replica10)
	assert.NoError(t, err)

	rollingUpgradeSQNodesByRG := map[string]typeutil.UniqueSet{
		DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902, 903, 904),
		"RG1":                    typeutil.NewUniqueSet(101, 102),
	}

	err = mgr.RecoverSQNodesInCollections(ctx, []int64{250}, rollingUpgradeSQNodesByRG)
	assert.NoError(t, err)

	updatedReplica8 := mgr.Get(ctx, 8)
	updatedReplica9 := mgr.Get(ctx, 9)
	updatedReplica10 := mgr.Get(ctx, 10)
	assert.Len(t, updatedReplica8.GetRWSQNodes(), 2)
	assert.Len(t, updatedReplica10.GetRWSQNodes(), 2)
	defaultPoolNodes := append(updatedReplica8.GetRWSQNodes(), updatedReplica10.GetRWSQNodes()...)
	assert.ElementsMatch(t, []int64{901, 902, 903, 904}, defaultPoolNodes)
	assert.ElementsMatch(t, []int64{101, 102}, updatedReplica9.GetRWSQNodes())

	// Test case 4: Strict isolation mode (with config enabled).
	// When streaming.queryNodeResourceGroupIsolation.enabled is true, uncovered replicas
	// should not get any streaming query nodes.
	paramtable.Get().Save(paramtable.Get().StreamingCfg.StrictResourceGroupIsolationEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.StrictResourceGroupIsolationEnabled.Key)

	replica6 := newReplica(&querypb.Replica{
		ID:            6,
		CollectionID:  300,
		ResourceGroup: "RG_NO_STREAMING_NODES", // Not covered by sqNodesByRG.
		Nodes:         []int64{},
	})
	replica7 := newReplica(&querypb.Replica{
		ID:            7,
		CollectionID:  300,
		ResourceGroup: "RG1",
		Nodes:         []int64{},
	})

	err = mgr.Put(ctx, replica6, replica7)
	assert.NoError(t, err)

	err = mgr.RecoverSQNodesInCollections(ctx, []int64{300}, sqNodesByRG)
	assert.NoError(t, err)

	// In strict isolation mode:
	// - Replica7 (RG1) should get nodes from RG1.
	// - Replica6 (RG_NO_STREAMING_NODES) should NOT get any nodes.
	updatedReplica6 := mgr.Get(ctx, 6)
	updatedReplica7 := mgr.Get(ctx, 7)

	// RG1 has 2 nodes, replica7 should get both.
	assert.ElementsMatch(t, []int64{101, 102}, updatedReplica7.GetRWSQNodes())
	// Uncovered replica should not get any streaming query nodes in strict isolation mode.
	assert.Equal(t, 0, len(updatedReplica6.GetRWSQNodes()))
}

// TestSQNodeRecoveryWithRONodes tests streaming query node recovery with existing RO nodes
// and RW nodes that are no longer available in the node set.
func TestSQNodeRecoveryWithRONodes(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	idAllocator := RandomIncrementIDAllocator()
	mgr := NewReplicaManager(idAllocator, catalog)
	ctx := context.Background()

	// Create a replica with existing SQ nodes
	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "RG1",
		Nodes:         []int64{},
		RwSqNodes:     []int64{101, 102, 103}, // RW SQ nodes
		RoSqNodes:     []int64{104},           // RO SQ node (previously removed from available set)
	})

	err := mgr.Put(ctx, replica1)
	assert.NoError(t, err)

	// Available SQ nodes: only 101, 102 remain. 103 is no longer available (will become RO).
	// 104 was already RO but is now back in the available set (will recover to RW).
	sqNodesByRG := map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(101, 102, 104, 105),
	}

	err = mgr.RecoverSQNodesInCollections(ctx, []int64{100}, sqNodesByRG)
	assert.NoError(t, err)

	updatedReplica := mgr.Get(ctx, 1)
	// Node 103 is no longer available, should become RO.
	// Node 104 was RO but is now available, should recover to RW.
	// Node 105 is a new incoming node.
	assert.Contains(t, updatedReplica.GetROSQNodes(), int64(103))
	assert.Contains(t, updatedReplica.GetRWSQNodes(), int64(101))
	assert.Contains(t, updatedReplica.GetRWSQNodes(), int64(102))
	assert.Contains(t, updatedReplica.GetRWSQNodes(), int64(104))
	assert.Contains(t, updatedReplica.GetRWSQNodes(), int64(105))
	assert.NotContains(t, updatedReplica.GetROSQNodes(), int64(104))
}

// TestSQNodeRecoveryWithUnrecoverableNodes tests that unrecoverable RO nodes
// (nodes not in any available resource group) remain as RO.
func TestSQNodeRecoveryWithUnrecoverableNodes(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	idAllocator := RandomIncrementIDAllocator()
	mgr := NewReplicaManager(idAllocator, catalog)
	ctx := context.Background()

	// Create a replica with RO SQ nodes that are not recoverable
	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "RG1",
		Nodes:         []int64{},
		RwSqNodes:     []int64{101},
		RoSqNodes:     []int64{999}, // Node 999 is not in any available set - unrecoverable
	})

	err := mgr.Put(ctx, replica1)
	assert.NoError(t, err)

	// Available SQ nodes don't include 999
	sqNodesByRG := map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(101, 102),
	}

	err = mgr.RecoverSQNodesInCollections(ctx, []int64{100}, sqNodesByRG)
	assert.NoError(t, err)

	updatedReplica := mgr.Get(ctx, 1)
	// Node 999 should remain in RO since it's unrecoverable (not in available set)
	assert.Contains(t, updatedReplica.GetROSQNodes(), int64(999))
	// Node 101 should remain RW
	assert.Contains(t, updatedReplica.GetRWSQNodes(), int64(101))
	// Node 102 should be added as new RW
	assert.Contains(t, updatedReplica.GetRWSQNodes(), int64(102))
}

func TestRecoverSQNodesInCollectionsPersistsSingleBatch(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replicas := []*Replica{
		newReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "RG1", RwSqNodes: []int64{101}}),
		newReplica(&querypb.Replica{ID: 2, CollectionID: 200, ResourceGroup: "RG1", RwSqNodes: []int64{101}}),
		newReplica(&querypb.Replica{ID: 3, CollectionID: 300, ResourceGroup: "RG1", RwSqNodes: []int64{101}}),
	}
	for _, replica := range replicas {
		mgr.putReplicasInMemory(replica.GetCollectionID(), replica)
	}

	err := mgr.RecoverSQNodesInCollections(ctx, []int64{100, 200, 300}, map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(int64(102)),
	})
	assert.NoError(t, err)
	for _, replica := range replicas {
		updated := mgr.Get(ctx, replica.GetID())
		assert.ElementsMatch(t, []int64{102}, updated.GetRWSQNodes())
		assert.ElementsMatch(t, []int64{101}, updated.GetROSQNodes())
	}
}

func TestRecoverSQNodesInCollectionsSkipsStaleCollection(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replica := newReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "RG1", RwSqNodes: []int64{101}})
	mgr.putReplicasInMemory(replica.GetCollectionID(), replica)

	err := mgr.RecoverSQNodesInCollections(ctx, []int64{999, 100}, map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(int64(102)),
	})
	assert.NoError(t, err)

	updated := mgr.Get(ctx, replica.GetID())
	assert.ElementsMatch(t, []int64{102}, updated.GetRWSQNodes())
	assert.ElementsMatch(t, []int64{101}, updated.GetROSQNodes())
}

func TestRecoverSQNodesInCollectionsReturnsSaveErrorAndKeepsMemory(t *testing.T) {
	saveErr := errors.New("save failed")
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(saveErr).Once()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replica := newReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "RG1", RwSqNodes: []int64{101}})
	mgr.putReplicasInMemory(replica.GetCollectionID(), replica)

	err := mgr.RecoverSQNodesInCollections(ctx, []int64{999, 100}, map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(int64(102)),
	})
	assert.ErrorIs(t, err, saveErr)

	updated := mgr.Get(ctx, replica.GetID())
	assert.ElementsMatch(t, []int64{101}, updated.GetRWSQNodes())
	assert.Empty(t, updated.GetROSQNodes())
}

func TestSQNodeCleanupBatchSave(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replicas := []*Replica{
		newReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "RG1", RwSqNodes: []int64{101}, RoSqNodes: []int64{201}}),
		newReplica(&querypb.Replica{ID: 2, CollectionID: 200, ResourceGroup: "RG1", RwSqNodes: []int64{102}, RoSqNodes: []int64{202}}),
		newReplica(&querypb.Replica{ID: 3, CollectionID: 300, ResourceGroup: "RG1", RwSqNodes: []int64{103}, RoSqNodes: []int64{203}}),
	}
	assert.NoError(t, mgr.Put(ctx, replicas...))

	err := mgr.RemoveSQNodesInCollections(ctx, []SQNodeRemoval{
		{CollectionID: 100, ReplicaID: 1, Nodes: []int64{201}},
		{CollectionID: 200, ReplicaID: 2, Nodes: []int64{202}},
		{CollectionID: 300, ReplicaID: 3, Nodes: []int64{203}},
	})
	assert.NoError(t, err)
	for _, replica := range replicas {
		updated := mgr.Get(ctx, replica.GetID())
		assert.Empty(t, updated.GetROSQNodes())
		assert.ElementsMatch(t, replica.GetRWSQNodes(), updated.GetRWSQNodes())
	}
}

func TestSQNodeCleanupBatchIgnoresStaleRemovalForRecoveredRWNode(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replica := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "RG1",
		RwSqNodes:     []int64{201},
	})
	assert.NoError(t, mgr.Put(ctx, replica))

	err := mgr.RemoveSQNodesInCollections(ctx, []SQNodeRemoval{
		{CollectionID: 100, ReplicaID: 1, Nodes: []int64{201}},
	})
	assert.NoError(t, err)

	updated := mgr.Get(ctx, replica.GetID())
	assert.ElementsMatch(t, []int64{201}, updated.GetRWSQNodes())
	assert.Empty(t, updated.GetROSQNodes())
}

func TestSQNodeCleanupBatchSkipsStaleReplicas(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "RG1",
		RwSqNodes:     []int64{101},
		RoSqNodes:     []int64{201},
	})
	replica2 := newReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  200,
		ResourceGroup: "RG1",
		RwSqNodes:     []int64{102},
		RoSqNodes:     []int64{202},
	})
	mgr.putReplicasInMemory(replica1.GetCollectionID(), replica1)
	mgr.putReplicasInMemory(replica2.GetCollectionID(), replica2)

	err := mgr.RemoveSQNodesInCollections(ctx, []SQNodeRemoval{
		{CollectionID: 100, ReplicaID: 1, Nodes: []int64{201}},
		{CollectionID: 100, ReplicaID: 999, Nodes: []int64{299}},
		{CollectionID: 200, ReplicaID: 1, Nodes: []int64{201}},
		{CollectionID: 200, ReplicaID: 2, Nodes: []int64{202}},
	})
	assert.NoError(t, err)

	updated1 := mgr.Get(ctx, replica1.GetID())
	assert.Empty(t, updated1.GetROSQNodes())
	updated2 := mgr.Get(ctx, replica2.GetID())
	assert.Empty(t, updated2.GetROSQNodes())
}

func TestRemoveSQNodesInCollectionsSaveErrorKeepsMemory(t *testing.T) {
	saveErr := errors.New("save failed")
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(saveErr).Once()

	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	replica := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "RG1",
		RwSqNodes:     []int64{101},
		RoSqNodes:     []int64{201},
	})
	mgr.putReplicasInMemory(replica.GetCollectionID(), replica)

	err := mgr.RemoveSQNodesInCollections(ctx, []SQNodeRemoval{
		{CollectionID: 100, ReplicaID: 1, Nodes: []int64{201}},
	})
	assert.ErrorIs(t, err, saveErr)

	updated := mgr.Get(ctx, replica.GetID())
	assert.ElementsMatch(t, []int64{101}, updated.GetRWSQNodes())
	assert.ElementsMatch(t, []int64{201}, updated.GetROSQNodes())
}

func TestReplicaManager(t *testing.T) {
	suite.Run(t, new(ReplicaManagerSuite))
	suite.Run(t, new(ReplicaManagerV2Suite))
}

func TestGetReplicasJSON(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	idAllocator := RandomIncrementIDAllocator()
	replicaManager := NewReplicaManager(idAllocator, catalog)
	ctx := context.Background()

	// Add some replicas to the ReplicaManager
	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  100,
		ResourceGroup: "rg1",
		Nodes:         []int64{1, 2, 3},
	})
	replica2 := newReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  200,
		ResourceGroup: "rg2",
		Nodes:         []int64{4, 5, 6},
	})

	err := replicaManager.Put(ctx, replica1)
	assert.NoError(t, err)

	err = replicaManager.Put(ctx, replica2)
	assert.NoError(t, err)

	meta := &Meta{
		CollectionManager: NewCollectionManager(catalog),
	}

	err = meta.PutCollectionWithoutSave(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: 100,
			DbID:         int64(1),
		},
	})
	assert.NoError(t, err)

	err = meta.PutCollectionWithoutSave(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: 200,
		},
	})
	assert.NoError(t, err)

	jsonOutput := replicaManager.GetReplicasJSON(ctx, meta)
	var replicas []*metricsinfo.Replica
	err = json.Unmarshal([]byte(jsonOutput), &replicas)
	assert.NoError(t, err)
	assert.Len(t, replicas, 2)

	checkResult := func(replica *metricsinfo.Replica) {
		switch replica.ID {
		case 1:
			assert.Equal(t, int64(100), replica.CollectionID)
			assert.Equal(t, "rg1", replica.ResourceGroup)
			assert.ElementsMatch(t, []int64{1, 2, 3}, replica.RWNodes)
			assert.Equal(t, int64(1), replica.DatabaseID)
		case 2:
			assert.Equal(t, int64(200), replica.CollectionID)
			assert.Equal(t, "rg2", replica.ResourceGroup)
			assert.ElementsMatch(t, []int64{4, 5, 6}, replica.RWNodes)
			assert.Equal(t, int64(0), replica.DatabaseID)
		default:
			assert.Failf(t, "unexpected replica id", "unexpected replica id %d", replica.ID)
		}
	}

	for _, replica := range replicas {
		checkResult(replica)
	}
}

func TestReplicaManagerCollectionViewAfterPartialUpdateAndRemove(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ReleaseReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	idAllocator := RandomIncrementIDAllocator()
	mgr := NewReplicaManager(idAllocator, catalog)
	ctx := context.Background()

	collID := int64(7000)
	replicas, err := mgr.Spawn(ctx, collID, map[string]int{"rg1": 3}, nil, commonpb.LoadPriority_LOW)
	assert.NoError(t, err)
	assert.Len(t, replicas, 3)

	err = mgr.MoveReplica(ctx, collID, "rg2", replicas[1:])
	assert.NoError(t, err)

	updated := mgr.GetByCollection(ctx, collID)
	assert.Len(t, updated, 3)
	assert.Equal(t, replicas[0].GetID(), updated[0].GetID())
	assert.Equal(t, "rg1", updated[0].GetResourceGroup())
	assert.Equal(t, replicas[1].GetID(), updated[1].GetID())
	assert.Equal(t, "rg2", updated[1].GetResourceGroup())
	assert.Equal(t, replicas[2].GetID(), updated[2].GetID())
	assert.Equal(t, "rg2", updated[2].GetResourceGroup())

	err = mgr.RemoveReplicas(ctx, collID, replicas[1].GetID())
	assert.NoError(t, err)

	remaining := mgr.GetByCollection(ctx, collID)
	assert.Len(t, remaining, 2)
	assert.Equal(t, replicas[0].GetID(), remaining[0].GetID())
	assert.Equal(t, replicas[2].GetID(), remaining[1].GetID())
	assert.Nil(t, mgr.Get(ctx, replicas[1].GetID()))
	assert.NotNil(t, mgr.Get(ctx, replicas[0].GetID()))
	assert.NotNil(t, mgr.Get(ctx, replicas[2].GetID()))
}

func TestReplicaManagerPutMaintainsIndexesAcrossCollections(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	idAllocator := RandomIncrementIDAllocator()
	mgr := NewReplicaManager(idAllocator, catalog)
	ctx := context.Background()

	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  10,
		ResourceGroup: "rg1",
		Nodes:         []int64{101},
	})
	replica2 := newReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  20,
		ResourceGroup: "rg2",
		Nodes:         []int64{201},
	})

	err := mgr.Put(ctx, replica1, replica2)
	assert.NoError(t, err)

	assert.Equal(t, replica1, mgr.Get(ctx, replica1.GetID()))
	assert.Equal(t, replica2, mgr.Get(ctx, replica2.GetID()))
	assert.Equal(t, []*Replica{replica1}, mgr.GetByCollection(ctx, replica1.GetCollectionID()))
	assert.Equal(t, []*Replica{replica2}, mgr.GetByCollection(ctx, replica2.GetCollectionID()))
	assert.Equal(t, replica1, mgr.GetByCollectionAndNode(ctx, replica1.GetCollectionID(), int64(101)))
	assert.Equal(t, replica2, mgr.GetByCollectionAndNode(ctx, replica2.GetCollectionID(), int64(201)))
	assert.ElementsMatch(t, []*Replica{replica1}, mgr.GetByNode(ctx, int64(101)))
	assert.ElementsMatch(t, []*Replica{replica2}, mgr.GetByResourceGroup(ctx, "rg2"))
}

func TestReplicaManagerPutCrossCollectionPersistErrorIsAtomic(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	saveErr := errors.New("save failed")
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(saveErr).Once()
	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()

	replica1 := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  10,
		ResourceGroup: "rg1",
	})
	replica2 := newReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  20,
		ResourceGroup: "rg2",
	})

	err := mgr.Put(ctx, replica1, replica2)
	assert.ErrorIs(t, err, saveErr)
	assert.Nil(t, mgr.Get(ctx, replica1.GetID()))
	assert.Nil(t, mgr.Get(ctx, replica2.GetID()))
	assert.Empty(t, mgr.GetByCollection(ctx, replica1.GetCollectionID()))
	assert.Empty(t, mgr.GetByCollection(ctx, replica2.GetCollectionID()))
}

func TestReplicaManagerRemoveCollectionReleasesKVWhenNotLoaded(t *testing.T) {
	collectionID := int64(10)
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		catalog := mocks.NewQueryCoordCatalog(t)
		catalog.EXPECT().ReleaseReplicas(mock.Anything, collectionID).Return(nil).Once()
		mgr := NewReplicaManager(nil, catalog)

		err := mgr.RemoveCollection(ctx, collectionID)
		assert.NoError(t, err)
		assert.Empty(t, mgr.GetByCollection(ctx, collectionID))
	})

	t.Run("release error", func(t *testing.T) {
		catalog := mocks.NewQueryCoordCatalog(t)
		releaseErr := errors.New("release failed")
		catalog.EXPECT().ReleaseReplicas(mock.Anything, collectionID).Return(releaseErr).Once()
		mgr := NewReplicaManager(nil, catalog)

		err := mgr.RemoveCollection(ctx, collectionID)
		assert.ErrorIs(t, err, releaseErr)
		assert.Empty(t, mgr.GetByCollection(ctx, collectionID))
	})
}

func TestReplicaManagerPersistErrorPaths(t *testing.T) {
	ctx := context.Background()
	saveErr := errors.New("save failed")

	t.Run("direct persist", func(t *testing.T) {
		catalog := mocks.NewQueryCoordCatalog(t)
		replica := newReplica(&querypb.Replica{ID: 1, CollectionID: 10, ResourceGroup: "rg1"})
		catalog.EXPECT().SaveReplica(mock.Anything, replica.replicaPB).Return(saveErr).Once()
		mgr := NewReplicaManager(nil, catalog)

		assert.NoError(t, mgr.put(ctx, replica.GetCollectionID()))
		err := mgr.put(ctx, replica.GetCollectionID(), replica)
		assert.ErrorIs(t, err, saveErr)
	})

	t.Run("remove node", func(t *testing.T) {
		catalog := mocks.NewQueryCoordCatalog(t)
		catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(saveErr).Once()
		mgr := NewReplicaManager(nil, catalog)
		replica := newReplica(&querypb.Replica{
			ID:            1,
			CollectionID:  10,
			ResourceGroup: "rg1",
			Nodes:         []int64{101},
			RoNodes:       []int64{102},
		})
		mgr.putReplicasInMemory(replica.GetCollectionID(), replica)

		err := mgr.RemoveNode(ctx, replica.GetCollectionID(), replica.GetID(), int64(102))
		assert.ErrorIs(t, err, saveErr)
		assert.ElementsMatch(t, []int64{102}, mgr.Get(ctx, replica.GetID()).GetRONodes())
	})

	t.Run("remove streaming query node", func(t *testing.T) {
		catalog := mocks.NewQueryCoordCatalog(t)
		catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(saveErr).Once()
		mgr := NewReplicaManager(nil, catalog)
		replica := newReplica(&querypb.Replica{
			ID:            1,
			CollectionID:  10,
			ResourceGroup: "rg1",
			RwSqNodes:     []int64{201},
			RoSqNodes:     []int64{202},
		})
		mgr.putReplicasInMemory(replica.GetCollectionID(), replica)

		err := mgr.RemoveSQNodesInCollections(ctx, []SQNodeRemoval{{
			CollectionID: replica.GetCollectionID(),
			ReplicaID:    replica.GetID(),
			Nodes:        []int64{202},
		}})
		assert.ErrorIs(t, err, saveErr)
		assert.ElementsMatch(t, []int64{202}, mgr.Get(ctx, replica.GetID()).GetROSQNodes())
	})
}

func TestReplicaManagerSpawnWaitRGReadyRecovery(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Twice()
	mgr := NewReplicaManager(RandomIncrementIDAllocator(), catalog)
	ctx := context.Background()
	collID := int64(10)

	replicas, err := mgr.Spawn(ctx, collID, map[string]int{"rg1": 1}, nil, commonpb.LoadPriority_LOW, WithNeedWaitRGReady())
	assert.NoError(t, err)
	assert.Len(t, replicas, 1)
	replicaID := replicas[0].GetID()
	assert.True(t, mgr.Get(ctx, replicaID).NeedWaitRGReady())

	err = mgr.RecoverNodesInCollection(ctx, collID, map[string]*ResourceGroup{
		"rg1": {
			name:  "rg1",
			nodes: typeutil.NewUniqueSet(int64(101)),
			cfg:   newResourceGroupConfig(2, 2),
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, mgr.Get(ctx, replicaID).GetRWNodes())
	assert.True(t, mgr.Get(ctx, replicaID).NeedWaitRGReady())

	err = mgr.RecoverNodesInCollection(ctx, collID, map[string]*ResourceGroup{
		"rg1": newTestResourceGroup("rg1", typeutil.NewUniqueSet(101, 102)),
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{101, 102}, mgr.Get(ctx, replicaID).GetRWNodes())
	assert.False(t, mgr.Get(ctx, replicaID).NeedWaitRGReady())
}

func TestReplicaManagerMoveReplicaPersistError(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	saveErr := errors.New("move failed")
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(saveErr).Once()
	mgr := NewReplicaManager(nil, catalog)
	ctx := context.Background()
	replica := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  10,
		ResourceGroup: "rg1",
	})
	assert.NoError(t, mgr.Put(ctx, replica))

	err := mgr.MoveReplica(ctx, replica.GetCollectionID(), "rg2", []*Replica{replica})
	assert.ErrorIs(t, err, saveErr)
	assert.Equal(t, "rg1", mgr.Get(ctx, replica.GetID()).GetResourceGroup())
}
