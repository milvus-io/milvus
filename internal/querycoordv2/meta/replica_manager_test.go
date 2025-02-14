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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

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

	rgs         map[string]typeutil.UniqueSet
	collections map[int64]collectionLoadConfig
	idAllocator func() (int64, error)
	kv          kv.MetaKv
	catalog     metastore.QueryCoordCatalog
	mgr         *ReplicaManager
	ctx         context.Context
}

func (suite *ReplicaManagerSuite) SetupSuite() {
	paramtable.Init()

	suite.rgs = map[string]typeutil.UniqueSet{
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
		config.EtcdTLSMinVersion.GetValue())
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

func (suite *ReplicaManagerSuite) TestSpawn() {
	mgr := suite.mgr
	ctx := suite.ctx

	mgr.idAllocator = ErrorIDAllocator()
	_, err := mgr.Spawn(ctx, 1, map[string]int{DefaultResourceGroupName: 1}, nil)
	suite.Error(err)

	replicas := mgr.GetByCollection(ctx, 1)
	suite.Len(replicas, 0)

	mgr.idAllocator = suite.idAllocator
	replicas, err = mgr.Spawn(ctx, 1, map[string]int{DefaultResourceGroupName: 1}, []string{"channel1", "channel2"})
	suite.NoError(err)
	for _, replica := range replicas {
		suite.Len(replica.replicaPB.GetChannelNodeInfos(), 0)
	}

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
	replicas, err = mgr.Spawn(ctx, 2, map[string]int{DefaultResourceGroupName: 1}, []string{"channel1", "channel2"})
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
			expectedNodes = append(expectedNodes, suite.rgs[rg].Collect()...)
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
	rgs := map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(1, 7),
		"RG2": typeutil.NewUniqueSet(2, 3, 8),
		"RG3": typeutil.NewUniqueSet(4, 5, 6, 9),
	}

	// Add node into rg.
	for collectionID, cfg := range suite.collections {
		rgsOfCollection := make(map[string]typeutil.UniqueSet)
		for rg := range cfg.spawnConfig {
			rgsOfCollection[rg] = rgs[rg]
		}
		mgr.RecoverNodesInCollection(ctx, collectionID, rgsOfCollection)
		for rg := range cfg.spawnConfig {
			for _, node := range rgs[rg].Collect() {
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
			for _, node := range rgs[rg].Collect() {
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
		replicas, err := mgr.Spawn(ctx, id, cfg.spawnConfig, nil)
		suite.NoError(err)
		totalSpawn := 0
		rgsOfCollection := make(map[string]typeutil.UniqueSet)
		for rg, spawnNum := range cfg.spawnConfig {
			totalSpawn += spawnNum
			rgsOfCollection[rg] = suite.rgs[rg]
		}
		mgr.RecoverNodesInCollection(ctx, id, rgsOfCollection)
		suite.Len(replicas, totalSpawn)
	}
}

func (suite *ReplicaManagerSuite) TestResourceGroup() {
	mgr := NewReplicaManager(suite.idAllocator, suite.catalog)
	ctx := suite.ctx
	replicas1, err := mgr.Spawn(ctx, int64(1000), map[string]int{DefaultResourceGroupName: 1}, nil)
	suite.NoError(err)
	suite.NotNil(replicas1)
	suite.Len(replicas1, 1)

	replica2, err := mgr.Spawn(ctx, int64(2000), map[string]int{DefaultResourceGroupName: 1}, nil)
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
	suite.mgr.replicas = make(map[int64]*Replica)
}

type ReplicaManagerV2Suite struct {
	suite.Suite

	rgs             map[string]typeutil.UniqueSet
	sqNodes         typeutil.UniqueSet
	outboundSQNodes []int64
	collections     map[int64]collectionLoadConfig
	kv              kv.MetaKv
	catalog         metastore.QueryCoordCatalog
	mgr             *ReplicaManager
	ctx             context.Context
}

func (suite *ReplicaManagerV2Suite) SetupSuite() {
	paramtable.Init()

	suite.rgs = map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(1),
		"RG2": typeutil.NewUniqueSet(2, 3),
		"RG3": typeutil.NewUniqueSet(4, 5, 6),
		"RG4": typeutil.NewUniqueSet(7, 8, 9, 10),
		"RG5": typeutil.NewUniqueSet(11, 12, 13, 14, 15),
	}
	suite.sqNodes = typeutil.NewUniqueSet(16, 17, 18, 19, 20)
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
		config.EtcdTLSMinVersion.GetValue())
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
		replicas, err := mgr.Spawn(ctx, id, cfg.spawnConfig, nil)
		suite.NoError(err)
		rgsOfCollection := make(map[string]typeutil.UniqueSet)
		for rg := range cfg.spawnConfig {
			rgsOfCollection[rg] = suite.rgs[rg]
		}
		mgr.RecoverNodesInCollection(ctx, id, rgsOfCollection)
		mgr.RecoverSQNodesInCollection(ctx, id, suite.sqNodes)
		for rg := range cfg.spawnConfig {
			for _, node := range suite.rgs[rg].Collect() {
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
				availableNodes := suite.rgs[r.GetResourceGroup()]
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
			suite.ElementsMatch(nodes, suite.rgs[replicas[0].GetResourceGroup()].Collect())
			suite.True(maximumNodes-minimumNodes <= 1)
		}
		availableSQNodes := suite.sqNodes.Clone()
		availableSQNodes.Remove(suite.outboundSQNodes...)
		suite.ElementsMatch(availableSQNodes.Collect(), sqNodes)
		suite.True(maximumSQNodes-minimumSQNodes <= 1)
	}
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
	suite.rgs["RG5"].Insert(16, 17, 18)
	suite.sqNodes.Insert(20, 21, 22)
	suite.recoverReplica(2, true)
	suite.testIfBalanced()
}

func (suite *ReplicaManagerV2Suite) TestTransferNode() {
	suite.rgs["RG4"].Remove(7)
	suite.rgs["RG5"].Insert(7)
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
			rgsOfCollection := make(map[string]typeutil.UniqueSet)
			for rg := range cfg.spawnConfig {
				rgsOfCollection[rg] = suite.rgs[rg]
			}
			sqNodes := suite.sqNodes.Clone()
			sqNodes.Remove(suite.outboundSQNodes...)
			suite.mgr.RecoverNodesInCollection(ctx, id, rgsOfCollection)
			suite.mgr.RecoverSQNodesInCollection(ctx, id, sqNodes)
		}

		// clear all outbound nodes
		if clearOutbound {
			for id := range suite.collections {
				replicas := suite.mgr.GetByCollection(ctx, id)
				for _, r := range replicas {
					outboundNodes := r.GetRONodes()
					suite.mgr.RemoveNode(ctx, r.GetID(), outboundNodes...)
					suite.mgr.RemoveSQNode(ctx, r.GetID(), r.GetROSQNodes()...)
				}
			}
		}
	}
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

	err := replicaManager.put(ctx, replica1)
	assert.NoError(t, err)

	err = replicaManager.put(ctx, replica2)
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
		if replica.ID == 1 {
			assert.Equal(t, int64(100), replica.CollectionID)
			assert.Equal(t, "rg1", replica.ResourceGroup)
			assert.ElementsMatch(t, []int64{1, 2, 3}, replica.RWNodes)
			assert.Equal(t, int64(1), replica.DatabaseID)
		} else if replica.ID == 2 {
			assert.Equal(t, int64(200), replica.CollectionID)
			assert.Equal(t, "rg2", replica.ResourceGroup)
			assert.ElementsMatch(t, []int64{4, 5, 6}, replica.RWNodes)
			assert.Equal(t, int64(0), replica.DatabaseID)
		} else {
			assert.Failf(t, "unexpected replica id", "unexpected replica id %d", replica.ID)
		}
	}

	for _, replica := range replicas {
		checkResult(replica)
	}
}
