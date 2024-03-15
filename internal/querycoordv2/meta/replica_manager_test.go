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

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type collectionLoadConfig struct {
	spawnConfig map[string]int
}

type ReplicaManagerSuite struct {
	suite.Suite

	rgs         map[string]typeutil.UniqueSet
	collections map[int64]collectionLoadConfig
	idAllocator func() (int64, error)
	kv          kv.MetaKv
	catalog     metastore.QueryCoordCatalog
	mgr         *ReplicaManager
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

	mgr.idAllocator = ErrorIDAllocator()
	_, err := mgr.Spawn(1, map[string]int{DefaultResourceGroupName: 1})
	suite.Error(err)

	replicas := mgr.GetByCollection(1)
	suite.Len(replicas, 0)
}

func (suite *ReplicaManagerSuite) TestGet() {
	mgr := suite.mgr

	for collectionID, collectionCfg := range suite.collections {
		replicas := mgr.GetByCollection(collectionID)
		replicaNodes := make(map[int64][]int64)
		nodes := make([]int64, 0)
		for _, replica := range replicas {
			suite.Equal(collectionID, replica.GetCollectionID())
			suite.Equal(replica, mgr.Get(replica.GetID()))
			suite.ElementsMatch(replica.GetNodes(), replica.GetNodes())
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
				replica := mgr.GetByCollectionAndNode(collectionID, node)
				suite.Equal(replicaID, replica.GetID())
			}
		}
	}
}

func (suite *ReplicaManagerSuite) TestRecover() {
	mgr := suite.mgr

	// Clear data in memory, and then recover from meta store
	suite.clearMemory()
	mgr.Recover(lo.Keys(suite.collections))
	suite.TestGet()

	// Test recover from 2.1 meta store
	replicaInfo := milvuspb.ReplicaInfo{
		ReplicaID:    2100,
		CollectionID: 1000,
		NodeIds:      []int64{1, 2, 3},
	}
	value, err := proto.Marshal(&replicaInfo)
	suite.NoError(err)
	suite.kv.Save(querycoord.ReplicaMetaPrefixV1+"/2100", string(value))

	suite.clearMemory()
	mgr.Recover(append(lo.Keys(suite.collections), 1000))
	replica := mgr.Get(2100)
	suite.NotNil(replica)
	suite.EqualValues(1000, replica.GetCollectionID())
	suite.EqualValues([]int64{1, 2, 3}, replica.GetNodes())
	suite.Len(replica.GetNodes(), len(replica.GetNodes()))
	for _, node := range replica.GetNodes() {
		suite.True(replica.Contains(node))
	}
}

func (suite *ReplicaManagerSuite) TestRemove() {
	mgr := suite.mgr

	for collection := range suite.collections {
		err := mgr.RemoveCollection(collection)
		suite.NoError(err)

		replicas := mgr.GetByCollection(collection)
		suite.Empty(replicas)
	}

	// Check whether the replicas are also removed from meta store
	mgr.Recover(lo.Keys(suite.collections))
	for collection := range suite.collections {
		replicas := mgr.GetByCollection(collection)
		suite.Empty(replicas)
	}
}

func (suite *ReplicaManagerSuite) TestNodeManipulate() {
	mgr := suite.mgr

	// add node into rg.
	rgs := map[string]typeutil.UniqueSet{
		"RG1": typeutil.NewUniqueSet(1, 7),
		"RG2": typeutil.NewUniqueSet(2, 3, 8),
		"RG3": typeutil.NewUniqueSet(4, 5, 6, 9),
	}

	// Add node into rg.
	for collectionID, cfg := range suite.collections {
		for rg := range cfg.spawnConfig {
			mgr.SetAvailableNodesInSameCollectionAndRG(collectionID, rg, rgs[rg])

			for _, node := range rgs[rg].Collect() {
				replica := mgr.GetByCollectionAndNode(collectionID, node)
				suite.Contains(replica.GetNodes(), node)
			}
		}
	}

	// Check these modifications are applied to meta store
	suite.clearMemory()
	mgr.Recover(lo.Keys(suite.collections))
	for collectionID, cfg := range suite.collections {
		for rg := range cfg.spawnConfig {
			for _, node := range rgs[rg].Collect() {
				replica := mgr.GetByCollectionAndNode(collectionID, node)
				suite.Contains(replica.GetNodes(), node)
			}
		}
	}
}

func (suite *ReplicaManagerSuite) spawnAll() {
	mgr := suite.mgr

	for id, cfg := range suite.collections {
		replicas, err := mgr.Spawn(id, cfg.spawnConfig)
		suite.NoError(err)
		totalSpawn := 0
		for rg, spawnNum := range cfg.spawnConfig {
			totalSpawn += spawnNum
			mgr.SetAvailableNodesInSameCollectionAndRG(id, rg, suite.rgs[rg])
		}
		suite.Len(replicas, totalSpawn)
	}
}

func (suite *ReplicaManagerSuite) TestResourceGroup() {
	mgr := NewReplicaManager(suite.idAllocator, suite.catalog)
	replicas1, err := mgr.Spawn(int64(1000), map[string]int{DefaultResourceGroupName: 1})
	suite.NoError(err)
	suite.NotNil(replicas1)
	suite.Len(replicas1, 1)

	replica2, err := mgr.Spawn(int64(2000), map[string]int{DefaultResourceGroupName: 1})
	suite.NoError(err)
	suite.NotNil(replica2)
	suite.Len(replica2, 1)

	replicas := mgr.GetByResourceGroup(DefaultResourceGroupName)
	suite.Len(replicas, 2)
	rgNames := mgr.GetResourceGroupByCollection(int64(1000))
	suite.Len(rgNames, 1)
	suite.True(rgNames.Contain(DefaultResourceGroupName))
}

func (suite *ReplicaManagerSuite) clearMemory() {
	suite.mgr.replicas = make(map[int64]*Replica)
}

func TestReplicaManager(t *testing.T) {
	suite.Run(t, new(ReplicaManagerSuite))
}

type ReplicaManagerWithResourceManagerSuite struct {
	suite.Suite
}
