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
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

type ReplicaManagerSuite struct {
	suite.Suite

	nodes          []int64
	collections    []int64
	replicaNumbers []int32
	idAllocator    func() (int64, error)
	kv             kv.MetaKv
	store          Store
	mgr            *ReplicaManager
}

func (suite *ReplicaManagerSuite) SetupSuite() {
	Params.Init()

	suite.nodes = []int64{1, 2, 3}
	suite.collections = []int64{100, 101, 102}
	suite.replicaNumbers = []int32{1, 2, 3}
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
	suite.store = NewMetaStore(suite.kv)

	suite.idAllocator = RandomIncrementIDAllocator()
	suite.mgr = NewReplicaManager(suite.idAllocator, suite.store)
	suite.spawnAndPutAll()
}

func (suite *ReplicaManagerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ReplicaManagerSuite) TestSpawn() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		replicas, err := mgr.Spawn(collection, suite.replicaNumbers[i], DefaultResourceGroupName)
		suite.NoError(err)
		suite.Len(replicas, int(suite.replicaNumbers[i]))
	}

	mgr.idAllocator = ErrorIDAllocator()
	for i, collection := range suite.collections {
		_, err := mgr.Spawn(collection, suite.replicaNumbers[i], DefaultResourceGroupName)
		suite.Error(err)
	}
}

func (suite *ReplicaManagerSuite) TestGet() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		replicas := mgr.GetByCollection(collection)
		replicaNodes := make(map[int64][]int64)
		nodes := make([]int64, 0)
		for _, replica := range replicas {
			suite.Equal(collection, replica.GetCollectionID())
			suite.Equal(replica, mgr.Get(replica.GetID()))
			suite.Equal(replica.Replica.GetNodes(), replica.GetNodes())
			replicaNodes[replica.GetID()] = replica.Replica.GetNodes()
			nodes = append(nodes, replica.Replica.Nodes...)
		}
		suite.Len(nodes, int(suite.replicaNumbers[i]))

		for replicaID, nodes := range replicaNodes {
			for _, node := range nodes {
				replica := mgr.GetByCollectionAndNode(collection, node)
				suite.Equal(replicaID, replica.GetID())
			}
		}
	}
}

func (suite *ReplicaManagerSuite) TestRecover() {
	mgr := suite.mgr

	// Clear data in memory, and then recover from meta store
	suite.clearMemory()
	mgr.Recover(suite.collections)
	suite.TestGet()

	// Test recover from 2.1 meta store
	replicaInfo := milvuspb.ReplicaInfo{
		ReplicaID:    2100,
		CollectionID: 1000,
		NodeIds:      []int64{1, 2, 3},
	}
	value, err := proto.Marshal(&replicaInfo)
	suite.NoError(err)
	suite.kv.Save(ReplicaMetaPrefixV1+"/2100", string(value))

	suite.clearMemory()
	mgr.Recover(append(suite.collections, 1000))
	replica := mgr.Get(2100)
	suite.NotNil(replica)
	suite.EqualValues(1000, replica.CollectionID)
	suite.EqualValues([]int64{1, 2, 3}, replica.Replica.Nodes)
	suite.Len(replica.GetNodes(), len(replica.Replica.GetNodes()))
	for _, node := range replica.Replica.GetNodes() {
		suite.True(replica.Contains(node))
	}
}

func (suite *ReplicaManagerSuite) TestRemove() {
	mgr := suite.mgr

	for _, collection := range suite.collections {
		err := mgr.RemoveCollection(collection)
		suite.NoError(err)

		replicas := mgr.GetByCollection(collection)
		suite.Empty(replicas)
	}

	// Check whether the replicas are also removed from meta store
	mgr.Recover(suite.collections)
	for _, collection := range suite.collections {
		replicas := mgr.GetByCollection(collection)
		suite.Empty(replicas)
	}
}

func (suite *ReplicaManagerSuite) TestNodeManipulate() {
	mgr := suite.mgr

	firstNode := suite.nodes[0]
	newNode := suite.nodes[len(suite.nodes)-1] + 1
	// Add a new node for the replica with node 1 of all collections,
	// then remove the node 1
	for _, collection := range suite.collections {
		replica := mgr.GetByCollectionAndNode(collection, firstNode)
		err := mgr.AddNode(replica.GetID(), newNode)
		suite.NoError(err)

		replica = mgr.GetByCollectionAndNode(collection, newNode)
		suite.Contains(replica.GetNodes(), newNode)
		suite.Contains(replica.Replica.GetNodes(), newNode)

		err = mgr.RemoveNode(replica.GetID(), firstNode)
		suite.NoError(err)
		replica = mgr.GetByCollectionAndNode(collection, firstNode)
		suite.Nil(replica)
	}

	// Check these modifications are applied to meta store
	suite.clearMemory()
	mgr.Recover(suite.collections)
	for _, collection := range suite.collections {
		replica := mgr.GetByCollectionAndNode(collection, firstNode)
		suite.Nil(replica)

		replica = mgr.GetByCollectionAndNode(collection, newNode)
		suite.Contains(replica.GetNodes(), newNode)
		suite.Contains(replica.Replica.GetNodes(), newNode)
	}
}

func (suite *ReplicaManagerSuite) spawnAndPutAll() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		replicas, err := mgr.Spawn(collection, suite.replicaNumbers[i], DefaultResourceGroupName)
		suite.NoError(err)
		suite.Len(replicas, int(suite.replicaNumbers[i]))
		for j, replica := range replicas {
			replica.AddNode(suite.nodes[j])
		}
		err = mgr.Put(replicas...)
		suite.NoError(err)
	}
}

func (suite *ReplicaManagerSuite) TestResourceGroup() {
	mgr := NewReplicaManager(suite.idAllocator, suite.store)
	replica1, err := mgr.spawn(int64(1000), DefaultResourceGroupName)
	replica1.AddNode(1)
	suite.NoError(err)
	mgr.Put(replica1)

	replica2, err := mgr.spawn(int64(2000), DefaultResourceGroupName)
	replica2.AddNode(1)
	suite.NoError(err)
	mgr.Put(replica2)

	replicas := mgr.GetByResourceGroup(DefaultResourceGroupName)
	suite.Len(replicas, 2)
	replicas = mgr.GetByCollectionAndRG(int64(1000), DefaultResourceGroupName)
	suite.Len(replicas, 1)
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
