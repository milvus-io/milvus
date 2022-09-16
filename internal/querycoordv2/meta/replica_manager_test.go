package meta

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
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
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)
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
		replicas, err := mgr.Spawn(collection, suite.replicaNumbers[i])
		suite.NoError(err)
		suite.Len(replicas, int(suite.replicaNumbers[i]))
	}

	mgr.idAllocator = ErrorIDAllocator()
	for i, collection := range suite.collections {
		_, err := mgr.Spawn(collection, suite.replicaNumbers[i])
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
			suite.Equal(replica.Replica.Nodes, replica.Nodes.Collect())
			replicaNodes[replica.GetID()] = replica.Replica.Nodes
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
	mgr.Recover()
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
	mgr.Recover()
	replica := mgr.Get(2100)
	suite.NotNil(replica)
	suite.EqualValues(1000, replica.CollectionID)
	suite.EqualValues([]int64{1, 2, 3}, replica.Replica.Nodes)
	suite.Len(replica.Nodes, len(replica.Replica.GetNodes()))
	for _, node := range replica.Replica.GetNodes() {
		suite.True(replica.Nodes.Contain(node))
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
	mgr.Recover()
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
		suite.Contains(replica.Nodes, newNode)
		suite.Contains(replica.Replica.GetNodes(), newNode)

		err = mgr.RemoveNode(replica.GetID(), firstNode)
		suite.NoError(err)
		replica = mgr.GetByCollectionAndNode(collection, firstNode)
		suite.Nil(replica)
	}

	// Check these modifications are applied to meta store
	suite.clearMemory()
	mgr.Recover()
	for _, collection := range suite.collections {
		replica := mgr.GetByCollectionAndNode(collection, firstNode)
		suite.Nil(replica)

		replica = mgr.GetByCollectionAndNode(collection, newNode)
		suite.Contains(replica.Nodes, newNode)
		suite.Contains(replica.Replica.GetNodes(), newNode)
	}
}

func (suite *ReplicaManagerSuite) spawnAndPutAll() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		replicas, err := mgr.Spawn(collection, suite.replicaNumbers[i])
		suite.NoError(err)
		suite.Len(replicas, int(suite.replicaNumbers[i]))
		for j, replica := range replicas {
			replica.AddNode(suite.nodes[j])
		}
		err = mgr.Put(replicas...)
		suite.NoError(err)
	}
}

func (suite *ReplicaManagerSuite) clearMemory() {
	suite.mgr.replicas = make(map[int64]*Replica)
}

func TestReplicaManager(t *testing.T) {
	suite.Run(t, new(ReplicaManagerSuite))
}
