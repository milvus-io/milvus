package meta

import (
	"sort"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
)

type CollectionManagerSuite struct {
	suite.Suite

	// Data
	collections    []int64
	partitions     map[int64][]int64 // CollectionID -> PartitionIDs
	loadTypes      []querypb.LoadType
	replicaNumber  []int32
	loadPercentage []int32

	// Mocks
	kv    kv.MetaKv
	store Store

	// Test object
	mgr *CollectionManager
}

func (suite *CollectionManagerSuite) SetupSuite() {
	Params.Init()

	suite.collections = []int64{100, 101, 102}
	suite.partitions = map[int64][]int64{
		100: {10},
		101: {11, 12},
		102: {13, 14, 15},
	}
	suite.loadTypes = []querypb.LoadType{
		querypb.LoadType_LoadCollection,
		querypb.LoadType_LoadPartition,
		querypb.LoadType_LoadCollection,
	}
	suite.replicaNumber = []int32{1, 2, 3}
	suite.loadPercentage = []int32{0, 50, 100}
}

func (suite *CollectionManagerSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)
	suite.store = NewMetaStore(suite.kv)

	suite.mgr = NewCollectionManager(suite.store)
	suite.loadAll()
}

func (suite *CollectionManagerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *CollectionManagerSuite) TestGetProperty() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		loadType := mgr.GetLoadType(collection)
		replicaNumber := mgr.GetReplicaNumber(collection)
		percentage := mgr.GetLoadPercentage(collection)
		exist := mgr.Exist(collection)
		suite.Equal(suite.loadTypes[i], loadType)
		suite.Equal(suite.replicaNumber[i], replicaNumber)
		suite.Equal(suite.loadPercentage[i], percentage)
		suite.True(exist)
	}

	invalidCollection := -1
	loadType := mgr.GetLoadType(int64(invalidCollection))
	replicaNumber := mgr.GetReplicaNumber(int64(invalidCollection))
	percentage := mgr.GetLoadPercentage(int64(invalidCollection))
	exist := mgr.Exist(int64(invalidCollection))
	suite.Equal(querypb.LoadType_UnKnownType, loadType)
	suite.EqualValues(-1, replicaNumber)
	suite.EqualValues(-1, percentage)
	suite.False(exist)
}

func (suite *CollectionManagerSuite) TestGet() {
	mgr := suite.mgr

	allCollections := mgr.GetAllCollections()
	allPartitions := mgr.GetAllPartitions()
	for i, collectionID := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			collection := mgr.GetCollection(collectionID)
			suite.Equal(collectionID, collection.GetCollectionID())
			suite.Contains(allCollections, collection)
		} else {
			partitions := mgr.GetPartitionsByCollection(collectionID)
			suite.Len(partitions, len(suite.partitions[collectionID]))

			for _, partitionID := range suite.partitions[collectionID] {
				partition := mgr.GetPartition(partitionID)
				suite.Equal(collectionID, partition.GetCollectionID())
				suite.Equal(partitionID, partition.GetPartitionID())
				suite.Contains(partitions, partition)
				suite.Contains(allPartitions, partition)
			}
		}
	}

	all := mgr.GetAll()
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	suite.Equal(suite.collections, all)
}

func (suite *CollectionManagerSuite) TestUpdate() {
	mgr := suite.mgr

	collections := mgr.GetAllCollections()
	partitions := mgr.GetAllPartitions()
	for _, collection := range collections {
		collection := collection.Clone()
		collection.LoadPercentage = 100
		ok := mgr.UpdateCollectionInMemory(collection)
		suite.True(ok)

		modified := mgr.GetCollection(collection.GetCollectionID())
		suite.Equal(collection, modified)
		suite.EqualValues(100, modified.LoadPercentage)

		collection.Status = querypb.LoadStatus_Loaded
		err := mgr.UpdateCollection(collection)
		suite.NoError(err)
	}
	for _, partition := range partitions {
		partition := partition.Clone()
		partition.LoadPercentage = 100
		ok := mgr.UpdatePartitionInMemory(partition)
		suite.True(ok)

		modified := mgr.GetPartition(partition.GetPartitionID())
		suite.Equal(partition, modified)
		suite.EqualValues(100, modified.LoadPercentage)

		partition.Status = querypb.LoadStatus_Loaded
		err := mgr.UpdatePartition(partition)
		suite.NoError(err)
	}

	suite.clearMemory()
	err := mgr.Recover()
	suite.NoError(err)
	collections = mgr.GetAllCollections()
	partitions = mgr.GetAllPartitions()
	for _, collection := range collections {
		suite.Equal(querypb.LoadStatus_Loaded, collection.GetStatus())
	}
	for _, partition := range partitions {
		suite.Equal(querypb.LoadStatus_Loaded, partition.GetStatus())
	}
}

func (suite *CollectionManagerSuite) TestRemove() {
	mgr := suite.mgr

	// Remove collections/partitions
	for i, collectionID := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			err := mgr.RemoveCollection(collectionID)
			suite.NoError(err)
		} else {
			err := mgr.RemovePartition(suite.partitions[collectionID]...)
			suite.NoError(err)
		}
	}

	// Try to get the removed items
	for i, collectionID := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			collection := mgr.GetCollection(collectionID)
			suite.Nil(collection)
		} else {
			partitions := mgr.GetPartitionsByCollection(collectionID)
			suite.Empty(partitions)
		}
	}

	// Make sure the removes applied to meta store
	err := mgr.Recover()
	suite.NoError(err)
	for i, collectionID := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			collection := mgr.GetCollection(collectionID)
			suite.Nil(collection)
		} else {
			partitions := mgr.GetPartitionsByCollection(collectionID)
			suite.Empty(partitions)
		}
	}

	// RemoveCollection also works for partitions
	suite.loadAll()
	for i, collectionID := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadPartition {
			err := mgr.RemoveCollection(collectionID)
			suite.NoError(err)
			partitions := mgr.GetPartitionsByCollection(collectionID)
			suite.Empty(partitions)
		}
	}
}

func (suite *CollectionManagerSuite) TestRecover() {
	mgr := suite.mgr

	suite.clearMemory()
	err := mgr.Recover()
	suite.NoError(err)
	for i, collection := range suite.collections {
		exist := suite.loadPercentage[i] == 100
		suite.Equal(exist, mgr.Exist(collection))
	}

	// Test recover from 2.1 meta store
	collectionInfo := querypb.CollectionInfo{
		CollectionID:  1000,
		PartitionIDs:  []int64{100, 101},
		LoadType:      querypb.LoadType_LoadCollection,
		ReplicaNumber: 3,
	}
	value, err := proto.Marshal(&collectionInfo)
	suite.NoError(err)
	err = suite.kv.Save(CollectionMetaPrefixV1+"/1000", string(value))
	suite.NoError(err)

	collectionInfo = querypb.CollectionInfo{
		CollectionID:  1001,
		PartitionIDs:  []int64{102, 103},
		LoadType:      querypb.LoadType_LoadPartition,
		ReplicaNumber: 1,
	}
	value, err = proto.Marshal(&collectionInfo)
	suite.NoError(err)
	err = suite.kv.Save(CollectionMetaPrefixV1+"/1001", string(value))
	suite.NoError(err)

	suite.clearMemory()
	err = mgr.Recover()
	suite.NoError(err)

	// Verify collection
	suite.True(mgr.Exist(1000))
	suite.Equal(querypb.LoadType_LoadCollection, mgr.GetLoadType(1000))
	suite.EqualValues(3, mgr.GetReplicaNumber(1000))

	// Verify partitions
	suite.True(mgr.Exist(1001))
	suite.Equal(querypb.LoadType_LoadPartition, mgr.GetLoadType(1001))
	suite.EqualValues(1, mgr.GetReplicaNumber(1001))
	suite.NotNil(mgr.GetPartition(102))
	suite.NotNil(mgr.GetPartition(103))
	suite.Len(mgr.getPartitionsByCollection(1001), 2)
}

func (suite *CollectionManagerSuite) loadAll() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		status := querypb.LoadStatus_Loaded
		if suite.loadPercentage[i] < 100 {
			status = querypb.LoadStatus_Loading
		}

		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			mgr.PutCollection(&Collection{
				CollectionLoadInfo: &querypb.CollectionLoadInfo{
					CollectionID:  collection,
					ReplicaNumber: suite.replicaNumber[i],
					Status:        status,
				},
				LoadPercentage: suite.loadPercentage[i],
				CreatedAt:      time.Now(),
			})
		} else {
			for _, partition := range suite.partitions[collection] {
				mgr.PutPartition(&Partition{
					PartitionLoadInfo: &querypb.PartitionLoadInfo{
						CollectionID:  collection,
						PartitionID:   partition,
						ReplicaNumber: suite.replicaNumber[i],
						Status:        status,
					},
					LoadPercentage: suite.loadPercentage[i],
					CreatedAt:      time.Now(),
				})
			}
		}
	}
}

func (suite *CollectionManagerSuite) clearMemory() {
	suite.mgr.collections = make(map[int64]*Collection)
	suite.mgr.partitions = make(map[int64]*Partition)
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}
