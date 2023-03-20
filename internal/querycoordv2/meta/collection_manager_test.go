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
	"sort"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

type CollectionManagerSuite struct {
	suite.Suite

	// Data
	collections    []int64
	partitions     map[int64][]int64 // CollectionID -> PartitionIDs
	loadTypes      []querypb.LoadType
	replicaNumber  []int32
	colLoadPercent []int32
	parLoadPercent map[int64][]int32

	// Mocks
	kv     kv.MetaKv
	store  Store
	broker *MockBroker

	// Test object
	mgr *CollectionManager
}

func (suite *CollectionManagerSuite) SetupSuite() {
	Params.Init()

	suite.collections = []int64{100, 101, 102, 103}
	suite.partitions = map[int64][]int64{
		100: {10},
		101: {11, 12},
		102: {13, 14, 15},
		103: {}, // not partition in this col
	}
	suite.loadTypes = []querypb.LoadType{
		querypb.LoadType_LoadCollection,
		querypb.LoadType_LoadPartition,
		querypb.LoadType_LoadCollection,
		querypb.LoadType_LoadCollection,
	}
	suite.replicaNumber = []int32{1, 2, 3, 1}
	suite.colLoadPercent = []int32{0, 50, 100, 100}
	suite.parLoadPercent = map[int64][]int32{
		100: {0},
		101: {0, 100},
		102: {100, 100, 100},
		103: {},
	}
}

func (suite *CollectionManagerSuite) SetupTest() {
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
	suite.broker = NewMockBroker(suite.T())

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
		percentage := mgr.GetCurrentLoadPercentage(collection)
		exist := mgr.Exist(collection)
		suite.Equal(suite.loadTypes[i], loadType)
		suite.Equal(suite.replicaNumber[i], replicaNumber)
		suite.Equal(suite.colLoadPercent[i], percentage)
		suite.True(exist)
	}

	invalidCollection := -1
	loadType := mgr.GetLoadType(int64(invalidCollection))
	replicaNumber := mgr.GetReplicaNumber(int64(invalidCollection))
	percentage := mgr.GetCurrentLoadPercentage(int64(invalidCollection))
	exist := mgr.Exist(int64(invalidCollection))
	suite.Equal(querypb.LoadType_UnKnownType, loadType)
	suite.EqualValues(-1, replicaNumber)
	suite.EqualValues(-1, percentage)
	suite.False(exist)
}

func (suite *CollectionManagerSuite) TestPut() {
	suite.releaseAll()
	// test put collection with partitions
	for i, collection := range suite.collections {
		status := querypb.LoadStatus_Loaded
		if suite.colLoadPercent[i] < 100 {
			status = querypb.LoadStatus_Loading
		}

		col := &Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[i],
				Status:        status,
				LoadType:      suite.loadTypes[i],
			},
			LoadPercentage: suite.colLoadPercent[i],
			CreatedAt:      time.Now(),
		}
		partitions := lo.Map(suite.partitions[collection], func(partition int64, j int) *Partition {
			return &Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID:  collection,
					PartitionID:   partition,
					ReplicaNumber: suite.replicaNumber[i],
					Status:        status,
				},
				LoadPercentage: suite.parLoadPercent[collection][j],
				CreatedAt:      time.Now(),
			}
		})
		err := suite.mgr.PutCollection(col, partitions...)
		suite.NoError(err)
	}
	suite.checkLoadResult()
}

func (suite *CollectionManagerSuite) TestGet() {
	suite.checkLoadResult()
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
	err := mgr.Recover(suite.broker)
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
	err := mgr.Recover(suite.broker)
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

	// remove collection would release its partitions also
	suite.releaseAll()
	suite.loadAll()
	for _, collectionID := range suite.collections {
		err := mgr.RemoveCollection(collectionID)
		suite.NoError(err)
		err = mgr.Recover(suite.broker)
		suite.NoError(err)
		collection := mgr.GetCollection(collectionID)
		suite.Nil(collection)
		partitions := mgr.GetPartitionsByCollection(collectionID)
		suite.Empty(partitions)
	}
}

func (suite *CollectionManagerSuite) TestRecover() {
	mgr := suite.mgr

	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	for i, collection := range suite.collections {
		exist := suite.colLoadPercent[i] == 100
		suite.Equal(exist, mgr.Exist(collection))
	}
}

func (suite *CollectionManagerSuite) TestUpgradeRecover() {
	suite.releaseAll()
	mgr := suite.mgr

	// put old version of collections and partitions
	for i, collection := range suite.collections {
		status := querypb.LoadStatus_Loaded
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			mgr.PutCollection(&Collection{
				CollectionLoadInfo: &querypb.CollectionLoadInfo{
					CollectionID:  collection,
					ReplicaNumber: suite.replicaNumber[i],
					Status:        status,
					LoadType:      querypb.LoadType_UnKnownType, // old version's collection didn't set loadType
				},
				LoadPercentage: suite.colLoadPercent[i],
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
					LoadPercentage: suite.colLoadPercent[i],
					CreatedAt:      time.Now(),
				})
			}
		}
	}

	// set expectations
	for i, collection := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)
		}
	}

	// do recovery
	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	suite.checkLoadResult()
}

func (suite *CollectionManagerSuite) loadAll() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		status := querypb.LoadStatus_Loaded
		if suite.colLoadPercent[i] < 100 {
			status = querypb.LoadStatus_Loading
		}

		mgr.PutCollection(&Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[i],
				Status:        status,
				LoadType:      suite.loadTypes[i],
			},
			LoadPercentage: suite.colLoadPercent[i],
			CreatedAt:      time.Now(),
		})

		for j, partition := range suite.partitions[collection] {
			mgr.PutPartition(&Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID: collection,
					PartitionID:  partition,
					Status:       status,
				},
				LoadPercentage: suite.parLoadPercent[collection][j],
				CreatedAt:      time.Now(),
			})
		}
	}
}

func (suite *CollectionManagerSuite) checkLoadResult() {
	mgr := suite.mgr

	allCollections := mgr.GetAllCollections()
	allPartitions := mgr.GetAllPartitions()
	for _, collectionID := range suite.collections {
		collection := mgr.GetCollection(collectionID)
		suite.Equal(collectionID, collection.GetCollectionID())
		suite.Contains(allCollections, collection)

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

	all := mgr.GetAll()
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	suite.Equal(suite.collections, all)
}

func (suite *CollectionManagerSuite) releaseAll() {
	for _, collection := range suite.collections {
		err := suite.mgr.RemoveCollection(collection)
		suite.NoError(err)
	}
}

func (suite *CollectionManagerSuite) clearMemory() {
	suite.mgr.collections = make(map[int64]*Collection)
	suite.mgr.partitions = make(map[int64]*Partition)
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}
