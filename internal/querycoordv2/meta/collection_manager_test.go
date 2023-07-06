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
	"fmt"
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
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	suite.colLoadPercent = []int32{0, 50, 100, -1}
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
		percentage := mgr.CalculateLoadPercentage(collection)
		exist := mgr.Exist(collection)
		suite.Equal(suite.loadTypes[i], loadType)
		suite.Equal(suite.replicaNumber[i], replicaNumber)
		suite.Equal(suite.colLoadPercent[i], percentage)
		suite.True(exist)
	}

	invalidCollection := -1
	loadType := mgr.GetLoadType(int64(invalidCollection))
	replicaNumber := mgr.GetReplicaNumber(int64(invalidCollection))
	percentage := mgr.CalculateLoadPercentage(int64(invalidCollection))
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

	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(nil, nil)
	for _, collection := range suite.collections {
		if len(suite.partitions[collection]) > 0 {
			suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)
		}
	}

	collections := mgr.GetAllCollections()
	partitions := mgr.GetAllPartitions()
	for _, collection := range collections {
		collection := collection.Clone()
		collection.LoadPercentage = 100
		err := mgr.PutCollectionWithoutSave(collection)
		suite.NoError(err)

		modified := mgr.GetCollection(collection.GetCollectionID())
		suite.Equal(collection, modified)
		suite.EqualValues(100, modified.LoadPercentage)

		collection.Status = querypb.LoadStatus_Loaded
		err = mgr.PutCollection(collection)
		suite.NoError(err)
	}
	for _, partition := range partitions {
		partition := partition.Clone()
		partition.LoadPercentage = 100
		err := mgr.PutPartitionWithoutSave(partition)
		suite.NoError(err)

		modified := mgr.GetPartition(partition.GetPartitionID())
		suite.Equal(partition, modified)
		suite.EqualValues(100, modified.LoadPercentage)

		partition.Status = querypb.LoadStatus_Loaded
		err = mgr.PutPartition(partition)
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

func (suite *CollectionManagerSuite) TestGetFieldIndex() {
	mgr := suite.mgr
	mgr.PutCollection(&Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  1,
			ReplicaNumber: 1,
			Status:        querypb.LoadStatus_Loading,
			LoadType:      querypb.LoadType_LoadCollection,
			FieldIndexID:  map[int64]int64{1: 1, 2: 2},
		},
		LoadPercentage: 0,
		CreatedAt:      time.Now(),
	})
	indexID := mgr.GetFieldIndex(1)
	suite.Len(indexID, 2)
	suite.Contains(indexID, int64(1))
	suite.Contains(indexID, int64(2))
}

func (suite *CollectionManagerSuite) TestRemove() {
	mgr := suite.mgr

	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(nil, nil)
	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
	}

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

func (suite *CollectionManagerSuite) TestRecover_normal() {
	mgr := suite.mgr

	// recover successfully
	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetCollectionSchema(mock.Anything, collection).Return(nil, nil)
		if len(suite.partitions[collection]) > 0 {
			suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)
		}
	}
	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	for i, collection := range suite.collections {
		exist := suite.colLoadPercent[i] == 100
		suite.Equal(exist, mgr.Exist(collection))
		if !exist {
			continue
		}
		for j, partitionID := range suite.partitions[collection] {
			partition := mgr.GetPartition(partitionID)
			exist = suite.parLoadPercent[collection][j] == 100
			suite.Equal(exist, partition != nil)
		}
	}
}

func (suite *CollectionManagerSuite) TestRecover_with_dropped() {
	mgr := suite.mgr

	droppedCollection := int64(101)
	droppedPartition := int64(13)

	for _, collection := range suite.collections {
		if collection == droppedCollection {
			suite.broker.EXPECT().GetCollectionSchema(mock.Anything, collection).Return(nil, merr.ErrCollectionNotFound)
		} else {
			suite.broker.EXPECT().GetCollectionSchema(mock.Anything, collection).Return(nil, nil)
		}
		if len(suite.partitions[collection]) != 0 {
			if collection == droppedCollection {
				suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(nil, merr.ErrCollectionNotFound)
			} else {
				suite.broker.EXPECT().GetPartitions(mock.Anything, collection).
					Return(lo.Filter(suite.partitions[collection], func(partition int64, _ int) bool {
						return partition != droppedPartition
					}), nil)
			}
		}
	}
	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	for i, collection := range suite.collections {
		exist := suite.colLoadPercent[i] == 100 && collection != droppedCollection
		suite.Equal(exist, mgr.Exist(collection))
		if !exist {
			continue
		}
		for j, partitionID := range suite.partitions[collection] {
			partition := mgr.GetPartition(partitionID)
			exist = suite.parLoadPercent[collection][j] == 100 && partitionID != droppedPartition
			suite.Equal(exist, partition != nil)
		}
	}
}

func (suite *CollectionManagerSuite) TestRecover_Failed() {
	mockErr1 := fmt.Errorf("mock GetCollectionSchema err")
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(nil, mockErr1)
	suite.clearMemory()
	err := suite.mgr.Recover(suite.broker)
	suite.Error(err)
	suite.ErrorIs(err, mockErr1)

	mockErr2 := fmt.Errorf("mock GetPartitions err")
	suite.broker.ExpectedCalls = suite.broker.ExpectedCalls[:0]
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(nil, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(nil, mockErr2)
	suite.clearMemory()
	err = suite.mgr.Recover(suite.broker)
	suite.Error(err)
	suite.ErrorIs(err, mockErr2)
}

func (suite *CollectionManagerSuite) TestUpdateLoadPercentage() {
	mgr := suite.mgr
	mgr.PutCollection(&Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  1,
			ReplicaNumber: 1,
			Status:        querypb.LoadStatus_Loading,
			LoadType:      querypb.LoadType_LoadCollection,
		},
		LoadPercentage: 0,
		CreatedAt:      time.Now(),
	})

	partitions := []int64{1, 2}
	for _, partition := range partitions {
		mgr.PutPartition(&Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: 1,
				PartitionID:  partition,
				Status:       querypb.LoadStatus_Loading,
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
		})
	}
	// test update partition load percentage
	mgr.UpdateLoadPercent(1, 30)
	partition := mgr.GetPartition(1)
	suite.Equal(int32(30), partition.LoadPercentage)
	suite.Equal(int32(30), mgr.GetPartitionLoadPercentage(partition.PartitionID))
	suite.Equal(querypb.LoadStatus_Loading, partition.Status)
	collection := mgr.GetCollection(1)
	suite.Equal(int32(15), collection.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loading, collection.Status)
	// expect nothing changed
	mgr.UpdateLoadPercent(1, 15)
	partition = mgr.GetPartition(1)
	suite.Equal(int32(30), partition.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loading, partition.Status)
	collection = mgr.GetCollection(1)
	suite.Equal(int32(15), collection.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loading, collection.Status)
	suite.Equal(querypb.LoadStatus_Loading, mgr.CalculateLoadStatus(collection.CollectionID))
	// test update partition load percentage to 100
	mgr.UpdateLoadPercent(1, 100)
	partition = mgr.GetPartition(1)
	suite.Equal(int32(100), partition.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loaded, partition.Status)
	collection = mgr.GetCollection(1)
	suite.Equal(int32(50), collection.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loading, collection.Status)
	// test update collection load percentage
	mgr.UpdateLoadPercent(2, 100)
	partition = mgr.GetPartition(1)
	suite.Equal(int32(100), partition.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loaded, partition.Status)
	collection = mgr.GetCollection(1)
	suite.Equal(int32(100), collection.LoadPercentage)
	suite.Equal(querypb.LoadStatus_Loaded, collection.Status)
	suite.Equal(querypb.LoadStatus_Loaded, mgr.CalculateLoadStatus(collection.CollectionID))
}

func (suite *CollectionManagerSuite) TestUpgradeRecover() {
	suite.releaseAll()
	mgr := suite.mgr

	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(nil, nil)
	for _, collection := range suite.collections {
		if len(suite.partitions[collection]) > 0 {
			suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)
		}
	}

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
