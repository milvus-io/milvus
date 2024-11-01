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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	kv      kv.MetaKv
	catalog metastore.QueryCoordCatalog
	broker  *MockBroker

	// Test object
	mgr *CollectionManager
}

func (suite *CollectionManagerSuite) SetupSuite() {
	paramtable.Init()

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
	suite.catalog = querycoord.NewCatalog(suite.kv)
	suite.broker = NewMockBroker(suite.T())

	suite.mgr = NewCollectionManager(suite.catalog)
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

	// Remove collections/partitions
	for i, collectionID := range suite.collections {
		if suite.loadTypes[i] == querypb.LoadType_LoadCollection {
			err := mgr.RemoveCollection(collectionID)
			suite.NoError(err)
		} else {
			err := mgr.RemovePartition(collectionID, suite.partitions[collectionID]...)
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

	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	for _, collection := range suite.collections {
		suite.True(mgr.Exist(collection))
		for _, partitionID := range suite.partitions[collection] {
			partition := mgr.GetPartition(partitionID)
			suite.NotNil(partition)
		}
	}
}

func (suite *CollectionManagerSuite) TestRecoverLoadingCollection() {
	mgr := suite.mgr
	suite.releaseAll()
	// test put collection with partitions
	for i, collection := range suite.collections {
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
		col := &Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[i],
				Status:        querypb.LoadStatus_Loading,
				LoadType:      suite.loadTypes[i],
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
		}
		partitions := lo.Map(suite.partitions[collection], func(partition int64, j int) *Partition {
			return &Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID:  collection,
					PartitionID:   partition,
					ReplicaNumber: suite.replicaNumber[i],
					Status:        querypb.LoadStatus_Loading,
				},
				LoadPercentage: 0,
				CreatedAt:      time.Now(),
			}
		})
		err := suite.mgr.PutCollection(col, partitions...)
		suite.NoError(err)
	}

	// recover for first time, expected recover success
	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	for _, collectionID := range suite.collections {
		collection := mgr.GetCollection(collectionID)
		suite.NotNil(collection)
		suite.Equal(int32(1), collection.GetRecoverTimes())
		for _, partitionID := range suite.partitions[collectionID] {
			partition := mgr.GetPartition(partitionID)
			suite.NotNil(partition)
			suite.Equal(int32(1), partition.GetRecoverTimes())
		}
	}

	// update load percent, then recover for second time
	for _, collectionID := range suite.collections {
		for _, partitionID := range suite.partitions[collectionID] {
			mgr.UpdateLoadPercent(partitionID, 10)
		}
	}
	suite.clearMemory()
	err = mgr.Recover(suite.broker)
	suite.NoError(err)
	for _, collectionID := range suite.collections {
		collection := mgr.GetCollection(collectionID)
		suite.NotNil(collection)
		suite.Equal(int32(2), collection.GetRecoverTimes())
		for _, partitionID := range suite.partitions[collectionID] {
			partition := mgr.GetPartition(partitionID)
			suite.NotNil(partition)
			suite.Equal(int32(2), partition.GetRecoverTimes())
		}
	}

	// test recover loading collection reach limit
	for i := 0; i < int(paramtable.Get().QueryCoordCfg.CollectionRecoverTimesLimit.GetAsInt32()); i++ {
		log.Info("stupid", zap.Int("count", i))
		suite.clearMemory()
		err = mgr.Recover(suite.broker)
		suite.NoError(err)
	}
	for _, collectionID := range suite.collections {
		collection := mgr.GetCollection(collectionID)
		suite.Nil(collection)
		for _, partitionID := range suite.partitions[collectionID] {
			partition := mgr.GetPartition(partitionID)
			suite.Nil(partition)
		}
	}
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
		suite.broker.EXPECT().DescribeCollection(mock.Anything, collection).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: common.RowIDField},
					{FieldID: common.TimeStampField},
					{FieldID: 100, Name: "pk"},
					{FieldID: 101, Name: "vector"},
				},
			},
		}, nil).Maybe()
	}

	// do recovery
	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	suite.checkLoadResult()

	for i, collection := range suite.collections {
		newColl := mgr.GetCollection(collection)
		suite.Equal(suite.loadTypes[i], newColl.GetLoadType())
	}
}

func (suite *CollectionManagerSuite) TestUpgradeLoadFields() {
	suite.releaseAll()
	mgr := suite.mgr

	// put old version of collections and partitions
	for i, collection := range suite.collections {
		mgr.PutCollection(&Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[i],
				Status:        querypb.LoadStatus_Loaded,
				LoadType:      suite.loadTypes[i],
				LoadFields:    nil, // use nil Load fields, mocking old load info
			},
			LoadPercentage: 100,
			CreatedAt:      time.Now(),
		})
		for j, partition := range suite.partitions[collection] {
			mgr.PutPartition(&Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID: collection,
					PartitionID:  partition,
					Status:       querypb.LoadStatus_Loaded,
				},
				LoadPercentage: suite.parLoadPercent[collection][j],
				CreatedAt:      time.Now(),
			})
		}
	}

	// set expectations
	for _, collection := range suite.collections {
		suite.broker.EXPECT().DescribeCollection(mock.Anything, collection).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: common.RowIDField},
					{FieldID: common.TimeStampField},
					{FieldID: 100, Name: "pk"},
					{FieldID: 101, Name: "vector"},
				},
			},
		}, nil)
	}

	// do recovery
	suite.clearMemory()
	err := mgr.Recover(suite.broker)
	suite.NoError(err)
	suite.checkLoadResult()

	for _, collection := range suite.collections {
		newColl := mgr.GetCollection(collection)
		suite.ElementsMatch([]int64{100, 101}, newColl.GetLoadFields())
	}
}

func (suite *CollectionManagerSuite) TestUpgradeLoadFieldsFail() {
	suite.Run("normal_error", func() {
		suite.releaseAll()
		mgr := suite.mgr

		mgr.PutCollection(&Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  100,
				ReplicaNumber: 1,
				Status:        querypb.LoadStatus_Loaded,
				LoadType:      querypb.LoadType_LoadCollection,
				LoadFields:    nil, // use nil Load fields, mocking old load info
			},
			LoadPercentage: 100,
			CreatedAt:      time.Now(),
		})
		mgr.PutPartition(&Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: 100,
				PartitionID:  1000,
				Status:       querypb.LoadStatus_Loaded,
			},
			LoadPercentage: 100,
			CreatedAt:      time.Now(),
		})

		suite.broker.EXPECT().DescribeCollection(mock.Anything, int64(100)).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()
		// do recovery
		suite.clearMemory()
		err := mgr.Recover(suite.broker)
		suite.Error(err)
	})

	suite.Run("normal_error", func() {
		suite.releaseAll()
		mgr := suite.mgr

		mgr.PutCollection(&Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  100,
				ReplicaNumber: 1,
				Status:        querypb.LoadStatus_Loaded,
				LoadType:      querypb.LoadType_LoadCollection,
				LoadFields:    nil, // use nil Load fields, mocking old load info
			},
			LoadPercentage: 100,
			CreatedAt:      time.Now(),
		})
		mgr.PutPartition(&Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: 100,
				PartitionID:  1000,
				Status:       querypb.LoadStatus_Loaded,
			},
			LoadPercentage: 100,
			CreatedAt:      time.Now(),
		})

		suite.broker.EXPECT().DescribeCollection(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Status(merr.WrapErrCollectionNotFound(100)),
		}, nil).Once()
		// do recovery
		suite.clearMemory()
		err := mgr.Recover(suite.broker)
		suite.NoError(err)
	})
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
				LoadFields:    []int64{100, 101},
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
