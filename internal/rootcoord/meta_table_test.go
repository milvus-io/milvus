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

package rootcoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockTestKV struct {
	kv.TxnKV

	loadWithPrefix               func(key string, ts typeutil.Timestamp) ([]string, []string, error)
	save                         func(key, value string, ts typeutil.Timestamp) error
	multiSave                    func(kvs map[string]string, ts typeutil.Timestamp) error
	multiSaveAndRemoveWithPrefix func(saves map[string]string, removals []string, ts typeutil.Timestamp) error
}

func (m *mockTestKV) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	return m.loadWithPrefix(key, ts)
}
func (m *mockTestKV) Load(key string, ts typeutil.Timestamp) (string, error) {
	return "", nil
}

func (m *mockTestKV) Save(key, value string, ts typeutil.Timestamp) error {
	return m.save(key, value, ts)
}

func (m *mockTestKV) MultiSave(kvs map[string]string, ts typeutil.Timestamp) error {
	return m.multiSave(kvs, ts)
}

func (m *mockTestKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	return m.multiSaveAndRemoveWithPrefix(saves, removals, ts)
}

type mockTestTxnKV struct {
	kv.TxnKV
	loadWithPrefix               func(key string) ([]string, []string, error)
	save                         func(key, value string) error
	multiSave                    func(kvs map[string]string) error
	multiSaveAndRemoveWithPrefix func(saves map[string]string, removals []string) error
	remove                       func(key string) error
	multiRemove                  func(keys []string) error
}

func (m *mockTestTxnKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return m.loadWithPrefix(key)
}

func (m *mockTestTxnKV) Save(key, value string) error {
	return m.save(key, value)
}

func (m *mockTestTxnKV) MultiSave(kvs map[string]string) error {
	return m.multiSave(kvs)
}

func (m *mockTestTxnKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	return m.multiSaveAndRemoveWithPrefix(saves, removals)
}

func (m *mockTestTxnKV) Remove(key string) error {
	return m.remove(key)
}

func (m *mockTestTxnKV) MultiRemove(keys []string) error {
	return m.multiRemove(keys)
}

func Test_MockKV(t *testing.T) {
	Params.Init()
	k1 := &mockTestKV{}
	kt := &mockTestTxnKV{}
	prefix := make(map[string][]string)
	k1.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
		if val, ok := prefix[key]; ok {
			return nil, val, nil
		}
		return nil, nil, fmt.Errorf("load prefix error")
	}
	kt.loadWithPrefix = func(key string) ([]string, []string, error) {
		if val, ok := prefix[key]; ok {
			return nil, val, nil
		}
		return nil, nil, fmt.Errorf("load prefix error")
	}

	_, err := NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "load prefix error")

	// collection
	prefix[kvmetestore.CollectionMetaPrefix] = []string{"collection-meta"}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)

	value, err := proto.Marshal(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{}})
	assert.Nil(t, err)
	prefix[kvmetestore.CollectionMetaPrefix] = []string{string(value)}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)

	// segment index
	prefix[kvmetestore.SegmentIndexMetaPrefix] = []string{"segment-index-meta"}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)

	value, err = proto.Marshal(&pb.SegmentIndexInfo{})
	assert.Nil(t, err)
	prefix[kvmetestore.SegmentIndexMetaPrefix] = []string{string(value)}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)

	prefix[kvmetestore.SegmentIndexMetaPrefix] = []string{string(value), string(value)}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "load prefix error")

	// index
	prefix[kvmetestore.IndexMetaPrefix] = []string{"index-meta"}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)

	value, err = proto.Marshal(&pb.IndexInfo{})
	assert.Nil(t, err)
	prefix[kvmetestore.IndexMetaPrefix] = []string{string(value)}
	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: kt, Snapshot: k1})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "load prefix error")
}

func TestMetaTable(t *testing.T) {
	const (
		collName        = "testColl"
		collNameInvalid = "testColl_invalid"
		aliasName1      = "alias1"
		aliasName2      = "alias2"
		collID          = typeutil.UniqueID(1)
		collIDInvalid   = typeutil.UniqueID(2)
		partIDDefault   = typeutil.UniqueID(10)
		partID          = typeutil.UniqueID(20)
		partName        = "testPart"
		partIDInvalid   = typeutil.UniqueID(21)
		segID           = typeutil.UniqueID(100)
		segID2          = typeutil.UniqueID(101)
		fieldID         = typeutil.UniqueID(110)
		fieldID2        = typeutil.UniqueID(111)
		indexID         = typeutil.UniqueID(10000)
		indexID2        = typeutil.UniqueID(10001)
		buildID         = typeutil.UniqueID(201)
		indexName       = "testColl_index_110"
	)

	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	require.Nil(t, err)
	defer etcdCli.Close()

	skv, err := kvmetestore.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mt, err := NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: txnKV, Snapshot: skv})
	assert.Nil(t, err)

	collInfo := &model.Collection{
		CollectionID: collID,
		Name:         collName,
		AutoID:       false,
		Fields: []*model.Field{
			{
				FieldID:      fieldID,
				Name:         "field110",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-k1",
						Value: "field110-v1",
					},
					{
						Key:   "field110-k2",
						Value: "field110-v2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-i1",
						Value: "field110-v1",
					},
					{
						Key:   "field110-i2",
						Value: "field110-v2",
					},
				},
			},
		},
		FieldIDToIndexID: []common.Int64Tuple{
			{
				Key:   fieldID,
				Value: indexID,
			},
		},
		CreateTime: 0,
		Partitions: []*model.Partition{
			{
				PartitionID:               partIDDefault,
				PartitionName:             Params.CommonCfg.DefaultPartitionName,
				PartitionCreatedTimestamp: 0,
			},
		},
		VirtualChannelNames: []string{
			fmt.Sprintf("dmChannel_%dv%d", collID, 0),
			fmt.Sprintf("dmChannel_%dv%d", collID, 1),
		},
		PhysicalChannelNames: []string{
			funcutil.ToPhysicalChannel(fmt.Sprintf("dmChannel_%dv%d", collID, 0)),
			funcutil.ToPhysicalChannel(fmt.Sprintf("dmChannel_%dv%d", collID, 1)),
		},
	}

	idxInfo := []*model.Index{
		{
			IndexName: indexName,
			IndexID:   indexID,
			FieldID:   fieldID,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "field110-i1",
					Value: "field110-v1",
				},
				{
					Key:   "field110-i2",
					Value: "field110-v2",
				},
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("add collection", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()

		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), ts)

		collMeta, err := mt.GetCollectionByName(collName, ts)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.CreateTime, ts)
		assert.Equal(t, collMeta.Partitions[0].PartitionCreatedTimestamp, ts)

		assert.Equal(t, partIDDefault, collMeta.Partitions[0].PartitionID)
		assert.Equal(t, 1, len(collMeta.Partitions))
		assert.True(t, mt.HasCollection(collInfo.CollectionID, 0))

		field, err := mt.GetFieldSchema(collName, "field110")
		assert.Nil(t, err)
		assert.Equal(t, collInfo.Fields[0].FieldID, field.FieldID)
	})

	wg.Add(1)
	t.Run("add alias", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		exists := mt.IsAlias(aliasName1)
		assert.False(t, exists)
		err = mt.AddAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		aliases := mt.ListAliases(collID)
		assert.Equal(t, aliases, []string{aliasName1})
		exists = mt.IsAlias(aliasName1)
		assert.True(t, exists)
	})
	wg.Add(1)
	t.Run("alter alias", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.AlterAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		err = mt.AlterAlias(aliasName1, collNameInvalid, ts)
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("delete alias", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.DropAlias(aliasName1, ts)
		assert.Nil(t, err)
	})

	wg.Add(1)
	t.Run("not load alias when load collection meta", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.AddAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		err = mt.reloadFromCatalog()
		assert.Nil(t, err)
		_, ok := mt.collName2ID[aliasName1]
		assert.False(t, ok)
	})

	wg.Add(1)
	t.Run("add partition", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.AddPartition(collID, partName, partID, ts, "")
		assert.Nil(t, err)
		//assert.Equal(t, ts, uint64(2))

		collMeta, ok := mt.collID2Meta[collID]
		assert.True(t, ok)
		assert.Equal(t, 2, len(collMeta.Partitions))
		assert.Equal(t, collMeta.Partitions[1].PartitionName, partName)
		assert.Equal(t, ts, collMeta.Partitions[1].PartitionCreatedTimestamp)
	})

	wg.Add(1)
	t.Run("add segment index", func(t *testing.T) {
		defer wg.Done()
		alreadyExists, err := mt.AddIndex(collName, "field110", idxInfo[0], []typeutil.UniqueID{segID})
		assert.Nil(t, err)
		assert.False(t, alreadyExists)

		index := model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      indexID,
			SegmentIndexes: map[int64]model.SegmentIndex{
				segID: {
					Segment: model.Segment{
						SegmentID:   segID,
						PartitionID: partID,
					},
					BuildID:     buildID,
					EnableIndex: true,
				},
			},
		}
		err = mt.AlterIndex(&index)
		assert.Nil(t, err)

		// it's legal to add index twice
		err = mt.AlterIndex(&index)
		assert.Nil(t, err)

		idxID, ok := mt.segID2IndexID[segID]
		assert.True(t, ok)
		indexMeta, ok := mt.indexID2Meta[idxID]
		assert.True(t, ok)
		segIdx, ok := indexMeta.SegmentIndexes[segID]
		assert.True(t, ok)
		assert.True(t, segIdx.EnableIndex)
	})

	wg.Add(1)
	t.Run("add diff index with same index name", func(t *testing.T) {
		defer wg.Done()
		params := []*commonpb.KeyValuePair{
			{
				Key:   "field110-k1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-k2",
				Value: "field110-v2",
			},
		}
		idxInfo := &model.Index{
			IndexName:   "testColl_index_110",
			IndexID:     indexID,
			IndexParams: params,
		}

		alreadyExists, err := mt.AddIndex("collTest", "field110", idxInfo, []typeutil.UniqueID{segID})
		assert.NotNil(t, err)
		assert.False(t, alreadyExists)
	})

	wg.Add(1)
	t.Run("get not indexed segments", func(t *testing.T) {
		defer wg.Done()
		params := []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-i2",
				Value: "field110-v2",
			},
		}

		tparams := []*commonpb.KeyValuePair{
			{
				Key:   "field110-k1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-k2",
				Value: "field110-v2",
			},
		}
		idxInfo := &model.Index{
			IndexName:   "field110",
			IndexID:     2000,
			IndexParams: params,
		}

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo, nil)
		assert.NotNil(t, err)
		seg, field, err := mt.GetNotIndexedSegments(collName, "field110", idxInfo, []typeutil.UniqueID{segID, segID2})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(seg))
		assert.Equal(t, segID2, seg[0])
		assert.True(t, EqualKeyPairArray(field.TypeParams, tparams))

		params = []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
		}
		idxInfo.IndexParams = params
		idxInfo.IndexID = 2001
		idxInfo.IndexName = "field110-1"

		seg, field, err = mt.GetNotIndexedSegments(collName, "field110", idxInfo, []typeutil.UniqueID{segID, segID2})
		assert.Nil(t, err)
		assert.Equal(t, 2, len(seg))
		assert.Equal(t, segID, seg[0])
		assert.Equal(t, segID2, seg[1])
		assert.True(t, EqualKeyPairArray(field.TypeParams, tparams))
	})

	wg.Add(1)
	t.Run("get index by name", func(t *testing.T) {
		defer wg.Done()
		_, idx, err := mt.GetIndexByName(collName, indexName)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(idx))
		assert.Equal(t, idxInfo[0].IndexID, idx[0].IndexID)
		params := []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-i2",
				Value: "field110-v2",
			},
		}
		assert.True(t, EqualKeyPairArray(idx[0].IndexParams, params))

		_, idx, err = mt.GetIndexByName(collName, "idx201")
		assert.Nil(t, err)
		assert.Zero(t, len(idx))
	})

	wg.Add(1)
	t.Run("drop partition", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		id, err := mt.DeletePartition(collID, partName, ts, "")
		assert.Nil(t, err)
		assert.Equal(t, partID, id)
	})

	wg.Add(1)
	t.Run("drop collection", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.DeleteCollection(collIDInvalid, ts, "")
		assert.NotNil(t, err)
		ts2 := ftso()
		err = mt.AddAlias(aliasName2, collName, ts2)
		assert.Nil(t, err)
		err = mt.DeleteCollection(collID, ts, "")
		assert.Nil(t, err)
		ts3 := ftso()
		err = mt.DropAlias(aliasName2, ts3)
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("delete credential", func(t *testing.T) {
		defer wg.Done()

		err = mt.DeleteCredential("")
		assert.Nil(t, err)

		err = mt.DeleteCredential("abcxyz")
		assert.Nil(t, err)
	})

	/////////////////////////// these tests should run at last, it only used to hit the error lines ////////////////////////
	txnkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mockKV := &mockTestKV{
		loadWithPrefix: func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		},
	}
	mockTxnKV := &mockTestTxnKV{
		TxnKV:          txnkv,
		loadWithPrefix: func(key string) ([]string, []string, error) { return txnkv.LoadWithPrefix(key) },
		save:           func(key, value string) error { return txnkv.Save(key, value) },
		multiSave:      func(kvs map[string]string) error { return txnkv.MultiSave(kvs) },
		multiSaveAndRemoveWithPrefix: func(kvs map[string]string, removal []string) error {
			return txnkv.MultiSaveAndRemoveWithPrefix(kvs, removal)
		},
		remove: func(key string) error { return txnkv.Remove(key) },
	}

	mt, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: mockTxnKV, Snapshot: mockKV})
	assert.Nil(t, err)

	wg.Add(1)
	t.Run("add collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save error")
		}
		collInfo.Partitions = []*model.Partition{}
		assert.Error(t, mt.AddCollection(collInfo, 0, ""))
	})

	wg.Add(1)
	t.Run("delete collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.multiSaveAndRemoveWithPrefix = func(save map[string]string, keys []string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}
		ts := ftso()
		assert.Error(t, mt.DeleteCollection(collInfo.CollectionID, ts, ""))
	})

	wg.Add(1)
	t.Run("get collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}

		ts := ftso()
		collInfo.Partitions = []*model.Partition{}
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]model.Collection)
		_, err = mt.GetCollectionByName(collInfo.Name, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection %s with id %d", collInfo.Name, collInfo.CollectionID))

	})

	wg.Add(1)
	t.Run("add partition failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		ts := ftso()
		collInfo.Partitions = []*model.Partition{}
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		ts = ftso()
		err = mt.AddPartition(2, "no-part", 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "can't find collection. id = 2")

		coll := mt.collID2Meta[collInfo.CollectionID]
		coll.Partitions = make([]*model.Partition, Params.RootCoordCfg.MaxPartitionNum)
		mt.collID2Meta[coll.CollectionID] = coll
		err = mt.AddPartition(coll.CollectionID, "no-part", 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("maximum partition's number should be limit to %d", Params.RootCoordCfg.MaxPartitionNum))

		coll.Partitions = []*model.Partition{{PartitionID: partID, PartitionName: partName, PartitionCreatedTimestamp: ftso()}}
		mt.collID2Meta[coll.CollectionID] = coll

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save error")
		}
		assert.Error(t, mt.AddPartition(coll.CollectionID, "no-part", 22, ts, ""))
		//err = mt.AddPartition(coll.CollectionID, "no-part", 22, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}

		collInfo.Partitions = []*model.Partition{}
		ts = ftso()
		err = mt.AddPartition(coll.CollectionID, partName, 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition name = %s already exists", partName))
		err = mt.AddPartition(coll.CollectionID, "no-part", partID, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition id = %d already exists", partID))
	})

	wg.Add(1)
	t.Run("has partition failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		assert.False(t, mt.HasPartition(collInfo.CollectionID, "no-partName", 0))

		mt.collID2Meta = make(map[int64]model.Collection)
		assert.False(t, mt.HasPartition(collInfo.CollectionID, partName, 0))
	})

	wg.Add(1)
	t.Run("delete partition failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{{PartitionID: partID, PartitionName: partName, PartitionCreatedTimestamp: ftso()}}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		ts = ftso()
		_, err = mt.DeletePartition(collInfo.CollectionID, Params.CommonCfg.DefaultPartitionName, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "default partition cannot be deleted")

		_, err = mt.DeletePartition(collInfo.CollectionID, "abc", ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "partition abc does not exist")

		mockKV.save = func(key, value string, ts typeutil.Timestamp) error { return errors.New("mocked error") }
		_, err = mt.DeletePartition(collInfo.CollectionID, partName, ts, "")
		assert.Error(t, err)

		mt.collID2Meta = make(map[int64]model.Collection)
		_, err = mt.DeletePartition(collInfo.CollectionID, "abc", ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection id = %d", collInfo.CollectionID))
	})

	wg.Add(1)
	t.Run("add index failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.save = func(key, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		index := &model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      indexID,
			SegmentIndexes: map[int64]model.SegmentIndex{
				segID: {
					Segment: model.Segment{
						SegmentID:   segID,
						PartitionID: partID,
					},
					BuildID:     buildID,
					CreateTime:  10,
					EnableIndex: false,
				},
			},
		}
		err = mt.AlterIndex(index)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", index.IndexID))

		mt.collID2Meta = make(map[int64]model.Collection)
		err = mt.AlterIndex(index)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection id = %d not found", collInfo.CollectionID))

		err = mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts = ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		newIdx := *index
		newIdx.SegmentIndexes = map[int64]model.SegmentIndex{
			segID: {
				Segment: model.Segment{
					SegmentID:   segID,
					PartitionID: partID,
				},
				BuildID:     buildID,
				CreateTime:  10,
				EnableIndex: true,
			},
		}

		mt.indexID2Meta[indexID] = index
		mockTxnKV.multiSave = func(kvs map[string]string) error {
			return fmt.Errorf("save error")
		}
		assert.Error(t, mt.AlterIndex(&newIdx))
	})

	wg.Add(1)
	t.Run("drop index failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)
		mt.indexID2Meta[indexID] = idxInfo[0]

		_, _, err = mt.DropIndex("abc", "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name = abc not exist")

		mt.collName2ID["abc"] = 2
		_, _, err = mt.DropIndex("abc", "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name  = abc not has meta")

		_, _, err = mt.DropIndex(collInfo.Name, "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s doesn't have filed abc", collInfo.Name))

		coll := mt.collID2Meta[collInfo.CollectionID]
		coll.FieldIDToIndexID = []common.Int64Tuple{
			{
				Key:   fieldID2,
				Value: indexID2,
			},
			{
				Key:   fieldID,
				Value: indexID,
			},
		}
		mt.collID2Meta[coll.CollectionID] = coll
		mt.indexID2Meta = make(map[int64]*model.Index)
		idxID, isDroped, err := mt.DropIndex(collInfo.Name, collInfo.Fields[0].Name, idxInfo[0].IndexName)
		assert.Zero(t, idxID)
		assert.False(t, isDroped)
		assert.Nil(t, err)

		err = mt.reloadFromCatalog()
		assert.Nil(t, err)
		collInfo.Partitions = []*model.Partition{}
		ts = ftso()
		err = mt.AddCollection(collInfo, ts, "")
		mt.indexID2Meta[indexID] = idxInfo[0]

		assert.Nil(t, err)
		mockTxnKV.multiSaveAndRemoveWithPrefix = func(saves map[string]string, removals []string) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}

		_, _, err = mt.DropIndex(collInfo.Name, collInfo.Fields[0].Name, idxInfo[0].IndexName)
		assert.Error(t, err)
	})

	wg.Add(1)
	t.Run("get segment index info by id", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockTxnKV.multiSave = func(kvs map[string]string) error {
			return nil
		}
		mockTxnKV.save = func(key, value string) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		alreadyExists, err := mt.AddIndex(collName, "field110", idxInfo[0], []typeutil.UniqueID{segID})
		assert.Nil(t, err)
		assert.False(t, alreadyExists)

		segIdxInfo := model.Index{
			CollectionID: collID,
			SegmentIndexes: map[int64]model.SegmentIndex{
				segID: {
					Segment: model.Segment{
						SegmentID:   segID,
						PartitionID: partID,
					},
					BuildID: buildID,
				},
			},
			FieldID: fieldID,
			IndexID: indexID,
		}
		err = mt.AlterIndex(&segIdxInfo)
		assert.Nil(t, err)
		idx, err := mt.GetSegmentIndexInfoByID(segID, segIdxInfo.FieldID, idxInfo[0].IndexName)
		assert.Nil(t, err)
		assert.Equal(t, segIdxInfo.IndexID, idx.IndexID)

		_, err = mt.GetSegmentIndexInfoByID(segID, segIdxInfo.FieldID, "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find index name = abc on segment = %d, with filed id = %d", segID, segIdxInfo.FieldID))

		_, err = mt.GetSegmentIndexInfoByID(segID, 11, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find index name = %s on segment = %d, with filed id = 11", idxInfo[0].IndexName, segID))
	})

	wg.Add(1)
	t.Run("get field schema failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]model.Collection)
		_, err = mt.getFieldSchemaInternal(collInfo.Name, collInfo.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Name))

		mt.collName2ID = make(map[string]int64)
		_, err = mt.getFieldSchemaInternal(collInfo.Name, collInfo.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Name))
	})

	wg.Add(1)
	t.Run("is segment indexed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)
		idx := &pb.SegmentIndexInfo{
			IndexID:   30,
			FieldID:   31,
			SegmentID: 32,
		}
		idxMeta := make(map[int64]pb.SegmentIndexInfo)
		idxMeta[idx.IndexID] = *idx

		field := model.Field{
			FieldID: 31,
		}
		assert.False(t, mt.IsSegmentIndexed(idx.SegmentID, &field, nil))
		field.FieldID = 34
		assert.False(t, mt.IsSegmentIndexed(idx.SegmentID, &field, nil))
	})

	wg.Add(1)
	t.Run("get not indexed segments", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		idx := &model.Index{
			IndexName: "no-idx",
			IndexID:   456,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "no-idx-k1",
					Value: "no-idx-v1",
				},
			},
		}

		mt.collName2ID["abc"] = 123
		_, _, err = mt.GetNotIndexedSegments("abc", "no-field", idx, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection abc not found")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		_, _, err = mt.GetNotIndexedSegments(collInfo.Name, "no-field", idx, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s doesn't have filed no-field", collInfo.Name))

		mt.indexID2Meta = make(map[int64]*model.Index)
		_, _, err = mt.GetNotIndexedSegments(collInfo.Name, collInfo.Fields[0].Name, idx, nil)
		assert.Nil(t, err)
	})

	wg.Add(1)
	t.Run("get index by name failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		mt.collName2ID["abc"] = 123
		_, _, err = mt.GetIndexByName("abc", "hij")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection abc not found")

		mockTxnKV.multiSave = func(kvs map[string]string) error {
			return nil
		}
		mockTxnKV.save = func(key string, value string) error {
			return nil
		}
		err = mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)
		mt.indexID2Meta = make(map[int64]*model.Index)
		_, _, err = mt.GetIndexByName(collInfo.Name, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", idxInfo[0].IndexID))

		_, err = mt.GetIndexByID(idxInfo[0].IndexID)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("cannot find index, id = %d", idxInfo[0].IndexID))
	})

	wg.Add(1)
	t.Run("add credential failed", func(t *testing.T) {
		defer wg.Done()
		mockTxnKV.save = func(key, value string) error {
			return fmt.Errorf("save error")
		}
		err = mt.AddCredential(&internalpb.CredentialInfo{Username: "x", EncryptedPassword: "a\xc5z"})
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("delete credential failed", func(t *testing.T) {
		defer wg.Done()
		mockTxnKV.remove = func(key string) error {
			return fmt.Errorf("delete error")
		}
		err := mt.DeleteCredential("")
		assert.Error(t, err)
	})
	wg.Wait()
}

func TestMetaWithTimestamp(t *testing.T) {
	const (
		collID1   = typeutil.UniqueID(1)
		collID2   = typeutil.UniqueID(2)
		collName1 = "t1"
		collName2 = "t2"
		partID1   = 11
		partID2   = 12
		partName1 = "p1"
		partName2 = "p2"
	)
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	var tsoStart typeutil.Timestamp = 100
	vtso := tsoStart
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()

	skv, err := kvmetestore.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mt, err := NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: txnKV, Snapshot: skv})
	assert.Nil(t, err)

	collInfo := &model.Collection{
		CollectionID: collID1,
		Name:         collName1,
	}

	collInfo.Partitions = []*model.Partition{{PartitionID: partID1, PartitionName: partName1, PartitionCreatedTimestamp: ftso()}}
	t1 := ftso()
	err = mt.AddCollection(collInfo, t1, "")
	assert.Nil(t, err)

	collInfo.CollectionID = collID2
	collInfo.Partitions = []*model.Partition{{PartitionID: partID2, PartitionName: partName2, PartitionCreatedTimestamp: ftso()}}
	collInfo.Name = collName2
	t2 := ftso()
	err = mt.AddCollection(collInfo, t2, "")
	assert.Nil(t, err)

	assert.True(t, mt.HasCollection(collID1, 0))
	assert.True(t, mt.HasCollection(collID2, 0))

	assert.True(t, mt.HasCollection(collID1, t2))
	assert.True(t, mt.HasCollection(collID2, t2))

	assert.True(t, mt.HasCollection(collID1, t1))
	assert.False(t, mt.HasCollection(collID2, t1))

	assert.False(t, mt.HasCollection(collID1, tsoStart))
	assert.False(t, mt.HasCollection(collID2, tsoStart))

	c1, err := mt.GetCollectionByID(collID1, 0)
	assert.Nil(t, err)
	c2, err := mt.GetCollectionByID(collID2, 0)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t2)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByID(collID2, tsoStart)
	assert.NotNil(t, err)

	c1, err = mt.GetCollectionByName(collName1, 0)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)
	assert.Equal(t, int64(2), c2.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)
	assert.Equal(t, int64(2), c2.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByName(collName2, tsoStart)
	assert.NotNil(t, err)

	getKeys := func(m map[string]*model.Collection) []string {
		keys := make([]string, 0, len(m))
		for key := range m {
			keys = append(keys, key)
		}
		return keys
	}

	s1, err := mt.ListCollections(0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1, collName2})

	s1, err = mt.ListCollections(t2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1, collName2})

	s1, err = mt.ListCollections(t1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1})

	s1, err = mt.ListCollections(tsoStart)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s1))

	p1, err := mt.GetPartitionByName(collID1, partName1, 0)
	assert.Nil(t, err)
	p2, err := mt.GetPartitionByName(collID2, partName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(partID1), p1)
	assert.Equal(t, int64(partID2), p2)

	p1, err = mt.GetPartitionByName(collID1, partName1, t2)
	assert.Nil(t, err)
	p2, err = mt.GetPartitionByName(collID2, partName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, int64(11), p1)
	assert.Equal(t, int64(12), p2)

	p1, err = mt.GetPartitionByName(1, partName1, t1)
	assert.Nil(t, err)
	_, err = mt.GetPartitionByName(2, partName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(11), p1)

	_, err = mt.GetPartitionByName(1, partName1, tsoStart)
	assert.NotNil(t, err)
	_, err = mt.GetPartitionByName(2, partName2, tsoStart)
	assert.NotNil(t, err)

	var cID UniqueID
	cID, err = mt.GetCollectionIDByName(collName1)
	assert.NoError(t, err)
	assert.Equal(t, collID1, cID)

	_, err = mt.GetCollectionIDByName("badname")
	assert.Error(t, err)

	name, err := mt.GetCollectionNameByID(collID2)
	assert.Nil(t, err)
	assert.Equal(t, collName2, name)

	_, err = mt.GetCollectionNameByID(int64(999))
	assert.Error(t, err)
}

func TestFixIssue10540(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()

	skv, err := kvmetestore.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	//txnKV := etcdkv.NewEtcdKVWithClient(etcdCli, rootPath)
	txnKV := memkv.NewMemoryKV()
	// compose rc7 legace tombstone cases
	txnKV.Save(path.Join(kvmetestore.SegmentIndexMetaPrefix, "2"), string(kvmetestore.SuffixSnapshotTombstone))
	txnKV.Save(path.Join(kvmetestore.IndexMetaPrefix, "3"), string(kvmetestore.SuffixSnapshotTombstone))

	_, err = NewMetaTable(context.TODO(), &kvmetestore.Catalog{Txn: txnKV, Snapshot: skv})
	assert.Nil(t, err)
}

func TestMetaTable_GetSegmentIndexInfos(t *testing.T) {
	meta := &MetaTable{
		segID2IndexID: make(map[typeutil.UniqueID]typeutil.UniqueID, 1),
		indexID2Meta:  make(map[typeutil.UniqueID]*model.Index, 1),
	}

	segID := typeutil.UniqueID(100)
	_, err := meta.GetSegmentIndexInfos(segID)
	assert.Error(t, err)

	meta.segID2IndexID[segID] = 5
	meta.indexID2Meta[5] = &model.Index{
		CollectionID: 1,
		FieldID:      4,
		IndexID:      5,
		SegmentIndexes: map[int64]model.SegmentIndex{
			segID: {
				Segment: model.Segment{
					SegmentID:   segID,
					PartitionID: 2,
				},
				BuildID:     6,
				EnableIndex: true,
			},
		},
	}

	indexMeta, err := meta.GetSegmentIndexInfos(segID)
	assert.NoError(t, err)
	segmentIndex, ok := indexMeta.SegmentIndexes[segID]
	assert.True(t, ok)
	assert.Equal(t, typeutil.UniqueID(1), indexMeta.CollectionID)
	segment := segmentIndex.Segment
	assert.Equal(t, typeutil.UniqueID(2), segment.PartitionID)
	assert.Equal(t, segID, segment.SegmentID)
	assert.Equal(t, typeutil.UniqueID(4), indexMeta.FieldID)
	assert.Equal(t, typeutil.UniqueID(5), indexMeta.IndexID)
	assert.Equal(t, typeutil.UniqueID(6), segmentIndex.BuildID)
	assert.Equal(t, true, segmentIndex.EnableIndex)
}

func TestMetaTable_unlockGetCollectionInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID: map[string]typeutil.UniqueID{"test": 100},
			collID2Meta: map[typeutil.UniqueID]model.Collection{
				100: {CollectionID: 100, Name: "test"},
			},
		}
		info, err := mt.getCollectionInfoInternal("test")
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(100), info.CollectionID)
		assert.Equal(t, "test", info.Name)
	})

	t.Run("collection name not found", func(t *testing.T) {
		mt := &MetaTable{collName2ID: nil, collAlias2ID: nil}
		_, err := mt.getCollectionInfoInternal("test")
		assert.Error(t, err)
	})

	t.Run("name found, meta not found", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID:  map[string]typeutil.UniqueID{"test": 100},
			collAlias2ID: nil,
			collID2Meta:  nil,
		}
		_, err := mt.getCollectionInfoInternal("test")
		assert.Error(t, err)
	})

	t.Run("alias found, meta not found", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID:  nil,
			collAlias2ID: map[string]typeutil.UniqueID{"test": 100},
			collID2Meta:  nil,
		}
		_, err := mt.getCollectionInfoInternal("test")
		assert.Error(t, err)
	})
}

func TestMetaTable_checkFieldCanBeIndexed(t *testing.T) {
	t.Run("field not indexed", func(t *testing.T) {
		mt := &MetaTable{}
		collMeta := model.Collection{
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{
			FieldID: 101,
		}
		idxInfo := &model.Index{}
		err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
	})

	t.Run("field already indexed", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]*model.Index{
				200: {IndexID: 200, IndexName: "test"},
			},
		}
		collMeta := model.Collection{
			Name:             "test",
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{Name: "test", FieldID: 100}
		idxInfo := &model.Index{IndexName: "not_test"}
		err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo)
		assert.Error(t, err)
	})

	t.Run("unexpected", func(t *testing.T) {
		mt := &MetaTable{
			// index meta incomplete.
			indexID2Meta: map[typeutil.UniqueID]*model.Index{},
		}
		collMeta := model.Collection{
			Name:             "test",
			CollectionID:     1000,
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{Name: "test", FieldID: 100}
		idxInfo := &model.Index{IndexName: "not_test"}
		err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
	})
}

func TestMetaTable_checkFieldIndexDuplicate(t *testing.T) {
	t.Run("index already exists", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]*model.Index{
				200: {IndexID: 200, IndexName: "test"},
			},
		}
		collMeta := model.Collection{
			Name:             "test",
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{Name: "test", FieldID: 101}
		idxInfo := &model.Index{IndexName: "test"}
		_, _, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.Error(t, err)
	})

	t.Run("index parameters mismatch", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]*model.Index{
				200: {IndexID: 200, IndexName: "test",
					IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "Value"}}},
			},
		}
		collMeta := model.Collection{
			Name:             "test",
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{Name: "test", FieldID: 100}
		idxInfo := &model.Index{IndexName: "test", IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "not_Value"}}}
		_, _, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.Error(t, err)
	})

	t.Run("index parameters match", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]*model.Index{
				200: {IndexID: 200, IndexName: "test",
					IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "Value"}}},
			},
		}
		collMeta := model.Collection{
			Name:             "test",
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{Name: "test", FieldID: 100}
		idxInfo := &model.Index{IndexName: "test", IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "Value"}}}
		duplicate, dupIdxInfo, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
		assert.True(t, duplicate)
		assert.Equal(t, idxInfo.IndexName, dupIdxInfo.IndexName)
	})

	t.Run("field not found", func(t *testing.T) {
		mt := &MetaTable{}
		collMeta := model.Collection{
			FieldIDToIndexID: []common.Int64Tuple{{Key: 100, Value: 200}},
		}
		fieldSchema := model.Field{
			FieldID: 101,
		}
		idxInfo := &model.Index{}
		duplicate, dupIdxInfo, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
		assert.False(t, duplicate)
		assert.Nil(t, dupIdxInfo)
	})
}

func TestMetaTable_GetInitBuildIDs(t *testing.T) {
	var (
		collName  = "GetInitBuildID-Coll"
		indexName = "GetInitBuildID-Index"
	)
	mt := &MetaTable{
		collID2Meta: map[typeutil.UniqueID]model.Collection{
			1: {
				FieldIDToIndexID: []common.Int64Tuple{
					{
						Key:   1,
						Value: 1,
					},
					{
						Key:   2,
						Value: 2,
					},
					{
						Key:   3,
						Value: 3,
					},
				},
			},
		},
		collName2ID: map[string]typeutil.UniqueID{
			"GetInitBuildID-Coll-1": 2,
		},
		indexID2Meta: map[typeutil.UniqueID]*model.Index{
			1: {
				IndexName: "GetInitBuildID-Index-1",
				IndexID:   1,
				SegmentIndexes: map[int64]model.SegmentIndex{
					4: {
						Segment: model.Segment{
							SegmentID: 4,
						},
						EnableIndex: true,
						CreateTime:  5,
					},
				},
				CreateTime: 10,
			},
			2: {
				IndexName:  "GetInitBuildID-Index-2",
				IndexID:    2,
				CreateTime: 10,
			},
		},
	}
	t.Run("coll not exist", func(t *testing.T) {
		buildIDs, err := mt.GetInitBuildIDs("collName", indexName)
		assert.Error(t, err)
		assert.Nil(t, buildIDs)
	})

	mt.collName2ID[collName] = 1

	t.Run("index not exist", func(t *testing.T) {
		buildIDs, err := mt.GetInitBuildIDs(collName, indexName)
		assert.Error(t, err)
		assert.Nil(t, buildIDs)
	})

	mt.indexID2Meta[3] = &model.Index{
		IndexName:  indexName,
		IndexID:    3,
		CreateTime: 10,
		SegmentIndexes: map[int64]model.SegmentIndex{
			5: {
				Segment: model.Segment{
					SegmentID: 5,
				},
				EnableIndex: true,
				CreateTime:  5,
			},
		},
	}

	mt.segID2IndexID = map[typeutil.UniqueID]typeutil.UniqueID{
		4: 1,
		5: 3,
	}

	t.Run("success", func(t *testing.T) {
		buildIDs, err := mt.GetInitBuildIDs(collName, indexName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(buildIDs))
	})
}

func TestMetaTable_GetDroppedIndex(t *testing.T) {
	mt := &MetaTable{
		collID2Meta: map[typeutil.UniqueID]model.Collection{
			1: {
				FieldIDToIndexID: []common.Int64Tuple{
					{
						Key:   1,
						Value: 1,
					},
					{
						Key:   2,
						Value: 2,
					},
					{
						Key:   3,
						Value: 3,
					},
				},
			},
		},
		indexID2Meta: map[typeutil.UniqueID]*model.Index{
			1: {
				IndexName: "GetInitBuildID-Index-1",
				IndexID:   1,
				IsDeleted: true,
			},
			2: {
				IndexName: "GetInitBuildID-Index-2",
				IndexID:   2,
				IsDeleted: false,
			},
		},
	}

	droppedIndexID := mt.GetDroppedIndex()
	indexInfo, ok := droppedIndexID[1]
	assert.True(t, ok)
	assert.Equal(t, 1, len(indexInfo))
}

func TestMetaTable_AlignSegmentsMeta(t *testing.T) {
	var (
		indexName = "GetDroppedIndex-Index"
		collID    = UniqueID(1)
		partID    = UniqueID(10)
		indexID   = UniqueID(1000)
	)
	mt := &MetaTable{
		catalog: &kvmetestore.Catalog{
			Txn: &mockTestTxnKV{
				multiRemove: func(keys []string) error {
					return nil
				},
			},
		},
		collID2Meta: map[typeutil.UniqueID]model.Collection{
			collID: {
				FieldIDToIndexID: []common.Int64Tuple{
					{
						Key:   1,
						Value: 1,
					},
				},
			},
		},
		partID2IndexedSegID: map[typeutil.UniqueID]map[typeutil.UniqueID]bool{
			partID: {
				100: true,
				101: true,
				102: true,
			},
		},
		indexID2Meta: map[typeutil.UniqueID]*model.Index{
			indexID: {
				IndexName: indexName,
				IndexID:   1,
				IsDeleted: true,
			},
		},
	}
	t.Run("success", func(t *testing.T) {
		mt.AlignSegmentsMeta(collID, partID, map[UniqueID]struct{}{101: {}, 102: {}, 103: {}})
	})

	t.Run("txn error", func(t *testing.T) {
		txn := &mockTestTxnKV{
			multiRemove: func(keys []string) error {
				return fmt.Errorf("error occurred")
			},
		}
		mt.catalog = &kvmetestore.Catalog{Txn: txn}
		mt.AlignSegmentsMeta(collID, partID, map[UniqueID]struct{}{103: {}, 104: {}, 105: {}})
	})
}

func TestMetaTable_MarkIndexDeleted(t *testing.T) {
	var (
		collName  = "MarkIndexDeleted-Coll"
		fieldName = "MarkIndexDeleted-Field"
		indexName = "MarkIndexDeleted-Index"
		collID    = UniqueID(1)
		partID    = UniqueID(10)
		fieldID   = UniqueID(100)
		indexID   = UniqueID(1000)
	)
	mt := &MetaTable{
		ctx: context.TODO(),
		collID2Meta: map[typeutil.UniqueID]model.Collection{
			collID: {
				FieldIDToIndexID: []common.Int64Tuple{
					{
						Key:   101,
						Value: 1001,
					},
				},
				Fields: []*model.Field{
					{
						FieldID: fieldID,
						Name:    fieldName,
					},
				},
			},
		},
		collName2ID: map[string]typeutil.UniqueID{
			collName: collID,
		},
		partID2IndexedSegID: map[typeutil.UniqueID]map[typeutil.UniqueID]bool{
			partID: {
				100: true,
				101: true,
				102: true,
			},
		},
		indexID2Meta: map[typeutil.UniqueID]*model.Index{
			1001: {
				IndexName: indexName,
				IndexID:   indexID,
				IsDeleted: true,
			},
		},
	}

	t.Run("get collection meta failed", func(t *testing.T) {
		err := mt.MarkIndexDeleted("collName", fieldName, indexName)
		assert.Error(t, err)
	})

	t.Run("get field meta failed", func(t *testing.T) {
		err := mt.MarkIndexDeleted(collName, "fieldName", indexName)
		assert.Error(t, err)

		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)
	})

	t.Run("indexMeta error", func(t *testing.T) {
		collMeta := model.Collection{
			FieldIDToIndexID: []common.Int64Tuple{
				{
					Key:   fieldID,
					Value: indexID,
				},
			},
			Fields: []*model.Field{
				{
					FieldID: fieldID,
					Name:    fieldName,
				},
			},
		}
		mt.collID2Meta[collID] = collMeta

		err := mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.Error(t, err)

		mt.indexID2Meta[indexID] = &model.Index{
			IndexName: "indexName",
			IndexID:   indexID,
		}

		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)
	})

	t.Run("txn save failed", func(t *testing.T) {
		mt.indexID2Meta[indexID] = &model.Index{
			IndexName: indexName,
			IndexID:   indexID,
		}
		mc := &MockedCatalog{}
		targetErr := errors.New("alter add index fail")

		mc.On("AlterIndex").Return(targetErr)
		mt.catalog = mc
		err := mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, targetErr))

		mc = &MockedCatalog{}
		mc.On("AlterIndex").Return(nil)
		mt.catalog = mc
		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)
		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)
	})
}

type MockedCatalog struct {
	mock.Mock
	metastore.Catalog
	alterIndexParamsVerification  func(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType)
	createIndexParamsVerification func(ctx context.Context, col *model.Collection, index *model.Index)
	dropIndexParamsVerification   func(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID)
}

func (mc *MockedCatalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	args := mc.Called()
	return args.Get(0).(map[string]*model.Collection), nil
}

func (mc *MockedCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	args := mc.Called()
	return args.Get(0).([]*model.Index), nil
}

func (mc *MockedCatalog) ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Collection, error) {
	args := mc.Called()
	return args.Get(0).([]*model.Collection), nil
}

func (mc *MockedCatalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType) error {
	if mc.alterIndexParamsVerification != nil {
		mc.alterIndexParamsVerification(ctx, oldIndex, newIndex, alterType)
	}
	args := mc.Called()
	err := args.Get(0)
	if err == nil {
		return nil
	}
	return err.(error)
}

func (mc *MockedCatalog) CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error {
	if mc.createIndexParamsVerification != nil {
		mc.createIndexParamsVerification(ctx, col, index)
	}
	args := mc.Called()
	err := args.Get(0)
	if err == nil {
		return nil
	}
	return err.(error)
}

func (mc *MockedCatalog) DropIndex(ctx context.Context, collectionInfo *model.Collection,
	dropIdxID typeutil.UniqueID) error {
	if mc.dropIndexParamsVerification != nil {
		mc.dropIndexParamsVerification(ctx, collectionInfo, dropIdxID)
	}

	args := mc.Called()
	err := args.Get(0)
	if err == nil {
		return nil
	}
	return err.(error)
}

func TestMetaTable_ReloadFromKV(t *testing.T) {
	mc := &MockedCatalog{}

	collectionName := "cn"
	collInfo := &model.Collection{
		CollectionID: 1,
		Name:         collectionName,
		AutoID:       false,
		Fields: []*model.Field{
			{
				FieldID:      1,
				Name:         "field110",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-k1",
						Value: "field110-v1",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-i1",
						Value: "field110-v1",
					},
				},
			},
		},
		FieldIDToIndexID: []common.Int64Tuple{{Key: 1, Value: 1}},
		Partitions: []*model.Partition{
			{
				PartitionID:               1,
				PartitionName:             Params.CommonCfg.DefaultPartitionName,
				PartitionCreatedTimestamp: 0,
			},
		},
		Aliases: []string{"a", "b"},
	}
	collections := map[string]*model.Collection{collectionName: collInfo}
	mc.On("ListCollections").Return(collections, nil)

	indexes := []*model.Index{
		{
			CollectionID: 1,
			IndexName:    "idx",
			IndexID:      1,
			FieldID:      1,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "field110-i1",
					Value: "field110-v1",
				},
			},
			SegmentIndexes: map[int64]model.SegmentIndex{
				1: {
					Segment: model.Segment{
						SegmentID:   1,
						PartitionID: 1,
					},
					BuildID:     1000,
					EnableIndex: true,
				},
			},
		},
	}
	mc.On("ListIndexes").Return(indexes, nil)

	alias1 := *collInfo
	alias1.Name = collInfo.Aliases[0]

	alias2 := *collInfo
	alias2.Name = collInfo.Aliases[1]
	mc.On("ListAliases").Return([]*model.Collection{&alias1, &alias2}, nil)

	mt := &MetaTable{}
	mt.catalog = mc
	mt.reloadFromCatalog()

	assert.True(t, len(mt.collID2Meta) == 1)
	assert.Equal(t, mt.collID2Meta[1], *collInfo)

	assert.True(t, len(mt.collName2ID) == 1)

	assert.True(t, len(mt.collAlias2ID) == 2)
	ret, ok := mt.collAlias2ID[collInfo.Aliases[0]]
	assert.True(t, ok)
	assert.Equal(t, int64(1), ret)

	assert.True(t, len(mt.partID2IndexedSegID) == 1)
	ret2, ok := mt.partID2IndexedSegID[1]
	assert.True(t, ok)
	assert.True(t, len(ret2) == 1)
	ret3, ok := ret2[typeutil.UniqueID(1)]
	assert.True(t, ok)
	assert.True(t, ret3)

	assert.True(t, len(mt.segID2IndexID) == 1)
	segID, ok := mt.segID2IndexID[1]
	assert.True(t, ok)
	assert.Equal(t, int64(1), segID)

	assert.True(t, len(mt.indexID2Meta) == 1)
	meta, ok := mt.indexID2Meta[1]
	assert.True(t, ok)
	assert.Equal(t, indexes[0], meta)
}

func TestMetaTable_AddIndex(t *testing.T) {
	var (
		collName   = "MarkIndexDeleted-Coll"
		fieldName  = "MarkIndexDeleted-Field"
		fieldName2 = "MarkIndexDeleted-Field2"
		indexName  = "MarkIndexDeleted-Index"
		collID     = UniqueID(1)
		fieldID    = UniqueID(100)
		fieldID2   = UniqueID(101)
		indexID    = UniqueID(1000)
		indexID2   = UniqueID(1001)
		segID      = UniqueID(10000)
	)

	colMeta := model.Collection{
		CollectionID: collID,
		Name:         collName,
		FieldIDToIndexID: []common.Int64Tuple{
			{
				Key:   fieldID,
				Value: indexID,
			},
		},
		Fields: []*model.Field{
			{
				FieldID:     fieldID,
				Name:        fieldName,
				DataType:    schemapb.DataType_FloatVector,
				IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			},
			{
				FieldID:     fieldID2,
				Name:        fieldName2,
				DataType:    schemapb.DataType_FloatVector,
				IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			},
		},
	}

	t.Run("test index already exists", func(t *testing.T) {
		mt := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]model.Collection{collID: colMeta},
			collName2ID: map[string]typeutil.UniqueID{collName: collID},
		}
		mt.indexID2Meta = map[typeutil.UniqueID]*model.Index{
			indexID: {
				IndexName:   indexName,
				IndexID:     indexID,
				IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
				IsDeleted:   false,
				CreateTime:  0,
			},
		}

		mc := &MockedCatalog{}
		mc.On("AlterIndex").Return(nil)
		mc.alterIndexParamsVerification = func(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType) {
			assert.NotNil(t, oldIndex)
			assert.NotNil(t, newIndex)
			assert.Equal(t, metastore.ADD, alterType)
			assert.Equal(t, oldIndex.IndexID, newIndex.IndexID)
			assert.Equal(t, oldIndex.IsDeleted, newIndex.IsDeleted)
			assert.Equal(t, oldIndex.IndexParams, newIndex.IndexParams)
			assert.Equal(t, oldIndex.CreateTime, uint64(0))
			assert.Equal(t, newIndex.CreateTime, uint64(100))
		}
		mt.catalog = mc

		idxInfo := &model.Index{
			IndexName:   indexName,
			IndexID:     indexID,
			IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			CreateTime:  100,
		}

		ret, err := mt.AddIndex(collName, fieldName, idxInfo, []UniqueID{segID})
		assert.True(t, ret)

		idxMeta, ok := mt.indexID2Meta[indexID]
		assert.True(t, ok)
		assert.Equal(t, uint64(100), idxMeta.CreateTime)
		assert.NoError(t, err)
	})

	t.Run("test add index firstly(create index)", func(t *testing.T) {
		mt := &MetaTable{
			collID2Meta:  map[typeutil.UniqueID]model.Collection{collID: colMeta},
			collName2ID:  map[string]typeutil.UniqueID{collName: collID},
			indexID2Meta: make(map[typeutil.UniqueID]*model.Index),
		}

		idxInfo := &model.Index{
			FieldID:     fieldID2,
			IndexName:   indexName,
			IndexID:     indexID2,
			IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			CreateTime:  100,
		}

		mc := &MockedCatalog{}
		mc.On("CreateIndex").Return(errors.New("create index fail"))
		mt.catalog = mc
		ret, err := mt.AddIndex(collName, fieldName2, idxInfo, []UniqueID{segID})
		assert.False(t, ret)
		assert.Error(t, err)
		_, ok := mt.indexID2Meta[indexID2]
		assert.False(t, ok)

		mc = &MockedCatalog{}
		mc.On("CreateIndex").Return(nil)
		mc.createIndexParamsVerification = func(ctx context.Context, col *model.Collection, index *model.Index) {
			assert.NotNil(t, col)
			assert.NotNil(t, index)
			assert.NotEqual(t, colMeta, col)
			assert.Equal(t, 2, len(col.FieldIDToIndexID))
			assert.Equal(t, fieldID, col.FieldIDToIndexID[0].Key)
			assert.Equal(t, fieldID2, col.FieldIDToIndexID[1].Key)
			assert.Equal(t, indexID2, index.IndexID)
			assert.Equal(t, uint64(100), index.CreateTime)
			assert.Equal(t, 1, len(index.SegmentIndexes))
			assert.Equal(t, segID, index.SegmentIndexes[segID].SegmentID)
			assert.Equal(t, false, index.SegmentIndexes[segID].EnableIndex)
		}

		mt.catalog = mc
		ret, err = mt.AddIndex(collName, fieldName2, idxInfo, []UniqueID{segID})
		assert.False(t, ret)
		assert.NoError(t, err)

		idxMeta, ok := mt.indexID2Meta[indexID2]
		assert.True(t, ok)
		assert.Equal(t, uint64(100), idxMeta.CreateTime)
		assert.NoError(t, err)

		newColMeta, ok := mt.collID2Meta[collID]
		assert.True(t, ok)
		assert.Equal(t, collID, newColMeta.CollectionID)
		assert.Equal(t, 2, len(newColMeta.FieldIDToIndexID))
		assert.Equal(t, fieldID, newColMeta.FieldIDToIndexID[0].Key)
		assert.Equal(t, indexID, newColMeta.FieldIDToIndexID[0].Value)
		assert.Equal(t, fieldID2, newColMeta.FieldIDToIndexID[1].Key)
		assert.Equal(t, indexID2, newColMeta.FieldIDToIndexID[1].Value)
	})
}

func TestMetaTable_RecycleDroppedIndex(t *testing.T) {
	colName := "c"
	fieldName := "f1"
	indexName1 := "idx1"
	indexName2 := "idx2"

	colMeta := model.Collection{
		CollectionID: 1,
		Name:         colName,
		FieldIDToIndexID: []common.Int64Tuple{
			{
				Key:   1,
				Value: 1,
			},
			{
				Key:   1,
				Value: 2,
			},
		},
		Fields: []*model.Field{
			{
				FieldID:     1,
				Name:        fieldName,
				DataType:    schemapb.DataType_FloatVector,
				IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			},
		},
		Partitions: []*model.Partition{
			{
				PartitionID:   1,
				PartitionName: "p",
			},
		},
	}

	mt := &MetaTable{
		collID2Meta: map[typeutil.UniqueID]model.Collection{1: colMeta},
		collName2ID: map[string]typeutil.UniqueID{colName: 1},
		segID2IndexID: map[typeutil.UniqueID]typeutil.UniqueID{
			1: 1,
		},
		partID2IndexedSegID: map[typeutil.UniqueID]map[typeutil.UniqueID]bool{
			1: {
				1: true,
			},
		},
	}
	mt.indexID2Meta = map[typeutil.UniqueID]*model.Index{
		1: {
			IndexName:   indexName1,
			IndexID:     1,
			IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			IsDeleted:   true,
			SegmentIndexes: map[int64]model.SegmentIndex{
				1: {
					Segment: model.Segment{
						SegmentID:   1,
						PartitionID: 1,
					},
					BuildID:     1000,
					EnableIndex: true,
				},
			},
		},
		2: {
			IndexName:   indexName2,
			IndexID:     2,
			IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}},
			IsDeleted:   false,
			SegmentIndexes: map[int64]model.SegmentIndex{
				1: {
					Segment: model.Segment{
						SegmentID:   1,
						PartitionID: 1,
					},
					BuildID:     1000,
					EnableIndex: true,
				},
			},
		},
	}

	mc := &MockedCatalog{}
	mc.On("DropIndex").Return(nil)
	mc.dropIndexParamsVerification = func(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID) {
		assert.NotNil(t, collectionInfo)
		assert.Equal(t, int64(1), dropIdxID)
		assert.Equal(t, int64(1), collectionInfo.CollectionID)
		assert.Equal(t, 1, len(collectionInfo.FieldIDToIndexID))
		assert.Equal(t, int64(1), collectionInfo.FieldIDToIndexID[0].Key)
		assert.Equal(t, int64(2), collectionInfo.FieldIDToIndexID[0].Value)
	}
	mt.catalog = mc

	mt.RecycleDroppedIndex()
	newColMeta, ok := mt.collID2Meta[1]
	assert.True(t, ok)
	assert.Equal(t, int64(1), newColMeta.CollectionID)
	assert.Equal(t, 1, len(newColMeta.FieldIDToIndexID))
	assert.Equal(t, int64(1), newColMeta.FieldIDToIndexID[0].Key)
	assert.Equal(t, int64(2), newColMeta.FieldIDToIndexID[0].Value)

	assert.Equal(t, 1, len(mt.indexID2Meta))
	idxMeta, ok := mt.indexID2Meta[2]
	assert.True(t, ok)
	assert.Equal(t, false, idxMeta.IsDeleted)

	assert.Equal(t, 0, len(mt.segID2IndexID))
	assert.Equal(t, 1, len(mt.partID2IndexedSegID))
}
