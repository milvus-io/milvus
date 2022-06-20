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
	"errors"
	"fmt"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
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

	skv, err := newMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mt, err := NewMetaTable(txnKV, skv)
	assert.Nil(t, err)

	collInfo := &pb.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:   collName,
			AutoID: false,
			Fields: []*schemapb.FieldSchema{
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
		},
		FieldIndexes: []*pb.FieldIndexInfo{
			{
				FiledID: fieldID,
				IndexID: indexID,
			},
		},
		CreateTime:                 0,
		PartitionIDs:               []typeutil.UniqueID{partIDDefault},
		PartitionNames:             []string{Params.CommonCfg.DefaultPartitionName},
		PartitionCreatedTimestamps: []uint64{0},
	}
	idxInfo := []*pb.IndexInfo{
		{
			IndexName: indexName,
			IndexID:   indexID,
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
		err = mt.AddCollection(collInfo, ts, nil, "")
		assert.NotNil(t, err)

		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), ts)

		collMeta, err := mt.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.CreateTime, ts)
		assert.Equal(t, collMeta.PartitionCreatedTimestamps[0], ts)

		assert.Equal(t, partIDDefault, collMeta.PartitionIDs[0])
		assert.Equal(t, 1, len(collMeta.PartitionIDs))
		assert.True(t, mt.HasCollection(collInfo.ID, 0))

		field, err := mt.GetFieldSchema(collName, "field110")
		assert.Nil(t, err)
		assert.Equal(t, collInfo.Schema.Fields[0].FieldID, field.FieldID)

		// check DD operation flag
		flag, err := mt.snapshot.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
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
		err = mt.reloadFromKV()
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
		assert.Equal(t, 2, len(collMeta.PartitionNames))
		assert.Equal(t, collMeta.PartitionNames[1], partName)
		assert.Equal(t, ts, collMeta.PartitionCreatedTimestamps[1])

		// check DD operation flag
		flag, err := mt.txn.Load(DDMsgSendPrefix)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	wg.Add(1)
	t.Run("add segment index", func(t *testing.T) {
		defer wg.Done()
		segIdxInfo := pb.SegmentIndexInfo{
			CollectionID: collID,
			PartitionID:  partID,
			SegmentID:    segID,
			FieldID:      fieldID,
			IndexID:      indexID,
			BuildID:      buildID,
		}
		err = mt.AddIndex(&segIdxInfo)
		assert.Nil(t, err)

		// it's legal to add index twice
		err = mt.AddIndex(&segIdxInfo)
		assert.Nil(t, err)

		segIdxInfo.BuildID = 202
		err = mt.AddIndex(&segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d exist", segIdxInfo.IndexID))
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
		idxInfo := &pb.IndexInfo{
			IndexName:   "testColl_index_110",
			IndexID:     indexID,
			IndexParams: params,
		}

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo, nil)
		assert.NotNil(t, err)
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

		idxInfo := &pb.IndexInfo{
			IndexName:   "field110",
			IndexID:     2000,
			IndexParams: params,
		}

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo, nil)
		assert.NotNil(t, err)
		_, _, err = mt.GetNotIndexedSegments(collName, "field110", idxInfo, []typeutil.UniqueID{segID, segID2})
		assert.NotNil(t, err)

		params = []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
		}
		idxInfo.IndexParams = params
		idxInfo.IndexID = 2001
		idxInfo.IndexName = "field110-1"

		_, _, err = mt.GetNotIndexedSegments(collName, "field110", idxInfo, []typeutil.UniqueID{segID, segID2})
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("get index by name", func(t *testing.T) {
		defer wg.Done()
		_, idx, err := mt.GetIndexByName(collName, indexName)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(idx))
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
	t.Run("drop index", func(t *testing.T) {
		defer wg.Done()
		idx, ok, err := mt.DropIndex(collName, "field110", indexName)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, idxInfo[0].IndexID, idx)

		_, ok, err = mt.DropIndex(collName, "field110", "field110-error")
		assert.Nil(t, err)
		assert.False(t, ok)

		_, idxs, err := mt.GetIndexByName(collName, "field110")
		assert.Nil(t, err)
		assert.Zero(t, len(idxs))

		_, idxs, err = mt.GetIndexByName(collName, "field110-1")
		assert.Nil(t, err)
		assert.Zero(t, len(idxs))

		_, err = mt.GetSegmentIndexInfoByID(segID, -1, "")
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("drop partition", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		id, err := mt.DeletePartition(collID, partName, ts, "")
		assert.Nil(t, err)
		assert.Equal(t, partID, id)

		// check DD operation flag
		flag, err := mt.txn.Load(DDMsgSendPrefix)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
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

		// check DD operation flag
		flag, err := mt.txn.Load(DDMsgSendPrefix)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
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
	mockKV := &mockTestKV{}
	mt.snapshot = mockKV
	mockTxnKV := &mockTestTxnKV{
		TxnKV:          mt.txn,
		loadWithPrefix: func(key string) ([]string, []string, error) { return txnkv.LoadWithPrefix(key) },
		save:           func(key, value string) error { return txnkv.Save(key, value) },
		multiSave:      func(kvs map[string]string) error { return txnkv.MultiSave(kvs) },
		multiSaveAndRemoveWithPrefix: func(kvs map[string]string, removal []string) error {
			return txnkv.MultiSaveAndRemoveWithPrefix(kvs, removal)
		},
		remove: func(key string) error { return txnkv.Remove(key) },
	}
	mt.txn = mockTxnKV

	wg.Add(1)
	t.Run("add collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save error")
		}
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		assert.Panics(t, func() { mt.AddCollection(collInfo, 0, idxInfo, "") })
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
		assert.Panics(t, func() { mt.DeleteCollection(collInfo.ID, ts, "") })
	})

	wg.Add(1)
	t.Run("get collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.GetCollectionByName(collInfo.Schema.Name, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection %s with id %d", collInfo.Schema.Name, collInfo.ID))

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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		ts = ftso()
		err = mt.AddPartition(2, "no-part", 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "can't find collection. id = 2")

		coll := mt.collID2Meta[collInfo.ID]
		coll.PartitionIDs = make([]int64, Params.RootCoordCfg.MaxPartitionNum)
		mt.collID2Meta[coll.ID] = coll
		err = mt.AddPartition(coll.ID, "no-part", 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("maximum partition's number should be limit to %d", Params.RootCoordCfg.MaxPartitionNum))

		coll.PartitionIDs = []int64{partID}
		coll.PartitionNames = []string{partName}
		coll.PartitionCreatedTimestamps = []uint64{ftso()}
		mt.collID2Meta[coll.ID] = coll
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save error")
		}
		assert.Panics(t, func() { mt.AddPartition(coll.ID, "no-part", 22, ts, "") })
		//err = mt.AddPartition(coll.ID, "no-part", 22, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil

		//_, err = mt.AddCollection(collInfo, idxInfo, nil)
		//assert.Nil(t, err)
		//_, err = mt.AddPartition(coll.ID, partName, partID, nil)
		//assert.Nil(t, err)
		ts = ftso()
		err = mt.AddPartition(coll.ID, partName, 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition name = %s already exists", partName))
		err = mt.AddPartition(coll.ID, "no-part", partID, ts, "")
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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		assert.False(t, mt.HasPartition(collInfo.ID, "no-partName", 0))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		assert.False(t, mt.HasPartition(collInfo.ID, partName, 0))
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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = []int64{partID}
		collInfo.PartitionNames = []string{partName}
		collInfo.PartitionCreatedTimestamps = []uint64{ftso()}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		ts = ftso()
		_, err = mt.DeletePartition(collInfo.ID, Params.CommonCfg.DefaultPartitionName, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "default partition cannot be deleted")

		_, err = mt.DeletePartition(collInfo.ID, "abc", ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "partition abc does not exist")

		mockKV.save = func(key, value string, ts typeutil.Timestamp) error { return errors.New("mocked error") }
		assert.Panics(t, func() { mt.DeletePartition(collInfo.ID, partName, ts, "") })
		//_, err = mt.DeletePartition(collInfo.ID, partName, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save and remove with prefix error")

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.DeletePartition(collInfo.ID, "abc", ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection id = %d", collInfo.ID))
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
		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		segIdxInfo := pb.SegmentIndexInfo{
			CollectionID: collID,
			PartitionID:  partID,
			SegmentID:    segID,
			FieldID:      fieldID,
			IndexID:      indexID2,
			BuildID:      buildID,
		}
		err = mt.AddIndex(&segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", segIdxInfo.IndexID))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		err = mt.AddIndex(&segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", segIdxInfo.IndexID))

		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts = ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		segIdxInfo.IndexID = indexID
		mockTxnKV.save = func(key string, value string) error {
			return fmt.Errorf("save error")
		}
		assert.Panics(t, func() { mt.AddIndex(&segIdxInfo) })
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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		_, _, err = mt.DropIndex("abc", "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name = abc not exist")

		mt.collName2ID["abc"] = 2
		_, _, err = mt.DropIndex("abc", "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name  = abc not has meta")

		_, _, err = mt.DropIndex(collInfo.Schema.Name, "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s doesn't have filed abc", collInfo.Schema.Name))

		coll := mt.collID2Meta[collInfo.ID]
		coll.FieldIndexes = []*pb.FieldIndexInfo{
			{
				FiledID: fieldID2,
				IndexID: indexID2,
			},
			{
				FiledID: fieldID,
				IndexID: indexID,
			},
		}
		mt.collID2Meta[coll.ID] = coll
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		idxID, isDroped, err := mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName)
		assert.Zero(t, idxID)
		assert.False(t, isDroped)
		assert.Nil(t, err)

		err = mt.reloadFromKV()
		assert.Nil(t, err)
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		coll.PartitionCreatedTimestamps = nil
		ts = ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)
		mockTxnKV.multiSaveAndRemoveWithPrefix = func(saves map[string]string, removals []string) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}
		assert.Panics(t, func() { mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName) })
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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		seg, err := mt.GetSegmentIndexInfoByID(segID2, fieldID, "abc")
		assert.Nil(t, err)
		assert.Equal(t, segID2, seg.SegmentID)
		assert.Equal(t, fieldID, seg.FieldID)
		assert.Equal(t, false, seg.EnableIndex)

		segIdxInfo := pb.SegmentIndexInfo{
			CollectionID: collID,
			PartitionID:  partID,
			SegmentID:    segID,
			FieldID:      fieldID,
			IndexID:      indexID,
			BuildID:      buildID,
		}
		err = mt.AddIndex(&segIdxInfo)
		assert.Nil(t, err)
		idx, err := mt.GetSegmentIndexInfoByID(segIdxInfo.SegmentID, segIdxInfo.FieldID, idxInfo[0].IndexName)
		assert.Nil(t, err)
		assert.Equal(t, segIdxInfo.IndexID, idx.IndexID)

		_, err = mt.GetSegmentIndexInfoByID(segIdxInfo.SegmentID, segIdxInfo.FieldID, "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find index name = abc on segment = %d, with filed id = %d", segIdxInfo.SegmentID, segIdxInfo.FieldID))

		_, err = mt.GetSegmentIndexInfoByID(segIdxInfo.SegmentID, 11, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find index name = %s on segment = %d, with filed id = 11", idxInfo[0].IndexName, segIdxInfo.SegmentID))
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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.getFieldSchemaInternal(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Schema.Name))

		mt.collName2ID = make(map[string]int64)
		_, err = mt.getFieldSchemaInternal(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Schema.Name))
	})

	wg.Add(1)
	t.Run("is segment indexed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)
		idx := &pb.SegmentIndexInfo{
			IndexID:   30,
			FieldID:   31,
			SegmentID: 32,
		}
		idxMeta := make(map[int64]pb.SegmentIndexInfo)
		idxMeta[idx.IndexID] = *idx

		field := schemapb.FieldSchema{
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
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		idx := &pb.IndexInfo{
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

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)

		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, "no-field", idx, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s doesn't have filed no-field", collInfo.Schema.Name))

		bakMeta := mt.indexID2Meta
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		mockTxnKV.multiSave = func(kvs map[string]string) error {
			return fmt.Errorf("multi save error")
		}
		assert.Panics(t, func() {
			_, _, _ = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0], []UniqueID{10001, 10002})
		})
		mt.indexID2Meta = bakMeta
	})

	wg.Add(1)
	t.Run("get index by name failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromKV()
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
		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, "")
		assert.Nil(t, err)
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		_, _, err = mt.GetIndexByName(collInfo.Schema.Name, idxInfo[0].IndexName)
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

	skv, err := newMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mt, err := NewMetaTable(txnKV, skv)
	assert.Nil(t, err)

	collInfo := &pb.CollectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Name: collName1,
		},
	}

	collInfo.PartitionIDs = []int64{partID1}
	collInfo.PartitionNames = []string{partName1}
	collInfo.PartitionCreatedTimestamps = []uint64{ftso()}
	t1 := ftso()
	err = mt.AddCollection(collInfo, t1, nil, "")
	assert.Nil(t, err)

	collInfo.ID = 2
	collInfo.PartitionIDs = []int64{partID2}
	collInfo.PartitionNames = []string{partName2}
	collInfo.PartitionCreatedTimestamps = []uint64{ftso()}
	collInfo.Schema.Name = collName2

	t2 := ftso()
	err = mt.AddCollection(collInfo, t2, nil, "")
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
	assert.Equal(t, collID1, c1.ID)
	assert.Equal(t, collID2, c2.ID)

	c1, err = mt.GetCollectionByID(collID1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t2)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.ID)
	assert.Equal(t, collID2, c2.ID)

	c1, err = mt.GetCollectionByID(collID1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), c1.ID)

	c1, err = mt.GetCollectionByID(collID1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByID(collID2, tsoStart)
	assert.NotNil(t, err)

	c1, err = mt.GetCollectionByName(collName1, 0)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), c1.ID)
	assert.Equal(t, int64(2), c2.ID)

	c1, err = mt.GetCollectionByName(collName1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), c1.ID)
	assert.Equal(t, int64(2), c2.ID)

	c1, err = mt.GetCollectionByName(collName1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), c1.ID)

	c1, err = mt.GetCollectionByName(collName1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByName(collName2, tsoStart)
	assert.NotNil(t, err)

	getKeys := func(m map[string]*pb.CollectionInfo) []string {
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

	p1, err := mt.GetPartitionByName(1, partName1, 0)
	assert.Nil(t, err)
	p2, err := mt.GetPartitionByName(2, partName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(11), p1)
	assert.Equal(t, int64(12), p2)
	assert.Nil(t, err)

	p1, err = mt.GetPartitionByName(1, partName1, t2)
	assert.Nil(t, err)
	p2, err = mt.GetPartitionByName(2, partName2, t2)
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

	skv, err := newMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	//txnKV := etcdkv.NewEtcdKVWithClient(etcdCli, rootPath)
	txnKV := memkv.NewMemoryKV()
	// compose rc7 legace tombstone cases
	txnKV.Save(path.Join(SegmentIndexMetaPrefix, "2"), string(suffixSnapshotTombstone))
	txnKV.Save(path.Join(IndexMetaPrefix, "3"), string(suffixSnapshotTombstone))

	_, err = NewMetaTable(txnKV, skv)
	assert.Nil(t, err)
}

func TestMetaTable_GetSegmentIndexInfos(t *testing.T) {
	meta := &MetaTable{
		segID2IndexMeta: map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo{},
	}

	segID := typeutil.UniqueID(100)
	_, err := meta.GetSegmentIndexInfos(segID)
	assert.Error(t, err)

	meta.segID2IndexMeta[segID] = map[typeutil.UniqueID]pb.SegmentIndexInfo{
		5: {
			CollectionID: 1,
			PartitionID:  2,
			SegmentID:    segID,
			FieldID:      4,
			IndexID:      5,
			BuildID:      6,
			EnableIndex:  true,
		},
	}
	infos, err := meta.GetSegmentIndexInfos(segID)
	assert.NoError(t, err)
	indexInfos, ok := infos[5]
	assert.True(t, ok)
	assert.Equal(t, typeutil.UniqueID(1), indexInfos.GetCollectionID())
	assert.Equal(t, typeutil.UniqueID(2), indexInfos.GetPartitionID())
	assert.Equal(t, segID, indexInfos.GetSegmentID())
	assert.Equal(t, typeutil.UniqueID(4), indexInfos.GetFieldID())
	assert.Equal(t, typeutil.UniqueID(5), indexInfos.GetIndexID())
	assert.Equal(t, typeutil.UniqueID(6), indexInfos.GetBuildID())
	assert.Equal(t, true, indexInfos.GetEnableIndex())
}

func TestMetaTable_unlockGetCollectionInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID: map[string]typeutil.UniqueID{"test": 100},
			collID2Meta: map[typeutil.UniqueID]pb.CollectionInfo{
				100: {ID: 100, Schema: &schemapb.CollectionSchema{Name: "test"}},
			},
		}
		info, err := mt.getCollectionInfoInternal("test")
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(100), info.ID)
		assert.Equal(t, "test", info.GetSchema().GetName())
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
		collMeta := pb.CollectionInfo{
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{
			FieldID: 101,
		}
		idxInfo := &pb.IndexInfo{}
		err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
	})

	t.Run("field already indexed", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
				200: {IndexID: 200, IndexName: "test"},
			},
		}
		collMeta := pb.CollectionInfo{
			Schema:       &schemapb.CollectionSchema{Name: "test"},
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{Name: "test", FieldID: 100}
		idxInfo := &pb.IndexInfo{IndexName: "not_test"}
		err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo)
		assert.Error(t, err)
	})

	t.Run("unexpected", func(t *testing.T) {
		mt := &MetaTable{
			// index meta incomplete.
			indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{},
		}
		collMeta := pb.CollectionInfo{
			Schema:       &schemapb.CollectionSchema{Name: "test"},
			ID:           1000,
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{Name: "test", FieldID: 100}
		idxInfo := &pb.IndexInfo{IndexName: "not_test"}
		err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
	})
}

func TestMetaTable_checkFieldIndexDuplicate(t *testing.T) {
	t.Run("index already exists", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
				200: {IndexID: 200, IndexName: "test"},
			},
		}
		collMeta := pb.CollectionInfo{
			Schema:       &schemapb.CollectionSchema{Name: "test"},
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{Name: "test", FieldID: 101}
		idxInfo := &pb.IndexInfo{IndexName: "test"}
		_, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.Error(t, err)
	})

	t.Run("index parameters mismatch", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
				200: {IndexID: 200, IndexName: "test",
					IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "Value"}}},
			},
		}
		collMeta := pb.CollectionInfo{
			Schema:       &schemapb.CollectionSchema{Name: "test"},
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{Name: "test", FieldID: 100}
		idxInfo := &pb.IndexInfo{IndexName: "test", IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "not_Value"}}}
		_, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.Error(t, err)
	})

	t.Run("index parameters match", func(t *testing.T) {
		mt := &MetaTable{
			indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
				200: {IndexID: 200, IndexName: "test",
					IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "Value"}}},
			},
		}
		collMeta := pb.CollectionInfo{
			Schema:       &schemapb.CollectionSchema{Name: "test"},
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{Name: "test", FieldID: 100}
		idxInfo := &pb.IndexInfo{IndexName: "test", IndexParams: []*commonpb.KeyValuePair{{Key: "Key", Value: "Value"}}}
		duplicate, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
		assert.True(t, duplicate)
	})

	t.Run("field not found", func(t *testing.T) {
		mt := &MetaTable{}
		collMeta := pb.CollectionInfo{
			FieldIndexes: []*pb.FieldIndexInfo{{FiledID: 100, IndexID: 200}},
		}
		fieldSchema := schemapb.FieldSchema{
			FieldID: 101,
		}
		idxInfo := &pb.IndexInfo{}
		duplicate, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
		assert.NoError(t, err)
		assert.False(t, duplicate)
	})
}

func TestMetaTable_GetInitBuildIDs(t *testing.T) {
	var (
		collName  = "GetInitBuildID-Coll"
		indexName = "GetInitBuildID-Index"
	)
	mt := &MetaTable{
		collID2Meta: map[typeutil.UniqueID]pb.CollectionInfo{
			1: {
				FieldIndexes: []*pb.FieldIndexInfo{
					{
						FiledID: 1,
						IndexID: 1,
					},
					{
						FiledID: 2,
						IndexID: 2,
					},
					{
						FiledID: 3,
						IndexID: 3,
					},
				},
			},
		},
		collName2ID: map[string]typeutil.UniqueID{
			"GetInitBuildID-Coll-1": 2,
		},
		indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
			1: {
				IndexName: "GetInitBuildID-Index-1",
				IndexID:   1,
			},
			2: {
				IndexName: "GetInitBuildID-Index-2",
				IndexID:   2,
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

	mt.indexID2Meta[3] = pb.IndexInfo{
		IndexName: indexName,
		IndexID:   3,
	}

	mt.segID2IndexMeta = map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo{
		4: {
			1: {
				IndexID:     1,
				EnableIndex: true,
			},
		},
		5: {
			3: {
				IndexID:     3,
				EnableIndex: true,
				ByAutoFlush: false,
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		buildIDs, err := mt.GetInitBuildIDs(collName, indexName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(buildIDs))
	})
}

func TestMetaTable_GetDroppedIndex(t *testing.T) {
	mt := &MetaTable{
		collID2Meta: map[typeutil.UniqueID]pb.CollectionInfo{
			1: {
				FieldIndexes: []*pb.FieldIndexInfo{
					{
						FiledID: 1,
						IndexID: 1,
					},
					{
						FiledID: 2,
						IndexID: 2,
					},
					{
						FiledID: 3,
						IndexID: 3,
					},
				},
			},
		},
		indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
			1: {
				IndexName: "GetInitBuildID-Index-1",
				IndexID:   1,
				Deleted:   true,
			},
			2: {
				IndexName: "GetInitBuildID-Index-2",
				IndexID:   2,
				Deleted:   false,
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
		txn: &mockTestTxnKV{
			multiRemove: func(keys []string) error {
				return nil
			},
		},
		collID2Meta: map[typeutil.UniqueID]pb.CollectionInfo{
			collID: {
				FieldIndexes: []*pb.FieldIndexInfo{
					{
						FiledID: 1,
						IndexID: 1,
					},
				},
			},
		},
		partID2SegID: map[typeutil.UniqueID]map[typeutil.UniqueID]bool{
			partID: {
				100: true,
				101: true,
				102: true,
			},
		},
		indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
			indexID: {
				IndexName: indexName,
				IndexID:   1,
				Deleted:   true,
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
		mt.txn = txn
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
		txn: &mockTestTxnKV{
			multiRemove: func(keys []string) error {
				return nil
			},
			save: func(key, value string) error {
				return nil
			},
		},
		collID2Meta: map[typeutil.UniqueID]pb.CollectionInfo{
			collID: {
				FieldIndexes: []*pb.FieldIndexInfo{
					{
						FiledID: 101,
						IndexID: 1001,
					},
				},
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							FieldID: fieldID,
							Name:    fieldName,
						},
					},
				},
			},
		},
		collName2ID: map[string]typeutil.UniqueID{
			collName: collID,
		},
		partID2SegID: map[typeutil.UniqueID]map[typeutil.UniqueID]bool{
			partID: {
				100: true,
				101: true,
				102: true,
			},
		},
		indexID2Meta: map[typeutil.UniqueID]pb.IndexInfo{
			1001: {
				IndexName: indexName,
				IndexID:   indexID,
				Deleted:   true,
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
		collMeta := pb.CollectionInfo{
			FieldIndexes: []*pb.FieldIndexInfo{
				{
					FiledID: fieldID,
					IndexID: indexID,
				},
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: fieldID,
						Name:    fieldName,
					},
				},
			},
		}
		mt.collID2Meta[collID] = collMeta

		err := mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.Error(t, err)

		mt.indexID2Meta[indexID] = pb.IndexInfo{
			IndexName: "indexName",
			IndexID:   indexID,
		}

		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)
	})

	t.Run("txn save failed", func(t *testing.T) {
		mt.indexID2Meta[indexID] = pb.IndexInfo{
			IndexName: indexName,
			IndexID:   indexID,
		}

		txn := &mockTestTxnKV{
			save: func(key, value string) error {
				return fmt.Errorf("error occurred")
			},
		}
		mt.txn = txn
		err := mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.Error(t, err)

		txn = &mockTestTxnKV{
			save: func(key, value string) error {
				return nil
			},
		}
		mt.txn = txn

		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)

		err = mt.MarkIndexDeleted(collName, fieldName, indexName)
		assert.NoError(t, err)
	})
}
