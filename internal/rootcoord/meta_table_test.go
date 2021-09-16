// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package rootcoord

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type mockTestKV struct {
	kv.TxnKV

	loadWithPrefix               func(key string, ts typeutil.Timestamp) ([]string, []string, error)
	save                         func(key, value string, ts typeutil.Timestamp) error
	multiSave                    func(kvs map[string]string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error
	multiSaveAndRemoveWithPrefix func(saves map[string]string, removals []string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error
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

func (m *mockTestKV) MultiSave(kvs map[string]string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error {
	return m.multiSave(kvs, ts, additions...)
}

func (m *mockTestKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp, additions ...func(ts typeutil.Timestamp) (string, string, error)) error {
	return m.multiSaveAndRemoveWithPrefix(saves, removals, ts, additions...)
}

func Test_MockKV(t *testing.T) {
	k1 := &mockTestKV{}
	prefix := make(map[string][]string)
	k1.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
		if val, ok := prefix[key]; ok {
			return nil, val, nil
		}
		return nil, nil, fmt.Errorf("load prefix error")
	}

	_, err := NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "load prefix error")

	prefix[TenantMetaPrefix] = []string{"tenant-prefix"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "RootCoord UnmarshalText pb.TenantMeta err:line 1.0: unknown field name \"tenant-prefix\" in milvus.proto.etcd.TenantMeta")

	prefix[TenantMetaPrefix] = []string{proto.MarshalTextString(&pb.TenantMeta{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[ProxyMetaPrefix] = []string{"porxy-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "RootCoord UnmarshalText pb.ProxyMeta err:line 1.0: unknown field name \"porxy-meta\" in milvus.proto.etcd.ProxyMeta")

	prefix[ProxyMetaPrefix] = []string{proto.MarshalTextString(&pb.ProxyMeta{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[CollectionMetaPrefix] = []string{"collection-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "RootCoord UnmarshalText pb.CollectionInfo err:line 1.0: unknown field name \"collection-meta\" in milvus.proto.etcd.CollectionInfo")

	prefix[CollectionMetaPrefix] = []string{proto.MarshalTextString(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{}})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[SegmentIndexMetaPrefix] = []string{"segment-index-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "RootCoord UnmarshalText pb.SegmentIndexInfo err:line 1.0: unknown field name \"segment-index-meta\" in milvus.proto.etcd.SegmentIndexInfo")

	prefix[SegmentIndexMetaPrefix] = []string{proto.MarshalTextString(&pb.SegmentIndexInfo{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[SegmentIndexMetaPrefix] = []string{proto.MarshalTextString(&pb.SegmentIndexInfo{}), proto.MarshalTextString(&pb.SegmentIndexInfo{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "load prefix error")

	prefix[IndexMetaPrefix] = []string{"index-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "RootCoord UnmarshalText pb.IndexInfo err:line 1.0: unknown field name \"index-meta\" in milvus.proto.etcd.IndexInfo")

	prefix[IndexMetaPrefix] = []string{proto.MarshalTextString(&pb.IndexInfo{})}
	m1, err := NewMetaTable(k1)
	assert.Nil(t, err)

	k1.save = func(key string, value string, ts typeutil.Timestamp) error {
		return fmt.Errorf("save tenant error")
	}
	assert.Panics(t, func() { m1.AddTenant(&pb.TenantMeta{}, 0) })
	//err = m1.AddTenant(&pb.TenantMeta{}, 0)
	//assert.NotNil(t, err)
	//assert.EqualError(t, err, "save tenant error")

	k1.save = func(key string, value string, ts typeutil.Timestamp) error {
		return fmt.Errorf("save proxy error")
	}
	assert.Panics(t, func() { m1.AddProxy(&pb.ProxyMeta{}, 0) })
	//err = m1.AddProxy(&pb.ProxyMeta{}, 0)
	//assert.NotNil(t, err)
	//assert.EqualError(t, err, "save proxy error")
}

func TestMetaTable(t *testing.T) {
	const (
		collID        = typeutil.UniqueID(1)
		collName      = "testColl"
		collIDInvalid = typeutil.UniqueID(2)
		partIDDefault = typeutil.UniqueID(10)
		partID        = typeutil.UniqueID(20)
		partName      = "testPart"
		partIDInvalid = typeutil.UniqueID(21)
		segID         = typeutil.UniqueID(100)
		segID2        = typeutil.UniqueID(101)
		fieldID       = typeutil.UniqueID(110)
		fieldID2      = typeutil.UniqueID(111)
		indexID       = typeutil.UniqueID(10000)
		indexID2      = typeutil.UniqueID(10001)
		buildID       = typeutil.UniqueID(201)
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

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
	assert.Nil(t, err)
	defer etcdCli.Close()
	skv, err := newMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	mt, err := NewMetaTable(skv)
	assert.Nil(t, err)

	collInfo := &pb.CollectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      fieldID,
					Name:         "field110",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     0,
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
		PartitionNames:             []string{Params.DefaultPartitionName},
		PartitionCreatedTimestamps: []uint64{0},
	}
	idxInfo := []*pb.IndexInfo{
		{
			IndexName: "testColl_index_110",
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

	ddOp := func(ts typeutil.Timestamp) (string, error) {
		return "", nil
	}

	t.Run("add collection", func(t *testing.T) {
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, nil, ddOp)
		assert.NotNil(t, err)

		err = mt.AddCollection(collInfo, ts, idxInfo, ddOp)
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), ts)

		collMeta, err := mt.GetCollectionByName("testColl", 0)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.CreateTime, ts)
		assert.Equal(t, collMeta.PartitionCreatedTimestamps[0], ts)

		assert.Equal(t, partIDDefault, collMeta.PartitionIDs[0])
		assert.Equal(t, 1, len(collMeta.PartitionIDs))
		assert.True(t, mt.HasCollection(collInfo.ID, 0))

		field, err := mt.GetFieldSchema("testColl", "field110")
		assert.Nil(t, err)
		assert.Equal(t, collInfo.Schema.Fields[0].FieldID, field.FieldID)

		// check DD operation flag
		flag, err := mt.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	t.Run("add partition", func(t *testing.T) {
		ts := ftso()
		err = mt.AddPartition(collID, partName, partID, ts, ddOp)
		assert.Nil(t, err)
		assert.Equal(t, ts, uint64(2))

		collMeta, ok := mt.collID2Meta[collID]
		assert.True(t, ok)
		assert.Equal(t, 2, len(collMeta.PartitionNames))
		assert.Equal(t, collMeta.PartitionNames[1], partName)
		assert.Equal(t, ts, collMeta.PartitionCreatedTimestamps[1])

		// check DD operation flag
		flag, err := mt.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	t.Run("add segment index", func(t *testing.T) {
		segIdxInfo := pb.SegmentIndexInfo{
			CollectionID: collID,
			PartitionID:  partID,
			SegmentID:    segID,
			FieldID:      fieldID,
			IndexID:      indexID,
			BuildID:      buildID,
		}
		err = mt.AddIndex(&segIdxInfo, 0)
		assert.Nil(t, err)

		// it's legal to add index twice
		err = mt.AddIndex(&segIdxInfo, 0)
		assert.Nil(t, err)

		segIdxInfo.BuildID = 202
		err = mt.AddIndex(&segIdxInfo, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d exist", segIdxInfo.IndexID))
	})

	t.Run("get not indexed segments", func(t *testing.T) {
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
		idxInfo := &pb.IndexInfo{
			IndexName:   "field110",
			IndexID:     2000,
			IndexParams: params,
		}

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo, nil, 0)
		assert.NotNil(t, err)
		seg, field, err := mt.GetNotIndexedSegments("testColl", "field110", idxInfo, []typeutil.UniqueID{segID, segID2}, 0)
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

		seg, field, err = mt.GetNotIndexedSegments("testColl", "field110", idxInfo, []typeutil.UniqueID{segID, segID2}, 0)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(seg))
		assert.Equal(t, segID, seg[0])
		assert.Equal(t, segID2, seg[1])
		assert.True(t, EqualKeyPairArray(field.TypeParams, tparams))

	})

	t.Run("get index by name", func(t *testing.T) {
		_, idx, err := mt.GetIndexByName("testColl", "field110")
		assert.Nil(t, err)
		assert.Equal(t, 1, len(idx))
		assert.Equal(t, indexID, idx[0].IndexID)
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

		_, idx, err = mt.GetIndexByName("testColl", "idx201")
		assert.Nil(t, err)
		assert.Zero(t, len(idx))
	})

	t.Run("reload meta", func(t *testing.T) {
		te := pb.TenantMeta{
			ID: 100,
		}
		err := mt.AddTenant(&te, 0)
		assert.Nil(t, err)
		po := pb.ProxyMeta{
			ID: 101,
		}
		err = mt.AddProxy(&po, 0)
		assert.Nil(t, err)

		_, err = NewMetaTable(skv)
		assert.Nil(t, err)
	})

	t.Run("drop index", func(t *testing.T) {
		idx, ok, err := mt.DropIndex("testColl", "field110", "field110", 0)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, indexID, idx)

		_, ok, err = mt.DropIndex("testColl", "field110", "field110-error", 0)
		assert.Nil(t, err)
		assert.False(t, ok)

		_, idxs, err := mt.GetIndexByName("testColl", "field110")
		assert.Nil(t, err)
		assert.Zero(t, len(idxs))

		_, idxs, err = mt.GetIndexByName("testColl", "field110-1")
		assert.Nil(t, err)
		assert.Equal(t, len(idxs), 1)
		assert.Equal(t, idxs[0].IndexID, int64(2001))

		_, err = mt.GetSegmentIndexInfoByID(segID, -1, "")
		assert.NotNil(t, err)
	})

	t.Run("drop partition", func(t *testing.T) {
		ts := ftso()
		id, err := mt.DeletePartition(collID, partName, ts, nil)
		assert.Nil(t, err)
		assert.Equal(t, partID, id)

		// check DD operation flag
		flag, err := mt.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	t.Run("drop collection", func(t *testing.T) {
		ts := ftso()
		err = mt.DeleteCollection(collIDInvalid, ts, nil)
		assert.NotNil(t, err)
		err = mt.DeleteCollection(collID, ts, nil)
		assert.Nil(t, err)

		// check DD operation flag
		flag, err := mt.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	/////////////////////////// these tests should run at last, it only used to hit the error lines ////////////////////////
	mockKV := &mockTestKV{}
	mt.client = mockKV

	t.Run("add collection failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save error")
		}
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		assert.Panics(t, func() { mt.AddCollection(collInfo, 0, idxInfo, nil) })
		//err = mt.AddCollection(collInfo, 0, idxInfo, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")
	})

	t.Run("delete collection failed", func(t *testing.T) {
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
			return nil
		}
		mockKV.multiSaveAndRemoveWithPrefix = func(save map[string]string, keys []string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		ts = ftso()
		assert.Panics(t, func() { mt.DeleteCollection(collInfo.ID, ts, nil) })
		//err = mt.DeleteCollection(collInfo.ID, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save and remove with prefix error")
	})

	t.Run("get collection failed", func(t *testing.T) {
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.GetCollectionByName(collInfo.Schema.Name, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection %s with id %d", collInfo.Schema.Name, collInfo.ID))

	})

	t.Run("add partition failed", func(t *testing.T) {
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
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		ts = ftso()
		err = mt.AddPartition(2, "no-part", 22, ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "can't find collection. id = 2")

		coll := mt.collID2Meta[collInfo.ID]
		coll.PartitionIDs = make([]int64, Params.MaxPartitionNum)
		mt.collID2Meta[coll.ID] = coll
		err = mt.AddPartition(coll.ID, "no-part", 22, ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("maximum partition's number should be limit to %d", Params.MaxPartitionNum))

		coll.PartitionIDs = []int64{partID}
		coll.PartitionNames = []string{partName}
		coll.PartitionCreatedTimestamps = []uint64{ftso()}
		mt.collID2Meta[coll.ID] = coll
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save error")
		}
		assert.Panics(t, func() { mt.AddPartition(coll.ID, "no-part", 22, ts, nil) })
		//err = mt.AddPartition(coll.ID, "no-part", 22, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
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
		err = mt.AddPartition(coll.ID, partName, 22, ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition name = %s already exists", partName))
		err = mt.AddPartition(coll.ID, "no-part", partID, ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition id = %d already exists", partID))
	})

	t.Run("has partition failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		assert.False(t, mt.HasPartition(collInfo.ID, "no-partName", 0))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		assert.False(t, mt.HasPartition(collInfo.ID, partName, 0))
	})

	t.Run("delete partition failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
			return nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = []int64{partID}
		collInfo.PartitionNames = []string{partName}
		collInfo.PartitionCreatedTimestamps = []uint64{ftso()}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		ts = ftso()
		_, err = mt.DeletePartition(collInfo.ID, Params.DefaultPartitionName, ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "default partition cannot be deleted")

		_, err = mt.DeletePartition(collInfo.ID, "abc", ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "partition abc does not exist")

		mockKV.multiSaveAndRemoveWithPrefix = func(saves map[string]string, removals []string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}
		assert.Panics(t, func() { mt.DeletePartition(collInfo.ID, partName, ts, nil) })
		//_, err = mt.DeletePartition(collInfo.ID, partName, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save and remove with prefix error")

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.DeletePartition(collInfo.ID, "abc", ts, nil)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection id = %d", collInfo.ID))
	})

	t.Run("add index failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
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
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		segIdxInfo := pb.SegmentIndexInfo{
			CollectionID: collID,
			PartitionID:  partID,
			SegmentID:    segID,
			FieldID:      fieldID,
			IndexID:      indexID2,
			BuildID:      buildID,
		}
		ts = ftso()
		err = mt.AddIndex(&segIdxInfo, ts)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", segIdxInfo.IndexID))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		err = mt.AddIndex(&segIdxInfo, ts)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection id = %d not found", collInfo.ID))

		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts = ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		segIdxInfo.IndexID = indexID
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return fmt.Errorf("save error")
		}
		ts = ftso()
		assert.Panics(t, func() { mt.AddIndex(&segIdxInfo, ts) })
		//err = mt.AddIndex(&segIdxInfo, ts)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "save error")
	})

	t.Run("drop index failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
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
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		ts = ftso()
		_, _, err = mt.DropIndex("abc", "abc", "abc", ts)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name = abc not exist")

		mt.collName2ID["abc"] = 2
		_, _, err = mt.DropIndex("abc", "abc", "abc", ts)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name  = abc not has meta")

		_, _, err = mt.DropIndex(collInfo.Schema.Name, "abc", "abc", ts)
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
		ts = ftso()
		idxID, isDroped, err := mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName, ts)
		assert.Zero(t, idxID)
		assert.False(t, isDroped)
		assert.Nil(t, err)

		err = mt.reloadFromKV()
		assert.Nil(t, err)
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		coll.PartitionCreatedTimestamps = nil
		ts = ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)
		mockKV.multiSaveAndRemoveWithPrefix = func(saves map[string]string, removals []string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}
		ts = ftso()
		assert.Panics(t, func() { mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName, ts) })
		//_, _, err = mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName, ts)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save and remove with prefix error")
	})

	t.Run("get segment index info by id", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
			return nil
		}
		mockKV.save = func(key, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
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
		ts = ftso()
		err = mt.AddIndex(&segIdxInfo, ts)
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

	t.Run("get field schema failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
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
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.unlockGetFieldSchema(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Schema.Name))

		mt.collName2ID = make(map[string]int64)
		_, err = mt.unlockGetFieldSchema(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Schema.Name))
	})

	t.Run("is segment indexed", func(t *testing.T) {
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

	t.Run("get not indexed segments", func(t *testing.T) {
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
		_, _, err = mt.GetNotIndexedSegments("abc", "no-field", idx, nil, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection abc not found")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
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
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)

		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, "no-field", idx, nil, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s doesn't have filed no-field", collInfo.Schema.Name))

		bakMeta := mt.indexID2Meta
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx, nil, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", idxInfo[0].IndexID))
		mt.indexID2Meta = bakMeta

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save error")
		}
		assert.Panics(t, func() { mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx, nil, 0) })
		//_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
			return nil
		}
		collInfo.PartitionIDs = nil
		collInfo.PartitionNames = nil
		collInfo.PartitionCreatedTimestamps = nil
		//err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		//assert.Nil(t, err)
		coll, ok := mt.collID2Meta[collInfo.ID]
		assert.True(t, ok)
		coll.FieldIndexes = append(coll.FieldIndexes, &pb.FieldIndexInfo{FiledID: coll.FieldIndexes[0].FiledID, IndexID: coll.FieldIndexes[0].IndexID + 1})

		mt.collID2Meta[coll.ID] = coll
		anotherIdx := pb.IndexInfo{
			IndexName: "no-index",
			IndexID:   coll.FieldIndexes[1].IndexID,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "no-idx-k1",
					Value: "no-idx-v1",
				},
			},
		}
		mt.indexID2Meta[anotherIdx.IndexID] = anotherIdx

		idx.IndexName = idxInfo[0].IndexName
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			return fmt.Errorf("multi save error")
		}
		assert.Panics(t, func() { mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx, nil, 0) })
		//_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")
	})

	t.Run("get index by name failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		mt.collName2ID["abc"] = 123
		_, _, err = mt.GetIndexByName("abc", "hij")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection abc not found")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp, addition ...func(ts typeutil.Timestamp) (string, string, error)) error {
			for _, a := range addition {
				if a != nil {
					a(ts)
				}
			}
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
		err = mt.AddCollection(collInfo, ts, idxInfo, nil)
		assert.Nil(t, err)
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		_, _, err = mt.GetIndexByName(collInfo.Schema.Name, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", idxInfo[0].IndexID))

		_, err = mt.GetIndexByID(idxInfo[0].IndexID)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("cannot find index, id = %d", idxInfo[0].IndexID))
	})
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

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
	assert.Nil(t, err)
	defer etcdCli.Close()

	skv, err := newMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	mt, err := NewMetaTable(skv)
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
	err = mt.AddCollection(collInfo, t1, nil, nil)
	assert.Nil(t, err)

	collInfo.ID = 2
	collInfo.PartitionIDs = []int64{partID2}
	collInfo.PartitionNames = []string{partName2}
	collInfo.PartitionCreatedTimestamps = []uint64{ftso()}
	collInfo.Schema.Name = collName2

	t2 := ftso()
	err = mt.AddCollection(collInfo, t2, nil, nil)
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
}
