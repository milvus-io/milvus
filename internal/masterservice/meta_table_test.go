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

package masterservice

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

type mockTestKV struct {
	kv.TxnKV

	loadWithPrefix               func(key string, ts typeutil.Timestamp) ([]string, []string, error)
	save                         func(key, value string) (typeutil.Timestamp, error)
	multiSave                    func(kvs map[string]string) (typeutil.Timestamp, error)
	multiSaveAndRemoveWithPrefix func(saves map[string]string, removals []string) (typeutil.Timestamp, error)
}

func (m *mockTestKV) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	return m.loadWithPrefix(key, ts)
}
func (m *mockTestKV) Load(key string, ts typeutil.Timestamp) (string, error) {
	return "", nil
}

func (m *mockTestKV) Save(key, value string) (typeutil.Timestamp, error) {
	return m.save(key, value)
}

func (m *mockTestKV) MultiSave(kvs map[string]string) (typeutil.Timestamp, error) {
	return m.multiSave(kvs)
}

func (m *mockTestKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) (typeutil.Timestamp, error) {
	return m.multiSaveAndRemoveWithPrefix(saves, removals)
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
	assert.EqualError(t, err, "MasterService UnmarshalText pb.TenantMeta err:line 1.0: unknown field name \"tenant-prefix\" in milvus.proto.etcd.TenantMeta")

	prefix[TenantMetaPrefix] = []string{proto.MarshalTextString(&pb.TenantMeta{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[ProxyMetaPrefix] = []string{"porxy-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "MasterService UnmarshalText pb.ProxyMeta err:line 1.0: unknown field name \"porxy-meta\" in milvus.proto.etcd.ProxyMeta")

	prefix[ProxyMetaPrefix] = []string{proto.MarshalTextString(&pb.ProxyMeta{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[CollectionMetaPrefix] = []string{"collection-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "MasterService UnmarshalText pb.CollectionInfo err:line 1.0: unknown field name \"collection-meta\" in milvus.proto.etcd.CollectionInfo")

	prefix[CollectionMetaPrefix] = []string{proto.MarshalTextString(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{}})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[PartitionMetaPrefix] = []string{"partition-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "MasterService UnmarshalText pb.PartitionInfo err:line 1.0: unknown field name \"partition-meta\" in milvus.proto.etcd.PartitionInfo")

	prefix[PartitionMetaPrefix] = []string{proto.MarshalTextString(&pb.PartitionInfo{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[SegmentIndexMetaPrefix] = []string{"segment-index-meta"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "MasterService UnmarshalText pb.SegmentIndexInfo err:line 1.0: unknown field name \"segment-index-meta\" in milvus.proto.etcd.SegmentIndexInfo")

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
	assert.EqualError(t, err, "MasterService UnmarshalText pb.IndexInfo err:line 1.0: unknown field name \"index-meta\" in milvus.proto.etcd.IndexInfo")

	prefix[IndexMetaPrefix] = []string{proto.MarshalTextString(&pb.IndexInfo{})}
	m1, err := NewMetaTable(k1)
	assert.Nil(t, err)

	k1.save = func(key, value string) (typeutil.Timestamp, error) {
		return 0, fmt.Errorf("save tenant error")
	}
	_, err = m1.AddTenant(&pb.TenantMeta{})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "save tenant error")

	k1.save = func(key, value string) (typeutil.Timestamp, error) {
		return 0, fmt.Errorf("save proxy error")
	}
	_, err = m1.AddProxy(&pb.ProxyMeta{})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "save proxy error")
}

func TestMetaTable(t *testing.T) {
	const (
		collID        = typeutil.UniqueID(1)
		collIDInvalid = typeutil.UniqueID(2)
		partIDDefault = typeutil.UniqueID(10)
		partID        = typeutil.UniqueID(20)
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
	etcdAddr := Params.EtcdAddress
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	skv, err := newMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7, ftso)
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
		CreateTime:   0,
		PartitionIDs: nil,
	}
	partInfoDefault := &pb.PartitionInfo{
		PartitionName: "_default",
		PartitionID:   partIDDefault,
		SegmentIDs:    nil,
	}
	partInfo := &pb.PartitionInfo{
		PartitionName: "testPart",
		PartitionID:   partID,
		SegmentIDs:    nil,
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

	t.Run("add collection", func(t *testing.T) {
		partInfoDefault.SegmentIDs = []int64{segID}
		_, err = mt.AddCollection(collInfo, partInfoDefault, idxInfo, "")
		assert.NotNil(t, err)
		partInfoDefault.SegmentIDs = []int64{}

		collInfo.PartitionIDs = []int64{segID}
		_, err = mt.AddCollection(collInfo, partInfoDefault, idxInfo, "")
		assert.NotNil(t, err)
		collInfo.PartitionIDs = []int64{}

		_, err = mt.AddCollection(collInfo, partInfoDefault, nil, "")
		assert.NotNil(t, err)

		_, err = mt.AddCollection(collInfo, partInfoDefault, idxInfo, "")
		assert.Nil(t, err)

		collMeta, err := mt.GetCollectionByName("testColl", 0)
		assert.Nil(t, err)
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
		_, err := mt.AddPartition(collID, partInfo.PartitionName, partInfo.PartitionID, "")
		assert.Nil(t, err)

		// check DD operation flag
		flag, err := mt.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	t.Run("add segment", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			ID:           segID,
			CollectionID: collID,
			PartitionID:  partID,
		}
		_, err := mt.AddSegment(seg)
		assert.Nil(t, err)

		_, err = mt.AddSegment(seg)
		assert.NotNil(t, err)

		seg.ID = segID2
		seg.CollectionID = collIDInvalid
		_, err = mt.AddSegment(seg)
		assert.NotNil(t, err)

		seg.CollectionID = collID
		seg.PartitionID = partIDInvalid
		_, err = mt.AddSegment(seg)
		assert.NotNil(t, err)

		seg.PartitionID = partID
		_, err = mt.AddSegment(seg)
		assert.Nil(t, err)
	})

	t.Run("add segment index", func(t *testing.T) {
		segIdxInfo := pb.SegmentIndexInfo{
			SegmentID: segID,
			FieldID:   fieldID,
			IndexID:   indexID,
			BuildID:   buildID,
		}
		_, err := mt.AddIndex(&segIdxInfo)
		assert.Nil(t, err)

		// it's legal to add index twice
		_, err = mt.AddIndex(&segIdxInfo)
		assert.Nil(t, err)

		segIdxInfo.BuildID = 202
		_, err = mt.AddIndex(&segIdxInfo)
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

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo)
		assert.NotNil(t, err)
		seg, field, err := mt.GetNotIndexedSegments("testColl", "field110", idxInfo)
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

		seg, field, err = mt.GetNotIndexedSegments("testColl", "field110", idxInfo)
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
		_, err := mt.AddTenant(&te)
		assert.Nil(t, err)
		po := pb.ProxyMeta{
			ID: 101,
		}
		_, err = mt.AddProxy(&po)
		assert.Nil(t, err)

		_, err = NewMetaTable(skv)
		assert.Nil(t, err)
	})

	t.Run("drop index", func(t *testing.T) {
		_, idx, ok, err := mt.DropIndex("testColl", "field110", "field110")
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, indexID, idx)

		_, _, ok, err = mt.DropIndex("testColl", "field110", "field110-error")
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
		_, id, err := mt.DeletePartition(collID, partInfo.PartitionName, "")
		assert.Nil(t, err)
		assert.Equal(t, partID, id)

		// check DD operation flag
		flag, err := mt.client.Load(DDMsgSendPrefix, 0)
		assert.Nil(t, err)
		assert.Equal(t, "false", flag)
	})

	t.Run("drop collection", func(t *testing.T) {
		_, err = mt.DeleteCollection(collIDInvalid, "")
		assert.NotNil(t, err)
		_, err = mt.DeleteCollection(collID, "")
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
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("multi save error")
		}
		collInfo.PartitionIDs = nil
		_, err := mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "multi save error")
	})

	t.Run("delete collection failed", func(t *testing.T) {
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.multiSaveAndRemoveWithPrefix = func(save map[string]string, keys []string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("milti save and remove with prefix error")
		}
		collInfo.PartitionIDs = nil
		_, err := mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)
		mt.partitionID2Meta = make(map[typeutil.UniqueID]pb.PartitionInfo)
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		_, err = mt.DeleteCollection(collInfo.ID, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "milti save and remove with prefix error")
	})

	t.Run("get collection failed", func(t *testing.T) {
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}

		collInfo.PartitionIDs = nil
		_, err := mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		seg := &datapb.SegmentInfo{
			ID:           100,
			CollectionID: collID,
			PartitionID:  partID,
		}
		_, err = mt.AddSegment(seg)
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.GetCollectionByName(collInfo.Schema.Name, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection: %s", collInfo.Schema.Name))

		_, err = mt.GetCollectionBySegmentID(seg.ID)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection id: %d", collInfo.ID))

		mt.segID2CollID = make(map[int64]int64)
		_, err = mt.GetCollectionBySegmentID(seg.ID)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("segment id %d not belong to any collection", seg.ID))
	})

	t.Run("add partition failed", func(t *testing.T) {
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		_, err = mt.AddPartition(2, "no-part", 22, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "can't find collection. id = 2")

		coll := mt.collID2Meta[collInfo.ID]
		coll.PartitionIDs = make([]int64, Params.MaxPartitionNum)
		mt.collID2Meta[coll.ID] = coll
		_, err = mt.AddPartition(coll.ID, "no-part", 22, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("maximum partition's number should be limit to %d", Params.MaxPartitionNum))

		coll.PartitionIDs = []int64{partInfo.PartitionID}
		mt.collID2Meta[coll.ID] = coll
		mt.partitionID2Meta = make(map[int64]pb.PartitionInfo)
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("multi save error")
		}
		_, err = mt.AddPartition(coll.ID, "no-part", 22, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)
		_, err = mt.AddPartition(coll.ID, partInfo.PartitionName, 22, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition name = %s already exists", partInfo.PartitionName))

		_, err = mt.AddPartition(coll.ID, "no-part", partInfo.PartitionID, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition id = %d already exists", partInfo.PartitionID))
	})

	t.Run("has partition failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		mt.partitionID2Meta = make(map[int64]pb.PartitionInfo)
		assert.False(t, mt.HasPartition(collInfo.ID, partInfo.PartitionName, 0))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		assert.False(t, mt.HasPartition(collInfo.ID, partInfo.PartitionName, 0))
	})

	t.Run("delete partition failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		_, _, err = mt.DeletePartition(collInfo.ID, Params.DefaultPartitionName, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "default partition cannot be deleted")

		_, _, err = mt.DeletePartition(collInfo.ID, "abc", "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "partition abc does not exist")

		pm := mt.partitionID2Meta[partInfo.PartitionID]
		pm.SegmentIDs = []int64{11, 12, 13}
		mt.partitionID2Meta[pm.PartitionID] = pm
		mockKV.multiSaveAndRemoveWithPrefix = func(saves map[string]string, removals []string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("multi save and remove with prefix error")
		}
		_, _, err = mt.DeletePartition(collInfo.ID, pm.PartitionName, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "multi save and remove with prefix error")

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, _, err = mt.DeletePartition(collInfo.ID, "abc", "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection id = %d", collInfo.ID))

		_, err = mt.GetPartitionByID(1, 11, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "partition id = 11 not exist")
	})

	t.Run("add segment failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		noPart := pb.PartitionInfo{
			PartitionName: "no-part",
			PartitionID:   partInfo.PartitionID + 1,
			SegmentIDs:    nil,
		}
		mt.partitionID2Meta[noPart.PartitionID] = noPart

		seg := &datapb.SegmentInfo{
			ID:           100,
			CollectionID: collInfo.ID,
			PartitionID:  noPart.PartitionID,
		}
		_, err = mt.AddSegment(seg)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition id = %d, not belong to collection id = %d", seg.PartitionID, seg.CollectionID))

		seg = &datapb.SegmentInfo{
			ID:           11,
			CollectionID: collInfo.ID,
			PartitionID:  partInfo.PartitionID,
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("save error")
		}
		_, err = mt.AddSegment(seg)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "save error")
	})

	t.Run("add index failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		seg := &datapb.SegmentInfo{
			ID:           100,
			CollectionID: collID,
			PartitionID:  partID,
		}
		_, err = mt.AddSegment(seg)
		assert.Nil(t, err)

		segIdxInfo := &pb.SegmentIndexInfo{
			SegmentID: segID,
			FieldID:   fieldID,
			IndexID:   indexID2,
			BuildID:   buildID,
		}
		_, err = mt.AddIndex(segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", segIdxInfo.IndexID))

		mt.segID2PartitionID = make(map[int64]int64)
		_, err = mt.AddIndex(segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("segment id = %d not belong to any partition", segIdxInfo.SegmentID))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.AddIndex(segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection id = %d not found", collInfo.ID))

		mt.segID2CollID = make(map[int64]int64)
		_, err = mt.AddIndex(segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("segment id = %d not belong to any collection", segIdxInfo.SegmentID))

		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)
		_, err = mt.AddSegment(seg)
		assert.Nil(t, err)

		segIdxInfo.IndexID = indexID
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("save error")
		}
		_, err = mt.AddIndex(segIdxInfo)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "save error")
	})

	t.Run("drop index failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		_, _, _, err = mt.DropIndex("abc", "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name = abc not exist")

		mt.collName2ID["abc"] = 2
		_, _, _, err = mt.DropIndex("abc", "abc", "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection name  = abc not has meta")

		_, _, _, err = mt.DropIndex(collInfo.Schema.Name, "abc", "abc")
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
		_, idxID, isDroped, err := mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName)
		assert.Zero(t, idxID)
		assert.False(t, isDroped)
		assert.Nil(t, err)

		err = mt.reloadFromKV()
		assert.Nil(t, err)
		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)
		mt.partitionID2Meta = make(map[int64]pb.PartitionInfo)
		mockKV.multiSaveAndRemoveWithPrefix = func(saves map[string]string, removals []string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("multi save and remove with prefix error")
		}
		_, _, _, err = mt.DropIndex(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "multi save and remove with prefix error")
	})

	t.Run("get segment index info by id", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		_, err = mt.GetSegmentIndexInfoByID(segID2, fieldID, "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "segment id 101 hasn't flushed, there is no index meta")

		err = mt.AddFlushedSegment(segID2)
		assert.Nil(t, err)
		seg, err := mt.GetSegmentIndexInfoByID(segID2, fieldID, "abc")
		assert.Nil(t, err)
		assert.Equal(t, segID2, seg.SegmentID)
		assert.Equal(t, fieldID, seg.FieldID)
		assert.Equal(t, false, seg.EnableIndex)

		segInfo := &datapb.SegmentInfo{
			ID:           100,
			CollectionID: collID,
			PartitionID:  partID,
		}
		_, err = mt.AddSegment(segInfo)
		assert.Nil(t, err)
		segIdx := &pb.SegmentIndexInfo{
			SegmentID: segID,
			FieldID:   fieldID,
			IndexID:   indexID,
			BuildID:   buildID,
		}
		_, err = mt.AddIndex(segIdx)
		assert.Nil(t, err)
		idx, err := mt.GetSegmentIndexInfoByID(segIdx.SegmentID, segIdx.FieldID, idxInfo[0].IndexName)
		assert.Nil(t, err)
		assert.Equal(t, segIdx.IndexID, idx.IndexID)

		_, err = mt.GetSegmentIndexInfoByID(segIdx.SegmentID, segIdx.FieldID, "abc")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find index name = abc on segment = %d, with filed id = %d", segIdx.SegmentID, segIdx.FieldID))

		_, err = mt.GetSegmentIndexInfoByID(segIdx.SegmentID, 11, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find index name = %s on segment = %d, with filed id = 11", idxInfo[0].IndexName, segIdx.SegmentID))
	})

	t.Run("get field schema failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
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
		mt.segID2IndexMeta = make(map[int64]*map[int64]pb.SegmentIndexInfo)
		mt.segID2IndexMeta[idx.SegmentID] = &idxMeta

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
		_, _, err = mt.GetNotIndexedSegments("abc", "no-field", idx)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "collection abc not found")

		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)

		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, "no-field", idx)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s doesn't have filed no-field", collInfo.Schema.Name))

		bakMeta := mt.indexID2Meta
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", idxInfo[0].IndexID))
		mt.indexID2Meta = bakMeta

		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("multi save error")
		}
		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)
		coll := mt.collID2Meta[collInfo.ID]
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
		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("multi save error")
		}
		_, _, err = mt.GetNotIndexedSegments(collInfo.Schema.Name, collInfo.Schema.Fields[0].Name, idx)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "multi save error")
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

		mockKV.multiSave = func(kvs map[string]string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		mockKV.save = func(key, value string) (typeutil.Timestamp, error) {
			return 0, nil
		}
		err = mt.reloadFromKV()
		assert.Nil(t, err)

		collInfo.PartitionIDs = nil
		_, err = mt.AddCollection(collInfo, partInfo, idxInfo, "")
		assert.Nil(t, err)
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		_, _, err = mt.GetIndexByName(collInfo.Schema.Name, idxInfo[0].IndexName)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("index id = %d not found", idxInfo[0].IndexID))

		_, err = mt.GetIndexByID(idxInfo[0].IndexID)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("cannot find index, id = %d", idxInfo[0].IndexID))
	})

	t.Run("add flused segment failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromKV()
		assert.Nil(t, err)
		err = mt.AddFlushedSegment(222)
		assert.Nil(t, err)
		err = mt.AddFlushedSegment(222)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "segment id = 222 exist")
	})
}
