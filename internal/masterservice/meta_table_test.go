package masterservice

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
)

type mockTestKV struct {
	kv.TxnBase

	loadWithPrefix        func(key string) ([]string, []string, error)
	save                  func(key, value string) error
	multiSave             func(kvs map[string]string) error
	multiRemoveWithPrefix func(keys []string) error
}

func (m *mockTestKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return m.loadWithPrefix(key)
}

func (m *mockTestKV) Save(key, value string) error {
	return m.save(key, value)
}

func (m *mockTestKV) MultiSave(kvs map[string]string) error {
	return m.multiSave(kvs)
}

func (m *mockTestKV) MultiRemoveWithPrefix(keys []string) error {
	return m.multiRemoveWithPrefix(keys)
}

func Test_MockKV(t *testing.T) {
	k1 := &mockTestKV{}
	prefix := make(map[string][]string)
	k1.loadWithPrefix = func(key string) ([]string, []string, error) {
		if val, ok := prefix[key]; ok {
			return nil, val, nil
		}
		return nil, nil, fmt.Errorf("error test")
	}

	_, err := NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[TenantMetaPrefix] = []string{"true"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[TenantMetaPrefix] = []string{proto.MarshalTextString(&pb.TenantMeta{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[ProxyMetaPrefix] = []string{"true"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[ProxyMetaPrefix] = []string{proto.MarshalTextString(&pb.ProxyMeta{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[CollectionMetaPrefix] = []string{"true"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[CollectionMetaPrefix] = []string{proto.MarshalTextString(&pb.CollectionInfo{Schema: &schemapb.CollectionSchema{}})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[PartitionMetaPrefix] = []string{"true"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[PartitionMetaPrefix] = []string{proto.MarshalTextString(&pb.PartitionInfo{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[SegmentIndexMetaPrefix] = []string{"true"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[SegmentIndexMetaPrefix] = []string{proto.MarshalTextString(&pb.SegmentIndexInfo{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[SegmentIndexMetaPrefix] = []string{proto.MarshalTextString(&pb.SegmentIndexInfo{}), proto.MarshalTextString(&pb.SegmentIndexInfo{})}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[IndexMetaPrefix] = []string{"true"}
	_, err = NewMetaTable(k1)
	assert.NotNil(t, err)

	prefix[IndexMetaPrefix] = []string{proto.MarshalTextString(&pb.IndexInfo{})}
	m1, err := NewMetaTable(k1)
	assert.Nil(t, err)

	k1.save = func(key, value string) error {
		return fmt.Errorf("error test")
	}

	err = m1.AddTenant(&pb.TenantMeta{})
	assert.NotNil(t, err)

	err = m1.AddProxy(&pb.ProxyMeta{})
	assert.NotNil(t, err)
}

func TestMetaTable(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	etcdAddr := Params.EtcdAddress
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	ekv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	assert.NotNil(t, ekv)
	mt, err := NewMetaTable(ekv)
	assert.Nil(t, err)

	collInfo := &pb.CollectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      110,
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
				FiledID: 110,
				IndexID: 10000,
			},
		},
		CreateTime:   0,
		PartitionIDs: nil,
	}
	partInfo := &pb.PartitionInfo{
		PartitionName: "testPart",
		PartitionID:   10,
		SegmentIDs:    nil,
	}
	idxInfo := []*pb.IndexInfo{
		{
			IndexName: "testColl_index_110",
			IndexID:   10000,
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
		partInfo.SegmentIDs = []int64{100}
		err = mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.NotNil(t, err)
		partInfo.SegmentIDs = []int64{}

		collInfo.PartitionIDs = []int64{100}
		err = mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.NotNil(t, err)
		collInfo.PartitionIDs = []int64{}

		err = mt.AddCollection(collInfo, partInfo, nil)
		assert.NotNil(t, err)

		err = mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.Nil(t, err)

		collMeta, err := mt.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, collMeta.PartitionIDs[0], int64(10))
		assert.Equal(t, len(collMeta.PartitionIDs), 1)
		assert.True(t, mt.HasCollection(collInfo.ID))

		field, err := mt.GetFieldSchema("testColl", "field110")
		assert.Nil(t, err)
		assert.Equal(t, field.FieldID, collInfo.Schema.Fields[0].FieldID)
	})

	t.Run("add segment", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			ID:           100,
			CollectionID: 1,
			PartitionID:  10,
		}
		assert.Nil(t, mt.AddSegment(seg))
		assert.NotNil(t, mt.AddSegment(seg))
		seg.ID = 101
		seg.CollectionID = 2
		assert.NotNil(t, mt.AddSegment(seg))
		seg.CollectionID = 1
		seg.PartitionID = 11
		assert.NotNil(t, mt.AddSegment(seg))
		seg.PartitionID = 10
		assert.Nil(t, mt.AddSegment(seg))
	})

	t.Run("add segment index", func(t *testing.T) {
		seg := pb.SegmentIndexInfo{
			SegmentID: 100,
			FieldID:   110,
			IndexID:   10000,
			BuildID:   201,
		}
		err := mt.AddIndex(&seg)
		assert.Nil(t, err)
		assert.NotNil(t, mt.AddIndex(&seg))
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
		assert.Equal(t, len(seg), 1)
		assert.Equal(t, seg[0], int64(101))
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
		assert.Equal(t, len(seg), 2)
		assert.Equal(t, seg[0], int64(100))
		assert.Equal(t, seg[1], int64(101))
		assert.True(t, EqualKeyPairArray(field.TypeParams, tparams))

	})

	t.Run("get index by name", func(t *testing.T) {
		idx, err := mt.GetIndexByName("testColl", "field110", "field110")
		assert.Nil(t, err)
		assert.Equal(t, len(idx), 1)
		assert.Equal(t, idx[0].IndexID, int64(10000))
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

		_, err = mt.GetIndexByName("testColl", "field111", "idx200")
		assert.NotNil(t, err)
		idx, err = mt.GetIndexByName("testColl", "field110", "idx201")
		assert.Nil(t, err)
		assert.Zero(t, len(idx))
	})

	t.Run("reload meta", func(t *testing.T) {
		te := pb.TenantMeta{
			ID: 100,
		}
		err := mt.AddTenant(&te)
		assert.Nil(t, err)
		po := pb.ProxyMeta{
			ID: 101,
		}
		err = mt.AddProxy(&po)
		assert.Nil(t, err)

		_, err = NewMetaTable(ekv)
		assert.Nil(t, err)
	})

	t.Run("drop index", func(t *testing.T) {
		idx, ok, err := mt.DropIndex("testColl", "field110", "field110")
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, idx, int64(10000))

		_, ok, err = mt.DropIndex("testColl", "field110", "field110-error")
		assert.Nil(t, err)
		assert.False(t, ok)

		idxs, err := mt.GetIndexByName("testColl", "field110", "field110")
		assert.Nil(t, err)
		assert.Zero(t, len(idxs))

		idxs, err = mt.GetIndexByName("testColl", "field110", "field110-1")
		assert.Nil(t, err)
		assert.Equal(t, len(idxs), 1)
		assert.Equal(t, idxs[0].IndexID, int64(2001))

		_, err = mt.GetSegmentIndexInfoByID(100, -1, "")
		assert.NotNil(t, err)

	})

	t.Run("drop collection", func(t *testing.T) {
		err := mt.DeleteCollection(2)
		assert.NotNil(t, err)
		err = mt.DeleteCollection(1)
		assert.Nil(t, err)
	})

	/////////////////////////// these tests should run at last, it only used to hit the error lines ////////////////////////
	mockKV := &mockTestKV{}
	mt.client = mockKV

	t.Run("add collection failed", func(t *testing.T) {
		mockKV.loadWithPrefix = func(key string) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string) error {
			return fmt.Errorf("error test")
		}
		collInfo.PartitionIDs = nil
		err := mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.NotNil(t, err)
	})

	t.Run("delete collection failed", func(t *testing.T) {
		mockKV.multiSave = func(kvs map[string]string) error {
			return nil
		}
		mockKV.multiRemoveWithPrefix = func(keys []string) error {
			return fmt.Errorf("error test")
		}
		collInfo.PartitionIDs = nil
		err := mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.Nil(t, err)
		mt.partitionID2Meta = make(map[typeutil.UniqueID]pb.PartitionInfo)
		mt.indexID2Meta = make(map[int64]pb.IndexInfo)
		err = mt.DeleteCollection(collInfo.ID)
		assert.NotNil(t, err)
	})

	t.Run("get collection failed", func(t *testing.T) {
		mockKV.save = func(key, value string) error {
			return nil
		}

		collInfo.PartitionIDs = nil
		err := mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.Nil(t, err)

		seg := &datapb.SegmentInfo{
			ID:           100,
			CollectionID: 1,
			PartitionID:  10,
		}
		assert.Nil(t, mt.AddSegment(seg))

		mt.collID2Meta = make(map[int64]pb.CollectionInfo)
		_, err = mt.GetCollectionByName(collInfo.Schema.Name)
		assert.NotNil(t, err)
		_, err = mt.GetCollectionBySegmentID(seg.ID)
		assert.NotNil(t, err)

		mt.segID2CollID = make(map[int64]int64)
		_, err = mt.GetCollectionBySegmentID(seg.ID)
		assert.NotNil(t, err)
	})
}
