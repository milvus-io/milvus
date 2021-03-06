package masterservice

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.etcd.io/etcd/clientv3"
)

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
		err = mt.AddCollection(collInfo, partInfo, idxInfo)
		assert.Nil(t, err)

		collMeta, err := mt.GetCollectionByName("testColl")
		assert.Nil(t, err)
		assert.Equal(t, collMeta.PartitionIDs[0], int64(10))
		assert.Equal(t, len(collMeta.PartitionIDs), 1)
	})

	t.Run("add segment", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			SegmentID:    100,
			CollectionID: 1,
			PartitionID:  10,
		}
		assert.Nil(t, mt.AddSegment(seg))
		assert.NotNil(t, mt.AddSegment(seg))
		seg.SegmentID = 101
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

}
