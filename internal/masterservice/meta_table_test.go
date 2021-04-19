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
	etcdAddr := "127.0.0.1:2379"
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
		CreateTime:   0,
		PartitionIDs: nil,
	}
	partInfo := &pb.PartitionInfo{
		PartitionName: "testPart",
		PartitionID:   10,
		SegmentIDs:    nil,
	}

	t.Run("add collection", func(t *testing.T) {
		err = mt.AddCollection(collInfo, partInfo)
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
			IndexID:   200,
			BuildID:   201,
		}
		idx := pb.IndexInfo{
			IndexName: "idx200",
			IndexID:   200,
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
		}
		err := mt.AddIndex(&seg, &idx)
		assert.Nil(t, err)
		assert.NotNil(t, mt.AddIndex(&seg, &idx))
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

		_, field, err := mt.GetNotIndexedSegments("collTest", "field110", params)
		assert.NotNil(t, err)
		seg, field, err := mt.GetNotIndexedSegments("testColl", "field110", params)
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

		seg, field, err = mt.GetNotIndexedSegments("testColl", "field110", params)
		assert.Nil(t, err)
		assert.Equal(t, len(seg), 2)
		assert.Equal(t, seg[0], int64(100))
		assert.Equal(t, seg[1], int64(101))
		assert.True(t, EqualKeyPairArray(field.TypeParams, tparams))

	})

	t.Run("get index by name", func(t *testing.T) {
		idx, err := mt.GetIndexByName("testColl", "field110", "idx200")
		assert.Nil(t, err)
		assert.Equal(t, len(idx), 1)
		assert.Equal(t, idx[0].IndexID, int64(200))
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

}
