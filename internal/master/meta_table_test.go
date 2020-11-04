package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.etcd.io/etcd/clientv3"
)

func TestMetaTable_Collection(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	assert.Nil(t, err)
	etcd_kv := kv.NewEtcdKV(cli, "/etcd/test/root")

	meta, err := NewMetaTable(etcd_kv)
	assert.Nil(t, err)
	defer meta.client.Close()

	col_meta := pb.CollectionMeta{
		Id: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{},
		PartitionTags: []string{},
	}
	col_meta_2 := pb.CollectionMeta{
		Id: 50,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{},
		PartitionTags: []string{},
	}
	col_meta_3 := pb.CollectionMeta{
		Id: 30,
		Schema: &schemapb.CollectionSchema{
			Name: "coll2",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{},
		PartitionTags: []string{},
	}
	col_meta_4 := pb.CollectionMeta{
		Id: 30,
		Schema: &schemapb.CollectionSchema{
			Name: "coll2",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{1},
		PartitionTags: []string{},
	}
	col_meta_5 := pb.CollectionMeta{
		Id: 30,
		Schema: &schemapb.CollectionSchema{
			Name: "coll2",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{1},
		PartitionTags: []string{"1"},
	}
	seg_id_1 := pb.SegmentMeta{
		SegmentId:    200,
		CollectionId: 100,
		PartitionTag: "p1",
	}
	seg_id_2 := pb.SegmentMeta{
		SegmentId:    300,
		CollectionId: 100,
		PartitionTag: "p1",
	}
	seg_id_3 := pb.SegmentMeta{
		SegmentId:    400,
		CollectionId: 100,
		PartitionTag: "p2",
	}
	err = meta.AddCollection(&col_meta)
	assert.Nil(t, err)
	err = meta.AddCollection(&col_meta_2)
	assert.NotNil(t, err)
	err = meta.AddCollection(&col_meta_3)
	assert.Nil(t, err)
	err = meta.AddCollection(&col_meta_4)
	assert.NotNil(t, err)
	err = meta.AddCollection(&col_meta_5)
	assert.NotNil(t, err)
	has_collection := meta.HasCollection(col_meta.Id)
	assert.True(t, has_collection)
	err = meta.AddPartition(col_meta.Id, "p1")
	assert.Nil(t, err)
	err = meta.AddPartition(col_meta.Id, "p2")
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id_1)
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id_2)
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id_3)
	assert.Nil(t, err)
	get_col_meta, err := meta.GetCollectionByName(col_meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(get_col_meta.SegmentIds))
	err = meta.DeleteCollection(col_meta.Id)
	assert.Nil(t, err)
	has_collection = meta.HasCollection(col_meta.Id)
	assert.False(t, has_collection)
	_, err = meta.GetSegmentById(seg_id_1.SegmentId)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentById(seg_id_2.SegmentId)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentById(seg_id_3.SegmentId)
	assert.NotNil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(meta.proxyId2Meta))
	assert.Equal(t, 0, len(meta.tenantId2Meta))
	assert.Equal(t, 1, len(meta.collName2Id))
	assert.Equal(t, 1, len(meta.collId2Meta))
	assert.Equal(t, 0, len(meta.segId2Meta))

	err = meta.DeleteCollection(col_meta_3.Id)
	assert.Nil(t, err)
}

func TestMetaTable_DeletePartition(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	assert.Nil(t, err)
	etcd_kv := kv.NewEtcdKV(cli, "/etcd/test/root")

	meta, err := NewMetaTable(etcd_kv)
	assert.Nil(t, err)
	defer meta.client.Close()

	col_meta := pb.CollectionMeta{
		Id: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{},
		PartitionTags: []string{},
	}
	seg_id_1 := pb.SegmentMeta{
		SegmentId:    200,
		CollectionId: 100,
		PartitionTag: "p1",
	}
	seg_id_2 := pb.SegmentMeta{
		SegmentId:    300,
		CollectionId: 100,
		PartitionTag: "p1",
	}
	seg_id_3 := pb.SegmentMeta{
		SegmentId:    400,
		CollectionId: 100,
		PartitionTag: "p2",
	}
	err = meta.AddCollection(&col_meta)
	assert.Nil(t, err)
	err = meta.AddPartition(col_meta.Id, "p1")
	assert.Nil(t, err)
	err = meta.AddPartition(col_meta.Id, "p2")
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id_1)
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id_2)
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id_3)
	assert.Nil(t, err)
	after_coll_meta, err := meta.GetCollectionByName("coll1")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(after_coll_meta.PartitionTags))
	assert.Equal(t, 3, len(after_coll_meta.SegmentIds))
	err = meta.DeletePartition(100, "p1")
	assert.Nil(t, err)
	after_coll_meta, err = meta.GetCollectionByName("coll1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(after_coll_meta.PartitionTags))
	assert.Equal(t, 1, len(after_coll_meta.SegmentIds))
	has_partition := meta.HasPartition(col_meta.Id, "p1")
	assert.False(t, has_partition)
	has_partition = meta.HasPartition(col_meta.Id, "p2")
	assert.True(t, has_partition)
	_, err = meta.GetSegmentById(seg_id_1.SegmentId)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentById(seg_id_2.SegmentId)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentById(seg_id_3.SegmentId)
	assert.Nil(t, err)
	after_coll_meta, err = meta.GetCollectionByName("coll1")
	assert.Nil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(meta.proxyId2Meta))
	assert.Equal(t, 0, len(meta.tenantId2Meta))
	assert.Equal(t, 1, len(meta.collName2Id))
	assert.Equal(t, 1, len(meta.collId2Meta))
	assert.Equal(t, 1, len(meta.segId2Meta))
}

func TestMetaTable_Segment(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	assert.Nil(t, err)
	etcd_kv := kv.NewEtcdKV(cli, "/etcd/test/root")

	meta, err := NewMetaTable(etcd_kv)
	assert.Nil(t, err)
	defer meta.client.Close()

	keys, _, err := meta.client.LoadWithPrefix("")
	assert.Nil(t, err)
	err = meta.client.MultiRemove(keys)
	assert.Nil(t, err)

	col_meta := pb.CollectionMeta{
		Id: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIds:    []UniqueID{},
		PartitionTags: []string{},
	}
	seg_meta := pb.SegmentMeta{
		SegmentId:    200,
		CollectionId: 100,
		PartitionTag: "p1",
	}
	err = meta.AddCollection(&col_meta)
	assert.Nil(t, err)
	err = meta.AddPartition(col_meta.Id, "p1")
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_meta)
	assert.Nil(t, err)
	get_seg_meta, err := meta.GetSegmentById(seg_meta.SegmentId)
	assert.Nil(t, err)
	assert.Equal(t, &seg_meta, get_seg_meta)
	err = meta.CloseSegment(seg_meta.SegmentId, Timestamp(11), 111)
	assert.Nil(t, err)
	get_seg_meta, err = meta.GetSegmentById(seg_meta.SegmentId)
	assert.Nil(t, err)
	assert.Equal(t, get_seg_meta.NumRows, int64(111))
	assert.Equal(t, get_seg_meta.CloseTime, uint64(11))
	err = meta.DeleteSegment(seg_meta.SegmentId)
	assert.Nil(t, err)
	get_seg_meta, err = meta.GetSegmentById(seg_meta.SegmentId)
	assert.Nil(t, get_seg_meta)
	assert.NotNil(t, err)
	get_col_meta, err := meta.GetCollectionByName(col_meta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(get_col_meta.SegmentIds))

	meta.tenantId2Meta = make(map[UniqueID]pb.TenantMeta)
	meta.proxyId2Meta = make(map[UniqueID]pb.ProxyMeta)
	meta.collId2Meta = make(map[UniqueID]pb.CollectionMeta)
	meta.collName2Id = make(map[string]UniqueID)
	meta.segId2Meta = make(map[UniqueID]pb.SegmentMeta)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(meta.proxyId2Meta))
	assert.Equal(t, 0, len(meta.tenantId2Meta))
	assert.Equal(t, 1, len(meta.collName2Id))
	assert.Equal(t, 1, len(meta.collId2Meta))
	assert.Equal(t, 0, len(meta.segId2Meta))

}
