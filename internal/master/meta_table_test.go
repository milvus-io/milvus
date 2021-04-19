package master

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.etcd.io/etcd/clientv3"
)

func TestMetaTable_Collection(t *testing.T) {
	Init()

	etcdAddr := Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	colMeta := pb.CollectionMeta{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	colMeta2 := pb.CollectionMeta{
		ID: 50,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	colMeta3 := pb.CollectionMeta{
		ID: 30,
		Schema: &schemapb.CollectionSchema{
			Name: "coll2",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	colMeta4 := pb.CollectionMeta{
		ID: 30,
		Schema: &schemapb.CollectionSchema{
			Name: "coll2",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{1},
		PartitionTags: []string{},
	}
	colMeta5 := pb.CollectionMeta{
		ID: 30,
		Schema: &schemapb.CollectionSchema{
			Name: "coll2",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{"1"},
	}
	segID1 := pb.SegmentMeta{
		SegmentID:    200,
		CollectionID: 100,
		PartitionTag: "p1",
	}
	segID2 := pb.SegmentMeta{
		SegmentID:    300,
		CollectionID: 100,
		PartitionTag: "p1",
	}
	segID3 := pb.SegmentMeta{
		SegmentID:    400,
		CollectionID: 100,
		PartitionTag: "p2",
	}
	err = meta.AddCollection(&colMeta)
	assert.Nil(t, err)
	err = meta.AddCollection(&colMeta2)
	assert.NotNil(t, err)
	err = meta.AddCollection(&colMeta3)
	assert.Nil(t, err)
	err = meta.AddCollection(&colMeta4)
	assert.NotNil(t, err)
	err = meta.AddCollection(&colMeta5)
	assert.NotNil(t, err)

	collsName, err := meta.ListCollections()
	assert.Nil(t, err)
	assert.Equal(t, len(collsName), 2)
	e1 := reflect.DeepEqual(collsName, []string{"coll1", "coll2"})
	e2 := reflect.DeepEqual(collsName, []string{"coll2", "coll1"})
	assert.True(t, e1 || e2)

	hasCollection := meta.HasCollection(colMeta.ID)
	assert.True(t, hasCollection)
	err = meta.AddPartition(colMeta.ID, "p1")
	assert.Nil(t, err)
	err = meta.AddPartition(colMeta.ID, "p2")
	assert.Nil(t, err)
	err = meta.AddSegment(&segID1)
	assert.Nil(t, err)
	err = meta.AddSegment(&segID2)
	assert.Nil(t, err)
	err = meta.AddSegment(&segID3)
	assert.Nil(t, err)
	getColMeta, err := meta.GetCollectionByName("coll5")
	assert.NotNil(t, err)
	assert.Nil(t, getColMeta)
	getColMeta, err = meta.GetCollectionByName(colMeta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(getColMeta.SegmentIDs))
	err = meta.DeleteCollection(colMeta.ID)
	assert.Nil(t, err)
	err = meta.DeleteCollection(500)
	assert.NotNil(t, err)
	hasCollection = meta.HasCollection(colMeta.ID)
	assert.False(t, hasCollection)
	_, err = meta.GetSegmentByID(segID1.SegmentID)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentByID(segID2.SegmentID)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentByID(segID3.SegmentID)
	assert.NotNil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(meta.proxyID2Meta))
	assert.Equal(t, 0, len(meta.tenantID2Meta))
	assert.Equal(t, 1, len(meta.collName2ID))
	assert.Equal(t, 1, len(meta.collID2Meta))
	assert.Equal(t, 0, len(meta.segID2Meta))

	err = meta.DeleteCollection(colMeta3.ID)
	assert.Nil(t, err)
}

func TestMetaTable_DeletePartition(t *testing.T) {
	Init()

	etcdAddr := Params.EtcdAddress

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	colMeta := pb.CollectionMeta{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	segID1 := pb.SegmentMeta{
		SegmentID:    200,
		CollectionID: 100,
		PartitionTag: "p1",
	}
	segID2 := pb.SegmentMeta{
		SegmentID:    300,
		CollectionID: 100,
		PartitionTag: "p1",
	}
	segID3 := pb.SegmentMeta{
		SegmentID:    400,
		CollectionID: 100,
		PartitionTag: "p2",
	}
	err = meta.AddCollection(&colMeta)
	assert.Nil(t, err)
	err = meta.AddPartition(500, "p1")
	assert.NotNil(t, err)
	err = meta.AddPartition(colMeta.ID, "p1")
	assert.Nil(t, err)
	err = meta.AddPartition(colMeta.ID, "p2")
	assert.Nil(t, err)
	err = meta.AddPartition(colMeta.ID, "p2")
	assert.NotNil(t, err)
	err = meta.AddSegment(&segID1)
	assert.Nil(t, err)
	err = meta.AddSegment(&segID2)
	assert.Nil(t, err)
	err = meta.AddSegment(&segID3)
	assert.Nil(t, err)
	afterCollMeta, err := meta.GetCollectionByName("coll1")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(afterCollMeta.PartitionTags))
	assert.Equal(t, 3, len(afterCollMeta.SegmentIDs))
	err = meta.DeletePartition(100, "p1")
	assert.Nil(t, err)
	err = meta.DeletePartition(500, "p1")
	assert.NotNil(t, err)
	afterCollMeta, err = meta.GetCollectionByName("coll1")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(afterCollMeta.PartitionTags))
	assert.Equal(t, 1, len(afterCollMeta.SegmentIDs))
	hasPartition := meta.HasPartition(colMeta.ID, "p1")
	assert.False(t, hasPartition)
	hasPartition = meta.HasPartition(colMeta.ID, "p2")
	assert.True(t, hasPartition)
	_, err = meta.GetSegmentByID(segID1.SegmentID)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentByID(segID2.SegmentID)
	assert.NotNil(t, err)
	_, err = meta.GetSegmentByID(segID3.SegmentID)
	assert.Nil(t, err)
	afterCollMeta, err = meta.GetCollectionByName("coll1")
	assert.Nil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(meta.proxyID2Meta))
	assert.Equal(t, 0, len(meta.tenantID2Meta))
	assert.Equal(t, 1, len(meta.collName2ID))
	assert.Equal(t, 1, len(meta.collID2Meta))
	assert.Equal(t, 1, len(meta.segID2Meta))

	// delete not exist
	err = meta.DeletePartition(100, "not_exist")
	assert.NotNil(t, err)
}

func TestMetaTable_Segment(t *testing.T) {
	Init()

	etcdAddr := Params.EtcdAddress

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	keys, _, err := meta.client.LoadWithPrefix("")
	assert.Nil(t, err)
	err = meta.client.MultiRemove(keys)
	assert.Nil(t, err)

	colMeta := pb.CollectionMeta{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	segMeta := pb.SegmentMeta{
		SegmentID:    200,
		CollectionID: 100,
		PartitionTag: "p1",
	}
	err = meta.AddCollection(&colMeta)
	assert.Nil(t, err)
	err = meta.AddPartition(colMeta.ID, "p1")
	assert.Nil(t, err)
	err = meta.AddSegment(&segMeta)
	assert.Nil(t, err)
	getSegMeta, err := meta.GetSegmentByID(segMeta.SegmentID)
	assert.Nil(t, err)
	assert.Equal(t, &segMeta, getSegMeta)
	segMeta.NumRows = 111
	segMeta.MemSize = 100000
	err = meta.UpdateSegment(&segMeta)
	assert.Nil(t, err)
	err = meta.CloseSegment(segMeta.SegmentID, Timestamp(11))
	assert.Nil(t, err)
	err = meta.CloseSegment(1000, Timestamp(11))
	assert.NotNil(t, err)
	getSegMeta, err = meta.GetSegmentByID(segMeta.SegmentID)
	assert.Nil(t, err)
	assert.Equal(t, getSegMeta.NumRows, int64(111))
	assert.Equal(t, getSegMeta.CloseTime, uint64(11))
	assert.Equal(t, int64(100000), getSegMeta.MemSize)
	err = meta.DeleteSegment(segMeta.SegmentID)
	assert.Nil(t, err)
	err = meta.DeleteSegment(1000)
	assert.NotNil(t, err)
	getSegMeta, err = meta.GetSegmentByID(segMeta.SegmentID)
	assert.Nil(t, getSegMeta)
	assert.NotNil(t, err)
	getColMeta, err := meta.GetCollectionByName(colMeta.Schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(getColMeta.SegmentIDs))

	meta.tenantID2Meta = make(map[UniqueID]pb.TenantMeta)
	meta.proxyID2Meta = make(map[UniqueID]pb.ProxyMeta)
	meta.collID2Meta = make(map[UniqueID]pb.CollectionMeta)
	meta.collName2ID = make(map[string]UniqueID)
	meta.segID2Meta = make(map[UniqueID]pb.SegmentMeta)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(meta.proxyID2Meta))
	assert.Equal(t, 0, len(meta.tenantID2Meta))
	assert.Equal(t, 1, len(meta.collName2ID))
	assert.Equal(t, 1, len(meta.collID2Meta))
	assert.Equal(t, 0, len(meta.segID2Meta))

}

func TestMetaTable_UpdateSegment(t *testing.T) {
	Init()

	etcdAddr := Params.EtcdAddress

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	colMeta := pb.CollectionMeta{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	segMeta := pb.SegmentMeta{
		SegmentID:    200,
		CollectionID: 100,
		PartitionTag: "p1",
		NumRows:      110,
	}
	err = meta.AddCollection(&colMeta)
	assert.Nil(t, err)
	err = meta.UpdateSegment(&segMeta)
	assert.Nil(t, err)
	seg, err := meta.GetSegmentByID(200)
	assert.Nil(t, err)
	assert.Equal(t, seg.NumRows, int64(110))
	segMeta.NumRows = 210
	err = meta.UpdateSegment(&segMeta)
	assert.Nil(t, err)
	seg, err = meta.GetSegmentByID(200)
	assert.Nil(t, err)
	assert.Equal(t, seg.NumRows, int64(210))
}

func TestMetaTable_AddPartition_Limit(t *testing.T) {
	Init()
	Params.MaxPartitionNum = 256 // adding 4096 partitions is too slow
	etcdAddr := Params.EtcdAddress

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	defer meta.client.Close()

	colMeta := pb.CollectionMeta{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name: "coll1",
		},
		CreateTime:    0,
		SegmentIDs:    []UniqueID{},
		PartitionTags: []string{},
	}
	err = meta.AddCollection(&colMeta)
	assert.Nil(t, err)

	for i := 0; i < int(Params.MaxPartitionNum); i++ {
		err := meta.AddPartition(100, "partition_"+strconv.Itoa(i))
		assert.Nil(t, err)
	}
	err = meta.AddPartition(100, "partition_limit")
	assert.NotNil(t, err)
}
