package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"go.etcd.io/etcd/clientv3"
)

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
		SegmentIds:    []int64{200},
		PartitionTags: []string{"p1"},
	}
	seg_id := pb.SegmentMeta{
		SegmentId:    200,
		CollectionId: 100,
		PartitionTag: "p1",
	}
	err = meta.AddCollection(&col_meta)
	assert.Nil(t, err)
	err = meta.AddSegment(&seg_id)
	assert.Nil(t, err)
	err = meta.DeletePartition("p1", "coll1")
	assert.Nil(t, err)
}
