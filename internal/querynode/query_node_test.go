package querynode

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

const ctxTimeInMillisecond = 200
const closeWithDeadline = true

func setup() {
	Params.Init()
}

func genTestCollectionMeta(collectionName string, collectionID UniqueID) *etcdpb.CollectionMeta {
	fieldVec := schemapb.FieldSchema{
		Name:         "vec",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "metric_type",
				Value: "L2",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:         "age",
		IsPrimaryKey: false,
		DataType:     schemapb.DataType_INT32,
	}

	schema := schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}
	return &collectionMeta
}

func initTestMeta(t *testing.T, node *QueryNode, collectionName string, collectionID UniqueID, segmentID UniqueID) {
	collectionMeta := genTestCollectionMeta(collectionName, collectionID)

	collectionMetaBlob := proto.MarshalTextString(collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = node.replica.addCollection(collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := node.replica.getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, node.replica.getCollectionNum(), 1)

	err = node.replica.addPartition(collection.ID(), collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	err = node.replica.addSegment(segmentID, collectionMeta.PartitionTags[0], collectionID)
	assert.NoError(t, err)
}

func newQueryNode() *QueryNode {

	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	svr := NewQueryNode(ctx, 0)
	return svr

}

func TestMain(m *testing.M) {
	setup()
	exitCode := m.Run()
	os.Exit(exitCode)
}

// NOTE: start pulsar and etcd before test
func TestQueryNode_Start(t *testing.T) {
	localNode := newQueryNode()
	err := localNode.Start()
	assert.Nil(t, err)
	localNode.Close()
}
