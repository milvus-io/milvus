package reader

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestPartition_Segments(t *testing.T) {
	ctx := context.Background()
	pulsarUrl := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarUrl)

	fieldVec := schemapb.FieldSchema{
		Name:     "vec",
		DataType: schemapb.DataType_VECTOR_FLOAT,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "16",
			},
		},
	}

	fieldInt := schemapb.FieldSchema{
		Name:     "age",
		DataType: schemapb.DataType_INT32,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "1",
			},
		},
	}

	schema := schemapb.CollectionSchema{
		Name: "collection0",
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIds:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var collection = node.container.addCollection(&collectionMeta, collectionMetaBlob)

	assert.Equal(t, collection.meta.Schema.Name, "collection0")
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, len(node.container.collections), 1)

	for _, tag := range collectionMeta.PartitionTags {
		_, err := node.container.addPartition(collection, tag)
		assert.NoError(t, err)
	}

	partitions := collection.Partitions()
	assert.Equal(t, len(collectionMeta.PartitionTags), len(*partitions))

	targetPartition := (*partitions)[0]

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		_, err := node.container.addSegment(collection, targetPartition, UniqueID(i))
		assert.NoError(t, err)
	}

	segments := targetPartition.Segments()
	assert.Equal(t, segmentNum, len(*segments))
}

func TestPartition_newPartition(t *testing.T) {
	partitionTag := "default"
	partition := newPartition(partitionTag)
	assert.Equal(t, partition.partitionTag, partitionTag)
}
