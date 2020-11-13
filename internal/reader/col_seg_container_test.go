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

//----------------------------------------------------------------------------------------------------- collection
func TestColSegContainer_addCollection(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
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
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			&fieldVec, &fieldInt,
		},
	}

	collectionMeta := etcdpb.CollectionMeta{
		ID:            UniqueID(0),
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)
}

func TestColSegContainer_removeCollection(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		Name: collectionName,
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

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	err = (*node.container).removeCollection(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, (*node.container).getCollectionNum(), 0)
}

func TestColSegContainer_getCollectionByID(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
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
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	targetCollection, err := (*node.container).getCollectionByID(UniqueID(0))
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.meta.Schema.Name, "collection0")
	assert.Equal(t, targetCollection.meta.ID, UniqueID(0))
}

func TestColSegContainer_getCollectionByName(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
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
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	targetCollection, err := (*node.container).getCollectionByName("collection0")
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.meta.Schema.Name, "collection0")
	assert.Equal(t, targetCollection.meta.ID, UniqueID(0))
}

//----------------------------------------------------------------------------------------------------- partition
func TestColSegContainer_addPartition(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.container).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.container).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, "default")
	}
}

func TestColSegContainer_removePartition(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
	partitionTag := "default"
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{partitionTag},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.container).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.container).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, partitionTag)
		err = (*node.container).removePartition(collectionID, partitionTag)
		assert.NoError(t, err)
	}
}

func TestColSegContainer_getPartitionByTag(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.container).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.container).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, "default")
		assert.NotNil(t, partition)
	}
}

//----------------------------------------------------------------------------------------------------- segment
func TestColSegContainer_addSegment(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	err = (*node.container).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.container).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.container).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}
}

func TestColSegContainer_removeSegment(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	err = (*node.container).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.container).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.container).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		err = (*node.container).removeSegment(UniqueID(i))
		assert.NoError(t, err)
	}
}

func TestColSegContainer_getSegmentByID(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	err = (*node.container).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.container).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.container).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}
}

func TestColSegContainer_hasSegment(t *testing.T) {
	ctx := context.Background()
	pulsarURL := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarURL)

	collectionName := "collection0"
	collectionID := UniqueID(0)
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
		ID:            collectionID,
		Schema:        &schema,
		CreateTime:    Timestamp(0),
		SegmentIDs:    []UniqueID{0},
		PartitionTags: []string{"default"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.container).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.container).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.container).getCollectionNum(), 1)

	err = (*node.container).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.container).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.container).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		hasSeg := (*node.container).hasSegment(UniqueID(i))
		assert.Equal(t, hasSeg, true)
		hasSeg = (*node.container).hasSegment(UniqueID(i + 100))
		assert.Equal(t, hasSeg, false)
	}
}
