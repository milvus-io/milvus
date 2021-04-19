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
func TestColSegContainer_getCollectionNum(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	assert.Equal(t, (*node.replica).getCollectionNum(), 1)
}

func TestColSegContainer_addCollection(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)
}

func TestColSegContainer_removeCollection(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).removeCollection(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, (*node.replica).getCollectionNum(), 0)
}

func TestColSegContainer_getCollectionByID(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	targetCollection, err := (*node.replica).getCollectionByID(UniqueID(0))
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.meta.Schema.Name, "collection0")
	assert.Equal(t, targetCollection.meta.ID, UniqueID(0))
}

func TestColSegContainer_getCollectionByName(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	targetCollection, err := (*node.replica).getCollectionByName("collection0")
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.meta.Schema.Name, "collection0")
	assert.Equal(t, targetCollection.meta.ID, UniqueID(0))
}

func TestColSegContainer_hasCollection(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	hasCollection := (*node.replica).hasCollection(UniqueID(0))
	assert.Equal(t, hasCollection, true)
	hasCollection = (*node.replica).hasCollection(UniqueID(1))
	assert.Equal(t, hasCollection, false)
}

//----------------------------------------------------------------------------------------------------- partition
func TestColSegContainer_getPartitionNum(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.replica).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.replica).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, "default")
	}

	partitionNum, err := (*node.replica).getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 1)
}

func TestColSegContainer_addPartition(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.replica).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.replica).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, "default")
	}
}

func TestColSegContainer_removePartition(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.replica).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.replica).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, partitionTag)
		err = (*node.replica).removePartition(collectionID, partitionTag)
		assert.NoError(t, err)
	}
}

func TestColSegContainer_addPartitionsByCollectionMeta(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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
		PartitionTags: []string{"p0"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	collectionMeta.PartitionTags = []string{"p0", "p1", "p2"}

	err = (*node.replica).addPartitionsByCollectionMeta(&collectionMeta)
	assert.NoError(t, err)
	partitionNum, err := (*node.replica).getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 3)
	hasPartition := (*node.replica).hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, true)
	hasPartition = (*node.replica).hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, true)
	hasPartition = (*node.replica).hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, true)
}

func TestColSegContainer_removePartitionsByCollectionMeta(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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
		PartitionTags: []string{"p0", "p1", "p2"},
	}

	collectionMetaBlob := proto.MarshalTextString(&collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	collectionMeta.PartitionTags = []string{"p0"}

	err = (*node.replica).addPartitionsByCollectionMeta(&collectionMeta)
	assert.NoError(t, err)
	partitionNum, err := (*node.replica).getPartitionNum(UniqueID(0))
	assert.NoError(t, err)
	assert.Equal(t, partitionNum, 1)
	hasPartition := (*node.replica).hasPartition(UniqueID(0), "p0")
	assert.Equal(t, hasPartition, true)
	hasPartition = (*node.replica).hasPartition(UniqueID(0), "p1")
	assert.Equal(t, hasPartition, false)
	hasPartition = (*node.replica).hasPartition(UniqueID(0), "p2")
	assert.Equal(t, hasPartition, false)
}

func TestColSegContainer_getPartitionByTag(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	for _, tag := range collectionMeta.PartitionTags {
		err := (*node.replica).addPartition(collectionID, tag)
		assert.NoError(t, err)
		partition, err := (*node.replica).getPartitionByTag(collectionID, tag)
		assert.NoError(t, err)
		assert.Equal(t, partition.partitionTag, "default")
		assert.NotNil(t, partition)
	}
}

func TestColSegContainer_hasPartition(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)
	hasPartition := (*node.replica).hasPartition(UniqueID(0), "default")
	assert.Equal(t, hasPartition, true)
	hasPartition = (*node.replica).hasPartition(UniqueID(0), "default1")
	assert.Equal(t, hasPartition, false)
}

//----------------------------------------------------------------------------------------------------- segment
func TestColSegContainer_addSegment(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.replica).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.replica).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}
}

func TestColSegContainer_removeSegment(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.replica).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.replica).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		err = (*node.replica).removeSegment(UniqueID(i))
		assert.NoError(t, err)
	}
}

func TestColSegContainer_getSegmentByID(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.replica).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.replica).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
	}
}

func TestColSegContainer_hasSegment(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0)

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

	var err = (*node.replica).addCollection(&collectionMeta, collectionMetaBlob)
	assert.NoError(t, err)

	collection, err := (*node.replica).getCollectionByName(collectionName)
	assert.NoError(t, err)

	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, UniqueID(0))
	assert.Equal(t, (*node.replica).getCollectionNum(), 1)

	err = (*node.replica).addPartition(collectionID, collectionMeta.PartitionTags[0])
	assert.NoError(t, err)

	const segmentNum = 3
	for i := 0; i < segmentNum; i++ {
		err := (*node.replica).addSegment(UniqueID(i), collectionMeta.PartitionTags[0], collectionID)
		assert.NoError(t, err)
		targetSeg, err := (*node.replica).getSegmentByID(UniqueID(i))
		assert.NoError(t, err)
		assert.Equal(t, targetSeg.segmentID, UniqueID(i))
		hasSeg := (*node.replica).hasSegment(UniqueID(i))
		assert.Equal(t, hasSeg, true)
		hasSeg = (*node.replica).hasSegment(UniqueID(i + 100))
		assert.Equal(t, hasSeg, false)
	}
}
