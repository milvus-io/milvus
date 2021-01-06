package writenode

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/stretchr/testify/assert"
)

func newReplica() collectionReplica {
	collections := make([]*Collection, 0)

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
	}
	return replica
}

func genTestCollectionMeta(collectionName string, collectionID UniqueID) *etcdpb.CollectionMeta {
	fieldVec := schemapb.FieldSchema{
		FieldID:      UniqueID(100),
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
		FieldID:      UniqueID(101),
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

func initTestMeta(t *testing.T, replica collectionReplica, collectionName string, collectionID UniqueID, segmentID UniqueID) {
	collectionMeta := genTestCollectionMeta(collectionName, collectionID)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	var err = replica.addCollection(collectionMeta.ID, schemaBlob)
	assert.NoError(t, err)

	collection, err := replica.getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, replica.getCollectionNum(), 1)

}

//----------------------------------------------------------------------------------------------------- collection
func TestCollectionReplica_getCollectionNum(t *testing.T) {
	replica := newReplica()
	initTestMeta(t, replica, "collection0", 0, 0)
	assert.Equal(t, replica.getCollectionNum(), 1)
}

func TestCollectionReplica_addCollection(t *testing.T) {
	replica := newReplica()
	initTestMeta(t, replica, "collection0", 0, 0)
}

func TestCollectionReplica_removeCollection(t *testing.T) {
	replica := newReplica()
	initTestMeta(t, replica, "collection0", 0, 0)
	assert.Equal(t, replica.getCollectionNum(), 1)

	err := replica.removeCollection(0)
	assert.NoError(t, err)
	assert.Equal(t, replica.getCollectionNum(), 0)
}

func TestCollectionReplica_getCollectionByID(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, replica, collectionName, collectionID, 0)
	targetCollection, err := replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.Name(), collectionName)
	assert.Equal(t, targetCollection.ID(), collectionID)
}

func TestCollectionReplica_getCollectionByName(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, replica, collectionName, collectionID, 0)

	targetCollection, err := replica.getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.Name(), collectionName)
	assert.Equal(t, targetCollection.ID(), collectionID)

}

func TestCollectionReplica_hasCollection(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, replica, collectionName, collectionID, 0)

	hasCollection := replica.hasCollection(collectionID)
	assert.Equal(t, hasCollection, true)
	hasCollection = replica.hasCollection(UniqueID(1))
	assert.Equal(t, hasCollection, false)

}

func TestCollectionReplica_freeAll(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, replica, collectionName, collectionID, 0)

}
