package querynodeimp

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestCollection_Partitions(t *testing.T) {
	node := newQueryNodeMock()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestMeta(t, node, collectionName, collectionID, 0)

	collection, err := node.replica.getCollectionByName(collectionName)
	assert.NoError(t, err)

	partitions := collection.Partitions()
	assert.Equal(t, 1, len(*partitions))
}

func TestCollection_newCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
}

func TestCollection_deleteCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID, false)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
	deleteCollection(collection)
}
