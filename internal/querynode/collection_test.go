package querynode

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestCollection_Partitions(t *testing.T) {
	node := newQueryNode()
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
	collectionMeta := genTestCollectionMeta(collectionName, collectionID)

	collectionMetaBlob := proto.MarshalTextString(collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
}

func TestCollection_deleteCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionName, collectionID)

	collectionMetaBlob := proto.MarshalTextString(collectionMeta)
	assert.NotEqual(t, "", collectionMetaBlob)

	collection := newCollection(collectionMeta, collectionMetaBlob)
	assert.Equal(t, collection.meta.Schema.Name, collectionName)
	assert.Equal(t, collection.meta.ID, collectionID)
	deleteCollection(collection)
}
