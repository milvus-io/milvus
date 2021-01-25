package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection_newCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(1)
	Factory := &MetaFactory{}
	collectionMeta := Factory.CollectionMetaFactory(collectionID, collectionName)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
}

func TestCollection_deleteCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(1)
	Factory := &MetaFactory{}
	collectionMeta := Factory.CollectionMetaFactory(collectionID, collectionName)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
}
