package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection_newCollection(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)
}

func TestCollection_deleteCollection(t *testing.T) {
	collectionID := UniqueID(0)
	collectionMeta := genTestCollectionMeta(collectionID, false)

	collection := newCollection(collectionMeta.ID, collectionMeta.Schema)
	assert.Equal(t, collection.ID(), collectionID)
	deleteCollection(collection)
}
