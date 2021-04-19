package datanode

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/datanode/factory"
)

func TestCollection_newCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(1)
	Factory := &factory.MetaFactory{}
	collectionMeta := Factory.CollectionMetaFactory(collectionID, collectionName)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
}

func TestCollection_deleteCollection(t *testing.T) {
	collectionName := "collection0"
	collectionID := UniqueID(1)
	Factory := &factory.MetaFactory{}
	collectionMeta := Factory.CollectionMetaFactory(collectionID, collectionName)

	schemaBlob := proto.MarshalTextString(collectionMeta.Schema)
	assert.NotEqual(t, "", schemaBlob)

	collection := newCollection(collectionMeta.ID, schemaBlob)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
}
