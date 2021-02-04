package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection_Group(t *testing.T) {
	Factory := &MetaFactory{}

	collName := "collection0"
	collID := UniqueID(1)
	collMeta := Factory.CollectionMetaFactory(collID, collName)

	t.Run("new_collection_nil_schema", func(t *testing.T) {
		coll, err := newCollection(collID, nil)
		assert.Error(t, err)
		assert.Nil(t, coll)
	})

	t.Run("new_collection_right_schema", func(t *testing.T) {
		coll, err := newCollection(collID, collMeta.Schema)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		assert.Equal(t, collName, coll.GetName())
		assert.Equal(t, collID, coll.GetID())
		assert.Equal(t, collMeta.Schema, coll.GetSchema())
		assert.Equal(t, *collMeta.Schema, *coll.GetSchema())
	})

	t.Run("getters", func(t *testing.T) {
		coll := new(Collection)
		assert.Empty(t, coll.GetName())
		assert.Empty(t, coll.GetID())
		assert.Empty(t, coll.GetSchema())

		coll, err := newCollection(collID, collMeta.Schema)
		assert.NoError(t, err)
		assert.Equal(t, collName, coll.GetName())
		assert.Equal(t, collID, coll.GetID())
		assert.Equal(t, collMeta.Schema, coll.GetSchema())
		assert.Equal(t, *collMeta.Schema, *coll.GetSchema())
	})

}
