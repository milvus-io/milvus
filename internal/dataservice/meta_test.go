package dataservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	testSchema := newTestSchema()
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:         id,
		Schema:     testSchema,
		Partitions: []UniqueID{100},
	})
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:     id,
		Schema: testSchema,
	})
	assert.NotNil(t, err)
	has := meta.HasCollection(id)
	assert.True(t, has)
	collection, err := meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, id, collection.ID)
	assert.EqualValues(t, testSchema, collection.Schema)
	assert.EqualValues(t, 1, len(collection.Partitions))
	assert.EqualValues(t, 100, collection.Partitions[0])
	err = meta.DropCollection(id)
	assert.Nil(t, err)
	has = meta.HasCollection(id)
	assert.False(t, has)
	_, err = meta.GetCollection(id)
	assert.NotNil(t, err)
}
