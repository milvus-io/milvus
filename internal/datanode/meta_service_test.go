package datanode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	collectionID0   = UniqueID(0)
	collectionID1   = UniqueID(1)
	collectionName0 = "collection_0"
	collectionName1 = "collection_1"
)

func TestMetaService_All(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica := newReplica()
	mFactory := &MasterServiceFactory{}
	mFactory.setCollectionID(collectionID0)
	mFactory.setCollectionName(collectionName0)
	ms := newMetaService(ctx, replica, mFactory)

	t.Run("Test getCollectionNames", func(t *testing.T) {
		names, err := ms.getCollectionNames(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(names))
		assert.Equal(t, collectionName0, names[0])
	})

	t.Run("Test createCollection", func(t *testing.T) {
		hasColletion := ms.replica.hasCollection(collectionID0)
		assert.False(t, hasColletion)

		err := ms.createCollection(ctx, collectionName0)
		assert.NoError(t, err)
		hasColletion = ms.replica.hasCollection(collectionID0)
		assert.True(t, hasColletion)
	})

	t.Run("Test loadCollections", func(t *testing.T) {
		hasColletion := ms.replica.hasCollection(collectionID1)
		assert.False(t, hasColletion)

		mFactory.setCollectionID(1)
		mFactory.setCollectionName(collectionName1)
		err := ms.loadCollections(ctx)
		assert.NoError(t, err)

		hasColletion = ms.replica.hasCollection(collectionID0)
		assert.True(t, hasColletion)
		hasColletion = ms.replica.hasCollection(collectionID1)
		assert.True(t, hasColletion)
	})

	t.Run("Test Init", func(t *testing.T) {
		ms1 := newMetaService(ctx, replica, mFactory)
		ms1.init()
	})

	t.Run("Test printCollectionStruct", func(t *testing.T) {
		mf := &MetaFactory{}
		collectionMeta := mf.CollectionMetaFactory(collectionID0, collectionName0)
		printCollectionStruct(collectionMeta)
	})
}
