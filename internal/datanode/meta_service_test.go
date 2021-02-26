package datanode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaService_All(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replica := newReplica()
	mFactory := &MasterServiceFactory{}
	mFactory.setCollectionID(0)
	mFactory.setCollectionName("a-collection")
	metaService := newMetaService(ctx, replica, mFactory)

	t.Run("Test getCollectionNames", func(t *testing.T) {
		names, err := metaService.getCollectionNames(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(names))
		assert.Equal(t, "a-collection", names[0])
	})

	t.Run("Test createCollection", func(t *testing.T) {
		hasColletion := metaService.replica.hasCollection(0)
		assert.False(t, hasColletion)

		err := metaService.createCollection(ctx, "a-collection")
		assert.NoError(t, err)
		hasColletion = metaService.replica.hasCollection(0)
		assert.True(t, hasColletion)
	})

	t.Run("Test loadCollections", func(t *testing.T) {
		hasColletion := metaService.replica.hasCollection(1)
		assert.False(t, hasColletion)

		mFactory.setCollectionID(1)
		mFactory.setCollectionName("a-collection-1")
		err := metaService.loadCollections(ctx)
		assert.NoError(t, err)

		hasColletion = metaService.replica.hasCollection(1)
		assert.True(t, hasColletion)
		hasColletion = metaService.replica.hasCollection(0)
		assert.True(t, hasColletion)
	})

}
