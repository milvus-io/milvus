package tombstone

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
)

// collectionTombstone is the singleton instance of CollectionTtombstone
var collectionTombstone *collectionTombstoneImpl

// RecoverCollectionTombstone recovers the collection tombstone from the metastore
func RecoverCollectionTombstone(ctx context.Context, catalog metastore.DataCoordCatalog) error {
	if collectionTombstone != nil {
		return nil
	}

	var err error
	collectionTombstone, err = recoverCollectionTombstone(ctx, catalog)
	return err
}

// CollectionTombstone returns the singleton instance of CollectionTombstone
func CollectionTombstone() *collectionTombstoneImpl {
	if collectionTombstone == nil {
		panic("collection tombstone is not initialized")
	}
	return collectionTombstone
}
