package tombstone

import (
	"context"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/metastore"
)

// collectionTombstone is the singleton instance of CollectionTtombstone
var collectionTombstone atomic.Pointer[collectionTombstoneImpl]

// RecoverCollectionTombstone recovers the collection tombstone from the metastore
func RecoverCollectionTombstone(ctx context.Context, catalog metastore.DataCoordCatalog) error {
	if collectionTombstone.Load() != nil {
		return nil
	}
	tombstone, err := recoverCollectionTombstone(ctx, catalog)
	if err != nil {
		return err
	}
	if swapped := collectionTombstone.CompareAndSwap(nil, tombstone); !swapped {
		tombstone.Close()
	}
	return nil
}

// CollectionTombstone returns the singleton instance of CollectionTombstone
func CollectionTombstone() *collectionTombstoneImpl {
	tombstone := collectionTombstone.Load()
	if tombstone == nil {
		panic("collection tombstone is not initialized")
	}
	return tombstone
}
