//go:build test

package tombstone

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore"
)

// Only for test
func RecoverCollectionTombstoneForTest(ctx context.Context, catalog metastore.DataCoordCatalog) error {
	tombstone, err := recoverCollectionTombstone(ctx, catalog)
	if err != nil {
		return err
	}
	old := collectionTombstone.Swap(tombstone)
	if old != nil {
		old.Close()
	}
	return nil
}
