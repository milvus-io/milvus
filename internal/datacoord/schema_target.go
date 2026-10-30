package datacoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func coordinatorPreparationSchema(ctx context.Context, collectionID int64, published *schemapb.CollectionSchema) (*schemapb.CollectionSchema, error) {
	return qvresource.PreparationSchema(ctx, collectionID, func(context.Context, int64) (*schemapb.CollectionSchema, error) {
		return published, nil
	})
}

// collectionForPreparation overlays pending intent on an immutable cache
// snapshot. Neither the public cache nor previously issued jobs are mutated.
func (m *meta) collectionForPreparation(ctx context.Context, collection *collectionInfo) (*collectionInfo, error) {
	if collection == nil || m.preparationSchema == nil {
		return collection, nil
	}
	schema, err := m.preparationSchema(ctx, collection.ID, collection.Schema)
	if err != nil {
		return nil, err
	}
	copy := *collection
	copy.Schema = schema
	if schema != collection.Schema {
		copy.Properties = funcutil.KeyValuePair2Map(schema.GetProperties())
	}
	return &copy, nil
}

// collectionsForPreparation leaves collections whose callback is still being
// recovered for the next policy pass. Other collections continue progressing.
// GC and snapshot enumeration keep using the full committed metadata cache.
func (m *meta) collectionsForPreparation(ctx context.Context) []*collectionInfo {
	collections := m.GetCollections()
	ready := make([]*collectionInfo, 0, len(collections))
	for _, collection := range collections {
		prepared, err := m.collectionForPreparation(ctx, collection)
		if err != nil {
			mlog.Debug(ctx, "collection schema target is not ready for compaction planning", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
			continue
		}
		ready = append(ready, prepared)
	}
	return ready
}
