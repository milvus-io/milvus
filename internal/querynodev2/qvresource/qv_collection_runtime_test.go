//go:build test && dynamic

package qvresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryViewCollectionRuntimeManager_AcquireRefsCollectionAndReleaseUnrefs(t *testing.T) {
	collection := &fakeQVCollectionManager{}
	provider := &fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID:    1,
			DbName:          "db",
			UpdateTimestamp: 9,
			Properties:      []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			Schema:          &schemapb.CollectionSchema{Name: "coll"},
		},
		partitionIDs: []int64{10, 20},
		loadFields:   []int64{100, 101},
	}
	manager := newQueryViewCollectionRuntimeManager(provider, collection)

	guard, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			CollectionId:    1,
			LoadInfoVersion: 7,
		},
		&viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1}},
				{PartitionId: 20, SegmentIds: []int64{2}},
			},
		},
	).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	require.NotNil(t, guard)

	assert.Equal(t, int64(1), guard.CollectionID())
	assert.Equal(t, "db", guard.DatabaseName())
	assert.Equal(t, "coll", guard.Schema().GetName())
	assert.Equal(t, int64(9), guard.SchemaVersion())
	assert.Equal(t, int64(1), collection.putCollectionID)
	assert.Equal(t, "coll", collection.putSchema.GetName())
	require.NotNil(t, collection.putLoadMeta)
	assert.Equal(t, querypb.LoadType_LoadCollection, collection.putLoadMeta.GetLoadType())
	assert.Equal(t, int64(1), collection.putLoadMeta.GetCollectionID())
	assert.Equal(t, "db", collection.putLoadMeta.GetDbName())
	assert.Equal(t, uint64(9), collection.putLoadMeta.GetSchemaBarrierTs())
	assert.Equal(t, []int64{10, 20}, collection.putLoadMeta.GetPartitionIDs())
	assert.Equal(t, []int64{100, 101}, collection.putLoadMeta.GetLoadFields())

	guard.Release()
	assert.Equal(t, int64(1), collection.unrefCollection)
	assert.Equal(t, uint32(1), collection.unrefCount)
}

func TestQueryViewCollectionRuntimeManager_AcquireUsesLoadInfoVersion(t *testing.T) {
	collection := &fakeQVCollectionManager{}
	provider := &fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID:    1,
			DbName:          "db",
			UpdateTimestamp: 9,
			Schema:          &schemapb.CollectionSchema{Name: "coll"},
		},
		partitionIDs: []int64{10, 20},
		loadFields:   []int64{100, 101, 102},
	}
	manager := newQueryViewCollectionRuntimeManager(provider, collection)

	guard, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			CollectionId:    1,
			LoadInfoVersion: 7,
		},
		&viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1}},
			},
		},
	).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	require.NotNil(t, guard)

	assert.Equal(t, []int64{10, 20}, collection.putLoadMeta.GetPartitionIDs())
	assert.Equal(t, []int64{100, 101, 102}, collection.putLoadMeta.GetLoadFields())
}

func TestQueryViewCollectionRuntimeGuard_UpdateIndexMetaRefsAndUnrefsCollection(t *testing.T) {
	collection := &fakeQVCollectionManager{}
	guard := &queryViewCollectionRuntimeGuard{
		collections:   collection,
		collectionID:  1,
		databaseName:  "db",
		schema:        &schemapb.CollectionSchema{Name: "coll"},
		schemaVersion: 9,
	}
	indexes := []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 100, IndexName: "vec_idx"}}

	err := guard.UpdateIndexMeta(context.Background(), indexes)
	require.NoError(t, err)

	assert.Equal(t, int64(1), collection.putCollectionID)
	assert.Equal(t, "coll", collection.putSchema.GetName())
	require.NotNil(t, collection.putIndexMeta)
	require.Len(t, collection.putIndexMeta.GetIndexMetas(), 1)
	assert.Equal(t, int64(100), collection.putIndexMeta.GetIndexMetas()[0].GetFieldID())
	assert.Equal(t, "vec_idx", collection.putIndexMeta.GetIndexMetas()[0].GetIndexName())
	require.NotNil(t, collection.putLoadMeta)
	assert.Equal(t, querypb.LoadType_LoadCollection, collection.putLoadMeta.GetLoadType())
	assert.Equal(t, int64(1), collection.putLoadMeta.GetCollectionID())
	assert.Equal(t, "db", collection.putLoadMeta.GetDbName())
	assert.Equal(t, uint64(9), collection.putLoadMeta.GetSchemaBarrierTs())
	assert.Equal(t, int64(1), collection.unrefCollection)
	assert.Equal(t, uint32(1), collection.unrefCount)
}
