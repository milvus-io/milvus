//go:build test && dynamic

package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewCollectionRuntimeManager_AcquireRefsCollectionAndReleaseUnrefs(t *testing.T) {
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{Name: "coll"})
	collection := &fakeQVCollectionManager{collection: localCollection}
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

	guard, retryable, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
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
	assert.False(t, retryable)
	require.NotNil(t, guard)
	assert.Same(t, localCollection, guard.(*queryViewCollectionRuntimeGuard).collection)

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

	guard, retryable, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
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
	assert.False(t, retryable)
	require.NotNil(t, guard)

	assert.Equal(t, []int64{10, 20}, collection.putLoadMeta.GetPartitionIDs())
	assert.Equal(t, []int64{100, 101, 102}, collection.putLoadMeta.GetLoadFields())
}

func TestQueryViewCollectionRuntimeManager_AcquireClassifiesRetryability(t *testing.T) {
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1, LoadInfoVersion: 7},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)

	t.Run("transient", func(t *testing.T) {
		manager := newQueryViewCollectionRuntimeManager(
			&fakeQVLoadMetadataProvider{err: merr.WrapErrNodeNotMatch(1, 2)},
			&fakeQVCollectionManager{},
		)

		guard, retryable, err := manager.Acquire(context.Background(), view)

		assert.Nil(t, guard)
		assert.True(t, retryable)
		require.ErrorIs(t, err, merr.ErrNodeNotMatch)
	})

	t.Run("not found", func(t *testing.T) {
		manager := newQueryViewCollectionRuntimeManager(
			&fakeQVLoadMetadataProvider{err: merr.WrapErrCollectionNotFound(1)},
			&fakeQVCollectionManager{},
		)

		guard, retryable, err := manager.Acquire(context.Background(), view)

		assert.Nil(t, guard)
		assert.False(t, retryable)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}

func TestQueryViewCollectionRuntimeGuard_UpdateIndexMetaUsesPinnedCollection(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll"}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collection := &fakeQVCollectionManager{collection: localCollection}
	guard := &queryViewCollectionRuntimeGuard{
		collections:  collection,
		collection:   localCollection,
		collectionID: 1,
		schema:       schema,
	}
	indexes := []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 100, IndexName: "vec_idx"}}

	var updatedCollection *segments.Collection
	var updatedMeta *segcorepb.CollectionIndexMeta
	patch := mockey.Mock((*segments.Collection).UpdateIndexMeta).
		To(func(collection *segments.Collection, meta *segcorepb.CollectionIndexMeta) error {
			updatedCollection = collection
			updatedMeta = meta
			return nil
		}).
		Build()
	t.Cleanup(func() {
		patch.UnPatch()
	})

	err := guard.UpdateIndexMeta(context.Background(), indexes)
	require.NoError(t, err)

	assert.Same(t, localCollection, updatedCollection)
	require.Len(t, updatedMeta.GetIndexMetas(), 1)
	assert.Equal(t, int64(100), updatedMeta.GetIndexMetas()[0].GetFieldID())
	assert.Equal(t, "vec_idx", updatedMeta.GetIndexMetas()[0].GetIndexName())
	assert.Zero(t, collection.putCount)
	assert.Zero(t, collection.unrefCount)
}
