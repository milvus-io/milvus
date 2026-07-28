//go:build test && dynamic

package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestQueryViewPhysicalSegmentLoader_LoadBorrowsCollectionAndWrapsSegment(t *testing.T) {
	collection := &fakeQVCollectionManager{}
	segments := &fakeQVSegmentManager{}
	loader := &fakeQVLoader{segment: &fakeQVSegment{id: 10, partitionID: 100}}
	physical := newQueryViewPhysicalSegmentLoader(collection, segments, loader)

	loaded, err := physical.Load(
		context.Background(),
		&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, PartitionID: 100, InsertChannel: "v1", DeltaPosition: &msgpb.MsgPosition{Timestamp: 50}},
		fakeQVCollectionRuntime{collectionID: 1, schema: &schemapb.CollectionSchema{Name: "coll"}, schemaVersion: 9},
	)
	require.NoError(t, err)

	assert.Zero(t, collection.putCollectionID)
	assert.Zero(t, collection.unrefCount)
	assert.Zero(t, collection.refCount)
	assert.Equal(t, int64(1), loader.collectionID)
	assert.True(t, loader.newCalled)
	assert.True(t, loader.loadCalled)
	assert.True(t, loader.deltaCalled)
	assert.True(t, loader.pkCalled)
	assert.Equal(t, int64(10), loaded.ID())
	assert.Equal(t, int64(100), loaded.PartitionID())
	assert.Equal(t, "v1", loaded.VChannel())
	assert.Equal(t, uint64(50), loaded.TransformStartAfterTimeTick())
}

func TestRealQVSegmentLoader_NewSegmentUsesPinnedCollection(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll"}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &fakeQVCollectionManager{collection: localCollection}
	runtime := &queryViewCollectionRuntimeGuard{
		collections:  collections,
		collection:   localCollection,
		collectionID: 1,
		schema:       schema,
	}

	var usedCollection *segments.Collection
	patch := mockey.Mock(segments.NewSegment).
		To(func(_ context.Context, collection *segments.Collection, _ segments.SegmentManager, _ segments.SegmentType, _ int64, _ *querypb.SegmentLoadInfo) (segments.Segment, error) {
			usedCollection = collection
			return nil, assert.AnError
		}).
		Build()
	t.Cleanup(func() {
		patch.UnPatch()
	})

	loader := realQVSegmentLoader{collections: collections}
	_, err := loader.NewSegment(context.Background(), runtime, &querypb.SegmentLoadInfo{CollectionID: 1})
	require.ErrorIs(t, err, assert.AnError)
	assert.Same(t, localCollection, usedCollection)
	assert.Zero(t, collections.getCount)
}
