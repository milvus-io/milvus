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
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestQueryViewPhysicalSegmentLoader_LoadBorrowsCollectionAndWrapsSegment(t *testing.T) {
	segments := &fakeQVSegmentManager{}
	loader := &fakeQVLoader{segment: &fakeQVSegment{id: 10, partitionID: 100}}
	physical := newQueryViewPhysicalSegmentLoader(segments, loader)

	loaded, err := physical.Load(
		context.Background(),
		&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, PartitionID: 100, InsertChannel: "v1", DeltaPosition: &msgpb.MsgPosition{Timestamp: 50}},
		fakeQVCollectionRuntime{collectionID: 1, schema: &schemapb.CollectionSchema{Name: "coll"}, schemaVersion: 9},
	)
	require.NoError(t, err)

	assert.Equal(t, int64(1), loader.collectionID)
	assert.True(t, loader.newCalled)
	assert.True(t, loader.loadCalled)
	assert.True(t, loader.deltaCalled)
	assert.True(t, loader.pkCalled)
	assert.True(t, loader.finalized)
	assert.Equal(t, int64(10), loaded.ID())
	assert.Equal(t, int64(100), loaded.PartitionID())
	assert.Equal(t, "v1", loaded.VChannel())
	assert.Equal(t, uint64(50), loaded.TransformStartAfterTimeTick())
}

func TestQueryViewPhysicalSegmentLoaderUpdateDoesNotUseContentHashAsOrderedLoadVersion(t *testing.T) {
	loader := &fakeQVLoader{}
	physical := newQueryViewPhysicalSegmentLoader(&fakeQVSegmentManager{}, loader)
	segment := newQueryViewTransformSegment(&fakeQVSegment{id: 10, partitionID: 100}, &fakeQVSegmentManager{}, "v1", 50)

	err := physical.Update(
		context.Background(),
		segment,
		fakeQVCollectionRuntime{collectionID: 1},
		qnview.SegmentLoadInfoSnapshot{
			SegmentID: 10,
			Revision:  qnview.SegmentLoadInfoRevision{Revision: ^uint64(0)},
			LoadInfo:  &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10},
		},
		qnview.SegmentUpdateLoadIndex,
	)
	require.NoError(t, err)
	assert.True(t, loader.loadIndexCalled)
	assert.Zero(t, loader.version,
		"SegmentLoadInfoRevision is a non-monotonic equality token and must not enter an ordered loader version slot")
}

func TestQueryViewPhysicalSegmentLoaderUpdateUnwrapsTransformSegment(t *testing.T) {
	loader := &fakeQVLoader{}
	physical := newQueryViewPhysicalSegmentLoader(&fakeQVSegmentManager{}, loader)
	segment := newQueryViewTransformSegment(&fakeQVSegment{id: 10, partitionID: 100}, &fakeQVSegmentManager{}, "v1", 50)
	wrapper := &testTransformSegmentWrapper{TransformSegment: &testTransformSegmentWrapper{TransformSegment: segment}}

	err := physical.Update(
		context.Background(),
		wrapper,
		fakeQVCollectionRuntime{collectionID: 1},
		qnview.SegmentLoadInfoSnapshot{
			SegmentID: 10,
			Revision:  qnview.SegmentLoadInfoRevision{Revision: 2},
			LoadInfo:  &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10},
		},
		qnview.SegmentUpdateReopen|qnview.SegmentUpdateLoadIndex,
	)
	require.NoError(t, err)
	assert.True(t, loader.reopenCalled)
	assert.False(t, loader.loadIndexCalled, "Reopen already applies index updates")
}

type testTransformSegmentWrapper struct {
	qnview.TransformSegment
}

func (s *testTransformSegmentWrapper) UnwrapTransformSegment() qnview.TransformSegment {
	return s.TransformSegment
}

func TestRealQVSegmentLoader_NewSegmentUsesPinnedCollection(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll"}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &fakeQVCollectionManager{collection: localCollection}
	runtime := &queryViewCollectionRuntimeGuard{
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

	loader := realQVSegmentLoader{}
	_, err := loader.NewSegment(context.Background(), runtime, &querypb.SegmentLoadInfo{CollectionID: 1})
	require.ErrorIs(t, err, assert.AnError)
	assert.Same(t, localCollection, usedCollection)
	assert.Zero(t, collections.getCount)
}

func TestQueryViewPhysicalSegmentReopenWaitsForHistoricalDeletes(t *testing.T) {
	loader := &fakeQVLoader{}
	physical := newQueryViewPhysicalSegmentLoader(nil, loader)
	segment := newQueryViewTransformSegment(&fakeQVSegment{id: 10}, nil, "v1", 50)
	snapshot := qnview.SegmentLoadInfoSnapshot{LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 10}}
	delta := mockey.Mock((*fakeQVLoader).LoadDeltaLogs).Return(assert.AnError).Build()
	defer delta.UnPatch()
	err := physical.Update(t.Context(), segment, fakeQVCollectionRuntime{}, snapshot, qnview.SegmentUpdateReopen)
	require.ErrorIs(t, err, assert.AnError)
	require.True(t, loader.reopenCalled)
	delta.UnPatch()
	require.NoError(t, physical.Update(t.Context(), segment, fakeQVCollectionRuntime{}, snapshot, qnview.SegmentUpdateReopen))
	require.True(t, loader.deltaCalled)
}
