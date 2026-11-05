//go:build test && dynamic

package qvresource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
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
		&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, PartitionID: 100, InsertChannel: "v1", DeltaPosition: &msgpb.MsgPosition{Timestamp: 999}, TransformStartAfterTimetick: 50},
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

type testTransformSegmentWrapper struct {
	qnview.TransformSegment
}

func (s *testTransformSegmentWrapper) UnwrapTransformSegment() qnview.TransformSegment {
	return s.TransformSegment
}

func TestRealQVSegmentLoader_NewSegmentUsesCollection(t *testing.T) {
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

func TestQueryViewPhysicalSegmentLoaderLoadsPKCandidateWithSegmentData(t *testing.T) {
	pkStarted := make(chan struct{})
	loader := &fakeQVLoader{
		segment: &fakeQVSegment{id: 10, partitionID: 100},
		pkCandidateHook: func(context.Context) error {
			close(pkStarted)
			return nil
		},
		// Blocks the segment load until the PK candidate load has started.
		loadSegmentHook: func(context.Context) error {
			select {
			case <-pkStarted:
				return nil
			case <-time.After(10 * time.Second):
				return errors.New("PK candidate load did not overlap the segment load")
			}
		},
	}
	physical := newQueryViewPhysicalSegmentLoader(&fakeQVSegmentManager{}, loader)

	_, err := physical.Load(
		context.Background(),
		&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, PartitionID: 100},
		fakeQVCollectionRuntime{collectionID: 1},
	)
	require.NoError(t, err)
	assert.True(t, loader.deltaCalled)
	assert.True(t, loader.pkCalled)
}

func TestQueryViewPhysicalSegmentLoaderFailedLoadReleasesAfterPKCandidateStops(t *testing.T) {
	var pkStopped atomic.Bool
	var pkStoppedAtRelease bool
	segment := &fakeQVSegment{id: 10, partitionID: 100, onRelease: func() {
		pkStoppedAtRelease = pkStopped.Load()
	}}
	loadErr := errors.New("segment load failed")
	loader := &fakeQVLoader{
		segment: segment,
		pkCandidateHook: func(ctx context.Context) error {
			<-ctx.Done()
			pkStopped.Store(true)
			return ctx.Err()
		},
		loadSegmentHook: func(context.Context) error {
			return loadErr
		},
	}
	physical := newQueryViewPhysicalSegmentLoader(&fakeQVSegmentManager{}, loader)

	_, err := physical.Load(
		context.Background(),
		&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, PartitionID: 100},
		fakeQVCollectionRuntime{collectionID: 1},
	)
	require.ErrorIs(t, err, loadErr)
	assert.False(t, loader.deltaCalled)
	assert.Equal(t, 1, segment.releaseCount)
	assert.True(t, pkStoppedAtRelease)
}

func TestQueryViewPhysicalSegmentLoaderPKCandidateErrorCancelsSegmentLoad(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	pkErr := errors.New("bloom filter load failed")
	loader := &fakeQVLoader{
		segment: segment,
		pkCandidateHook: func(context.Context) error {
			return pkErr
		},
		loadSegmentHook: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return errors.New("segment load was not canceled")
			}
		},
	}
	physical := newQueryViewPhysicalSegmentLoader(&fakeQVSegmentManager{}, loader)

	_, err := physical.Load(
		context.Background(),
		&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, PartitionID: 100},
		fakeQVCollectionRuntime{collectionID: 1},
	)
	require.ErrorIs(t, err, pkErr)
	assert.False(t, loader.deltaCalled)
	assert.Equal(t, 1, segment.releaseCount)
}
