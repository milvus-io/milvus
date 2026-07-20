//go:build test && dynamic

package qnview

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func testSegmentLoadSnapshot(segmentID int64, partitionID int64, indexes ...*indexpb.IndexInfo) SegmentLoadInfoSnapshot {
	return SegmentLoadInfoSnapshot{
		CollectionID: testCollectionID,
		SegmentID:    segmentID,
		Revision:     SegmentLoadInfoRevision{Revision: 1},
		LoadInfo: &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  partitionID,
			CollectionID: testCollectionID,
		},
		IndexInfos: indexes,
	}
}

func TestQueryViewSegmentLoadScheduler_ReservesAndReleasesResourceAroundLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	estimator := &fakeSegmentResourceEstimator{}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader, estimator)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:    context.Background(),
		Meta:       meta,
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   testSegmentLoadSnapshot(1000, 10),
		OnLoaded:   func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 1
	}, time.Second, 10*time.Millisecond)
	require.Len(t, estimator.infos, 1)
	assert.Equal(t, int64(1000), estimator.infos[0].GetSegmentID())
	require.Len(t, estimator.collections, 1)
	assert.Same(t, runtime, estimator.collections[0])
	require.Len(t, estimator.reservations, 1)
	assert.True(t, estimator.reservations[0].released)
	require.Len(t, loader.loadInfos, 1)
	require.Len(t, loader.collections, 1)
	assert.Same(t, runtime, loader.collections[0])
}

func TestQueryViewSegmentLoadScheduler_UsesSegmentLoadInfoFromWatchSnapshot(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeQueryViewLoadMetadataProvider{}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:    context.Background(),
		Meta:       meta,
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   testSegmentLoadSnapshot(1000, 10, indexes...),
		OnLoaded:   func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(err error) {
			t.Fatalf("unexpected unrecoverable: %v", err)
		},
	})

	select {
	case loaded := <-loadedCh:
		assert.Equal(t, int64(1000), loaded.ID())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.Empty(t, provider.loadInfoCalled)
	assert.False(t, provider.describeCalled)
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
	require.Len(t, loader.loadInfos, 1)
	assert.Equal(t, int64(1000), loader.loadInfos[0].GetSegmentID())
}

func TestQueryViewSegmentLoadScheduler_LoadsFromSnapshotWithoutMetadataLookup(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeQueryViewLoadMetadataProvider{
		err: errors.New("metadata lookup should not be called"),
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:    context.Background(),
		Meta:       meta,
		SegmentID:  1000,
		Collection: runtime,
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    1000,
			Revision:     SegmentLoadInfoRevision{Revision: 1},
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, PartitionID: 10, CollectionID: testCollectionID},
			IndexInfos:   indexes,
		},
		OnLoaded: func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(err error) {
			t.Fatalf("unexpected unrecoverable: %v", err)
		},
	})

	select {
	case loaded := <-loadedCh:
		assert.Equal(t, int64(1000), loaded.ID())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.Empty(t, provider.loadInfoCalled)
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
}

func TestQueryViewSegmentLoadScheduler_RequiresWatchSnapshotForLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	loader := &fakePhysicalLoader{}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader)

	unrecoverableCh := make(chan error, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:    context.Background(),
		Meta:       meta,
		SegmentID:  1000,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		OnLoaded: func(TransformSegment) {
			t.Fatal("unexpected loaded")
		},
		OnUnrecoverable: func(err error) {
			unrecoverableCh <- err
		},
	})

	select {
	case err := <-unrecoverableCh:
		require.Error(t, err)
		assert.ErrorContains(t, err, "watch snapshot")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable")
	}
	assert.Empty(t, provider.loadInfoCalled)
}

func TestQueryViewSegmentLoadScheduler_UsesTaskTransformStartTick(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID(), startAfter: 10}, nil
		},
	}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:                     context.Background(),
		Meta:                        meta,
		SegmentID:                   1000,
		TransformStartAfterTimeTick: 99,
		Snapshot:                    testSegmentLoadSnapshot(1000, 10),
		OnLoaded:                    func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	var loaded TransformSegment
	select {
	case loaded = <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.Equal(t, uint64(99), loaded.TransformStartAfterTimeTick())
}

func TestQueryViewSegmentLoadScheduler_PreservesReadableSegment(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	collection := &segments.Collection{}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
			return &fakeReadableTransformSegment{
				fakeTransformSegment: fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()},
				collection:           collection,
			}, nil
		},
	}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:                     context.Background(),
		Meta:                        meta,
		SegmentID:                   1000,
		TransformStartAfterTimeTick: 99,
		Snapshot:                    testSegmentLoadSnapshot(1000, 10),
		OnLoaded:                    func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	var loaded TransformSegment
	select {
	case loaded = <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	readable, ok := loaded.(ReadableSealedSegment)
	require.True(t, ok)
	assert.Same(t, collection, readable.Collection())
}

func TestQueryViewSegmentLoadScheduler_UpdatesCollectionIndexMetaBeforeLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos:      []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		loadIndexInfos: indexes,
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:    context.Background(),
		Meta:       meta,
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   testSegmentLoadSnapshot(1000, 10, indexes...),
		OnLoaded:   func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	select {
	case <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
}

func TestQueryViewSegmentLoadScheduler_IndexMetaUpdateFailureSkipsReserveAndLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID, updateErr: errors.New("index meta update failed")}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos:      []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		loadIndexInfos: indexes,
	}
	loader := &fakePhysicalLoader{}
	estimator := &fakeSegmentResourceEstimator{}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader, estimator)

	unrecoverableCh := make(chan error, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:         context.Background(),
		Meta:            meta,
		SegmentID:       1000,
		Collection:      runtime,
		Snapshot:        testSegmentLoadSnapshot(1000, 10, indexes...),
		OnLoaded:        func(TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func(err error) { unrecoverableCh <- err },
	})

	select {
	case err := <-unrecoverableCh:
		require.Error(t, err)
		assert.ErrorContains(t, err, "index meta update failed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable")
	}
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
	assert.Empty(t, estimator.infos)
	assert.Empty(t, loader.loadInfos)
}

func TestQueryViewSegmentLoadScheduler_ReservationFailureSkipsPhysicalLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	loader := &fakePhysicalLoader{}
	estimator := &fakeSegmentResourceEstimator{err: errors.New("resource rejected")}
	scheduler := NewQueryViewSegmentLoadScheduler(provider, loader, estimator)

	unrecoverableCh := make(chan error, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:         context.Background(),
		Meta:            meta,
		SegmentID:       1000,
		Collection:      runtime,
		Snapshot:        testSegmentLoadSnapshot(1000, 10),
		OnLoaded:        func(TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func(err error) { unrecoverableCh <- err },
	})

	select {
	case err := <-unrecoverableCh:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable")
	}
	require.Len(t, estimator.infos, 1)
	require.Len(t, estimator.collections, 1)
	assert.Same(t, runtime, estimator.collections[0])
	assert.Empty(t, loader.loadInfos)
}

func TestQueryViewSegmentLoadScheduler_UpdateClassifiesRevisionChange(t *testing.T) {
	loader := &fakePhysicalLoader{}
	scheduler := NewQueryViewSegmentLoadScheduler(&fakeQueryViewLoadMetadataProvider{}, loader)
	updatedCh := make(chan SegmentLoadInfoRevision, 1)

	current := SegmentLoadInfoRevision{Revision: 10}
	next := SegmentLoadInfoRevision{Revision: 11}
	scheduler.Update(SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000, partitionID: 10},
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Current:    current,
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    1000,
			Revision:     next,
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
		},
		OnUpdated: func(revision SegmentLoadInfoRevision) { updatedCh <- revision },
		OnFailed: func(err error) {
			t.Fatalf("unexpected update failure: %v", err)
		},
	})

	select {
	case got := <-updatedCh:
		require.Equal(t, next, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
	require.Len(t, loader.updateActions, 1)
	require.True(t, loader.updateActions[0].Has(SegmentUpdateReopen))
	require.True(t, loader.updateActions[0].Has(SegmentUpdateLoadIndex))
}

func TestQueryViewSegmentLoadScheduler_UpdateClassifiesDataChange(t *testing.T) {
	loader := &fakePhysicalLoader{}
	scheduler := NewQueryViewSegmentLoadScheduler(&fakeQueryViewLoadMetadataProvider{}, loader)
	updatedCh := make(chan SegmentLoadInfoRevision, 1)

	current := SegmentLoadInfoRevision{Revision: 10}
	next := SegmentLoadInfoRevision{Revision: 12}
	scheduler.Update(SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000, partitionID: 10},
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Current:    current,
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    1000,
			Revision:     next,
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
		},
		OnUpdated: func(revision SegmentLoadInfoRevision) { updatedCh <- revision },
		OnFailed: func(err error) {
			t.Fatalf("unexpected update failure: %v", err)
		},
	})

	select {
	case got := <-updatedCh:
		require.Equal(t, next, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
	require.Len(t, loader.updateActions, 1)
	require.True(t, loader.updateActions[0].Has(SegmentUpdateReopen))
	require.True(t, loader.updateActions[0].Has(SegmentUpdateLoadIndex))
}
