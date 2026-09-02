package qvresource

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func patchCollectionLifetime(t *testing.T, patch *mockey.Mocker) {
	t.Helper()
	t.Cleanup(func() { patch.UnPatch() })
}

func pinnedCollectionForTest(t *testing.T) (qvCollectionManager, *queryViewCollectionRuntimeGuard) {
	t.Helper()
	schema := &schemapb.CollectionSchema{Name: "collection"}
	collection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	patchCollectionLifetime(t, mockey.Mock(segments.NewCollection).Return(collection, nil).Build())
	manager := segments.NewCollectionManager()
	require.NoError(t, manager.PutOrRef(1, schema, nil, nil))
	runtime := &queryViewCollectionRuntimeGuard{collections: manager, collection: collection, collectionID: 1, schema: schema}
	t.Cleanup(runtime.Release)
	return manager, runtime
}

type lifetimePhysicalManager struct{ qnview.PhysicalSegmentManager }

func (*lifetimePhysicalManager) Acquire(qnview.AcquirePhysicalSegments) { panic("mockey") }
func (*lifetimePhysicalManager) Release(qnview.ReleaseSegments)         { panic("mockey") }

type lifetimeCollectionManager struct {
	qnview.QueryViewCollectionRuntimeManager
}

func (*lifetimeCollectionManager) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	panic("mockey")
}

type lifetimeTransformBuffer struct{ qnview.TransformLogBuffer }

func (*lifetimeTransformBuffer) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	panic("mockey")
}

func (*lifetimeTransformBuffer) RegisterSegment(context.Context, qnview.TransformSegment) (qnview.TransformRegistration, error) {
	panic("mockey")
}

type lifetimeTransformGuard struct{ qnview.TransformLogGuard }

func (*lifetimeTransformGuard) Release() { panic("mockey") }

type lifetimeTransformRegistration struct{ qnview.TransformRegistration }

func (*lifetimeTransformRegistration) WaitCatchup(context.Context) error { panic("mockey") }
func (*lifetimeTransformRegistration) Unregister()                       { panic("mockey") }

func TestQueryHandlesKeepPinnedCollectionAfterLastViewDrops(t *testing.T) {
	collections, runtime := pinnedCollectionForTest(t)
	native := &segments.LocalSegment{}
	patchCollectionLifetime(t, mockey.Mock(segments.NewSegment).Return(native, nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*segments.LocalSegment).ID).Return(int64(10)).Build())
	patchCollectionLifetime(t, mockey.Mock((*segments.LocalSegment).Partition).Return(int64(100)).Build())
	release := mockey.Mock((*segments.LocalSegment).Release).Return().Build()
	patchCollectionLifetime(t, release)
	loader := realQVSegmentLoader{collections: collections}
	loaded, err := loader.NewSegment(context.Background(), runtime, &querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, loaded.Release(context.Background())) })
	segment := newQueryViewTransformSegment(loaded, &fakeQVSegmentManager{}, "p_1v0", 0)
	patchCollectionLifetime(t, mockey.Mock((*lifetimePhysicalManager).Acquire).To(func(_ *lifetimePhysicalManager, req qnview.AcquirePhysicalSegments) {
		req.OnLoaded([]qnview.TransformSegment{segment})
	}).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimePhysicalManager).Release).To(func(_ *lifetimePhysicalManager, req qnview.ReleaseSegments) { req.OnDropped() }).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeCollectionManager).Acquire).Return(runtime, false, nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeTransformBuffer).Acquire).Return(&lifetimeTransformGuard{}, nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeTransformGuard).Release).Return().Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeTransformBuffer).RegisterSegment).Return(&lifetimeTransformRegistration{}, nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeTransformRegistration).WaitCatchup).Return(nil).Build())
	patchCollectionLifetime(t, mockey.Mock((*lifetimeTransformRegistration).Unregister).Return().Build())
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	manager := qnview.NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, &lifetimePhysicalManager{}, &lifetimeTransformBuffer{}, 1, &lifetimeCollectionManager{})
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		Vchannel:     "p_1v0",
		Version:      &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, QueryVersion: 1},
	}
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 100, SegmentIds: []int64{10}}}}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	ready := make(chan struct{}, 1)
	manager.Acquire(qnview.AcquireSegments{Key: key, Meta: meta, View: view, OnReady: func(map[int64][]int64) { ready <- struct{}{} }})
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("view did not become ready")
	}
	first, err := manager.AcquireSealedSegmentHandles(context.Background(), key, view)
	require.NoError(t, err)
	defer first[0].Release()
	second, err := manager.AcquireSealedSegmentHandles(context.Background(), key, view)
	require.NoError(t, err)
	defer second[0].Release()
	dropped := make(chan struct{})
	manager.Release(qnview.ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("view did not drop")
	}
	readable := second[0].Segment().(qnview.ReadableSealedSegment)
	require.Same(t, runtime.collection, collections.Get(1))
	require.Same(t, runtime.collection, readable.Collection())
	require.Same(t, native, readable.QuerySegment())
	require.Equal(t, 0, release.Times())
	first[0].Release()
	require.NotNil(t, collections.Get(1))
	second[0].Release()
	require.Nil(t, collections.Get(1), "last query must release the collection")
	require.Equal(t, 1, release.Times())
	require.NoError(t, loaded.Release(context.Background()))
	require.Equal(t, 1, release.Times(), "release must be idempotent")
}

func TestPhysicalSegmentCollectionReferenceFailureCleanup(t *testing.T) {
	for _, stage := range []string{"new-segment", "load-segment", "delta-logs", "pk-candidate", "missing-collection"} {
		t.Run(stage, func(t *testing.T) {
			collections, runtime := pinnedCollectionForTest(t)
			loader := realQVSegmentLoader{collections: collections}
			if stage == "missing-collection" {
				runtime.Release()
				_, err := loader.NewSegment(context.Background(), runtime, &querypb.SegmentLoadInfo{CollectionID: 1})
				require.ErrorIs(t, err, merr.ErrCollectionNotFound)
				return
			}
			loadErr := merr.WrapErrServiceInternalMsg("injected load failure")
			native := &segments.LocalSegment{}
			var newErr error
			if stage == "new-segment" {
				newErr = loadErr
			}
			patchCollectionLifetime(t, mockey.Mock(segments.NewSegment).Return(native, newErr).Build())
			release := mockey.Mock((*segments.LocalSegment).Release).Return().Build()
			patchCollectionLifetime(t, release)
			for _, method := range []struct {
				name string
				fn   interface{}
			}{{"load-segment", realQVSegmentLoader.LoadSegment}, {"delta-logs", realQVSegmentLoader.LoadDeltaLogs}, {"pk-candidate", realQVSegmentLoader.LoadPKCandidate}} {
				var err error
				if method.name == stage {
					err = loadErr
				}
				patchCollectionLifetime(t, mockey.Mock(method.fn).Return(err).Build())
			}
			physical := newQueryViewPhysicalSegmentLoader(collections, &fakeQVSegmentManager{}, loader)
			_, err := physical.Load(context.Background(), &querypb.SegmentLoadInfo{CollectionID: 1}, runtime)
			require.ErrorIs(t, err, loadErr)
			require.Same(t, runtime.collection, collections.Get(1), "load failure must preserve the view's reference")
			runtime.Release()
			require.Nil(t, collections.Get(1), "load failure must not leak a collection reference")
			wantReleases := 1
			if stage == "new-segment" {
				wantReleases = 0
			}
			require.Equal(t, wantReleases, release.Times())
		})
	}
}
