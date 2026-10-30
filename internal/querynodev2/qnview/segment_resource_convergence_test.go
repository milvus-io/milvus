package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentResourceWaitsForManifestAndCatchup(t *testing.T) {
	mockey.PatchConvey("both manifest and transform catch-up gate readiness", t, func() {
		f := setupResourceTest(t, nil)
		f.registerMock.UnPatch()
		entered, caught := make(chan struct{}), make(chan struct{})
		reg := &fakeTransformRegistration{waitCh: caught}
		mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).To(func(_ *fakeTransformLogBuffer, _ context.Context, segment TransformSegment) (TransformRegistration, error) {
			require.EqualValues(t, 50, segment.TransformStartAfterTimeTick())
			close(entered)
			return reg, nil
		}).Build()
		req := f.request(1, 1, 1)
		req.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(req)
		require.Never(t, func() bool { return f.loads.Load() != 0 }, 100*time.Millisecond, time.Millisecond)
		f.manifest.Store(2)
		<-entered
		require.Empty(t, f.ready)
		_, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.Error(t, err)
		close(caught)
		f.await(t, 1)
		require.NoError(t, f.manager.WaitTransformVisible(t.Context(), req.Key, 100))
		f.drop(t, req)
		require.True(t, reg.unregistered)
	})
}

func TestSegmentResourcePendingViewRetainsSnapshot(t *testing.T) {
	mockey.PatchConvey("a complete pending binding survives release of the previous View", t, func() {
		f := setupResourceTest(t, nil)
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		f.dependencyMock.UnPatch()
		entered, resume := make(chan struct{}), make(chan struct{})
		mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			close(entered)
			select {
			case <-resume:
				return &fakeCollectionRuntimeGuard{schema: &schemapb.CollectionSchema{Version: 2}}, false, nil
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}).Build()
		next := f.acquire(2, 2, 2)
		<-entered
		f.drop(t, first)
		require.Zero(t, f.releases.Load())
		require.EqualValues(t, 1, f.gets.Load(), "pending binding retains the existing snapshot")
		close(resume)
		f.await(t, 2)
		require.EqualValues(t, 1, f.loads.Load())
		f.drop(t, next)
	})
}

func TestSegmentResourceRetriesLoadAndReleasesReservation(t *testing.T) {
	mockey.PatchConvey("a transient load failure converges without another coordinator event", t, func() {
		f := setupResourceTest(t, nil)
		f.loadMock.UnPatch()
		var attempts, reserved, released atomic.Int32
		mockey.Mock((*resourceTestLoader).Load).To(func(_ *resourceTestLoader, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
			if attempts.Add(1) == 1 {
				return nil, merr.WrapErrServiceNotReadyMsg("temporary storage failure")
			}
			return &fakeTransformSegment{id: info.SegmentID, partitionID: info.PartitionID}, nil
		}).Build()
		f.manager.estimator = &resourceTestEstimator{}
		mockey.Mock((*resourceTestEstimator).Reserve).To(func(*resourceTestEstimator, context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (ResourceReservation, error) {
			reserved.Add(1)
			return &resourceTestReservation{}, nil
		}).Build()
		mockey.Mock((*resourceTestReservation).Release).To(func(*resourceTestReservation) { released.Add(1) }).Build()
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		require.EqualValues(t, 2, attempts.Load())
		require.Equal(t, reserved.Load(), released.Load())
		f.drop(t, req)
	})
}

type resourceTestEstimator struct{}

func (*resourceTestEstimator) Reserve(context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (ResourceReservation, error) {
	panic("unpatched")
}

type resourceTestReservation struct{}

func (*resourceTestReservation) Release() { panic("unpatched") }

func TestSegmentResourceMetadataFailureLeavesReadyViewsQueryable(t *testing.T) {
	mockey.PatchConvey("terminal Get failure affects only the new unmet demand", t, func() {
		f := setupResourceTest(t, nil)
		old := f.acquire(1, 1, 1)
		f.await(t, 1)
		f.getMock.UnPatch()
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).Return(SegmentLoadInfoSnapshot{}, merr.WrapErrSegmentNotFound(1000)).Build()
		failed := make(chan struct{}, 1)
		next := f.request(2, 1, 1)
		next.View.Partitions[0].SegmentManifestVersions = []int64{2}
		next.OnUnrecoverable = func() { failed <- struct{}{} }
		f.manager.Acquire(next)
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("missing metadata failure")
		}
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), old.Key, old.View, 100)
		require.NoError(t, err)
		handles[0].Release()
		f.drop(t, next)
		f.drop(t, old)
	})
}

func TestSegmentResourceCatchupFailureDoesNotReportReady(t *testing.T) {
	mockey.PatchConvey("a failed registration cannot acknowledge readiness", t, func() {
		f := setupResourceTest(t, nil)
		f.registerMock.UnPatch()
		mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(nil, merr.WrapErrServiceNotReadyMsg("buffer unavailable")).Build()
		failed := make(chan struct{}, 1)
		req := f.request(1, 1, 1)
		req.OnUnrecoverable = func() { failed <- struct{}{} }
		f.manager.Acquire(req)
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("catch-up failure not reported")
		}
		require.Empty(t, f.ready)
		f.drop(t, req)
	})
}

func TestSegmentResourcePrunesDepartedScope(t *testing.T) {
	mockey.PatchConvey("the physical base adopts a retained read after another scope exits", t, func() {
		f := setupResourceTest(t, nil)
		f.manager.loader = &adoptingResourceLoader{PhysicalSegmentLoader: f.manager.loader}
		var adoptions atomic.Int32
		mockey.Mock((*adoptingResourceLoader).AdoptReadState).To(func(*adoptingResourceLoader, TransformSegment, segments.ReadState) error {
			adoptions.Add(1)
			return nil
		}).Build()
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		second := f.acquire(2, 1, 2)
		f.await(t, 1)
		f.drop(t, second)
		require.Eventually(t, func() bool {
			f.manager.mu.Lock()
			defer f.manager.mu.Unlock()
			r := f.manager.resources[1000]
			return len(r.reads) == 1 && r.reads[0].load == 1
		}, 5*time.Second, time.Millisecond)
		require.Positive(t, adoptions.Load())
		f.drop(t, first)
	})
}

type adoptingResourceLoader struct{ PhysicalSegmentLoader }

func (l *adoptingResourceLoader) PrepareReadState(ctx context.Context, s TransformSegment, reuse segments.ReadState, c CollectionRuntime, info SegmentLoadInfoSnapshot) (segments.ReadState, error) {
	return l.PhysicalSegmentLoader.PrepareReadState(ctx, s, reuse, c, info)
}

func (*adoptingResourceLoader) AdoptReadState(TransformSegment, segments.ReadState) error {
	panic("unpatched")
}

func TestSegmentResourceHandleValidationAndDuplicateAcquire(t *testing.T) {
	mockey.PatchConvey("duplicate bindings are idempotent and invalid handles have no references", t, func() {
		f := setupResourceTest(t, nil)
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		f.manager.Acquire(req)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := f.manager.AcquireSealedSegmentHandles(ctx, req.Key, req.View, 0)
		require.ErrorIs(t, err, context.Canceled)
		f.drop(t, req)
		_, err = f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.Error(t, err)
		require.Error(t, f.manager.WaitTransformVisible(t.Context(), req.Key, 100))
		f.drop(t, req)
		require.EqualValues(t, 1, f.releases.Load())
	})
}

func TestSegmentResourceRetriesBufferPreparation(t *testing.T) {
	mockey.PatchConvey("retryable transport preparation does not lose the already bound requirements", t, func() {
		f := setupResourceTest(t, nil)
		var calls atomic.Int32
		mockey.Mock((*fakeTransformLogGuard).Prepare).To(func(*fakeTransformLogGuard, context.Context) (bool, error) {
			if calls.Add(1) == 1 {
				return true, merr.WrapErrServiceNotReadyMsg("stream rebinding")
			}
			return false, nil
		}).Build()
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		require.EqualValues(t, 2, calls.Load())
		f.drop(t, req)
	})
}

func TestSegmentResourceInvalidatesAfterLiveTransformFailure(t *testing.T) {
	mockey.PatchConvey("a registration failure after Ready still reaches its View", t, func() {
		f := setupResourceTest(t, nil)
		observer := make(chan func(error), 1)
		reg := newFakeTransformRegistration()
		close(reg.waitCh)
		f.registerMock.UnPatch()
		mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(reg, nil).Build()
		mockey.Mock((*fakeTransformRegistration).OnFailure).To(func(_ *fakeTransformRegistration, callback func(error)) {
			observer <- callback
		}).Build()
		failed := make(chan struct{}, 1)
		req := f.request(1, 1, 1)
		req.OnUnrecoverable = func() { failed <- struct{}{} }
		f.manager.Acquire(req)
		f.await(t, 1)
		callback := <-observer
		callback(merr.WrapErrServiceNotReadyMsg("live application failed"))
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("live failure did not invalidate View")
		}
		_, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.Error(t, err)
		f.drop(t, req)
	})
}

func TestSegmentResourceQueryWaitCancellationReleasesPins(t *testing.T) {
	mockey.PatchConvey("a query canceled while waiting for its instance cannot leak resource pins", t, func() {
		f := setupResourceTest(t, nil)
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		entered := make(chan struct{})
		mockey.Mock((instantTransformRegistration).WaitTransformVisible).To(func(_ instantTransformRegistration, ctx context.Context, _ uint64) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}).Build()
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			_, err := f.manager.AcquireSealedSegmentHandles(ctx, req.Key, req.View, 100)
			result <- err
		}()
		<-entered
		f.drop(t, req)
		require.Zero(t, f.releases.Load(), "pending query owns the instance after its View exits")
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.Eventually(t, func() bool { return f.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
	})
}

func TestSegmentResourceUsesFetchedCursorAcrossViewVersions(t *testing.T) {
	mockey.PatchConvey("View versions reuse the actual base and its fetched replay cursor", t, func() {
		f := setupResourceTest(t, nil)
		later := f.request(2, 1, 1)
		f.manager.Acquire(later)
		f.await(t, 1)
		earlier := f.request(1, 1, 1)
		f.manager.Acquire(earlier)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		f.drop(t, later)
		require.Zero(t, f.releases.Load())
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), earlier.Key, earlier.View, 0)
		require.NoError(t, err)
		require.EqualValues(t, 50, handles[0].Segment().TransformStartAfterTimeTick())
		handles[0].Release()
		f.drop(t, earlier)
		require.EqualValues(t, 1, f.releases.Load())
	})
}

func TestSegmentResourceFailureLeavesOtherInstancesQueryable(t *testing.T) {
	mockey.PatchConvey("only Views referencing the failed base are invalidated", t, func() {
		f := setupResourceTest(t, nil)
		f.registerMock.UnPatch()
		broken := make(chan struct{})
		failedReg := &fakeTransformRegistration{waitCh: make(chan struct{})}
		close(failedReg.waitCh)
		failure := merr.WrapErrServiceInternalMsg("transform application failed")
		var registered atomic.Int32
		mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).To(func(_ *fakeTransformLogBuffer, _ context.Context, segment TransformSegment) (TransformRegistration, error) {
			if segment.ID() == 1000 && registered.Add(1) == 1 {
				return failedReg, nil
			}
			return instantTransformRegistration{}, nil
		}).Build()
		observer := make(chan func(error), 1)
		mockey.Mock((*fakeTransformRegistration).OnFailure).To(func(_ *fakeTransformRegistration, callback func(error)) {
			observer <- callback
		}).Build()
		mockey.Mock((*fakeTransformRegistration).WaitTransformVisible).To(func(_ *fakeTransformRegistration, _ context.Context, _ uint64) error {
			select {
			case <-broken:
				return failure
			default:
				return nil
			}
		}).Build()
		failed := make(chan struct{}, 1)
		first := f.request(1, 1, 1)
		first.OnUnrecoverable = func() { failed <- struct{}{} }
		f.manager.Acquire(first)
		f.await(t, 1)
		healthy := f.request(2, 1, 1)
		healthy.View.Partitions[0].SegmentIds = []int64{2000}
		f.manager.Acquire(healthy)
		f.await(t, 1)
		// Existing query references still own the failed base until they release.
		held, err := f.manager.AcquireSealedSegmentHandles(t.Context(), first.Key, first.View, 50)
		require.NoError(t, err)
		close(broken)
		callback := <-observer
		callback(failure)
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("instance failure did not reach its View")
		}
		for _, tick := range []uint64{0, 50, 100} {
			_, err = f.manager.AcquireSealedSegmentHandles(t.Context(), first.Key, first.View, tick)
			require.Error(t, err)
		}
		query, err := f.manager.AcquireSealedSegmentHandles(t.Context(), healthy.Key, healthy.View, 100)
		require.NoError(t, err)
		query[0].Release()
		f.drop(t, first)
		require.Zero(t, f.releases.Load(), "failure cannot destroy a resource borrowed by an executing query")
		held[0].Release()
		require.Eventually(t, func() bool { return f.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
		// Reconstruction uses a fresh registration even for the same SegmentID.
		replacement := f.acquire(3, 1, 1)
		f.await(t, 1)
		query, err = f.manager.AcquireSealedSegmentHandles(t.Context(), replacement.Key, replacement.View, 100)
		require.NoError(t, err)
		query[0].Release()
		f.drop(t, replacement)
		f.drop(t, healthy)
	})
}

func TestSegmentResourceUsesViewIndexSelection(t *testing.T) {
	mockey.PatchConvey("same-schema Views select different index artifacts without mutating cached metadata", t, func() {
		f := setupResourceTest(t, nil)
		f.getMock.UnPatch()
		source := resourceSnapshot(1, 1000, 1)
		source.LoadInfo.IndexInfos = []*querypb.FieldIndexInfo{{FieldID: 100, IndexID: 300}, {FieldID: 100, IndexID: 301}}
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, _ context.Context, collection, segment int64) (SegmentLoadInfoSnapshot, error) {
			snapshot := source
			snapshot.CollectionID, snapshot.SegmentID = collection, segment
			// The fixture's IDs match the acquired View; the metadata itself is
			// intentionally shared across preparations to exercise immutability.
			snapshot.LoadInfo.CollectionID, snapshot.LoadInfo.SegmentID = collection, segment
			return snapshot, nil
		}).Build()
		f.dependencyMock.UnPatch()
		mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			id := int64(299 + view.IntoProto().GetMeta().GetLoadInfoVersion())
			return &fakeCollectionRuntimeGuard{schema: &schemapb.CollectionSchema{Version: 1}, updatedIndexes: []*indexpb.IndexInfo{{FieldID: 100, IndexID: id}}}, false, nil
		}).Build()
		observed := make(chan int64, 2)
		f.manager.estimator = &resourceTestEstimator{}
		mockey.Mock((*resourceTestEstimator).Reserve).To(func(_ *resourceTestEstimator, _ context.Context, info *querypb.SegmentLoadInfo, c CollectionRuntime) (ResourceReservation, error) {
			require.Len(t, info.IndexInfos, 1)
			require.Equal(t, c.IndexInfos()[0].IndexID, info.IndexInfos[0].IndexID)
			observed <- info.IndexInfos[0].IndexID
			return nil, nil
		}).Build()
		a := f.acquire(1, 1, 1)
		f.await(t, 1)
		require.EqualValues(t, 300, <-observed)
		b := f.acquire(2, 1, 2)
		f.await(t, 1)
		require.EqualValues(t, 301, <-observed)
		require.Len(t, source.LoadInfo.IndexInfos, 2)
		require.EqualValues(t, 1, f.loads.Load(), "read bindings share compatible physical rows")
		f.drop(t, a)
		f.drop(t, b)
	})
}
