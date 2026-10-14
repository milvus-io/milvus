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
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type resourceTestLoader struct{ PhysicalSegmentLoader }

func (*resourceTestLoader) AdoptReadState(TransformSegment, segments.ReadState) error {
	panic("unpatched")
}

func (*resourceTestLoader) Load(context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (TransformSegment, error) {
	panic("unpatched")
}

func (*resourceTestLoader) PrepareReadState(context.Context, TransformSegment, segments.ReadState, CollectionRuntime, SegmentLoadInfoSnapshot) (segments.ReadState, error) {
	panic("unpatched")
}

type resourceTestRead struct{ segments.Segment }

func (*resourceTestRead) RetainReadState() func() { panic("unpatched") }
func (*resourceTestRead) ReleaseReadState()       { panic("unpatched") }

type resourceTestStream struct{ SegmentLoadInfoStream }

func (*resourceTestStream) Subscribe(SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
	panic("unpatched")
}

type resourceTestSubscription struct{ SegmentLoadInfoSubscription }

func (*resourceTestSubscription) Error() error { return nil }

func (*resourceTestSubscription) Close() { panic("unpatched") }

type resourceTestFixture struct {
	dependencyMock *mockey.Mocker
	loadMock       *mockey.Mocker
	registerMock   *mockey.Mocker
	manager        *SegmentResourceManager
	ready          chan int32
	handler        chan SegmentLoadInfoEventHandler
	loads          atomic.Int32
	prepares       atomic.Int32
	releases       atomic.Int32
}

func setupResourceTest(t *testing.T, prepare func(CollectionRuntime) error) *resourceTestFixture {
	mockey.Mock((*resourceTestLoader).AdoptReadState).Return(nil).Build()
	f := &resourceTestFixture{ready: make(chan int32, 20), handler: make(chan SegmentLoadInfoEventHandler, 20)}
	f.loadMock = mockey.Mock((*resourceTestLoader).Load).To(func(_ *resourceTestLoader, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
		f.loads.Add(1)
		return &fakeTransformSegment{id: info.SegmentID, partitionID: info.PartitionID}, nil
	}).Build()
	mockey.Mock((*resourceTestLoader).PrepareReadState).To(func(_ *resourceTestLoader, _ context.Context, _ TransformSegment, _ segments.ReadState, c CollectionRuntime, _ SegmentLoadInfoSnapshot) (segments.ReadState, error) {
		f.prepares.Add(1)
		if prepare != nil {
			if err := prepare(c); err != nil {
				return nil, err
			}
		}
		return &resourceTestRead{}, nil
	}).Build()
	mockey.Mock((*resourceTestRead).RetainReadState).Return(func() {}).Build()
	mockey.Mock((*resourceTestRead).ReleaseReadState).Return().Build()
	mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error { f.releases.Add(1); return nil }).Build()
	mockey.Mock((*resourceTestSubscription).Close).Return().Build()
	mockey.Mock((*resourceTestStream).Subscribe).To(func(_ *resourceTestStream, option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
		f.handler <- option.Handler
		require.NoError(t, option.Handler.Handle(SegmentLoadInfoSnapshot{SegmentID: option.SegmentID, CollectionID: option.CollectionID, Revision: SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: option.SegmentID, CollectionID: option.CollectionID, PartitionID: 10, NumOfRows: 10}}))
		return &resourceTestSubscription{}
	}).Build()
	f.dependencyMock = mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
		version := view.IntoProto().Meta.SchemaVersion
		return &fakeCollectionRuntimeGuard{schemaVersion: int64(version), schema: &schemapb.CollectionSchema{Version: version}}, false, nil
	}).Build()
	f.registerMock = mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(instantTransformRegistration{}, nil).Build()
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	f.manager = NewSegmentResourceManager(t.Context(), scheduler, &resourceTestLoader{}, nil, &resourceTestStream{}, &fakeTransformLogBuffer{}, &fakeQueryViewCollectionRuntimeManager{})
	return f
}

func (f *resourceTestFixture) request(version int64, schema int32, load uint64) AcquireSegments {
	meta := buildHandlerTestMeta(version)
	meta.SchemaVersion, meta.LoadInfoVersion = schema, load
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	req := AcquireSegments{Meta: meta, View: view, Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), OnReady: func(map[int64][]int64) { f.ready <- schema }}
	return req
}

func (f *resourceTestFixture) acquire(version int64, schema int32, load uint64) AcquireSegments {
	req := f.request(version, schema, load)
	f.manager.Acquire(req)
	return req
}

func (f *resourceTestFixture) drop(t *testing.T, req AcquireSegments) {
	done := make(chan struct{})
	f.manager.Release(ReleaseSegments{Key: req.Key, OnDropped: func() { close(done) }})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("release did not complete")
	}
}

func (f *resourceTestFixture) await(t *testing.T, schema int32) {
	select {
	case got := <-f.ready:
		require.Equal(t, schema, got)
	case <-time.After(5 * time.Second):
		t.Fatal("resource did not converge")
	}
}

func TestSegmentResourceOutOfOrderSchemasAndQueryLifetime(t *testing.T) {
	mockey.PatchConvey("older schema prepares after newer schema without reloading shared rows", t, func() {
		f := setupResourceTest(t, nil)
		newer := f.acquire(2, 2, 99)
		f.await(t, 2)
		older := f.acquire(3, 1, 2)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), older.Key, older.View, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1000, handles[0].ID())
		require.EqualValues(t, 10, handles[0].PartitionID())
		require.NotNil(t, handles[0].Segment())
		f.drop(t, newer)
		f.drop(t, older)
		require.Zero(t, f.releases.Load(), "executing query owns the concrete resource")
		handles[0].Release()
		handles[0].Release()
		require.Eventually(t, func() bool { return f.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
	})
}

func TestSegmentResourceBlockedRequirementDoesNotStarveOthers(t *testing.T) {
	mockey.PatchConvey("unavailable materialization does not block another binding", t, func() {
		var materialized atomic.Bool
		f := setupResourceTest(t, func(c CollectionRuntime) error {
			if c.SchemaVersion() == 2 && !materialized.Load() {
				return merr.WrapErrServiceNotReadyMsg("materialization pending")
			}
			return nil
		})
		pending := f.acquire(1, 2, 99)
		require.Eventually(t, func() bool { return f.prepares.Load() > 0 }, 5*time.Second, time.Millisecond)
		serving := f.acquire(2, 1, 2)
		f.await(t, 1)
		materialized.Store(true)
		f.await(t, 2) // no repeated coordinator push or metadata change
		f.drop(t, pending)
		f.drop(t, serving)
		require.EqualValues(t, 1, f.releases.Load())
	})
}

func TestSegmentResourceMetadataBeforeDependencies(t *testing.T) {
	mockey.PatchConvey("first metadata is retained until collection preparation resolves", t, func() {
		f := setupResourceTest(t, nil)
		gate := make(chan struct{})
		f.dependencyMock.UnPatch()
		mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			select {
			case <-gate:
				return &fakeCollectionRuntimeGuard{schema: &schemapb.CollectionSchema{Version: 1}}, false, nil
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}).Build()
		req := f.acquire(1, 1, 1)
		<-f.handler // synchronous first snapshot accepted before dependency
		require.Zero(t, f.loads.Load())
		close(gate)
		f.await(t, 1)
		f.drop(t, req)
	})
}

func TestSegmentResourceReleaseSerializesReconstruction(t *testing.T) {
	mockey.PatchConvey("a late load cannot publish into its same-ID replacement", t, func() {
		f := setupResourceTest(t, nil)
		entered, resume := make(chan struct{}), make(chan struct{})
		var attempts atomic.Int32
		// Delay read preparation after the base exists. Release must cancel and
		// drain this owner before the replacement subscribes or loads.
		f.manager.loader = &blockedResourceTestLoader{base: f.manager.loader}
		mockey.Mock((*blockedResourceTestLoader).PrepareReadState).To(func(l *blockedResourceTestLoader, ctx context.Context, segment TransformSegment, reuse segments.ReadState, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot) (segments.ReadState, error) {
			if attempts.Add(1) == 1 {
				close(entered)
				<-resume
			}
			return l.base.PrepareReadState(ctx, segment, reuse, collection, snapshot)
		}).Build()
		first := f.acquire(1, 1, 1)
		<-entered
		dropped := make(chan struct{})
		f.manager.Release(ReleaseSegments{Key: first.Key, OnDropped: func() { close(dropped) }})
		next := f.acquire(2, 2, 2)
		require.EqualValues(t, 1, f.loads.Load())
		close(resume)
		f.await(t, 2)
		select {
		case <-dropped:
		case <-time.After(5 * time.Second):
			t.Fatal("old release stalled")
		}
		require.EqualValues(t, 2, f.loads.Load())
		require.EqualValues(t, 1, f.releases.Load())
		f.drop(t, next)
		require.EqualValues(t, 2, f.releases.Load())
	})
}

type blockedResourceTestLoader struct {
	PhysicalSegmentLoader
	base PhysicalSegmentLoader
}

func (l *blockedResourceTestLoader) Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
	return l.base.Load(ctx, info, collection)
}

func (*blockedResourceTestLoader) PrepareReadState(context.Context, TransformSegment, segments.ReadState, CollectionRuntime, SegmentLoadInfoSnapshot) (segments.ReadState, error) {
	panic("unpatched")
}

func TestSegmentResourceSameSchemaDifferentScopeAndMetadataRefresh(t *testing.T) {
	mockey.PatchConvey("scope identity and refreshed indexes prepare separate results on shared rows", t, func() {
		f := setupResourceTest(t, nil)
		a := f.acquire(1, 1, 99)
		f.await(t, 1)
		b := f.acquire(2, 1, 2)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		handler := <-f.handler
		require.NoError(t, handler.Handle(SegmentLoadInfoSnapshot{SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 2}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000, PartitionID: 10, NumOfRows: 10}}))
		require.Eventually(t, func() bool {
			f.manager.mu.Lock()
			defer f.manager.mu.Unlock()
			return f.manager.views[a.Key].bindings[1000].prepared.revision.Revision == 2 && f.manager.views[b.Key].bindings[1000].prepared.revision.Revision == 2
		}, 5*time.Second, time.Millisecond)
		require.Empty(t, f.ready, "resource refresh does not re-report Ready")
		require.EqualValues(t, 1, f.loads.Load())
		f.drop(t, a)
		f.drop(t, b)
	})
}

func TestSegmentResourceReleaseWhileDependenciesPending(t *testing.T) {
	mockey.PatchConvey("release cancels dependency work and finishes without creating native state", t, func() {
		f := setupResourceTest(t, nil)
		entered := make(chan struct{})
		f.dependencyMock.UnPatch()
		mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			close(entered)
			<-ctx.Done()
			return nil, false, ctx.Err()
		}).Build()
		req := f.acquire(1, 1, 1)
		<-entered
		f.drop(t, req)
		require.Empty(t, f.ready)
		require.Zero(t, f.loads.Load())
	})
}

func TestSegmentResourceRetriesDependencies(t *testing.T) {
	mockey.PatchConvey("dependency retry wakes the complete binding without another snapshot", t, func() {
		f := setupResourceTest(t, nil)
		f.dependencyMock.UnPatch()
		var calls atomic.Int32
		mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			if calls.Add(1) == 1 {
				return nil, true, merr.WrapErrServiceNotReadyMsg("schema pending")
			}
			return &fakeCollectionRuntimeGuard{schema: &schemapb.CollectionSchema{Version: 1}}, false, nil
		}).Build()
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		require.EqualValues(t, 2, calls.Load())
		f.drop(t, req)
	})
}

func TestSegmentResourceEmptyAssignmentPreparesDependencies(t *testing.T) {
	mockey.PatchConvey("an empty assignment still prepares schema and buffer", t, func() {
		f := setupResourceTest(t, nil)
		meta := buildHandlerTestMeta(1)
		view := &viewpb.QueryViewOfQueryNode{NodeId: 1}
		ready := make(chan map[int64][]int64, 1)
		req := AcquireSegments{Meta: meta, View: view, Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), OnReady: func(ids map[int64][]int64) { ready <- ids }}
		f.manager.Acquire(req)
		select {
		case ids := <-ready:
			require.Empty(t, ids)
		case <-time.After(time.Second):
			t.Fatal("empty view stalled")
		}
		f.manager.mu.Lock()
		require.NotNil(t, f.manager.views[req.Key].collection)
		require.NotNil(t, f.manager.views[req.Key].buffer)
		f.manager.mu.Unlock()
		f.drop(t, req)
		require.Zero(t, f.loads.Load())
	})
}

func TestSegmentResourceSharedPreparationSurvivesInitiatorRelease(t *testing.T) {
	mockey.PatchConvey("releasing the initiating view leaves a shared preparation alive", t, func() {
		entered, resume := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		f := setupResourceTest(t, func(CollectionRuntime) error {
			if calls.Add(1) == 1 {
				close(entered)
				<-resume
			}
			return nil
		})
		first := f.acquire(1, 1, 1)
		<-entered
		next := f.acquire(2, 1, 1)
		f.drop(t, first)
		require.Zero(t, f.releases.Load())
		close(resume)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		f.drop(t, next)
	})
}

func (l *blockedResourceTestLoader) AdoptReadState(s TransformSegment, r segments.ReadState) error {
	return l.base.AdoptReadState(s, r)
}

func TestPrunedResourceSlicesReleaseBackingReferences(t *testing.T) {
	first := &segmentBase{caughtUp: true}
	second := &segmentBase{caughtUp: true}
	retired := &preparedSegmentRead{base: first, read: &resourceTestRead{}}
	live := &preparedSegmentRead{base: second, read: &resourceTestRead{}}
	r := &segmentResource{
		reads:    []*preparedSegmentRead{retired, live},
		bases:    []*segmentBase{first, second},
		bindings: []*segmentBinding{{prepared: live}},
	}
	readBacking, baseBacking := r.reads[:], r.bases[:]
	require.Len(t, r.pruneReadsLocked(), 1)
	require.Equal(t, []*preparedSegmentRead{live}, r.reads)
	require.Nil(t, readBacking[1])
	require.Equal(t, []*segmentBase{first}, r.pruneBasesLocked())
	require.Equal(t, []*segmentBase{second}, r.bases)
	require.Nil(t, baseBacking[1])
}
