package qnview

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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

type resourceTestMetadata struct{}

func (*resourceTestMetadata) GetQueryViewSegmentLoadInfo(context.Context, int64, int64) (SegmentLoadInfoSnapshot, error) {
	panic("unpatched")
}

type resourceTestFixture struct {
	dependencyMock *mockey.Mocker
	loadMock       *mockey.Mocker
	prepareMock    *mockey.Mocker
	releaseMock    *mockey.Mocker
	registerMock   *mockey.Mocker
	manager        *SegmentResourceManager
	ready          chan int32
	getMock        *mockey.Mocker
	gets           atomic.Int32
	manifest       atomic.Int64
	loads          atomic.Int32
	prepares       atomic.Int32
	releases       atomic.Int32
}

func setupResourceTest(t *testing.T, prepare func(CollectionRuntime) error) *resourceTestFixture {
	mockey.Mock((*resourceTestLoader).AdoptReadState).Return(nil).Build()
	f := &resourceTestFixture{ready: make(chan int32, 20)}
	f.manifest.Store(1)
	f.loadMock = mockey.Mock((*resourceTestLoader).Load).To(func(_ *resourceTestLoader, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
		f.loads.Add(1)
		return &fakeTransformSegment{id: info.SegmentID, partitionID: info.PartitionID}, nil
	}).Build()
	f.prepareMock = mockey.Mock((*resourceTestLoader).PrepareReadState).To(func(_ *resourceTestLoader, _ context.Context, _ TransformSegment, _ segments.ReadState, c CollectionRuntime, _ SegmentLoadInfoSnapshot) (segments.ReadState, error) {
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
	f.releaseMock = mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error { f.releases.Add(1); return nil }).Build()
	f.getMock = mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, _ context.Context, collectionID, segmentID int64) (SegmentLoadInfoSnapshot, error) {
		f.gets.Add(1)
		return resourceSnapshot(collectionID, segmentID, f.manifest.Load()), nil
	}).Build()
	f.dependencyMock = mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
		version := view.IntoProto().Meta.SchemaVersion
		return &fakeCollectionRuntimeGuard{schemaVersion: int64(version), schema: &schemapb.CollectionSchema{Version: version}}, false, nil
	}).Build()
	f.registerMock = mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(instantTransformRegistration{}, nil).Build()
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	f.manager = NewSegmentResourceManager(t.Context(), scheduler, &resourceTestLoader{}, nil, &resourceTestMetadata{}, &fakeTransformLogBuffer{}, &fakeQueryViewCollectionRuntimeManager{}, nil)
	return f
}

func (f *resourceTestFixture) request(version int64, schema int32, load uint64) AcquireSegments {
	meta := buildHandlerTestMeta(version)
	meta.SchemaVersion, meta.LoadInfoVersion = schema, load
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}, SegmentManifestVersions: []int64{1}}}}
	req := AcquireSegments{Meta: meta, View: view, Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), OnReady: func(map[int64][]int64) { f.ready <- schema }}
	return req
}

func (f *resourceTestFixture) acquire(version int64, schema int32, load uint64) AcquireSegments {
	req := f.request(version, schema, load)
	f.manager.Acquire(req)
	return req
}

func (f *resourceTestFixture) beginDrop(req AcquireSegments) <-chan struct{} {
	done := make(chan struct{})
	f.manager.Release(ReleaseSegments{Key: req.Key, OnDropped: func() { close(done) }})
	return done
}

func (f *resourceTestFixture) drop(t *testing.T, req AcquireSegments) {
	done := f.beginDrop(req)
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
		var retiredCollections atomic.Int32
		mockey.Mock((*fakeCollectionRuntimeGuard).Release).To(func(*fakeCollectionRuntimeGuard) { retiredCollections.Add(1) }).Build()
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
		dropped := f.beginDrop(older)
		requireDropPending(t, dropped)
		require.EqualValues(t, 1, retiredCollections.Load(), "the executing query retains its View's collection")
		require.Zero(t, f.releases.Load(), "executing query owns the concrete resource")
		handles[0].Release()
		handles[0].Release()
		awaitShutdown(t, dropped)
		require.Eventually(t, func() bool { return f.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
		require.Eventually(t, func() bool { return retiredCollections.Load() == 2 }, 5*time.Second, time.Millisecond)
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

func TestSegmentResourceFetchesAfterDependencies(t *testing.T) {
	mockey.PatchConvey("metadata is fetched only after dependencies resolve", t, func() {
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
		require.Never(t, func() bool { return f.gets.Load() != 0 }, 50*time.Millisecond, time.Millisecond)
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
		// drain this owner before the replacement fetches metadata or loads.
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

func TestSegmentResourceSameSchemaDifferentScopeAndManifest(t *testing.T) {
	mockey.PatchConvey("new View prepares new manifest while retained Views keep their reads", t, func() {
		f := setupResourceTest(t, nil)
		a := f.acquire(1, 1, 99)
		f.await(t, 1)
		b := f.acquire(2, 1, 2)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		require.EqualValues(t, 1, f.gets.Load())
		f.manifest.Store(2)
		next := f.request(3, 1, 2)
		next.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(next)
		f.await(t, 1)
		f.manager.mu.Lock()
		require.EqualValues(t, 1, f.manager.views[a.Key].bindings[1000].prepared.manifest)
		require.EqualValues(t, 1, f.manager.views[b.Key].bindings[1000].prepared.manifest)
		require.EqualValues(t, 2, f.manager.views[next.Key].bindings[1000].prepared.manifest)
		f.manager.mu.Unlock()
		require.EqualValues(t, 2, f.gets.Load())
		require.EqualValues(t, 1, f.loads.Load(), "compatible rows share their physical base")
		f.drop(t, a)
		f.drop(t, b)
		f.drop(t, next)
	})
}

func resourceSnapshot(collectionID, segmentID, manifest int64) SegmentLoadInfoSnapshot {
	return SegmentLoadInfoSnapshot{CollectionID: collectionID, SegmentID: segmentID, LoadInfo: &querypb.SegmentLoadInfo{
		CollectionID: collectionID, SegmentID: segmentID, PartitionID: 10, NumOfRows: 10,
		ManifestPath: packed.MarshalManifestPath("test", manifest), StorageVersion: storage.StorageV3, TransformStartAfterTimetick: 50,
	}}
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
		var retiredCollections atomic.Int32
		mockey.Mock((*fakeCollectionRuntimeGuard).Release).To(func(*fakeCollectionRuntimeGuard) { retiredCollections.Add(1) }).Build()
		first := f.acquire(1, 1, 1)
		<-entered
		next := f.acquire(2, 1, 1)
		dropped := f.beginDrop(first)
		requireDropPending(t, dropped)
		require.Zero(t, retiredCollections.Load(), "native preparation still borrows the released View's schema")
		require.Zero(t, f.releases.Load())
		close(resume)
		f.await(t, 1)
		awaitShutdown(t, dropped)
		require.Eventually(t, func() bool { return retiredCollections.Load() == 1 }, 5*time.Second, time.Millisecond)
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

func TestSegmentResourceManagerListsQueryViewCollections(t *testing.T) {
	mockey.PatchConvey("collections are listed while a QueryView holds them", t, func() {
		f := setupResourceTest(t, nil)
		require.Empty(t, f.manager.CollectionIDs())
		first := f.acquire(1, 1, 1)
		second := f.acquire(2, 1, 1)
		require.Equal(t, []int64{testCollectionID}, f.manager.CollectionIDs())
		f.await(t, 1)
		f.await(t, 1)
		f.drop(t, first)
		require.Equal(t, []int64{testCollectionID}, f.manager.CollectionIDs())
		f.drop(t, second)
		require.Empty(t, f.manager.CollectionIDs())
	})
}

func TestSegmentResourceSharesManifestWithinPreparation(t *testing.T) {
	mockey.PatchConvey("each preparation shares one opened revision", t, func() {
		dir := t.TempDir()
		config := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
		first, err := packed.CreateManifestForSegment(filepath.Join(dir, "segment"), []string{"100"}, "parquet",
			[]packed.Fragment{{FilePath: filepath.Join(dir, "segment", "data.parquet"), EndRow: 10}}, config)
		require.NoError(t, err)
		artifact := packed.ManifestIndexInfo{
			ColumnName: "100", IndexName: "vector", IndexType: "HNSW", FieldID: 100, IndexID: 5,
			BuildID: 30, IndexVersion: 1, Path: "30/1", IndexFileKeys: []string{"index.bin"},
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		}
		first, err = packed.CommitManifestUpdates(first, config, &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{artifact}})
		require.NoError(t, err)
		artifact.BuildID, artifact.Path = 31, "31/1"
		second, err := packed.CommitManifestUpdates(first, config, &packed.ManifestUpdates{
			Indexes:   []packed.ManifestIndexInfo{artifact},
			DeltaLogs: []packed.DeltaLogEntry{{Path: "delta.parquet", NumEntries: 1}},
		})
		require.NoError(t, err)
		_, firstVersion, err := packed.UnmarshalManifestPath(first)
		require.NoError(t, err)
		_, secondVersion, err := packed.UnmarshalManifestPath(second)
		require.NoError(t, err)

		f := setupResourceTest(t, nil)
		defer f.manager.Close()
		f.dependencyMock.To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			version := view.IntoProto().Meta.SchemaVersion
			return &fakeCollectionRuntimeGuard{
				schemaVersion: int64(version), schema: &schemapb.CollectionSchema{Version: version},
				updatedIndexes: []*indexpb.IndexInfo{{IndexID: 5, FieldID: 100}},
			}, false, nil
		})
		var prepared []*querypb.SegmentLoadInfo
		var prepareManifests []*packed.LoadManifest
		f.prepareMock.To(func(_ *resourceTestLoader, ctx context.Context, _ TransformSegment, _ segments.ReadState, _ CollectionRuntime, snapshot SegmentLoadInfoSnapshot) (segments.ReadState, error) {
			prepared = append(prepared, snapshot.LoadInfo)
			prepareManifests = append(prepareManifests, packed.LoadManifestFromContext(ctx, snapshot.LoadInfo.GetManifestPath()))
			return &resourceTestRead{}, nil
		})
		f.getMock.UnPatch()
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, _ context.Context, collectionID, segmentID int64) (SegmentLoadInfoSnapshot, error) {
			snapshot := resourceSnapshot(collectionID, segmentID, f.manifest.Load())
			snapshot.LoadInfo.ManifestPath = first
			buildID := int64(30)
			if f.manifest.Load() == secondVersion {
				snapshot.LoadInfo.ManifestPath = second
				snapshot.LoadInfo.TransformStartAfterTimetick = 55
				buildID = 31
			}
			snapshot.ManifestIndexes = &querypb.QueryViewManifestIndexes{Summaries: []*datapb.SegmentIndexSummary{{
				IndexId: 5, FieldId: 100, BuildId: buildID, IndexVersion: 1, HasArtifact: true,
			}}}
			return snapshot, nil
		}).Build()

		var opened []*packed.LoadManifest
		var failedOnce bool
		failed := make(chan struct{})
		retryOpen := make(chan struct{})
		f.manager.openManifest = func(ctx context.Context, path string, _ *indexpb.StorageConfig) (*packed.LoadManifest, error) {
			if packed.SameManifestPath(path, second) {
				if !failedOnce {
					failedOnce = true
					close(failed)
					return nil, merr.ErrServiceUnavailable
				}
				select {
				case <-retryOpen:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			manifest, err := packed.OpenLoadManifest(ctx, path, config)
			opened = append(opened, manifest)
			return manifest, err
		}
		f.manifest.Store(firstVersion)
		one := f.request(1, 1, 1)
		one.View.Partitions[0].SegmentManifestVersions = []int64{firstVersion}
		f.manager.Acquire(one)
		f.await(t, 1)
		require.Len(t, opened, 1)
		require.Len(t, prepared, 1)
		require.EqualValues(t, 30, prepared[0].GetIndexInfos()[0].GetBuildID())
		require.Same(t, opened[0], prepareManifests[0])
		require.Nil(t, opened[0].Native())

		two := f.request(2, 2, 2)
		two.View.Partitions[0].SegmentManifestVersions = []int64{firstVersion}
		f.manager.Acquire(two)
		f.await(t, 2)
		require.Len(t, opened, 2)
		require.Len(t, prepared, 2)
		require.EqualValues(t, 30, prepared[1].GetIndexInfos()[0].GetBuildID())
		require.Same(t, opened[1], prepareManifests[1])
		require.Nil(t, opened[1].Native())

		f.manifest.Store(secondVersion)
		three := f.request(3, 3, 3)
		three.View.Partitions[0].SegmentManifestVersions = []int64{secondVersion}
		f.manager.Acquire(three)
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("new revision was not prepared")
		}
		require.Empty(t, f.ready)
		f.manager.mu.Lock()
		previousRead := f.manager.views[one.Key].bindings[1000].prepared
		f.manager.mu.Unlock()
		require.NotNil(t, previousRead)
		require.Equal(t, firstVersion, previousRead.manifest)

		close(retryOpen)
		f.await(t, 3)
		require.Len(t, opened, 3)
		require.Len(t, prepared, 3)
		require.EqualValues(t, 31, prepared[2].GetIndexInfos()[0].GetBuildID())
		require.EqualValues(t, 55, prepared[2].GetTransformStartAfterTimetick())
		require.Same(t, opened[2], prepareManifests[2])
		require.Nil(t, opened[2].Native())
		require.Len(t, opened[2].DeltaPaths(), 1)

		f.drop(t, three)
		f.drop(t, two)
		f.drop(t, one)
	})
}

func TestSegmentResourceClosesManifestOnRejectedReservation(t *testing.T) {
	mockey.PatchConvey("rejected reservation closes the attempt's manifest", t, func() {
		dir := t.TempDir()
		config := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
		basePath := filepath.Join(dir, "segment")
		path, err := packed.CreateManifestForSegment(basePath, []string{"100"}, "parquet",
			[]packed.Fragment{{FilePath: filepath.Join(basePath, "data.parquet"), EndRow: 10}}, config)
		require.NoError(t, err)
		_, version, err := packed.UnmarshalManifestPath(path)
		require.NoError(t, err)

		var opened []*packed.LoadManifest
		manager := &SegmentResourceManager{
			estimator: &resourceTestEstimator{},
			openManifest: func(ctx context.Context, path string, _ *indexpb.StorageConfig) (*packed.LoadManifest, error) {
				manifest, err := packed.OpenLoadManifest(ctx, path, config)
				opened = append(opened, manifest)
				return manifest, err
			},
		}
		var admissionManifest *packed.LoadManifest
		mockey.Mock((*resourceTestEstimator).Reserve).To(func(_ *resourceTestEstimator, ctx context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (ResourceReservation, error) {
			admissionManifest = packed.LoadManifestFromContext(ctx, info.GetManifestPath())
			return nil, merr.ErrServiceUnavailable
		}).Build()

		resource := &segmentResource{manager: manager, id: 1000, collectionID: testCollectionID}
		snapshot := resourceSnapshot(testCollectionID, resource.id, version)
		snapshot.LoadInfo.ManifestPath = path
		snapshot.ManifestIndexes = &querypb.QueryViewManifestIndexes{}
		collection := &fakeCollectionRuntimeGuard{schema: &schemapb.CollectionSchema{}}
		for attempt := 1; attempt <= 2; attempt++ {
			read, err := resource.prepare(t.Context(), &segmentBinding{}, collection, snapshot)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.Nil(t, read)
			require.Len(t, opened, attempt)
			require.Same(t, opened[attempt-1], admissionManifest)
			require.Nil(t, admissionManifest.Native())
		}
	})
}
