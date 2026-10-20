package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentResourceGetRetriesWithoutPollingReadyView(t *testing.T) {
	mockey.PatchConvey("transient metadata failures retry only while preparation needs metadata", t, func() {
		f := setupResourceTest(t, nil)
		f.getMock.UnPatch()
		var calls atomic.Int32
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, _ context.Context, collection, segment int64) (SegmentLoadInfoSnapshot, error) {
			if calls.Add(1) == 1 {
				return SegmentLoadInfoSnapshot{}, merr.WrapErrServiceNotReadyMsg("metadata recovering")
			}
			return resourceSnapshot(collection, segment, 1), nil
		}).Build()
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		require.EqualValues(t, 2, calls.Load())
		f.manager.mu.Lock()
		f.manager.resources[1000].wakeLocked()
		f.manager.mu.Unlock()
		require.Never(t, func() bool { return calls.Load() != 2 }, 250*time.Millisecond, time.Millisecond)
		f.drop(t, req)
	})
}

func TestSegmentResourceSharedGetSurvivesViewRelease(t *testing.T) {
	mockey.PatchConvey("one View's release cannot cancel another View's shared Get", t, func() {
		f := setupResourceTest(t, nil)
		f.getMock.UnPatch()
		entered, resume := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, ctx context.Context, collection, segment int64) (SegmentLoadInfoSnapshot, error) {
			calls.Add(1)
			close(entered)
			select {
			case <-resume:
				return resourceSnapshot(collection, segment, 1), nil
			case <-ctx.Done():
				return SegmentLoadInfoSnapshot{}, ctx.Err()
			}
		}).Build()
		first := f.acquire(1, 1, 1)
		<-entered
		next := f.acquire(2, 2, 2)
		f.drop(t, first)
		close(resume)
		f.await(t, 2)
		require.EqualValues(t, 1, calls.Load())
		f.drop(t, next)
	})
}

func TestSegmentResourceLateGetCannotPopulateReplacement(t *testing.T) {
	mockey.PatchConvey("last release cancels Get and discards its late response before reconstruction", t, func() {
		f := setupResourceTest(t, nil)
		f.getMock.UnPatch()
		entered, canceled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, ctx context.Context, collection, segment int64) (SegmentLoadInfoSnapshot, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-resume
				return resourceSnapshot(collection, segment, 1), nil
			}
			return resourceSnapshot(collection, segment, 2), nil
		}).Build()
		first := f.acquire(1, 1, 1)
		<-entered
		done := make(chan struct{})
		f.manager.Release(ReleaseSegments{Key: first.Key, OnDropped: func() { close(done) }})
		select {
		case <-canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("Get was not canceled")
		}
		next := f.request(2, 2, 2)
		next.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(next)
		close(resume)
		f.await(t, 2)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("old resource did not drain")
		}
		require.EqualValues(t, 2, calls.Load())
		require.EqualValues(t, 1, f.loads.Load())
		f.manager.mu.Lock()
		require.EqualValues(t, 2, f.manager.views[next.Key].bindings[1000].prepared.manifest)
		f.manager.mu.Unlock()
		f.drop(t, next)
	})
}

func TestSegmentResourceExternalManifestMismatch(t *testing.T) {
	mockey.PatchConvey("current external metadata cannot replace a historical row domain", t, func() {
		f := setupResourceTest(t, nil)
		f.manifest.Store(2)
		f.dependencyMock.UnPatch()
		mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, _ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			v := view.IntoProto().Meta.SchemaVersion
			return &fakeCollectionRuntimeGuard{schemaVersion: int64(v), schema: &schemapb.CollectionSchema{Version: v, Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "external_id"}}}}, false, nil
		}).Build()
		failed := make(chan struct{}, 1)
		req := f.request(1, 1, 1)
		req.OnUnrecoverable = func() { failed <- struct{}{} }
		f.manager.Acquire(req)
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatal("external manifest mismatch not reported")
		}
		require.Zero(t, f.loads.Load())
		require.EqualValues(t, 1, f.gets.Load())
		f.drop(t, req)
	})
}
