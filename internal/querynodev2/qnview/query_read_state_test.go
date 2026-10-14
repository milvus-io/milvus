package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type readStateFixture struct{ segments.Segment }

func (*readStateFixture) RetainReadState() func() { panic("mock with mockey") }
func (*readStateFixture) ReleaseReadState()       { panic("mock with mockey") }

func TestQueryReadStateOutlivesView(t *testing.T) {
	oldRead, newRead := &readStateFixture{}, &readStateFixture{}
	refs := map[*readStateFixture]int{oldRead: 1, newRead: 1}
	release := mockey.Mock((*readStateFixture).ReleaseReadState).To(func(r *readStateFixture) { refs[r]-- }).Build()
	defer release.UnPatch()
	pin := mockey.Mock((*readStateFixture).RetainReadState).To(func(r *readStateFixture) func() {
		refs[r]++
		return r.ReleaseReadState
	}).Build()
	defer pin.UnPatch()
	oldKey := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
	newKey := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	physical := &fakeTransformSegment{id: 10, partitionID: 1}
	guard := &fakeCollectionRuntimeGuard{}
	manager := &QueryViewSegmentReadinessManager{
		views: map[qviews.QueryViewKey]*transformViewRef{
			oldKey: {collectionGuard: guard, segments: map[int64]int64{10: 1}, readers: map[int64]segments.ReadState{10: oldRead}},
			newKey: {collectionGuard: guard, segments: map[int64]int64{10: 1}, readers: map[int64]segments.ReadState{10: newRead}},
		},
		segments: map[int64]*transformSegmentState{10: {
			state: transformSegmentLoaded, segment: physical,
			refs: map[qviews.QueryViewKey]struct{}{oldKey: {}, newKey: {}},
		}},
	}
	view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10}}}}
	a, err := manager.AcquireSealedSegmentHandles(t.Context(), oldKey, view)
	require.NoError(t, err)
	b, err := manager.AcquireSealedSegmentHandles(t.Context(), newKey, view)
	require.NoError(t, err)
	require.Same(t, oldRead, a[0].Segment().(ReadableSealedSegment).QuerySegment())
	require.Same(t, newRead, b[0].Segment().(ReadableSealedSegment).QuerySegment())
	manager.detachView(oldKey).releaseSegments()
	manager.detachView(newKey).releaseSegments()
	require.Equal(t, 1, refs[oldRead])
	require.Equal(t, 1, refs[newRead])
	require.False(t, physical.released)
	a[0].Release()
	require.Zero(t, refs[oldRead])
	require.False(t, physical.released)
	// A physical release must never wait for a snapshot owned by this handle.
	physicalRelease := mockey.Mock((*fakeTransformSegment).Release).To(func(s *fakeTransformSegment, _ context.Context) error {
		require.Zero(t, refs[oldRead])
		require.Zero(t, refs[newRead])
		s.released = true
		return nil
	}).Build()
	defer physicalRelease.UnPatch()
	b[0].Release()
	b[0].Release()
	require.True(t, physical.released)
}

func TestStalePreparedReadStateIsReleasedBeforePhysicalSegment(t *testing.T) {
	read := &readStateFixture{}
	released := false
	unpin := mockey.Mock((*readStateFixture).ReleaseReadState).To(func(*readStateFixture) { released = true }).Build()
	defer unpin.UnPatch()
	physical := &fakeTransformSegment{id: 10}
	closePhysical := mockey.Mock((*fakeTransformSegment).Release).To(func(s *fakeTransformSegment, _ context.Context) error {
		require.True(t, released)
		s.released = true
		return nil
	}).Build()
	defer closePhysical.UnPatch()
	manager := &QueryViewSegmentReadinessManager{}
	manager.onPhysicalLoaded(qviews.QueryViewKey{}, nil, []TransformSegment{&preparedReadSegment{TransformSegment: physical, read: read}})
	require.True(t, physical.released)
}

func TestViewReleaseDoesNotWaitForPendingReadCallbacks(t *testing.T) {
	mockey.PatchConvey("detach synchronously, destroy outside the shard lock, report only after both complete", t, func() {
		key := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
		segment := &fakeTransformSegment{id: 10}
		callbackCanFinish := make(chan struct{})
		var unblock sync.Once
		defer unblock.Do(func() { close(callbackCanFinish) })
		entered := make(chan struct{})
		var destroyed atomic.Bool
		mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error {
			close(entered)
			<-callbackCanFinish
			destroyed.Store(true)
			return nil
		}).Build()
		physicalDetached := make(chan struct{})
		var physicalDone func()
		mockey.Mock(fakePhysicalSegmentManager.Release).To(func(_ fakePhysicalSegmentManager, req ReleaseSegments) {
			physicalDone = req.OnDropped
			close(physicalDetached)
		}).Build()
		mgr := &QueryViewSegmentReadinessManager{
			physical: fakePhysicalSegmentManager{},
			views:    map[qviews.QueryViewKey]*transformViewRef{key: {segments: map[int64]int64{10: 1}}},
			segments: map[int64]*transformSegmentState{10: {segment: segment, refs: map[qviews.QueryViewKey]struct{}{key: {}}}},
		}
		returned := make(chan struct{})
		dropped := make(chan struct{})
		go func() {
			mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(time.Second):
			unblock.Do(func() { close(callbackCanFinish) })
			t.Fatal("Release waited for a read callback blocked by its caller")
		}
		<-physicalDetached
		require.Empty(t, mgr.views)
		require.Empty(t, mgr.segments)
		<-entered
		physicalDone()
		select {
		case <-dropped:
			t.Fatal("reported Dropped before native destruction completed")
		default:
		}
		unblock.Do(func() { close(callbackCanFinish) })
		select {
		case <-dropped:
			require.True(t, destroyed.Load())
		case <-time.After(time.Second):
			t.Fatal("release did not finish after the callback unpinned its snapshot")
		}
	})
}

func TestPhysicalReleaseDoesNotWaitForMetadataCallback(t *testing.T) {
	mockey.PatchConvey("subscription close cannot hold the caller lock or report early", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		callbackCanFinish := make(chan struct{})
		var unblock sync.Once
		defer unblock.Do(func() { close(callbackCanFinish) })
		entered := make(chan struct{})
		mockey.Mock((*fakeSegmentLoadInfoSubscription).Close).To(func(*fakeSegmentLoadInfoSubscription) {
			close(entered)
			<-callbackCanFinish
		}).Build()
		key := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
		manager := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(scheduler, nil)
		manager.views[key] = &viewRef{segments: map[int64]int64{10: 1}}
		manager.segments[10] = &physicalSegmentState{refs: map[qviews.QueryViewKey]struct{}{key: {}}, subscription: &fakeSegmentLoadInfoSubscription{}}
		returned, dropped := make(chan struct{}), make(chan struct{})
		go func() {
			manager.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(time.Second):
			unblock.Do(func() { close(callbackCanFinish) })
			t.Fatal("Release blocked on a metadata callback")
		}
		require.Empty(t, manager.views)
		require.Empty(t, manager.segments, "new Acquire cannot reuse a retiring segment")
		<-entered
		select {
		case <-dropped:
			t.Fatal("reported Dropped before metadata callback finished")
		default:
		}
		unblock.Do(func() { close(callbackCanFinish) })
		select {
		case <-dropped:
		case <-time.After(time.Second):
			t.Fatal("release did not finish")
		}
	})
}

func TestReleasedLoadCallbackCannotPopulateReplacementGeneration(t *testing.T) {
	oldKey := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
	newKey := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	oldState := &transformSegmentState{}
	replacement := &transformSegmentState{state: transformSegmentWaiting, refs: map[qviews.QueryViewKey]struct{}{newKey: {}}}
	oldSegment := &fakeTransformSegment{id: 10}
	manager := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{10: replacement}}
	manager.onPhysicalLoaded(oldKey, map[int64]*transformSegmentState{10: oldState}, []TransformSegment{oldSegment})
	require.True(t, oldSegment.released)
	require.Same(t, replacement, manager.segments[10])
	require.Nil(t, replacement.segment, "late callback must not register the old physical segment for the new generation")
	require.Equal(t, transformSegmentWaiting, replacement.state)
}

func TestLateCatchupCannotChangeReplacementGeneration(t *testing.T) {
	for _, phase := range []string{"register failure", "catchup failure", "catchup success"} {
		t.Run(phase, func(t *testing.T) {
			key := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
			oldSegment := &fakeTransformSegment{id: 10}
			nextSegment := &fakeTransformSegment{id: 10}
			ready := false
			replacement := &transformSegmentState{
				state: transformSegmentCatchingUp, segment: nextSegment,
				refs:    map[qviews.QueryViewKey]struct{}{key: {}},
				waiters: map[qviews.QueryViewKey]transformSegmentWaiter{key: {physicalReady: true, onReady: func(map[int64][]int64) { ready = true }}},
			}
			old := &transformSegmentState{state: transformSegmentCatchingUp, segment: oldSegment, refs: map[qviews.QueryViewKey]struct{}{key: {}}}
			manager := &QueryViewSegmentReadinessManager{
				buffer:   &fakeTransformLogBuffer{},
				segments: map[int64]*transformSegmentState{10: old},
				views:    map[qviews.QueryViewKey]*transformViewRef{key: {}},
			}
			replace := func() {
				manager.mu.Lock()
				manager.segments[10] = replacement
				manager.mu.Unlock()
			}
			registration := &fakeTransformRegistration{}
			register := mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).To(func(*fakeTransformLogBuffer, context.Context, TransformSegment) (TransformRegistration, error) {
				if phase == "register failure" {
					replace()
					return nil, context.Canceled
				}
				return registration, nil
			}).Build()
			defer register.UnPatch()
			wait := mockey.Mock((*fakeTransformRegistration).WaitCatchup).To(func(*fakeTransformRegistration, context.Context) error {
				replace()
				if phase == "catchup failure" {
					return context.Canceled
				}
				return nil
			}).Build()
			defer wait.UnPatch()
			unregister := mockey.Mock((*fakeTransformRegistration).Unregister).Return().Build()
			defer unregister.UnPatch()
			manager.registerAndCatchup(oldSegment)
			require.Same(t, replacement, manager.segments[10])
			require.Equal(t, transformSegmentCatchingUp, replacement.state)
			require.False(t, nextSegment.released)
			require.False(t, ready, "an old catchup result cannot make the replacement ready")
		})
	}
}
