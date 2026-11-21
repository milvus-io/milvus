package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type gcProbeSegment struct {
	*fakeTransformSegment
	calls   chan uint64
	replies chan error
}

func (s *gcProbeSegment) GCUpdates(_ context.Context, ts uint64) error {
	s.calls <- ts
	return <-s.replies
}

func TestSealedUpdateRetentionReservations(t *testing.T) {
	mockey.PatchConvey("a reserved store excludes late Views while native GC is blocked", t, func() {
		f := setupResourceTest(t, nil)
		physical := make(chan *gcProbeSegment, 4)
		f.loadMock.To(func(_ *resourceTestLoader, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
			s := &gcProbeSegment{fakeTransformSegment: &fakeTransformSegment{id: info.GetSegmentID(), partitionID: 10}, calls: make(chan uint64, 4), replies: make(chan error)}
			physical <- s
			return s, nil
		})
		bind := func(id int64, floor uint64) AcquireSegments {
			req := f.request(id, 1, 1)
			req.Meta.ReadAdmissionFloor = &viewpb.QueryPlanMVCC{TransformingTimetick: floor}
			f.manager.Acquire(req)
			f.await(t, 1)
			return req
		}
		read := func(req AcquireSegments, ts uint64) SealedSegmentHandle {
			handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, ts)
			require.NoError(t, err)
			require.Len(t, handles, 1)
			return handles[0]
		}
		take := func(s *gcProbeSegment, want uint64) {
			select {
			case got := <-s.calls:
				require.Equal(t, want, got)
			case <-time.After(time.Second * 4):
				t.Fatal("missing native GC call", want)
			}
		}
		old := bind(1, 20)
		first := <-physical
		current := bind(2, 60)
		require.Empty(t, physical)
		read(current, 70).Release()
		take(first, 20)
		// Phase two and exact requery still use the same leased old base.
		pinned := read(old, 25)
		read(old, 25).Release()
		dropped := f.beginDrop(old)
		requireDropPending(t, dropped)
		read(current, 70).Release()
		first.replies <- nil
		take(first, 25)
		pinned.Release()
		awaitShutdown(t, dropped)
		// The later acquisition coalesces a higher reservation before its native call.
		read(current, 70).Release()
		late := bind(3, 30)
		second := <-physical
		first.replies <- nil
		take(first, 60)
		read(late, 30).Release()
		take(second, 30)
		second.replies <- nil
		first.replies <- merr.WrapErrServiceInternalMsg("injected native GC failure")
		require.Eventually(t, func() bool {
			f.manager.mu.Lock()
			defer f.manager.mu.Unlock()
			b := f.manager.views[current.Key].bindings[1000].prepared.base
			return b.operationRefs == 0
		}, 3*time.Second, time.Millisecond)
		read(current, 70).Release()
		take(first, 60)
		// A failed call keeps the watermark; an old admission never enters the first store.
		_, err := f.manager.AcquireSealedSegmentHandles(t.Context(), current.Key, current.View, 59)
		require.Error(t, err)
		f.drop(t, late)
		closing := make(chan struct{})
		go func() { f.manager.Close(); close(closing) }()
		requireDropPending(t, closing)
		first.replies <- nil
		awaitShutdown(t, closing)
	})
}

func TestPendingViewLimitsSealedGCBeforeCollectionPreparation(t *testing.T) {
	mockey.PatchConvey("metadata and collection waits already protect historical reads", t, func() {
		f := setupResourceTest(t, nil)
		native := &gcProbeSegment{fakeTransformSegment: &fakeTransformSegment{id: 1000, partitionID: 10}, calls: make(chan uint64, 1), replies: make(chan error, 1)}
		f.loadMock.Return(native, nil)
		current := f.request(1, 1, 1)
		current.Meta.ReadAdmissionFloor.TransformingTimetick = 60
		f.manager.Acquire(current)
		f.await(t, 1)
		gate := make(chan struct{})
		f.dependencyMock.To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-gate:
				return &fakeCollectionRuntimeGuard{}, false, nil
			}
		})
		old := f.request(2, 1, 1)
		old.Meta.ReadAdmissionFloor.TransformingTimetick = 20
		f.manager.Acquire(old)
		h, err := f.manager.AcquireSealedSegmentHandles(t.Context(), current.Key, current.View, 70)
		require.NoError(t, err)
		require.EqualValues(t, 20, <-native.calls)
		native.replies <- nil
		h[0].Release()
		close(gate)
		f.await(t, 1)
		f.drop(t, old)
		f.drop(t, current)
	})
}

func TestSealedLateFloorRejectsChangedOriginalBase(t *testing.T) {
	mockey.PatchConvey("a fresh metadata fetch cannot substitute a same-numbered revision from a different base", t, func() {
		f := setupResourceTest(t, nil)
		current := f.request(1, 1, 1)
		current.Meta.ReadAdmissionFloor.TransformingTimetick = 60
		f.manager.Acquire(current)
		f.await(t, 1)
		hs, err := f.manager.AcquireSealedSegmentHandles(t.Context(), current.Key, current.View, 70)
		require.NoError(t, err)
		hs[0].Release()
		f.getMock.To(func(_ *resourceTestMetadata, _ context.Context, collection, id int64) (SegmentLoadInfoSnapshot, error) {
			s := resourceSnapshot(collection, id, 1)
			s.LoadInfo.ManifestPath = packed.MarshalManifestPath("replacement", 1)
			return s, nil
		})
		late := f.request(2, 1, 1)
		late.Meta.ReadAdmissionFloor.TransformingTimetick = 30
		failed := make(chan struct{}, 1)
		late.OnUnrecoverable = func() { failed <- struct{}{} }
		f.manager.Acquire(late)
		select {
		case <-failed:
		case <-time.After(3 * time.Second):
			t.Fatal("same revision with incompatible source should fail")
		}
		_, err = f.manager.AcquireSealedSegmentHandles(t.Context(), late.Key, late.View, 40)
		require.Error(t, err)
		require.EqualValues(t, 1, f.loads.Load())
		healthy, err := f.manager.AcquireSealedSegmentHandles(t.Context(), current.Key, current.View, 70)
		require.NoError(t, err)
		healthy[0].Release()
		f.drop(t, late)
		f.drop(t, current)
	})
}

type observedRetentionRegistration struct {
	instantTransformRegistration
	fail chan func(error)
}

func (r observedRetentionRegistration) OnFailure(callback func(error)) {
	// Only the first pending failure callback is used.
	select {
	case r.fail <- callback:
	default:
	}
}

func TestSealedLateFloorRejectsChangedAdditiveManifest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retireRead bool
		retireBase bool
	}{
		{name: "active"},
		{name: "retired_read", retireRead: true},
		{name: "retired_base", retireBase: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey("a shared physical base retains the source of additive reads", t, func() {
				f := setupResourceTest(t, nil)
				defer f.manager.Close()
				registered := make(chan func(error), 1)
				if tc.retireBase {
					f.registerMock.Return(observedRetentionRegistration{fail: registered}, nil)
				}
				prepare := func(version, manifest int64) AcquireSegments {
					f.manifest.Store(manifest)
					req := f.request(version, 1, 1)
					req.View.Partitions[0].SegmentManifestVersions[0] = manifest
					req.Meta.ReadAdmissionFloor.TransformingTimetick = 60
					f.manager.Acquire(req)
					f.await(t, 1)
					return req
				}
				first := prepare(1, 1)
				second := prepare(2, 2)
				require.EqualValues(t, 1, f.loads.Load(), "A/2 must fork the A/1 base")
				require.EqualValues(t, 2, f.prepares.Load())
				hs, err := f.manager.AcquireSealedSegmentHandles(t.Context(), second.Key, second.View, 70)
				require.NoError(t, err)
				owner := hs[0].(*resourceSegmentHandle).base
				hs[0].Release()
				healthy := second
				if tc.retireRead {
					f.drop(t, second)
					healthy = first
					require.Eventually(t, func() bool {
						f.manager.mu.Lock()
						defer f.manager.mu.Unlock()
						reads := f.manager.resources[1000].reads
						return len(reads) == 1 && reads[0].manifest == 1
					}, time.Second, time.Millisecond)
				}
				f.getMock.To(func(_ *resourceTestMetadata, _ context.Context, collection, id int64) (SegmentLoadInfoSnapshot, error) {
					snapshot := resourceSnapshot(collection, id, 2)
					snapshot.LoadInfo.ManifestPath = packed.MarshalManifestPath("replacement", 2)
					return snapshot, nil
				})
				late := f.request(3, 1, 1)
				late.View.Partitions[0].SegmentManifestVersions[0] = 2
				late.Meta.ReadAdmissionFloor.TransformingTimetick = 30
				ready := make(chan bool, 1)
				late.OnReady = func(map[int64][]int64) { ready <- true }
				late.OnUnrecoverable = func() { ready <- false }
				resume := make(chan struct{})
				if tc.retireBase {
					f.dependencyMock.To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
						select {
						case <-resume:
							return &fakeCollectionRuntimeGuard{}, false, nil
						case <-ctx.Done():
							return nil, false, ctx.Err()
						}
					})
				}
				f.manager.Acquire(late)
				if tc.retireBase {
					// An outstanding binding survives physical retirement while its
					// dependency fetch blocks. Its frozen source must survive too.
					select {
					case fail := <-registered:
						fail(merr.WrapErrServiceInternalMsg("retiring transform generation"))
					case <-time.After(3 * time.Second):
						t.Fatal("missing transform registration failure callback")
					}
					f.drop(t, first)
					f.drop(t, second)
					require.Eventually(t, func() bool { return f.releases.Load() == 1 }, time.Second, time.Millisecond)
					close(resume)
				}
				select {
				case got := <-ready:
					require.False(t, got, "a late View must not serve B/2 in place of A/2")
				case <-time.After(3 * time.Second):
					t.Fatal("late View did not finish preparation")
				}
				require.EqualValues(t, 1, f.loads.Load(), "B/2 must never reach physical load")
				_, err = f.manager.AcquireSealedSegmentHandles(t.Context(), late.Key, late.View, 40)
				require.Error(t, err)
				if tc.retireBase {
					return
				}
				hs, err = f.manager.AcquireSealedSegmentHandles(t.Context(), healthy.Key, healthy.View, 70)
				require.NoError(t, err)
				require.Same(t, owner, hs[0].(*resourceSegmentHandle).base)
				hs[0].Release()
			})
		})
	}
}
