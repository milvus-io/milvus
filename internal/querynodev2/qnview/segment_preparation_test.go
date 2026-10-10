package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSegmentPreparationAdmissionRetryAndNativeFastFail(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "load"
		if reopen {
			name = "reopen"
		}
		t.Run(name, func(t *testing.T) {
			reserveCalls, nativeCalls, failures, finished := 0, 0, 0, 0
			reservation := &fakeResourceReservation{}
			patchLifetime(t, mockey.Mock((*fakeSegmentResourceEstimator).Reserve).To(func(_ *fakeSegmentResourceEstimator, _ context.Context, _ *querypb.SegmentLoadInfo, _ CollectionRuntime) (ResourceReservation, error) {
				reserveCalls++
				if reserveCalls == 1 {
					return nil, merr.WrapErrSegmentRequestResourceFailed("memory")
				}
				return reservation, nil
			}).Build())
			nativeError := merr.WrapErrServiceInternalMsg("native preparation failed")
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).To(func(_ *fakePhysicalLoader, _ context.Context, _ *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
				nativeCalls++
				return nil, nativeError
			}).Build())
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Update).To(func(_ *fakePhysicalLoader, _ context.Context, _ TransformSegment, _ CollectionRuntime, _ SegmentLoadInfoSnapshot, _ SegmentUpdateAction) error {
				nativeCalls++
				return nativeError
			}).Build())
			var task nodescheduler.Task
			if reopen {
				task = newSegmentUpdateTask(&fakePhysicalLoader{}, SegmentUpdateTask{Segment: &fakeTransformSegment{id: 1000}, Snapshot: testSegmentLoadSnapshot(1000, 10), OnFailed: func(error) { failures++ }, OnFinished: func() { finished++ }}, &fakeSegmentResourceEstimator{})
			} else {
				task = newSegmentLoadTask(&fakePhysicalLoader{}, &fakeSegmentResourceEstimator{}, SegmentLoadTask{Snapshot: testSegmentLoadSnapshot(1000, 10), OnUnrecoverable: func(error) { failures++ }, OnFinished: func() { finished++ }})
			}
			require.ErrorIs(t, task.Execute(context.Background()), nodescheduler.ErrDelay)
			require.Zero(t, nativeCalls)
			require.Zero(t, failures)
			require.Zero(t, finished)
			require.ErrorIs(t, task.Execute(context.Background()), nativeError)
			require.Equal(t, 2, reserveCalls)
			require.Equal(t, 1, nativeCalls)
			require.Equal(t, 1, failures)
			require.Equal(t, 1, finished)
			require.True(t, reservation.released)
		})
	}
}

func TestSegmentLoadInfoUnionUsesNewestFieldConfiguration(t *testing.T) {
	old := QueryViewLoadInfo{Version: 1, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 11}}, IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 11}}}
	newer := QueryViewLoadInfo{Version: 2, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 12}, {FieldId: 102}}, IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 12}}}
	k1, k2 := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}, qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	requests := map[qviews.QueryViewKey]segmentLoadRequest{k1: {loadInfo: &old}, k2: {loadInfo: &newer}}
	union := unionLoadInfo(requests)
	require.Len(t, union.LoadFields, 3)
	for i, wanted := range []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 12}, {FieldId: 102}} {
		require.True(t, proto.Equal(wanted, union.LoadFields[i]))
	}
	require.True(t, coversLoadRequirements(union, &old))
	require.True(t, coversLoadRequirements(union, &newer))
	require.False(t, coversLoadRequirements(&old, &newer))
	snapshot := testSegmentLoadSnapshot(1000, 10)
	snapshot.LoadInfo.IndexInfos = []*querypb.FieldIndexInfo{{FieldID: 101, IndexID: 11}, {FieldID: 101, IndexID: 12}}
	planned, ready := planSegmentSnapshot(snapshot, union)
	require.True(t, ready)
	require.Len(t, planned.LoadInfo.IndexInfos, 1)
	require.Equal(t, int64(12), planned.LoadInfo.IndexInfos[0].GetIndexID())
	require.Len(t, snapshot.LoadInfo.IndexInfos, 2, "planning must not mutate metadata")
	delete(requests, k2)
	require.Equal(t, int64(11), unionLoadInfo(requests).LoadFields[1].GetIndexId())
	snapshot.LoadInfo.IndexInfos = snapshot.LoadInfo.IndexInfos[:1]
	_, ready = planSegmentSnapshot(snapshot, union)
	require.False(t, ready, "the requested new index is not available yet")
}

func TestSharedSegmentReopenGatesNewViewAndPreservesOldView(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			scheduler := nodescheduler.New(2)
			t.Cleanup(scheduler.Close)
			options := make(chan SegmentLoadInfoSubscriptionOption, 4)
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(proceed) }) })
			var updateCalls atomic.Int32
			patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).To(func(_ *fakeSegmentLoadInfoStream, option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
				options <- option
				return &lifetimeSubscription{}
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).To(func(*lifetimeSubscription) {}).Build())
			segment := &fakeTransformSegment{id: 1000, partitionID: 10}
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).To(func(_ *fakePhysicalLoader, _ context.Context, _ *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
				return segment, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Update).To(func(_ *fakePhysicalLoader, ctx context.Context, _ TransformSegment, _ CollectionRuntime, snapshot SegmentLoadInfoSnapshot, action SegmentUpdateAction) error {
				updateCalls.Add(1)
				require.Equal(t, SegmentUpdateReopen, action)
				require.Equal(t, int64(12), snapshot.LoadInfo.IndexInfos[0].GetIndexID())
				close(entered)
				select {
				case <-proceed:
				case <-ctx.Done():
					return ctx.Err()
				}
				if fail {
					return merr.WrapErrServiceInternalMsg("reopen failed")
				}
				return nil
			}).Build())
			manager := newTestSegmentPreparerWithStream(scheduler, &fakePhysicalLoader{}, &fakeSegmentLoadInfoStream{})
			ready := make(chan int, 4)
			failed := make(chan int, 4)
			acquire := func(version int64, fields []*messagespb.LoadFieldConfig) qviews.QueryViewKey {
				meta := buildHandlerTestMeta(version)
				view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
				key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
				manager.Acquire(segmentPreparationRequest{Key: key, Meta: meta, View: view, Collection: &fakeCollectionRuntimeGuard{}, LoadInfo: &QueryViewLoadInfo{CollectionID: testCollectionID, Version: QueryViewLoadInfoVersion(version), LoadFields: fields}, OnLoaded: func([]TransformSegment) { ready <- int(version) }, OnUnrecoverable: func() { failed <- int(version) }})
				return key
			}
			oldKey := acquire(1, []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101, IndexId: 11}})
			oldOption := <-options
			snapshot := testSegmentLoadSnapshot(1000, 10)
			snapshot.DataVersion = oldOption.DataVersion
			snapshot.LoadInfo.IndexInfos = []*querypb.FieldIndexInfo{{FieldID: 101, IndexID: 11}}
			require.NoError(t, oldOption.Handler.Handle(snapshot))
			select {
			case v := <-ready:
				require.Equal(t, 1, v)
			case <-time.After(time.Second):
				t.Fatal("old view did not load")
			}
			newKey := acquire(2, []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 12}, {FieldId: 102}})
			newOption := <-options
			require.Len(t, newOption.LoadInfo.LoadFields, 3)
			require.Equal(t, int64(100), newOption.LoadInfo.LoadFields[0].GetFieldId())
			snapshot.DataVersion = newOption.DataVersion
			// Keep the metadata revision unchanged: the changed configuration must
			// still trigger native Reopen and readiness must wait for it.
			snapshot.LoadInfo.IndexInfos = []*querypb.FieldIndexInfo{{FieldID: 101, IndexID: 12}}
			require.NoError(t, newOption.Handler.Handle(snapshot))
			waitGenerationEvent(t, entered)
			require.Empty(t, ready)
			require.Empty(t, failed)
			once.Do(func() { close(proceed) })
			if fail {
				select {
				case v := <-failed:
					require.Equal(t, 2, v)
				case <-time.After(time.Second):
					t.Fatal("new view did not fail")
				}
				require.Empty(t, ready)
			} else {
				select {
				case v := <-ready:
					require.Equal(t, 2, v)
				case <-time.After(time.Second):
					t.Fatal("new view did not become ready")
				}
				require.Empty(t, failed)
			}
			require.Equal(t, int32(1), updateCalls.Load())
			manager.mu.Lock()
			require.Same(t, segment, manager.segments[1000].segment)
			require.Contains(t, manager.segments[1000].refs, oldKey)
			manager.mu.Unlock()
			manager.Release(ReleaseSegments{Key: newKey})
			manager.Release(ReleaseSegments{Key: oldKey})
		})
	}
}

func TestDataVersionProofGatesReadyIndependentlyOfContentRevision(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	manager := newTestSegmentPreparer(scheduler, nil)
	key := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	old := qviews.DataVersion{StreamingVersion: 1, CompactVersion: 99}
	target := qviews.DataVersion{StreamingVersion: 2}
	ready := 0
	ref := &queryViewRef{dataVersion: target, loaded: make(map[int64]bool), segments: map[int64]int64{1000: 10}, onLoaded: func([]TransformSegment) { ready++ }}
	manager.views[key] = ref
	state := &segmentState{segment: &fakeTransformSegment{id: 1000}, dataVersion: old, acceptedVersion: old, revision: SegmentLoadInfoRevision{Revision: 10}, refs: map[qviews.QueryViewKey]struct{}{key: {}}, requests: map[qviews.QueryViewKey]segmentLoadRequest{key: {}}}
	manager.segments[1000] = state
	require.Empty(t, manager.loadedNotificationsLocked(state))
	snapshot := testSegmentLoadSnapshot(1000, 10)
	snapshot.Revision = state.revision
	snapshot.DataVersion = target
	_, update, ok := manager.recordSegmentSnapshot(context.Background(), snapshot, state)
	require.True(t, ok)
	require.Zero(t, ready)
	// No loader is installed: identical content needs only a newer certified
	// DataVersion, and must not run native Reopen.
	require.NoError(t, update.task.Execute(context.Background()))
	require.Equal(t, 1, ready)
	require.Equal(t, target, state.dataVersion)
	snapshot.DataVersion = old
	snapshot.Revision.Revision = 999
	_, _, ok = manager.recordSegmentSnapshot(context.Background(), snapshot, state)
	require.False(t, ok, "a larger content hash does not make stale data newer")
	require.Equal(t, target, state.dataVersion)
}

func TestReopenAdmissionCancellationReleasesAttemptReferences(t *testing.T) {
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	entered := make(chan struct{}, 1)
	patchLifetime(t, mockey.Mock((*fakeSegmentResourceEstimator).Reserve).To(func(_ *fakeSegmentResourceEstimator, _ context.Context, _ *querypb.SegmentLoadInfo, _ CollectionRuntime) (ResourceReservation, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		return nil, merr.WrapErrSegmentRequestResourceFailed("memory")
	}).Build())
	manager := newTestSegmentPreparer(scheduler, nil, &fakeSegmentResourceEstimator{})
	key := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
	manager.views[key] = &queryViewRef{segments: map[int64]int64{1000: 10}}
	manager.segments[1000] = &segmentState{segment: &fakeTransformSegment{id: 1000}, revision: SegmentLoadInfoRevision{Revision: 1}, refs: map[qviews.QueryViewKey]struct{}{key: {}}, requests: map[qviews.QueryViewKey]segmentLoadRequest{key: {}}}
	manager.views[key].states = map[int64]*segmentState{1000: manager.segments[1000]}
	snapshot := testSegmentLoadSnapshot(1000, 10)
	snapshot.Revision.Revision = 2
	manager.ApplyLoadInfoSnapshot(context.Background(), snapshot)
	waitGenerationEvent(t, entered)
	dropped := make(chan struct{})
	manager.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	waitGenerationEvent(t, dropped)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	require.Empty(t, manager.views)
	require.Empty(t, manager.segments)
}

func TestLoadPlanKeepsPackedFieldsAndUsesNewLoadParameters(t *testing.T) {
	old := QueryViewLoadInfo{CollectionID: 1, Version: 1, PartitionIDs: []int64{10}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 101, IndexId: 11}}, IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 11, IndexParams: []*commonpb.KeyValuePair{{Key: "warmup", Value: "sync"}}}}}
	newer := CloneQueryViewLoadInfo(old)
	newer.Version = 2
	newer.IndexInfos[0].IndexParams[0].Value = "async"
	key1 := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
	key2 := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	oldUnion := unionLoadInfo(map[qviews.QueryViewKey]segmentLoadRequest{key1: {loadInfo: &old}})
	union := unionLoadInfo(map[qviews.QueryViewKey]segmentLoadRequest{key1: {loadInfo: &old}, key2: {loadInfo: &newer}})
	require.False(t, coversLoadRequirements(oldUnion, &newer), "same index ID with different load parameters still needs preparation")
	require.False(t, sameLoadRequirements(oldUnion, union))
	require.True(t, sameLoadRequirements(union, unionLoadInfo(map[qviews.QueryViewKey]segmentLoadRequest{key2: {loadInfo: &newer}, key1: {loadInfo: &old}})))
	require.True(t, coversLoadRequirements(union, &old))
	snapshot := testSegmentLoadSnapshot(1000, 10)
	snapshot.LoadInfo.BinlogPaths = []*datapb.FieldBinlog{{FieldID: 0}, {FieldID: 100, ChildFields: []int64{101, 102}}, {FieldID: 103}}
	snapshot.LoadInfo.IndexInfos = []*querypb.FieldIndexInfo{{FieldID: 101, IndexID: 11, IndexParams: []*commonpb.KeyValuePair{{Key: "warmup", Value: "sync"}, {Key: "dim", Value: "128"}}}}
	plan, ready := planSegmentSnapshot(snapshot, union)
	require.True(t, ready)
	require.Len(t, plan.LoadInfo.BinlogPaths, 2)
	require.Equal(t, int64(0), plan.LoadInfo.BinlogPaths[0].GetFieldID())
	require.Equal(t, []int64{101, 102}, plan.LoadInfo.BinlogPaths[1].GetChildFields())
	params := make(map[string]string)
	for _, p := range plan.LoadInfo.IndexInfos[0].IndexParams {
		params[p.GetKey()] = p.GetValue()
	}
	require.Equal(t, map[string]string{"warmup": "async", "dim": "128"}, params)
	newer.LoadFields[0].IndexId = 99
	require.Equal(t, int64(11), union.LoadFields[0].GetIndexId(), "queued plans own their immutable requirements")
	changed := CloneQueryViewLoadInfo(*union)
	changed.LoadFields[0].IndexId = 99
	require.False(t, sameLoadRequirements(union, &changed))
	require.False(t, coversLoadRequirements(nil, &old))
}

func TestTransformReadySegmentStillWaitsForViewPhysicalReadiness(t *testing.T) {
	key := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	ready := 0
	state := &segmentState{state: transformSegmentLoaded, segment: segment, refs: map[qviews.QueryViewKey]struct{}{key: {}}, waiters: map[qviews.QueryViewKey]transformSegmentWaiter{key: {partitionID: 10, segmentID: 1000, onReady: func(map[int64][]int64) { ready++ }}}}
	ref := &queryViewRef{physicalReady: make(map[int64]bool), states: map[int64]*segmentState{1000: state}}
	manager := &QueryViewSegmentManager{views: map[qviews.QueryViewKey]*queryViewRef{key: ref}, segments: map[int64]*segmentState{1000: state}}
	view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	manager.onPhysicalLoaded([]TransformSegment{segment}, ref.states)
	require.Zero(t, ready)
	_, err := manager.AcquireSealedSegmentHandles(context.Background(), key, view)
	require.Error(t, err, "another view's loaded instance cannot bypass this view's preparation")
	manager.onViewPhysicalLoaded(key, ref, []TransformSegment{segment})
	require.Equal(t, 1, ready)
	handles, err := manager.AcquireSealedSegmentHandles(context.Background(), key, view)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	handles[0].Release()
}
