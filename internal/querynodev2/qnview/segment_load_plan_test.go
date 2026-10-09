package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSharedLoadPlanSelectionAndLifetime(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			patchLifetime(t, mockey.Mock((*fakeCollectionRuntimeGuard).SchemaVersion).To(func(g *fakeCollectionRuntimeGuard) int64 { return g.schemaVersion }).Build())
			patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).Return(&lifetimeSubscription{}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).Return().Build())
			scheduler := nodescheduler.New(1)
			t.Cleanup(scheduler.Close)
			manager := newTestSegmentPreparerWithStream(scheduler, nil, &fakeSegmentLoadInfoStream{})
			view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
			var requests []segmentPreparationRequest
			// Schema wins over config version; within one schema config version
			// wins. Equal versions are resolved by a stable view identity.
			for i, versions := range [][3]uint64{{1, 9, 0}, {2, 1, 70}, {2, 2, 80}, {2, 2, 90}} {
				meta := buildHandlerTestMeta(int64(i + 1))
				meta.LoadInfoVersion = versions[1]
				meta.TransformStartAfterTimetick = versions[2]
				requests = append(requests, segmentPreparationRequest{
					Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view,
					Collection: &fakeCollectionRuntimeGuard{schemaVersion: int64(versions[0])},
					LoadInfo:   &QueryViewLoadInfo{Version: QueryViewLoadInfoVersion(versions[1]), LoadFields: []*messagespb.LoadFieldConfig{{FieldId: int64(100 + i)}}},
				})
			}
			for i := range requests {
				j := i
				if reverse {
					j = len(requests) - 1 - i
				}
				manager.Acquire(requests[j])
			}
			snapshot := testSegmentLoadSnapshot(1000, 10)
			submission, _, ok := manager.recordSegmentSnapshot(context.Background(), snapshot, nil)
			require.True(t, ok)
			require.Same(t, requests[2].Collection, submission.request.collection)
			require.Zero(t, submission.request.transformStartAfterTimeTick, "zero is a real frontier, not an unspecified value")
			require.Len(t, submission.snapshot.resources.LoadFields, 4)
			dropped := make(chan struct{})
			manager.Release(ReleaseSegments{Key: requests[2].Key, OnDropped: func() { close(dropped) }})
			select {
			case <-dropped:
				t.Fatal("selected collection owner released before its preparation attempt finished")
			default:
			}
			manager.mu.Lock()
			next, found := manager.segments[1000].loadRequest()
			manager.mu.Unlock()
			require.True(t, found)
			require.Same(t, requests[3].Collection, next.collection)
			require.Same(t, requests[2].Collection, submission.request.collection, "queued plan must remain fixed")
			require.Len(t, submission.snapshot.resources.LoadFields, 4)
			submission.done()
			select {
			case <-dropped:
			case <-time.After(time.Second):
				t.Fatal("attempt completion did not release the selected view")
			}
			for _, req := range requests {
				manager.Release(ReleaseSegments{Key: req.Key})
			}
		})
	}
}

type plannedTestLoader struct{ PhysicalSegmentLoader }

func (*plannedTestLoader) LoadWithPlan(context.Context, SegmentLoadPlan) (TransformSegment, error) {
	panic("mockey")
}

func TestSegmentLoadTaskPassesExplicitPlan(t *testing.T) {
	for _, frontier := range []uint64{0, 99} {
		segment := &fakeTransformSegment{id: 1000}
		var captured SegmentLoadPlan
		patch := mockey.Mock((*plannedTestLoader).LoadWithPlan).To(func(_ *plannedTestLoader, _ context.Context, plan SegmentLoadPlan) (TransformSegment, error) {
			captured = plan
			return segment, nil
		}).Build()
		snapshot := testSegmentLoadSnapshot(1000, 10)
		collection := &fakeCollectionRuntimeGuard{}
		task := newSegmentLoadTask(&plannedTestLoader{}, nil, SegmentLoadTask{Snapshot: snapshot, Collection: collection, TransformStartAfterTimeTick: frontier})
		loaded, err := task.load(context.Background())
		patch.UnPatch()
		require.NoError(t, err)
		require.Same(t, segment, loaded)
		require.Same(t, snapshot.LoadInfo, captured.LoadInfo)
		require.Same(t, collection, captured.Collection)
		require.Equal(t, frontier, captured.TransformStartAfterTimeTick)
	}
}

func TestLegacyLoaderCannotPublishMismatchedTransformProgress(t *testing.T) {
	segment := &fakeTransformSegment{id: 1000, startAfter: 10, applied: 10}
	patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).Return(segment, nil).Build())
	released := false
	patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error { released = true; return nil }).Build())
	task := newSegmentLoadTask(&fakePhysicalLoader{}, nil, SegmentLoadTask{Snapshot: SegmentLoadInfoSnapshot{LoadInfo: &querypb.SegmentLoadInfo{}}, TransformStartAfterTimeTick: 99})
	loaded, err := task.load(context.Background())
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Nil(t, loaded)
	require.True(t, released)
}
