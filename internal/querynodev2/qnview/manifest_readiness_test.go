package qnview

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPhysicalReadinessRequiresRefreshedManifest(t *testing.T) {
	scheduler := &fakeNodeScheduler{}
	stream := &fakeSegmentLoadInfoStream{}
	manager := newTestViewScopedPhysicalSegmentManager(t, scheduler, stream)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	acquire := func(version int64, ready chan bool) {
		meta := buildHandlerTestMeta(version)
		view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{
			PartitionId: 10, SegmentIds: []int64{1000}, SegmentManifestVersions: []int64{version},
		}}}
		manager.Acquire(AcquirePhysicalSegments{
			Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view, Collection: runtime,
			OnLoaded:        func([]TransformSegment) { ready <- true },
			OnUnrecoverable: func() { t.Error("unexpected unrecoverable") },
		})
	}
	snapshot := func(version int64) SegmentLoadInfoSnapshot {
		return SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID, SegmentID: 1000,
			Revision: SegmentLoadInfoRevision{Revision: uint64(version)},
			LoadInfo: &querypb.SegmentLoadInfo{
				CollectionID: testCollectionID, SegmentID: 1000, PartitionID: 10,
				ManifestPath: packed.MarshalManifestPath("segment", version),
			},
		}
	}
	first, second := make(chan bool, 2), make(chan bool, 2)
	acquire(1, first)
	require.NoError(t, stream.Emit(snapshot(1)))
	require.Eventually(t, func() bool { return len(scheduler.Tasks()) == 1 }, time.Second, time.Millisecond)
	scheduler.Tasks()[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	require.Eventually(t, func() bool { return len(first) == 1 }, time.Second, time.Millisecond)

	// Same schema/load config and SegmentID, but a different base version.
	acquire(2, second)
	require.Empty(t, second)
	require.Empty(t, scheduler.Updates(), "the old snapshot must not be reopened in a loop")
	require.NoError(t, stream.Emit(snapshot(2)))
	require.Eventually(t, func() bool { return len(scheduler.Updates()) == 1 }, time.Second, time.Millisecond)
	require.Empty(t, second, "accepting metadata is not physical readiness")
	scheduler.Updates()[0].OnUpdated(snapshot(2).Revision)
	require.Eventually(t, func() bool { return len(second) == 1 }, time.Second, time.Millisecond)
	require.Len(t, stream.subscriptions, 1, "view handoff retains the shared subscription")
}
