package dataview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func newSnapshotTestManager(t *testing.T) *dataViewManager {
	t.Helper()
	patch := mockey.Mock((*datacatalog.Catalog).SaveDataView).Return(nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return newManager(t.Context(), datacatalog.NewCatalog(nil, "", ""), nil)
}

func TestNativeSnapshotScopeAndOwnership(t *testing.T) {
	m := newSnapshotTestManager(t)
	ctx := t.Context()
	for _, id := range []int64{1, 2} {
		_, err := m.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{
			CollectionID: id,
			VChannels:    []string{"v0", "v1"},
			Segments:     []LoadableSegment{segmentWithRows(id*10, "v0", 100, id*42)},
		})
		require.NoError(t, err)
	}
	all := m.DataViewSnapshot(ctx)
	_, ok := all.DataVersion(2)
	require.True(t, ok)
	empty := m.DataViewSnapshotForCollections(ctx, map[int64]struct{}{})
	_, ok = empty.DataVersion(1)
	require.False(t, ok)
	snapshot := m.DataViewSnapshotForCollections(ctx, map[int64]struct{}{1: {}, 99: {}})
	_, ok = snapshot.DataVersion(2)
	require.False(t, ok)
	_, ok = snapshot.DataVersion(99)
	require.False(t, ok)
	version, ok := snapshot.DataVersion(1)
	require.True(t, ok)
	require.Equal(t, qviews.DataVersion{StreamingVersion: 1}, version)
	shard, ok := snapshot.ShardView(1, "v1")
	require.True(t, ok, "empty declared shards must survive projection")
	require.Empty(t, shard.Partitions)
	segment, ok := snapshot.SegmentInfo(10)
	require.True(t, ok)
	require.Equal(t, int64(42), segment.RowNum)
	require.Equal(t, int64(100), segment.PartitionID)

	_, err := m.RecomputeNow(ctx, 1, projectSegments(segmentWithRows(11, "v0", 100, 99)))
	require.NoError(t, err)
	latest := m.DataViewSnapshot(ctx)
	_, ok = latest.SegmentInfo(10)
	require.False(t, ok)
	_, ok = snapshot.SegmentInfo(11)
	require.False(t, ok, "published snapshots must not change with the Manager")

	// Consumers own the detached native snapshot, not the Manager's backing data.
	segment.RowNum = -1
	ref, err := m.Get(ctx, 1, version.IntoProto())
	require.NoError(t, err)
	require.NotNil(t, ref)
	defer ref.Deref()
	stats, ok := ref.Stats(10)
	require.True(t, ok)
	require.Equal(t, int64(42), stats.RowNum)
}

func TestNativeSnapshotDoesNotHoldManagerLockWhileWaitingForCollection(t *testing.T) {
	m := newSnapshotTestManager(t)
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}})
	require.NoError(t, err)
	state := m.getState(1)
	entered := make(chan struct{})
	var original func(*dataViewManager, *collectionState) *api.CollectionDataView
	patch := mockey.Mock((*dataViewManager).collectionDataView).To(func(manager *dataViewManager, state *collectionState) *api.CollectionDataView {
		close(entered)
		return original(manager, state)
	}).Origin(&original).Build()
	t.Cleanup(func() { patch.UnPatch() })

	state.mu.Lock()
	done := make(chan struct{})
	go func() {
		m.DataViewSnapshot(t.Context())
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		state.mu.Unlock()
		t.Fatal("snapshot did not reach the collection")
	}
	unlocked := m.mu.TryLock()
	if unlocked {
		m.mu.Unlock()
	}
	state.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not complete")
	}
	require.True(t, unlocked, "a blocked collection must not prevent manager writers")
}

func TestNativeSnapshotSkipsCollectionDroppedAfterSelection(t *testing.T) {
	m := newSnapshotTestManager(t)
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}})
	require.NoError(t, err)
	drop := mockey.Mock((*datacatalog.Catalog).DropDataViews).Return(nil).Build()
	t.Cleanup(func() { drop.UnPatch() })
	var original func(*dataViewManager, *collectionState) *api.CollectionDataView
	patch := mockey.Mock((*dataViewManager).collectionDataView).To(func(manager *dataViewManager, state *collectionState) *api.CollectionDataView {
		_, dropErr := manager.OnDropCollection(context.Background(), state.id)
		require.NoError(t, dropErr)
		return original(manager, state)
	}).Origin(&original).Build()
	t.Cleanup(func() { patch.UnPatch() })

	snapshot := m.DataViewSnapshot(t.Context())
	_, ok := snapshot.DataVersion(1)
	require.False(t, ok)
}
