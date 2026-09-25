package dataview

import (
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newPublicationTestManager(t *testing.T) *dataViewManager {
	t.Helper()
	patch := mockey.Mock((*datacatalog.Catalog).SaveDataView).Return(nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return newManager(t.Context(), datacatalog.NewCatalog(nil, "", ""), nil)
}

func TestDataViewPublicationReplayCommitAndDrop(t *testing.T) {
	m := newPublicationTestManager(t)
	ctx := t.Context()
	_, err := m.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0", "v1"}, Segments: []LoadableSegment{segmentWithRows(10, "v0", 100, 42)}})
	require.NoError(t, err)
	var published []*api.CollectionDataView
	unsubscribe := m.RegisterDataViewListener(func(id int64, view *api.CollectionDataView) {
		require.Equal(t, int64(1), id)
		published = append(published, view)
	})
	require.Len(t, published, 1)
	first := published[0]
	require.Equal(t, int64(42), first.Shards[0].TotalRows)
	require.Equal(t, 1, first.Shards[0].SegmentCount)
	require.NotNil(t, first.Shard("v1"), "declared empty shards survive publication")
	require.Empty(t, first.Shard("v1").Partitions)
	segment, ok := first.Segment(10)
	require.True(t, ok)
	require.Equal(t, int64(100), segment.PartitionID)
	require.Equal(t, int64(42), segment.RowNum)
	_, err = m.RecomputeNow(ctx, 1, projectSegments(segmentWithRows(11, "v0", 100, 99)))
	require.NoError(t, err)
	require.Len(t, published, 2)
	require.Equal(t, int64(99), published[1].Shards[0].TotalRows)
	require.Equal(t, int64(42), first.Shards[0].TotalRows)
	_, err = m.RecomputeNow(ctx, 1, projectSegments(segmentWithRows(11, "v0", 100, 99)))
	require.NoError(t, err)
	require.Len(t, published, 2, "no-op must reuse the native projection")
	patch := mockey.Mock((*datacatalog.Catalog).DropDataViews).Return(nil).Build()
	defer patch.UnPatch()
	_, err = m.OnDropCollection(ctx, 1)
	require.NoError(t, err)
	require.Len(t, published, 3)
	require.Nil(t, published[2])
	unsubscribe()
	replay := m.RegisterDataViewListener(func(int64, *api.CollectionDataView) { t.Error("dropped state replayed") })
	replay()
	_, err = m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 2, VChannels: []string{"v0"}})
	require.NoError(t, err)
	require.Len(t, published, 3)
}

func TestDataViewFailedCommitDoesNotPublish(t *testing.T) {
	patch := mockey.Mock((*datacatalog.Catalog).SaveDataView).Return(merr.WrapErrServiceUnavailableMsg("catalog unavailable")).Build()
	defer patch.UnPatch()
	m := newManager(t.Context(), datacatalog.NewCatalog(nil, "", ""), nil)
	calls := 0
	unsubscribe := m.RegisterDataViewListener(func(int64, *api.CollectionDataView) { calls++ })
	defer unsubscribe()
	_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}})
	require.Error(t, err)
	require.Zero(t, calls)
}

func TestDataViewRegistrationSerializesConcurrentCommit(t *testing.T) {
	m := newPublicationTestManager(t)
	var wg sync.WaitGroup
	for id := int64(1); id <= 20; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			_, err := m.OnCreateCollection(t.Context(), CreateCollectionDataViewEvent{CollectionID: id, VChannels: []string{"v0"}})
			if err != nil {
				t.Error(err)
			}
		}(id)
	}
	seen := make(map[int64]int)
	unsubscribe := m.RegisterDataViewListener(func(id int64, _ *api.CollectionDataView) { seen[id]++ })
	wg.Wait()
	unsubscribe()
	require.Len(t, seen, 20)
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
}

func TestDataViewFlushPublishesOnlyCommit(t *testing.T) {
	m := newPublicationTestManager(t)
	ctx := t.Context()
	_, err := m.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}})
	require.NoError(t, err)
	var published []*api.CollectionDataView
	stop := m.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) { published = append(published, view) })
	defer stop()
	_, err = m.RecomputeNow(ctx, 1, projectSegments())
	require.NoError(t, err)
	require.Len(t, published, 1, "empty no-op must not republish")
	event := FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segmentWithRows(100, "v0", 10, 42)}}
	_, _, abort, err := m.PrepareFlush(ctx, event)
	require.NoError(t, err)
	require.Len(t, published, 1)
	abort()
	require.Len(t, published, 1)
	view, commit, abort, err := m.PrepareFlush(ctx, event)
	require.NoError(t, err)
	defer abort()
	require.Len(t, published, 1)
	require.NoError(t, m.catalog.SaveDataView(ctx, view))
	commit()
	commit()
	require.Len(t, published, 2)
	require.Equal(t, int64(42), published[1].Shards[0].TotalRows)
	require.Empty(t, published[0].Shards[0].Partitions)
}

func TestRecoveryFootprintFillReplacesPublishedObjects(t *testing.T) {
	view := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(2, 0), Shards: []*viewpb.DataViewOfShard{{Vchannel: "v0", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}}}}}}
	patch := mockey.Mock((*datacatalog.Catalog).ListAllDataViews).Return([]*viewpb.DataViewOfCollection{view}, nil).Build()
	defer patch.UnPatch()
	m, err := RecoverManager(t.Context(), datacatalog.NewCatalog(nil, "", ""), recoverAllCollections, nil, nil, nil)
	require.NoError(t, err)
	var published []*api.CollectionDataView
	stop := m.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) { published = append(published, view) })
	defer stop()
	require.Len(t, published, 1)
	require.Zero(t, published[0].Shards[0].TotalRows)
	oldRef, err := m.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer oldRef.Deref()
	_, known := oldRef.Stats(100)
	require.False(t, known)
	_, err = m.RecomputeNow(t.Context(), 1, projectSegments(segmentWithRows(100, "v0", 10, 42)))
	require.NoError(t, err)
	require.Len(t, published, 2)
	require.Equal(t, published[0].DataVersion, published[1].DataVersion)
	require.Equal(t, int64(42), published[1].Shards[0].TotalRows)
	require.Zero(t, published[0].Shards[0].TotalRows)
	_, known = oldRef.Stats(100)
	require.False(t, known, "old readers remain immutable through same-version fill")
	newRef, err := m.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer newRef.Deref()
	stats, known := newRef.Stats(100)
	require.True(t, known)
	require.Equal(t, int64(42), stats.RowNum)
}
