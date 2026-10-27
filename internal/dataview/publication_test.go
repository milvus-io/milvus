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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	view := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(2, 0), Shards: []*viewpb.DataViewOfShard{{Vchannel: "v0", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 10, SegmentIds: []int64{100}, SegmentManifestVersions: []int64{0}}}}}}
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

func TestLogicalSizeIsFrozenUntilNextDataView(t *testing.T) {
	p := paramtable.Get()
	key := p.QueryViewCfg.LogicalSizeSegmentBaseCost.Key
	original := p.QueryViewCfg.LogicalSizeSegmentBaseCost.GetValue()
	t.Cleanup(func() { require.NoError(t, p.Save(key, original)) })
	require.NoError(t, p.Save(key, "10"))
	m := newPublicationTestManager(t)
	ctx := t.Context()
	initial := []LoadableSegment{segmentWithRows(10, "v0", 100, 5), segmentWithRows(20, "v0", 100, 0)}
	_, err := m.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}, Segments: initial})
	require.NoError(t, err)
	var published []*api.CollectionDataView
	stop := m.RegisterDataViewListener(func(_ int64, view *api.CollectionDataView) { published = append(published, view) })
	defer stop()
	ref, err := m.Latest(ctx, 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.Equal(t, int64(5), published[0].TotalRows)
	require.Equal(t, int64(25), published[0].LogicalSize)
	stat, known := ref.Stats(20)
	require.True(t, known)
	require.Equal(t, SegmentStats{RowNum: 0, LogicalSize: 10}, stat)
	require.NoError(t, p.Save(key, "100"))
	_, err = m.RecomputeNow(ctx, 1, projectSegments(initial...))
	require.NoError(t, err)
	require.Len(t, published, 1, "cost changes and no-op recomputes must not republish")
	view, commit, abort, err := m.PrepareFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segmentWithRows(30, "v0", 100, 7)}})
	require.NoError(t, err)
	defer abort()
	require.NoError(t, p.Save(key, "1000")) // Construction, not commit, freezes the profile.
	require.NoError(t, m.catalog.SaveDataView(ctx, view))
	commit()
	require.Len(t, published, 2)
	require.Equal(t, int64(312), published[1].LogicalSize)
	require.Equal(t, int64(12), published[1].TotalRows)
	for id, want := range map[int64]int64{10: 105, 20: 100, 30: 107} {
		segment, found := published[1].Segment(id)
		require.True(t, found)
		require.Equal(t, want, segment.LogicalSize)
	}
	stat, known = ref.Stats(20)
	require.True(t, known)
	require.Equal(t, int64(10), stat.LogicalSize, "old refs preserve their own cost")
	view, commit, abort, err = m.PublishChange(ctx, ChangeGroupDataViewEvent{CollectionID: 1, NewSegments: []LoadableSegment{segmentWithRows(40, "v0", 100, 9)}, SupersededSegmentIDs: []int64{30}})
	require.NoError(t, err)
	defer abort()
	require.NoError(t, m.catalog.SaveDataView(ctx, view))
	commit()
	require.Len(t, published, 3)
	require.Equal(t, int64(3014), published[2].LogicalSize)
	require.Equal(t, int64(14), published[2].TotalRows)
	require.Equal(t, int64(25), published[0].LogicalSize)
	require.Equal(t, int64(312), published[1].LogicalSize)
	// Invalid configuration is ignored when the next real projection is built.
	require.NoError(t, p.Save(key, "-1"))
	_, err = m.RecomputeNow(ctx, 1, projectSegments(segmentWithRows(50, "v0", 100, 4)))
	require.NoError(t, err)
	require.Equal(t, int64(1004), published[3].LogicalSize)
}

func TestLogicalSizeModeIsFixedAtInitialization(t *testing.T) {
	p := paramtable.Get()
	mode, cost := &p.QueryViewCfg.LogicalSizeSource, &p.QueryViewCfg.LogicalSizeSegmentBaseCost
	unit := &p.QueryViewCfg.LogicalSizeBytesPerUnit
	originalUnit := unit.GetValue()
	originalMode, originalCost := mode.GetValue(), cost.GetValue()
	t.Cleanup(func() {
		require.NoError(t, p.Save(unit.Key, originalUnit))
		require.NoError(t, p.Save(mode.Key, originalMode))
		require.NoError(t, p.Save(cost.Key, originalCost))
	})
	for _, tc := range []struct {
		mode, changed, unit string
		want, replacement   int64
	}{
		{"rowCount", "insertBytes", "64KB", 100, 100},
		{"invalid", "insertBytes", "64KB", 100, 100},
		{"insertBytes", "rowCount", "64KB", 3, 1},
		{"insertBytes", "rowCount", "32KB", 5, 2},
		{"insertBytes", "rowCount", "0", 3, 1},
		{"insertBytes", "rowCount", "invalid", 3, 1},
	} {
		t.Run(tc.mode+"/"+tc.unit, func(t *testing.T) {
			require.NoError(t, p.Save(mode.Key, tc.mode))
			require.NoError(t, p.Save(unit.Key, tc.unit))
			require.NoError(t, p.Save(cost.Key, "10"))
			m := newPublicationTestManager(t)
			ctx := t.Context()
			initial := segmentWithRows(10, "v0", 100, 100)
			initial.Size = 131073
			_, err := m.OnBootstrapCollection(ctx, BootstrapCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v0"}, Segments: []LoadableSegment{initial}})
			require.NoError(t, err)
			old, err := m.Latest(ctx, 1)
			require.NoError(t, err)
			defer old.Deref()
			stat, ok := old.Stats(10)
			require.True(t, ok)
			require.Equal(t, SegmentStats{RowNum: 100, Size: 131073, LogicalSize: tc.want + 10}, stat)

			// Even future publications from this manager retain its original mode.
			require.NoError(t, p.Save(mode.Key, tc.changed))
			require.NoError(t, p.Save(unit.Key, "1MB"))
			require.NoError(t, p.Save(cost.Key, "20"))
			_, commit, abort, err := m.PrepareFlush(ctx, FlushDataViewEvent{CollectionID: 1, Segments: []LoadableSegment{segmentWithRows(20, "v0", 100, 7)}})
			require.NoError(t, err)
			defer abort()
			commit()
			ref, err := m.Latest(ctx, 1)
			require.NoError(t, err)
			defer ref.Deref()
			stat, ok = ref.Stats(10)
			require.True(t, ok)
			require.Equal(t, tc.want+20, stat.LogicalSize, "carried member retains raw size without double-counting the base cost")
			stat, ok = ref.Stats(20)
			require.True(t, ok)
			require.Equal(t, int64(27), stat.LogicalSize, "missing size falls back to row units")
			stat, _ = old.Stats(10)
			require.Equal(t, tc.want+10, stat.LogicalSize)

			replacement := segmentWithRows(30, "v0", 100, 100)
			replacement.Size = 65536
			_, commit, abort, err = m.PublishChange(ctx, ChangeGroupDataViewEvent{CollectionID: 1, NewSegments: []LoadableSegment{replacement}, SupersededSegmentIDs: []int64{10}})
			require.NoError(t, err)
			defer abort()
			commit()
			ref, err = m.Latest(ctx, 1)
			require.NoError(t, err)
			defer ref.Deref()
			_, ok = ref.Stats(10)
			require.False(t, ok)
			stat, ok = ref.Stats(30)
			require.True(t, ok)
			want := tc.replacement + 20
			require.Equal(t, want, stat.LogicalSize)
		})
	}
}

func TestLogicalSizeFromBytesRecoveryAndZero(t *testing.T) {
	p := paramtable.Get()
	mode, cost := &p.QueryViewCfg.LogicalSizeSource, &p.QueryViewCfg.LogicalSizeSegmentBaseCost
	originalMode, originalCost := mode.GetValue(), cost.GetValue()
	t.Cleanup(func() {
		require.NoError(t, p.Save(mode.Key, originalMode))
		require.NoError(t, p.Save(cost.Key, originalCost))
	})
	require.NoError(t, p.Save(mode.Key, "insertBytes"))
	require.NoError(t, p.Save(cost.Key, "10"))
	view := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version(2, 0), Shards: []*viewpb.DataViewOfShard{{Vchannel: "v0", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 100, SegmentIds: []int64{10, 20, 30}, SegmentManifestVersions: []int64{0, 0, 0}}}}}}
	patch := mockey.Mock((*datacatalog.Catalog).ListAllDataViews).Return([]*viewpb.DataViewOfCollection{view}, nil).Build()
	defer patch.UnPatch()
	segments := []LoadableSegment{segmentWithRows(10, "v0", 100, 100), segmentWithRows(20, "v0", 100, 0), segmentWithRows(30, "v0", 100, 5)}
	segments[0].Size = 4096
	manager, err := RecoverManager(t.Context(), datacatalog.NewCatalog(nil, "", ""), recoverAllCollections, projectSegments(segments...), []int64{1}, map[int64][]string{1: {"v0"}})
	require.NoError(t, err)
	ref, err := manager.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer ref.Deref()
	for id, want := range map[int64]int64{10: 11, 20: 10, 30: 15} {
		stat, ok := ref.Stats(id)
		require.True(t, ok, "zero and estimated sizes are known footprints")
		require.Equal(t, want, stat.LogicalSize)
	}
}
