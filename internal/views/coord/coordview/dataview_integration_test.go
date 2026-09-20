package coordview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ DataViewRefProvider = (dataview.Manager)(nil)

// Keep the Manager and its reference accounting real; only persistence and
// node transport are patched. No Coordinator runtime is involved.
func newInterfaceDataViews(t *testing.T) dataview.Manager {
	t.Helper()
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*datacatalog.Catalog).ListAllDataViews).Return(nil, nil).Build(),
		mockey.Mock((*datacatalog.Catalog).SaveDataView).Return(nil).Build(),
		mockey.Mock((*datacatalog.Catalog).DropDataView).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	m, err := dataview.RecoverManager(t.Context(), datacatalog.NewCatalog(nil, "", ""),
		func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	_, err = m.OnBootstrapCollection(t.Context(), dataview.BootstrapCollectionDataViewEvent{
		CollectionID: testCollectionID,
		VChannels:    []string{testVChannel},
		Segments:     []dataview.LoadableSegment{interfaceSegment(1001, 42)},
	})
	require.NoError(t, err)
	return m
}

func interfaceSegment(id, rows int64) dataview.LoadableSegment {
	return dataview.LoadableSegment{SegmentID: id, PartitionID: 10, VChannel: testVChannel, RowNum: rows}
}

func publishInterfaceDataView(t *testing.T, m dataview.Manager, id, rows int64) *viewpb.DataVersion {
	t.Helper()
	version, err := m.RecomputeNow(t.Context(), testCollectionID, func(context.Context, int64) ([]dataview.LoadableSegment, error) {
		return []dataview.LoadableSegment{interfaceSegment(id, rows)}, nil
	})
	require.NoError(t, err)
	return version
}

func newInterfaceRegistry(t *testing.T, m dataview.Manager, recovered []*viewpb.QueryViewOfShard, save func(context.Context, []*viewpb.QueryViewOfShard) error) *ShardViewRegistry {
	t.Helper()
	catalog := queryview.NewQueryViewCatalog(nil, "coord")
	list := mockey.Mock(mockey.GetMethod(catalog, "ListQueryViews")).Return(recovered, nil).Build()
	t.Cleanup(func() { list.UnPatch() })
	saver := mockey.Mock(mockey.GetMethod(catalog, "SaveQueryViews"))
	if save == nil {
		saver.Return(nil)
	} else {
		saver.To(save)
	}
	saved := saver.Build()
	t.Cleanup(func() { saved.UnPatch() })
	transport := mockey.Mock((*mockSyncer).SyncViews).Return(nil).Build()
	t.Cleanup(func() { transport.UnPatch() })
	registry, err := RecoverShardViewRegistry(t.Context(), catalog, &mockSyncer{}, m)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	return registry
}

func TestDataViewReferenceSurvivesUntilDurableQueryViewRemoval(t *testing.T) {
	m := newInterfaceDataViews(t)
	version := &viewpb.DataVersion{StreamingVersion: 1}
	assertRetained := func() {
		t.Helper()
		require.NoError(t, m.GarbageCollect(t.Context(), testCollectionID, 1))
		ref, err := m.Get(t.Context(), testCollectionID, version)
		require.NoError(t, err)
		require.NotNil(t, ref)
		ref.Deref()
	}
	registry := newInterfaceRegistry(t, m, nil, func(_ context.Context, views []*viewpb.QueryViewOfShard) error {
		for _, view := range views {
			if view.GetMeta().GetState() == viewpb.QueryViewState_QueryViewStateDropped {
				// The real Manager must still retain the old snapshot while the
				// final QueryView deletion is being persisted.
				assertRetained()
			}
		}
		return nil
	})
	mgr := registry.Ensure(testShardID)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 0, 1)))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	publishInterfaceDataView(t, m, 1002, 7)
	assertRetained()
	stats := mgr.Stats().Segments[1001]
	require.True(t, stats.HasRowNum)
	require.Equal(t, int64(42), stats.RowNum)

	require.NoError(t, mgr.RequestRelease(t.Context()))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	assertRetained()
	meta := testBuilder(1, 0, 1).Build().Meta
	meta.State = viewpb.QueryViewState_QueryViewStateDropped
	qv := testVersion(1, 0, 1)
	sn := qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
	qn := qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{NodeId: 1})
	mgr.makeOnSyncResponse(qv, sn)(sn)
	mgr.makeOnSyncResponse(qv, qn)(qn)
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))

	require.Nil(t, registry.Get(testShardID))
	require.NoError(t, m.GarbageCollect(t.Context(), testCollectionID, 1))
	ref, err := m.Get(t.Context(), testCollectionID, version)
	require.NoError(t, err)
	require.Nil(t, ref, "durable removal must release the last reference")
}

func TestPlanningSnapshotCollectedBeforeAddPreparing(t *testing.T) {
	m := newInterfaceDataViews(t)
	registry := newInterfaceRegistry(t, m, nil, nil)
	mgr := registry.Ensure(testShardID)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 0, 1)))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	current := mgr.preparingView
	staleVersion := publishInterfaceDataView(t, m, 1002, 7)
	snapshot := m.DataViewSnapshot(t.Context())
	publishInterfaceDataView(t, m, 1003, 9)
	require.NoError(t, m.GarbageCollect(t.Context(), testCollectionID, 1))
	_, ok := snapshot.SegmentInfo(1002)
	require.True(t, ok, "the detached planning snapshot outlives collection GC")

	err := mgr.AddPreparing(t.Context(), testBuilder(staleVersion.StreamingVersion, staleVersion.CompactVersion, 1))
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, merr.Status(err).GetRetriable())
	require.Same(t, current, mgr.preparingView)
	require.Len(t, mgr.views, 1)
	require.Equal(t, qviews.QueryViewStatePreparing, current.State())
	// A subsequent reconcile can successfully apply the fresh version.
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 2, 1)))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
}

func TestRecoveredQueryViewsUseEachRetainedVersionsRows(t *testing.T) {
	m := newInterfaceDataViews(t)
	publishInterfaceDataView(t, m, 1002, 7)
	old := testBuilder(1, 0, 1).Build()
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	latest := testBuilder(1, 1, 1).SetAssignments(map[int64]map[int64][]int64{2: {10: {1002}}}).Build()
	registry := newInterfaceRegistry(t, m, []*viewpb.QueryViewOfShard{old, latest}, nil)
	require.NoError(t, m.GarbageCollect(t.Context(), testCollectionID, 1))
	for range 20 {
		stats := registry.Get(testShardID).Stats()
		require.True(t, stats.Segments[1001].HasRowNum)
		require.Equal(t, int64(42), stats.Segments[1001].RowNum)
		require.True(t, stats.Segments[1002].HasRowNum)
		require.Equal(t, int64(7), stats.Segments[1002].RowNum)
	}
}
