package coordview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Exercise the extracted owners together, with real persisted load history.
// Transport and QueryView persistence are patched; this is not runtime recovery.
func TestLoadInfoRegistryRecoveryAndRelease(t *testing.T) {
	deps := loadmgrtest.Dependencies(t)
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{CollectionID: testCollectionID, PartitionIDs: []int64{10}}
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.GetConfigVersion(testCollectionID)
	cfg.PartitionIDs = []int64{11}
	require.NoError(t, store.Put(t.Context(), cfg))
	latest := store.GetConfigVersion(testCollectionID)
	store, err = loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.NoError(t, store.CollectLoadInfo(t.Context()), "GC must remain disabled during reference recovery")

	view := testBuilder(1, 1, 1).SetLoadInfoVersion(first).Build()
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	catalog := queryview.NewQueryViewCatalog(nil, "coord")
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(mockey.GetMethod(catalog, "ListQueryViews")).Return([]*viewpb.QueryViewOfShard{view}, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "SaveQueryViews")).To(func(_ context.Context, views []*viewpb.QueryViewOfShard) error {
			for _, v := range views {
				if v.GetMeta().GetState() == viewpb.QueryViewState_QueryViewStateDropped {
					require.Error(t, store.Remove(t.Context(), testCollectionID), "final persistence still owns its reference")
				}
			}
			return nil
		}).Build(),
		mockey.Mock((*mockSyncer).SyncViews).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	registry, err := RecoverShardViewRegistryWithDataViews(t.Context(), catalog, &mockSyncer{}, stubDataViewRefProvider{}, store)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	store.EnableGC()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	old, err := store.GetLoadInfo(t.Context(), testCollectionID, first)
	require.NoError(t, err)
	require.Equal(t, []int64{10}, old.PartitionIDs)

	store.StopPlanning(testCollectionID)
	_, err = store.GetLoadInfo(t.Context(), testCollectionID, 0)
	require.Error(t, err)
	require.Error(t, store.Remove(t.Context(), testCollectionID))
	manager := registry.Get(testShardID)
	require.NotNil(t, manager)
	require.NoError(t, manager.RequestRelease(t.Context()))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	meta := view.Meta
	meta.State = viewpb.QueryViewState_QueryViewStateDropped
	sn := qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
	qn := qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{NodeId: 1})
	version := testVersion(1, 1, 1)
	batch := registry.Begin()
	manager.makeOnSyncResponse(version, sn)(sn)
	manager.makeOnSyncResponse(version, qn)(qn)
	require.Error(t, store.Remove(t.Context(), testCollectionID), "node cleanup alone is insufficient")
	batch.Commit()
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.Empty(t, registry.CollectionShards(testCollectionID))
	require.NoError(t, store.Remove(t.Context(), testCollectionID))
	records, err := deps.Catalog.List(t.Context())
	require.NoError(t, err)
	require.Empty(t, records)
	restarted, err := loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.NoError(t, restarted.Put(t.Context(), cfg))
	require.Greater(t, restarted.GetConfigVersion(testCollectionID), latest)
}

func TestLoadInfoFailedRecoveryRollsBackReferences(t *testing.T) {
	deps := loadmgrtest.Dependencies(t)
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{CollectionID: testCollectionID, PartitionIDs: []int64{10}}
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.GetConfigVersion(testCollectionID)
	cfg.PartitionIDs = []int64{11}
	require.NoError(t, store.Put(t.Context(), cfg))
	store, err = loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	valid := testBuilder(1, 1, 1).SetLoadInfoVersion(first).Build()
	missing := testBuilder(1, 1, 2).SetLoadInfoVersion(first + 100).Build()
	catalog := queryview.NewQueryViewCatalog(nil, "coord")
	list := mockey.Mock(mockey.GetMethod(catalog, "ListQueryViews")).Return([]*viewpb.QueryViewOfShard{valid, missing}, nil).Build()
	defer list.UnPatch()
	transport := mockey.Mock((*mockSyncer).SyncViews).Return(nil).Build()
	defer transport.UnPatch()
	registry, err := RecoverShardViewRegistryWithDataViews(t.Context(), catalog, &mockSyncer{}, stubDataViewRefProvider{}, store)
	require.Error(t, err, "missing history must abort, never substitute the latest version")
	require.Nil(t, registry)
	require.Zero(t, transport.Times(), "partial recovery must not publish views")
	store.EnableGC()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	_, err = store.GetLoadInfo(t.Context(), testCollectionID, first)
	require.Error(t, err, "aborted recovery must return previously acquired references")
}
