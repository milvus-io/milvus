package querycoordv2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Keep the real config store and waiter; replace persistence and node reports.
func newHandoffServer(t *testing.T) (*Server, func(string, int64)) {
	t.Helper()
	previousCache := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	t.Cleanup(func() { meta.GlobalFailedLoadCache = previousCache })
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(catalogkv.Catalog.GetCollections).Return(nil, nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetPartitions).Return(nil, nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetReplicas).Return(nil, nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseReplicas).Return(nil).Build(),
		mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v0", "v1"}}, nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), catalogkv.Catalog{})
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{10}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 11}}}))
	registry := &coordview.ShardViewRegistry{}
	runtime := &qviewsRuntime{loadConfigStore: store, shardViewRegistry: registry, readyChanges: newCollectionReadiness(store, registry), loadManager: loadmgr.NewCollectionLoadManager(store, nil)}
	runtime.resources = &qvresource.Resources{LoadConfigs: store, LoadManager: runtime.loadManager, ShardViews: registry, Readiness: runtime.readyChanges, RefreshVersions: &runtime.refreshVersions, VChannels: func(context.Context, int64) ([]string, error) { return []string{"v0", "v1"}, nil }}
	resourcePatch := mockey.Mock(qvresource.Resource).Return(runtime.resources).Build()
	t.Cleanup(func() { resourcePatch.UnPatch() })
	t.Cleanup(runtime.readyChanges.Close)
	s := &Server{qviewsRuntime: runtime, broker: &meta.CoordinatorBroker{}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	var mu sync.Mutex
	stats := map[qviews.ShardID]*coordview.ShardStats{}
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, _ []qviews.ShardID) *coordview.ShardViewSnapshot {
		mu.Lock()
		defer mu.Unlock()
		copy := make(map[qviews.ShardID]*coordview.ShardStats, len(stats))
		for k, v := range stats {
			copy[k] = v
		}
		return coordview.NewShardViewSnapshot(1, copy)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	up := func(channel string, version int64) {
		mu.Lock()
		stats[qviews.ShardID{ReplicaID: 11, VChannel: channel}] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: version}}, UpLoadInfoVersion: store.GetConfigVersion(1)}
		mu.Unlock()
		runtime.readyChanges.Notify(1)
	}
	up("v0", 1)
	up("v1", 1)
	return s, up
}

func TestQueryViewDataHandoffAndRefreshProgress(t *testing.T) {
	s, up := newHandoffServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.qviewsRuntime.resources.WaitDataVersion(ctx, 1, qviews.DataVersion{StreamingVersion: 2})
	}()
	assertReadinessBlocked(t, done)
	up("v0", 2)
	assertReadinessBlocked(t, done)
	for _, partitions := range []bool{false, true} {
		if partitions {
			response, err := s.ShowLoadPartitions(ctx, &querypb.ShowPartitionsRequest{CollectionID: 1, PartitionIDs: []int64{10}})
			require.NoError(t, err)
			require.NoError(t, merr.Error(response.Status))
			require.Equal(t, []int64{50}, response.RefreshProgress)
		} else {
			response, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{1}})
			require.NoError(t, err)
			require.NoError(t, merr.Error(response.Status))
			require.Equal(t, []int64{100}, response.InMemoryPercentages)
			require.Equal(t, []int64{50}, response.RefreshProgress)
		}
	}
	// A config update invalidates even a sufficiently new Up data version.
	cfg := s.qviewsRuntime.loadConfigStore.GetConfig(1).Clone()
	cfg.PartitionIDs = append(cfg.PartitionIDs, 20)
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(ctx, cfg))
	up("v1", 3)
	assertReadinessBlocked(t, done)
	up("v0", 3)
	require.NoError(t, <-done)
	// A later caller requesting an older fence must not regress public progress.
	require.NoError(t, s.qviewsRuntime.resources.WaitDataVersion(ctx, 1, qviews.DataVersion{StreamingVersion: 1}))
	version, _ := s.qviewsRuntime.refreshVersions.Load(int64(1))
	require.Equal(t, qviews.DataVersion{StreamingVersion: 2}, version)
	progress, err := s.qviewsRefreshProgress(ctx, 1, 100)
	require.NoError(t, err)
	require.Equal(t, int64(100), progress)
}

func TestQueryViewDataHandoffCancellationAndRelease(t *testing.T) {
	for _, action := range []string{"cancel", "release"} {
		t.Run(action, func(t *testing.T) {
			s, _ := newHandoffServer(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- s.qviewsRuntime.resources.WaitDataVersion(ctx, 1, qviews.DataVersion{StreamingVersion: 2})
			}()
			assertReadinessBlocked(t, done)
			if action == "cancel" {
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
			} else {
				require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(ctx, 1))
				require.NoError(t, <-done)
			}
		})
	}
}

func TestCreatePartitionWaitsForQueryViewConfig(t *testing.T) {
	s, up := newHandoffServer(t)
	previous := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	t.Cleanup(func() { meta.GlobalFailedLoadCache = previous })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	applied := make(chan struct{}, 1)
	s.qviewsRuntime.loadManager = loadmgr.NewCollectionLoadManager(s.qviewsRuntime.loadConfigStore, func(int64) { applied <- struct{}{} })
	s.qviewsRuntime.resources.LoadManager = s.qviewsRuntime.loadManager
	done := make(chan error, 1)
	go func() {
		status, err := s.SyncNewCreatedPartition(ctx, &querypb.SyncNewCreatedPartitionRequest{CollectionID: 1, PartitionID: 20})
		done <- merr.CheckRPCCall(status, err)
	}()
	select {
	case <-applied:
	case <-ctx.Done():
		t.Fatal("partition config not applied")
	}
	require.ElementsMatch(t, []int64{10, 20}, s.qviewsRuntime.loadConfigStore.GetConfig(1).PartitionIDs)
	assertReadinessBlocked(t, done)
	up("v0", 1)
	assertReadinessBlocked(t, done)
	up("v1", 1)
	require.NoError(t, <-done)
	// Replay must wait for the same config rather than append a duplicate partition.
	status, err := s.SyncNewCreatedPartition(ctx, &querypb.SyncNewCreatedPartitionRequest{CollectionID: 1, PartitionID: 20})
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.Len(t, s.qviewsRuntime.loadConfigStore.GetConfig(1).PartitionIDs, 2)
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(ctx, 1))
	status, err = s.SyncNewCreatedPartition(ctx, &querypb.SyncNewCreatedPartitionRequest{CollectionID: 1, PartitionID: 30})
	require.NoError(t, merr.CheckRPCCall(status, err))
	require.False(t, s.qviewsRuntime.loadConfigStore.Contains(1))
}

func TestRefreshCollectionUsesQueryViews(t *testing.T) {
	s, up := newHandoffServer(t)
	reached := make(chan struct{}, 1)
	s.qviewsRuntime.resources.Reconciler = &datacoord.Server{}
	patch := mockey.Mock((*datacoord.Server).RefreshDataView).To(func(_ *datacoord.Server, _ context.Context, id int64) (*viewpb.DataVersion, error) {
		require.Equal(t, int64(1), id)
		reached <- struct{}{}
		return &viewpb.DataVersion{StreamingVersion: 2}, nil
	}).Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.refreshCollection(ctx, 1) }()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("refresh did not request a published DataView")
	}
	assertReadinessBlocked(t, done)
	up("v0", 2)
	up("v1", 2)
	require.NoError(t, <-done)
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(ctx, 1))
	require.ErrorIs(t, s.refreshCollection(ctx, 1), merr.ErrCollectionNotLoaded)
}
