package loadstatus

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestProgressInResourceGroup(t *testing.T) {
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(catalogkv.Catalog.GetCollections).Return([]*querypb.CollectionLoadInfo(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetReplicas).Return([]*querypb.Replica(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseReplica).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseReplicas).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{
		{ReplicaID: 10, ResourceGroup: "a"}, {ReplicaID: 20, ResourceGroup: "b"},
	}}
	require.NoError(t, store.Put(t.Context(), cfg))
	channels := []string{"v0", "v1"}
	stats := map[qviews.ShardID]*coordview.ShardStats{}
	var duringSnapshot func()
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, ids []qviews.ShardID) *coordview.ShardViewSnapshot {
		selected := make(map[qviews.ShardID]*coordview.ShardStats, len(ids))
		for _, id := range ids {
			if stat := stats[id]; stat != nil {
				selected[id] = stat
			}
		}
		if duringSnapshot != nil {
			duringSnapshot()
		}
		return coordview.NewShardViewSnapshot(1, selected)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	registry := &coordview.ShardViewRegistry{}
	check := func(group string, percent, total int64) {
		progress := GetInResourceGroup(store, registry, 1, channels, group)
		require.Equal(t, percent, progress.Percentage())
		require.Equal(t, total, progress.Total)
		require.Equal(t, percent == 100, progress.Ready())
	}
	up := func(replica int64, channel string) {
		stats[qviews.ShardID{ReplicaID: replica, VChannel: channel}] = &coordview.ShardStats{
			UpVersion: &qviews.QueryViewVersion{}, UpLoadInfoVersion: store.GetConfigVersion(1),
		}
	}
	check("empty", -1, 0)
	check("a", 0, 2)
	up(10, "v0")
	check("a", 50, 2) // The missing expected v1 still counts.
	up(10, "v1")
	check("a", 100, 2)
	check("b", 0, 2)
	check("", 50, 4)
	require.Equal(t, Get(store, registry, 1, channels), GetInResourceGroup(store, registry, 1, channels, ""))

	// Refresh observes the same group but requires exact schema and sufficient data.
	data := qviews.DataVersion{StreamingVersion: 2}
	refresh := func() Progress { return GetAtVersionsInResourceGroup(store, registry, 1, channels, 2, data, "a") }
	require.False(t, refresh().Ready())
	for _, channel := range channels {
		stat := stats[qviews.ShardID{ReplicaID: 10, VChannel: channel}]
		stat.UpSchemaVersion = 2
		stat.UpVersion.DataVersion = data
	}
	require.True(t, refresh().Ready(), "unready group b cannot hold up group a")
	stat := stats[qviews.ShardID{ReplicaID: 10, VChannel: "v1"}]
	stat.UpSchemaVersion = 3
	require.EqualValues(t, 50, refresh().Percentage(), "newer schema cannot replace exact schema")
	stat.UpBySchema = map[int32]coordview.SchemaUpView{2: {LoadInfoVersion: store.GetConfigVersion(1)}}
	require.False(t, refresh().Ready(), "old data cannot satisfy the refresh fence")
	stat.UpBySchema[2] = coordview.SchemaUpView{LoadInfoVersion: store.GetConfigVersion(1), Version: qviews.QueryViewVersion{DataVersion: data}}
	require.True(t, refresh().Ready())

	for _, action := range []string{"update", "release", "release_reload"} {
		t.Run(action, func(t *testing.T) {
			require.NoError(t, store.Put(t.Context(), cfg))
			up(10, "v0")
			up(10, "v1")
			duringSnapshot = func() {
				if action != "update" {
					store.StopPlanning(1)
					require.NoError(t, store.Remove(t.Context(), 1))
				}
				if action != "release" {
					require.NoError(t, store.Put(t.Context(), cfg))
				}
			}
			progress := GetInResourceGroup(store, registry, 1, channels, "a")
			require.False(t, progress.Ready(), "a snapshot across config changes cannot report ready")
			require.Zero(t, progress.Percentage())
			duringSnapshot = nil
		})
	}
}
