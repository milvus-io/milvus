package loadstatus

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestProgress(t *testing.T) {
	// Keep real store versioning and copy-on-write semantics; mock only persistence
	// and the registry snapshot to deterministically interleave a configuration write.
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
	ctx := context.Background()
	store, err := loadmgr.RecoverLoadConfigStore(ctx, catalogkv.Catalog{})
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}
	channels := []string{"v0", "v1"}
	stats := map[qviews.ShardID]*coordview.ShardStats{}
	var duringSnapshot func()
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, shards []qviews.ShardID) *coordview.ShardViewSnapshot {
		current, _ := store.GetConfigWithVersion(1)
		require.Len(t, shards, len(channels)*len(current.Replicas))
		snapshot := coordview.NewShardViewSnapshot(1, stats)
		if duringSnapshot != nil {
			duringSnapshot()
		}
		return snapshot
	}).Build()
	defer patch.UnPatch()
	check := func(want int64) Progress {
		p := Get(store, &coordview.ShardViewRegistry{}, 1, channels)
		require.Equal(t, want, p.Percentage())
		require.Equal(t, want == 100, p.Ready())
		return p
	}
	up := func(replica int64, channel string, version uint64) {
		stats[qviews.ShardID{ReplicaID: replica, VChannel: channel}] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{}, UpLoadInfoVersion: version}
	}
	require.Nil(t, check(0).Config)
	require.NoError(t, store.Put(ctx, cfg))
	version := store.GetConfigVersion(1)
	check(0)
	up(10, "v0", version)
	up(99, "unrelated", version)
	check(50) // Missing v1 must remain in the denominator.
	stats[qviews.ShardID{ReplicaID: 10, VChannel: "v1"}] = &coordview.ShardStats{UpLoadInfoVersion: version}
	check(50) // Preparing, without UpVersion.
	up(10, "v1", version)
	check(100)
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11})
	require.NoError(t, store.Put(ctx, cfg))
	version = store.GetConfigVersion(1)
	check(0) // Neither old Up view acknowledges the replica change yet.
	up(10, "v0", version)
	up(10, "v1", version)
	check(50) // Newly configured replica has no registry entry.
	up(11, "v0", version)
	check(75)
	up(11, "v1", version)
	check(100)
	cfg.LoadFields = []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}}
	require.NoError(t, store.Put(ctx, cfg))
	check(0) // Previous load fields must not confirm the new configuration.
	version = store.GetConfigVersion(1)
	for _, r := range cfg.Replicas {
		for _, c := range channels {
			up(r.ReplicaID, c, version)
		}
	}
	check(100)
	for _, action := range []string{"update", "release", "release_reload"} {
		t.Run(action, func(t *testing.T) {
			require.NoError(t, store.Put(ctx, cfg))
			version := store.GetConfigVersion(1)
			for _, r := range cfg.Replicas {
				for _, c := range channels {
					up(r.ReplicaID, c, version)
				}
			}
			duringSnapshot = func() {
				if action != "update" {
					require.NoError(t, store.Remove(ctx, 1))
				}
				if action != "release" {
					require.NoError(t, store.Put(ctx, cfg))
				}
			}
			p := check(0)
			require.Equal(t, action == "release", p.Config == nil)
			duringSnapshot = nil
			if action != "release" {
				require.Greater(t, store.GetConfigVersion(1), version)
				check(0)
			}
		})
	}
	require.NoError(t, store.Put(ctx, cfg))
	channels = nil
	check(0)
	cfg.Replicas = nil
	require.NoError(t, store.Put(ctx, cfg))
	channels = []string{"v0"}
	check(0)
}
