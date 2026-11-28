package querycoordv2

import (
	"maps"
	"slices"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// A single grant covers every expected vchannel of every pending assignment
// in the destination group, including assignments in different collections.
type pendingResourceGroup struct {
	placements      loadmgr.PendingPlacements
	shards          map[qviews.ShardID]uint64
	missingChannels bool
}

func (w *LoadConfigWatcher) promoteReadyGroups() {
	ctx := w.notifier.Context()
	runtime := w.s.qviewsRuntime
	groups := make(map[string]*pendingResourceGroup)
	for cfg, replicas := range runtime.loadConfigStore.PendingVisibility() {
		channels, err := runtime.resources.VChannels(ctx, cfg.CollectionID)
		for _, r := range replicas {
			group := groups[r.ResourceGroup]
			if group == nil {
				group = &pendingResourceGroup{placements: make(loadmgr.PendingPlacements), shards: make(map[qviews.ShardID]uint64)}
				groups[r.ResourceGroup] = group
			}
			group.placements[cfg] = append(group.placements[cfg], r)
			if err != nil || len(channels) == 0 {
				group.missingChannels = true
			}
			for _, ch := range channels {
				group.shards[qviews.ShardID{ReplicaID: r.ReplicaID, VChannel: ch}] = cfg.Version
			}
		}
	}
	for name, group := range groups {
		if w.unappliedGroups[name] {
			continue
		}
		ids := group.readyShards(runtime.shardViewRegistry)
		if len(ids) == 0 {
			continue
		}
		if !runtime.loadConfigStore.AdmitResourceGroup(group.placements) {
			continue
		}
		for _, id := range ids {
			if manager := runtime.shardViewRegistry.Get(id); manager != nil {
				manager.ResumeReady()
			}
		}
		w.Logger().Info(ctx, "cluster load config resource group is query visible", mlog.String("resourceGroup", name), mlog.Int("shards", len(ids)))
	}
}

func (g *pendingResourceGroup) readyShards(registry *coordview.ShardViewRegistry) []qviews.ShardID {
	if g.missingChannels {
		return nil
	}
	ids := slices.Collect(maps.Keys(g.shards))
	stats := registry.SnapshotForShards(ids).StatsMap()
	for id, version := range g.shards {
		if stat := stats[id]; stat == nil || stat.HeldReadyLoadInfoVersion != version {
			return nil
		}
	}
	return ids
}
