package balancer

import (
	"sort"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// DiscoveryPublisher applies authoritative serving coverage before view release.
// A false result means the inputs became stale and the collection must reconcile
// again without applying that batch's view changes.
type DiscoveryPublisher interface {
	UpdateCollectionDiscovery(loadmgr.CollectionDiscoveryUpdate) bool
}

func planDiscovery(p *planningContext, releases []qviews.ShardID, revision uint64) []loadmgr.CollectionDiscoveryUpdate {
	removed := make(map[qviews.ShardID]bool, len(releases))
	for _, id := range releases {
		removed[id] = true
	}
	var updates []loadmgr.CollectionDiscoveryUpdate
	for id, c := range p.collections {
		if c == nil {
			continue
		}
		update := loadmgr.CollectionDiscoveryUpdate{CollectionID: id, ConfigVersion: c.ConfigVersion(), Revision: revision}
		c.RangeShards(func(shard *balancercache.ShardEntry) bool {
			id := shard.ID()
			// A suspended view held by the serving-cover barrier remains
			// discoverable until its replacement can serve.
			if !removed[id] && p.ConfigForShard(id) != nil && healthyUp(p, id) {
				update.Shards = append(update.Shards, id)
			}
			return true
		})
		sort.Slice(update.Shards, func(i, j int) bool { return shardLess(update.Shards[i], update.Shards[j]) })
		updates = append(updates, update)
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].CollectionID < updates[j].CollectionID })
	return updates
}
