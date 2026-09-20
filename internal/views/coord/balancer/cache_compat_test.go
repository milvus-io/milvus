package balancer

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
)

// The old fixtures are a static Reader only in tests, allowing the unchanged
// policy scenarios to exercise the cache-based planner with the same inputs.
func (s *BalancerSnapshot) CollectionForReplica(id int64) (int64, bool) {
	if s.LoadConfigSnapshot != nil {
		if cfg := s.LoadConfigSnapshot.ReplicaToConfigMap()[id]; cfg != nil {
			return cfg.CollectionID, true
		}
	}
	for shard := range s.ShardStatsMap() {
		if shard.ReplicaID == id {
			if coll, ok := parseShardCollection(shard); ok {
				return coll, true
			}
			return -id, true
		}
	}
	return 0, false
}

func (s *BalancerSnapshot) GetCollection(id int64) *CollectionEntry {
	c := &CollectionEntry{id: id}
	if s.LoadConfigSnapshot != nil {
		c.config = s.ConfigsMap()[id]
		c.configVersion = s.ConfigVersion(id)
	}
	s.DataViewSnapshot.RangeCollections(func(data *CollectionDataView) bool {
		if data.CollectionID == id {
			c.data = data
		}
		return true
	})
	for shard, stats := range s.ShardStatsMap() {
		coll, ok := s.CollectionForReplica(shard.ReplicaID)
		if ok && coll == id {
			c.shards = c.shards.set(shardKey(shard), &ShardEntry{id: shard, stats: stats, rows: s.ShardRowStatsSnapshot[shard]})
		}
	}
	return c
}

func (s *BalancerSnapshot) GetNode(id int64) *NodeEntry {
	node := s.Nodes[id]
	if node == nil {
		return nil
	}
	entry := &NodeEntry{info: *node}
	for shard, rows := range s.ShardRowStatsSnapshot {
		if contribution, ok := rows[id]; ok {
			entry.contributions = entry.contributions.set(shardKey(shard), nodeContribution{shard: shard, rows: contribution})
		}
	}
	return entry
}

func (s *BalancerSnapshot) GetResourceGroup(name string) *ResourceGroupEntry {
	group := &ResourceGroupEntry{}
	for id, node := range s.Nodes {
		if node.ResourceGroup == name {
			group.nodes = group.nodes.set(idKey(id), id)
		}
	}
	return group
}

func (s *BalancerSnapshot) RangeCollectionIDs(fn func(int64) bool) {
	seen := make(map[int64]struct{})
	for id := range s.ConfigsMap() {
		seen[id] = struct{}{}
	}
	for shard := range s.ShardStatsMap() {
		if id, ok := s.CollectionForReplica(shard.ReplicaID); ok {
			seen[id] = struct{}{}
		}
	}
	for id := range seen {
		if !fn(id) {
			return
		}
	}
}

func (s *BalancerSnapshot) RangeNodeIDs(fn func(int64) bool) {
	for id := range s.Nodes {
		if !fn(id) {
			return
		}
	}
}

// Legacy fixture adapter seeds once; production controllers never use SnapshotBuilder.
func cacheFromBuilder(t *testing.T, b *SnapshotBuilder) *Cache {
	c := NewCache(b.config)
	if b.configStore != nil {
		t.Cleanup(b.configStore.RegisterLoadConfigListener(c.PublishLoadConfig))
	}
	if source, ok := b.dataViewProvider.(api.DataViewPublisher); ok {
		t.Cleanup(source.RegisterDataViewListener(c.PublishDataView))
	} else if b.dataViewProvider != nil {
		b.dataViewProvider.DataViewSnapshot(context.Background()).RangeCollections(func(data *CollectionDataView) bool { c.PublishDataView(data.CollectionID, data); return true })
	}
	if b.viewRegistry != nil {
		t.Cleanup(b.viewRegistry.RegisterPublicationListener(c.PublishShard))
	}
	if b.nodeProvider != nil {
		publish := func() {
			b.nodeProvider.Snapshot().Range(func(id int64, info *NodeInfo) bool { c.PublishNode(id, info); return true })
		}
		publish()
		if notifier, ok := b.nodeProvider.(NodeChangedNotifier); ok {
			notifier.RegisterNodeChangedNotifier(func() { publish(); c.changed(TriggerScope{NodeChanged: true}) })
		}
	}
	c.MarkReady()
	return c
}
