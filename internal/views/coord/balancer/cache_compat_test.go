package balancer

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// The old fixtures are a static balancercache.Reader only in tests, allowing the unchanged
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

// Legacy fixtures are projected through public publication APIs so tests do
// not reach into cache internals after the package split.
func (s *BalancerSnapshot) GetCollection(id int64) *balancercache.CollectionEntry {
	c := balancercache.New(s.Config)
	cfg := s.ConfigsMap()[id]
	c.PublishLoadConfig(id, cfg, s.ConfigVersion(id))
	s.DataViewSnapshot.RangeCollections(func(data *CollectionDataView) bool {
		if data.CollectionID == id {
			c.PublishDataView(id, data)
		}
		return true
	})
	for shard, stats := range s.ShardStatsMap() {
		collection, ok := s.CollectionForReplica(shard.ReplicaID)
		if !ok || collection != id {
			continue
		}
		if _, valid := parseShardCollection(shard); !valid {
			c.PublishLoadConfig(id, &loadmgr.LoadConfig{CollectionID: id, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: shard.ReplicaID}}}, s.ConfigVersion(id))
		}
		c.PublishShard(shard, stats)
	}
	c.PublishLoadConfig(id, cfg, s.ConfigVersion(id))
	return c.GetCollection(id)
}

func (s *BalancerSnapshot) GetNode(id int64) *balancercache.NodeEntry {
	node := s.Nodes[id]
	if node == nil {
		return nil
	}
	c := balancercache.New(s.Config)
	c.PublishNode(id, &NodeInfo{NodeID: id, Alive: node.Alive, Stopping: node.Stopping, ResourceGroup: node.ResourceGroup})
	var used NodeRowStats
	for shard, rows := range s.ShardRowStatsSnapshot {
		if contribution, ok := rows[id]; ok {
			// Some old fixtures use shorthand vchannels. Seed their replica binding.
			c.PublishLoadConfig(1, &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: shard.ReplicaID}}}, 1)
			publishFixtureContribution(c, shard, id, contribution)
			used.UpRowCount += contribution.UpRowCount
			used.PendingRowCount += contribution.PendingRowCount
		}
	}
	// Old fixtures can specify background node load without enumerating its
	// shards. Keep that load in a separate contribution, outside the test scope.
	background := qviews.ShardID{ReplicaID: -1, VChannel: "by-dev-rootcoord-dml_0_999999v0"}
	publishFixtureContribution(c, background, id, NodeRowStats{UpRowCount: node.UpRowCount - used.UpRowCount, PendingRowCount: node.PendingRowCount - used.PendingRowCount})
	return c.GetNode(id)
}

func publishFixtureContribution(c *balancercache.Cache, shard qviews.ShardID, node int64, rows NodeRowStats) {
	c.PublishShard(shard, &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{
		1: {SegmentID: 1, HasRowNum: true, RowNum: rows.UpRowCount, Nodes: map[int64]coordview.SegmentState{node: coordview.SegmentStateUp}},
		2: {SegmentID: 2, HasRowNum: true, RowNum: rows.PendingRowCount, Nodes: map[int64]coordview.SegmentState{node: coordview.SegmentStatePreparing}},
	}})
}

func (s *BalancerSnapshot) GetResourceGroup(name string) *balancercache.ResourceGroupEntry {
	c := balancercache.New(s.Config)
	for id, node := range s.Nodes {
		c.PublishNode(id, &NodeInfo{NodeID: id, Alive: node.Alive, Stopping: node.Stopping, ResourceGroup: node.ResourceGroup})
	}
	return c.GetResourceGroup(name)
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
func cacheFromBuilder(t *testing.T, b *SnapshotBuilder) *balancercache.Cache {
	c := balancercache.New(b.config)
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
	}
	c.MarkReady()
	return c
}
