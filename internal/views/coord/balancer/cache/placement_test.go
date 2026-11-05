package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPlacementResourceReferencesAndCleanup(t *testing.T) {
	c := New(nil)
	a, b := cacheShard(1, 10), cacheShard(1, 11)
	key := coordview.ResourceKey{SegmentID: 1000, PartitionID: 1, DataVersion: qviews.DataVersion{StreamingVersion: 1}, LoadInfoVersion: 1}
	stats := func() *coordview.ShardStats {
		return &coordview.ShardStats{UpNodes: []int64{1}, ResidentNodes: []int64{1}, Resources: map[int64]map[coordview.ResourceKey]struct{}{1: {key: {}}}}
	}
	c.PublishShard(a, stats())
	c.PublishShard(b, stats())
	before := c.GetCollection(1)
	node := before.PlacementNode(1)
	require.True(t, node.HasResource(key))
	require.Equal(t, 1, node.ResourceCount())
	var resources []coordview.ResourceKey
	before.RangePlacementNodes(func(n *PlacementNode) bool {
		require.Equal(t, int64(1), n.ID())
		n.RangeResources(func(k coordview.ResourceKey) bool { resources = append(resources, k); return true })
		return true
	})
	require.Equal(t, []coordview.ResourceKey{key}, resources)
	var refs []ReplicaFootprint
	node.RangeReplicas(func(ref ReplicaFootprint) bool { refs = append(refs, ref); return true })
	require.Len(t, refs, 2)
	c.PublishShard(a, nil)
	require.True(t, c.GetCollection(1).PlacementNode(1).HasResource(key))
	c.PublishShard(b, &coordview.ShardStats{ResidentNodes: []int64{1}})
	dropping := c.GetCollection(1)
	require.False(t, dropping.PlacementNode(1).HasResource(key))
	require.Equal(t, []int64{1}, dropping.GetShard(b).ResidentNodes())
	var current ReplicaFootprint
	dropping.PlacementNode(1).RangeReplicas(func(ref ReplicaFootprint) bool { current = ref; return true })
	require.Equal(t, ReplicaFootprint{ReplicaID: 11, Resident: 1}, current)
	c.PublishShard(b, nil)
	require.Nil(t, c.GetCollection(1))
	require.True(t, before.PlacementNode(1).HasResource(key), "retained immutable reader")
	require.NotNil(t, dropping.PlacementNode(1), "cleanup must not mutate prior readers")
}

func TestPlacementFootprintsSurviveRowFallback(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	stats := cacheStats(1000, 1, 0, coordview.SegmentStateUp, false)
	stats.UpNodes = []int64{1}
	stats.ResidentNodes = []int64{1, 2}
	stats.PreparingNodes = []int64{2}
	c.PublishShard(id, stats)
	c.PublishDataView(1, cacheData(1, id.VChannel, 123))
	require.Equal(t, []int64{1}, c.GetCollection(1).GetShard(id).UpNodes())
	require.Equal(t, []int64{1, 2}, c.GetCollection(1).GetShard(id).ResidentNodes())
	require.Equal(t, []int64{2}, c.GetCollection(1).GetShard(id).PreparingNodes())
}

func TestNodeLossInvalidatesUnplacedCollections(t *testing.T) {
	c := New(nil)
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, ResourceGroup: "rg1", Alive: true})
	var scope api.TriggerScope
	c.SetNotifier(func(s api.TriggerScope) { scope = s })
	c.PublishNode(1, nil)
	require.Contains(t, scope.DirtyCollections, int64(1))
}
