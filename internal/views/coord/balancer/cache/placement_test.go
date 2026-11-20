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

func TestPlacementReusesUnchangedFootprints(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	c.PublishShard(id, &coordview.ShardStats{
		PreparingNodes: []int64{1, 2},
		ResidentNodes:  []int64{1, 2},
	})
	before := c.GetCollection(1)

	reordered := &coordview.ShardStats{
		PreparingNodes: []int64{2, 1},
		ResidentNodes:  []int64{2, 1},
	}
	c.PublishShard(id, reordered)
	current := c.GetCollection(1)
	require.Same(t, reordered, current.GetShard(id).Stats())
	require.Same(t, before.placements.tree, current.placements.tree)

	key := coordview.ResourceKey{PartitionID: 1, SegmentID: 100, DataVersion: qviews.DataVersion{StreamingVersion: 1}, LoadInfoVersion: 1}
	progress := *reordered
	progress.Resources = map[int64]map[coordview.ResourceKey]struct{}{1: {key: {}}}
	c.PublishShard(id, &progress)
	current = c.GetCollection(1)
	require.True(t, current.PlacementNode(1).HasResource(key))
	require.False(t, before.PlacementNode(1).HasResource(key))
	require.Same(t, before.PlacementNode(1).replicas.tree, current.PlacementNode(1).replicas.tree)
	require.Same(t, before.PlacementNode(2), current.PlacementNode(2))

	promoted := progress
	promoted.UpNodes = []int64{1}
	promoted.PreparingNodes = []int64{2}
	c.PublishShard(id, &promoted)
	after := c.GetCollection(1)
	replicaKey := idKey(id.ReplicaID)
	ref, found := after.PlacementNode(1).replicas.get(replicaKey[:])
	require.True(t, found)
	require.Equal(t, ReplicaFootprint{ReplicaID: id.ReplicaID, Up: 1, Resident: 1}, ref)
	previousRef, found := current.PlacementNode(1).replicas.get(replicaKey[:])
	require.True(t, found)
	require.Equal(t, ReplicaFootprint{ReplicaID: id.ReplicaID, Preparing: 1, Resident: 1}, previousRef)
	require.Same(t, current.PlacementNode(2), after.PlacementNode(2))

	moved := promoted
	moved.UpNodes = []int64{3}
	moved.ResidentNodes = []int64{2, 3}
	moved.Resources = map[int64]map[coordview.ResourceKey]struct{}{3: {key: {}}}
	c.PublishShard(id, &moved)
	current = c.GetCollection(1)
	require.Nil(t, current.PlacementNode(1))
	require.Same(t, after.PlacementNode(2), current.PlacementNode(2))
	require.True(t, current.PlacementNode(3).HasResource(key))
	ref, found = current.PlacementNode(3).replicas.get(replicaKey[:])
	require.True(t, found)
	require.Equal(t, ReplicaFootprint{ReplicaID: id.ReplicaID, Up: 1, Resident: 1}, ref)
}

func BenchmarkCachePublishShardPlacement(b *testing.B) {
	id := cacheShard(1, 10)
	version := &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}, QueryVersion: 1}
	preparing := cacheStats(100, 1, 100, coordview.SegmentStatePreparing, true)
	preparing.PreparingVersion = version
	preparing.PreparingNodes = []int64{1}
	preparing.ResidentNodes = []int64{1}

	unchanged := *preparing
	resourceReady := cacheStats(100, 1, 100, coordview.SegmentStateReady, true)
	resourceReady.PreparingVersion = version
	resourceReady.PreparingNodes = preparing.PreparingNodes
	resourceReady.ResidentNodes = preparing.ResidentNodes
	key := coordview.ResourceKey{PartitionID: 1, SegmentID: 100, DataVersion: version.DataVersion, LoadInfoVersion: 1}
	resourceReady.Resources = map[int64]map[coordview.ResourceKey]struct{}{1: {key: {}}}

	up := cacheStats(100, 1, 100, coordview.SegmentStateUp, true)
	up.UpVersion = version
	up.UpNodes = []int64{1}
	up.ResidentNodes = preparing.ResidentNodes
	up.Resources = resourceReady.Resources

	loadedOn := func(viewVersion *qviews.QueryViewVersion, nodes ...int64) *coordview.ShardStats {
		stats := &coordview.ShardStats{
			UpVersion:     viewVersion,
			UpNodes:       nodes,
			ResidentNodes: nodes,
			Segments:      make(map[int64]*coordview.SegmentStats, len(nodes)),
			Resources:     make(map[int64]map[coordview.ResourceKey]struct{}, len(nodes)),
		}
		for i, node := range nodes {
			segmentKey := key
			segmentKey.SegmentID += int64(i)
			stats.Resources[node] = map[coordview.ResourceKey]struct{}{segmentKey: {}}
			stats.Segments[segmentKey.SegmentID] = &coordview.SegmentStats{
				SegmentID: segmentKey.SegmentID, PartitionID: segmentKey.PartitionID, LogicalSize: 100,
				Nodes: map[int64]coordview.SegmentState{node: coordview.SegmentStateUp},
			}
		}
		return stats
	}
	movedVersion := *version
	movedVersion.QueryVersion++

	for _, tc := range []struct {
		name     string
		old, new *coordview.ShardStats
	}{
		{"unchanged_placement", preparing, &unchanged},
		{"resource_readiness_change", preparing, resourceReady},
		{"preparing_up_transition", resourceReady, up},
		{"one_node_migration", loadedOn(version, 1, 2, 3, 4), loadedOn(&movedVersion, 1, 2, 3, 5)},
		{"load_and_final_release", nil, resourceReady},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := New(nil)
			previous, next := tc.old, tc.new
			c.PublishShard(id, previous)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				c.PublishShard(id, next)
				previous, next = next, previous
			}
		})
	}
}

func TestNodeLossInvalidatesUnplacedCollections(t *testing.T) {
	c := New(nil)
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, ResourceGroup: "rg1", Alive: true})
	var scope api.TriggerScope
	c.SetNotifier(func(s api.TriggerScope) { scope = s })
	c.PublishNode(1, nil)
	require.Contains(t, scope.DirtyResourceGroups, "rg1")
}
