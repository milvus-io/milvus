package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
)

func TestTargetReplacementFailureAndResourceIndependence(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	for _, node := range []int64{1, 2} {
		c.PublishNode(node, &api.NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	up := cacheStats(1000, 1, 100, coordview.SegmentStateUp, true)
	up.UpNodes = []int64{1}
	up.UpPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 1}, Rows: map[int64]int64{1: 100}}
	c.PublishShard(id, up)
	old := c.GetNode(1)
	require.Equal(t, int64(100), old.TargetRows())
	preparing := *up
	preparing.PreparingNodes = []int64{2}
	preparing.PreparingPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 2}, Rows: map[int64]int64{2: 100}}
	preparing.Segments = map[int64]*coordview.SegmentStats{1000: {SegmentID: 1000, RowNum: 100, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp, 2: coordview.SegmentStateReady}}}
	resource := coordview.ResourceKey{SegmentID: 1000, PartitionID: 1, LoadInfoVersion: 1}
	preparing.Resources = map[int64]map[coordview.ResourceKey]struct{}{2: {resource: {}}}
	c.PublishShard(id, &preparing)
	require.Zero(t, c.GetNode(1).TargetRows())
	require.Equal(t, int64(100), c.GetNode(2).TargetRows())
	require.Equal(t, int64(100), c.GetNode(1).Info().UpRowCount)
	require.Equal(t, int64(100), old.TargetRows(), "retained node objects never change")
	require.Equal(t, int64(100), c.GetCollection(1).ReplicaRows(10, 2))
	failed := preparing
	failed.PreparingPlacement = nil
	failed.PreparingNodes = nil
	c.PublishShard(id, &failed)
	require.Equal(t, int64(100), c.GetNode(1).TargetRows())
	require.Zero(t, c.GetNode(2).TargetRows())
	require.True(t, c.GetCollection(1).PlacementNode(2).HasResource(resource), "target failure must not erase ready resources")
	require.True(t, c.PublishReplicaActivity(1, map[int64]bool{10: false}))
	require.Zero(t, c.GetNode(1).TargetRows())
	require.False(t, c.PublishReplicaActivity(1, map[int64]bool{10: false}))
	require.True(t, c.PublishReplicaActivity(1, map[int64]bool{10: true}))
	require.Equal(t, int64(100), c.GetNode(1).TargetRows())
	c.PublishLoadConfig(1, nil, 2)
	require.Zero(t, c.GetNode(1).TargetRows())
	require.True(t, c.GetCollection(1).PlacementNode(2).HasResource(resource))
	c.PublishShard(id, nil)
	require.Zero(t, c.GetCollection(1).ReplicaRows(10, 1))
	require.Nil(t, c.GetCollection(1).PlacementNode(2))
}

func TestTargetEligibilityFallbackAndDemandBuckets(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	cfg := cacheConfig(1, 10)
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11, ResourceGroup: "rg1"})
	c.PublishLoadConfig(1, cfg, 1)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	for _, node := range []int64{1, 2} {
		c.PublishNode(node, &api.NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	require.Equal(t, int64(200), c.GetResourceGroup("rg1").Demand(2))
	require.Equal(t, int64(100), c.GetResourceGroup("rg1").Demand(1))
	require.Zero(t, c.GetResourceGroup("rg1").Demand(0))
	stats := cacheStats(1000, 1, 0, coordview.SegmentStateUp, false)
	stats.UpNodes, stats.PreparingNodes = []int64{1}, []int64{2}
	stats.UpPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 1}, UnknownRows: []int64{1000}}
	stats.PreparingPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 2}, UnknownRows: []int64{1000}}
	c.PublishShard(id, stats)
	require.Equal(t, int64(100), c.GetNode(2).TargetContribution(id))
	c.PublishNode(2, nil)
	require.Equal(t, int64(100), c.GetNode(1).TargetRows(), "lost Preparing falls back to surviving Up")
	if node := c.GetNode(2); node != nil {
		require.Zero(t, node.TargetRows())
	}
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg2"})
	require.Zero(t, c.GetNode(1).TargetRows(), "unknown-row fallback must still respect RG legality")
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishDataView(1, cacheData(1, id.VChannel, 250))
	require.Equal(t, int64(250), c.GetNode(1).TargetRows())
	require.Equal(t, int64(500), c.GetResourceGroup("rg1").Demand(2))
	c.PublishLoadConfig(1, cacheConfig(1, 10), 2)
	require.Equal(t, int64(250), c.GetResourceGroup("rg1").Demand(2))
	c.PublishLoadConfig(1, nil, 3)
	require.Zero(t, c.GetResourceGroup("rg1").Demand(2))
	c.PublishDataView(1, nil)
	c.PublishShard(id, nil)
	c.PublishReplicaActivity(1, nil)
	c.PublishNode(1, nil)
	require.Nil(t, c.GetCollection(1))
	require.Nil(t, c.GetNode(1))
	require.Nil(t, c.GetResourceGroup("rg1"))
}
