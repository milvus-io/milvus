package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestMemoryTargetRefreshSharesUnchangedShards(t *testing.T) {
	c := New(nil)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	ids := make([]qviews.ShardID, 3)
	for i := range ids {
		id := qviews.ShardID{ReplicaID: 10, VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_1v%d", i)}
		ids[i] = id
		node := int64(1)
		if i == 2 {
			node = 2
		}
		stats := cacheStats(int64(i), node, int64(i), coordview.SegmentStateUp, true)
		stats.UpNodes = []int64{node}
		stats.UpPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{int64(i): node}, PartitionSizes: map[int64]map[int64]int64{1: {node: int64(i)}}}
		c.PublishShard(id, stats)
	}
	before := c.GetCollection(1)
	c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, Stopping: true, ResourceGroup: "rg1"})
	after := c.GetCollection(1)
	for _, id := range ids[:2] {
		require.Same(t, before.GetShard(id), after.GetShard(id))
	}
	require.NotSame(t, before.GetShard(ids[2]), after.GetShard(ids[2]))
	require.Equal(t, int64(2), before.GetShard(ids[2]).TargetSize()[2])
	require.Empty(t, after.GetShard(ids[2]).TargetSize())
	require.Zero(t, after.ReplicaSize(10, 2))
	// A second refresh with no new target result also preserves the changed shard.
	c.PublishNode(2, nil)
	for _, id := range ids {
		require.Same(t, after.GetShard(id), c.GetCollection(1).GetShard(id))
	}
	c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	restored := c.GetCollection(1)
	require.Equal(t, int64(2), restored.ReplicaSize(10, 2))
	// Clearing/reusing publication scratch cannot mutate any retained shard map.
	require.Equal(t, map[int64]int64{1: 0}, before.GetShard(ids[0]).TargetSize())
	require.Equal(t, map[int64]int64{1: 1}, before.GetShard(ids[1]).TargetSize())
	require.Empty(t, after.GetShard(ids[2]).TargetSize())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, Stopping: i%2 == 0, ResourceGroup: "rg1"})
		}
	}()
	for i := 0; i < 100; i++ {
		require.Equal(t, int64(2), restored.GetShard(ids[2]).TargetSize()[2])
		require.Equal(t, int64(2), restored.ReplicaSize(10, 2))
	}
	wg.Wait()
}
