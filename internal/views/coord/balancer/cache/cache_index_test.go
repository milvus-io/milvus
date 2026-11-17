package cache

import (
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestFixedKeyEncoding(t *testing.T) {
	for _, tc := range []struct {
		id  int64
		hex string
	}{
		{0, "0000000000000000"},
		{256, "0000000000000100"},
		{-1, "ffffffffffffffff"},
		{math.MinInt64, "8000000000000000"},
		{math.MaxInt64, "7fffffffffffffff"},
	} {
		key := idKey(tc.id)
		require.Equal(t, tc.hex, hex.EncodeToString(key[:]))
	}
	pair := replicaNodeKey(-1, 256)
	require.Equal(t, "ffffffffffffffff0000000000000100", hex.EncodeToString(pair[:]))
	resource := resourceKey(coordview.ResourceKey{
		PartitionID: 0x0102030405060708, SegmentID: 0x1112131415161718,
		DataVersion:     qviews.DataVersion{StreamingVersion: 0x2122232425262728, CompactVersion: 0x3132333435363738},
		LoadInfoVersion: 0xf1f2f3f4f5f6f7f8,
	})
	require.Equal(t, "0102030405060708111213141516171821222324252627283132333435363738f1f2f3f4f5f6f7f8", hex.EncodeToString(resource[:]))
	require.Equal(t, "000000000000010000616263", hex.EncodeToString(shardKey(qviews.ShardID{ReplicaID: 256, VChannel: "\x00abc"})))
}

func TestShardKeyLookupRetainsFullIdentity(t *testing.T) {
	var index immutableIndex[int]
	_, found := index.getShard(qviews.ShardID{})
	require.False(t, found)
	var ids []qviews.ShardID
	var snapshots []immutableIndex[int]
	for _, length := range []int{0, 32, 120, 121, 1024} {
		prefix := strings.Repeat("x", length)
		for _, id := range []qviews.ShardID{
			{ReplicaID: 1, VChannel: prefix},
			{ReplicaID: 2, VChannel: prefix},
			{ReplicaID: 1, VChannel: prefix + "a"},
			{ReplicaID: 1, VChannel: prefix + "b"},
		} {
			ids = append(ids, id)
			index = index.set(shardKey(id), len(ids))
			snapshots = append(snapshots, index)
		}
	}
	for version, snapshot := range snapshots {
		for i, id := range ids {
			value, found := snapshot.getShard(id)
			require.Equal(t, i <= version, found, "replica IDs and complete channel suffixes must remain distinct")
			if found {
				require.Equal(t, i+1, value)
			}
		}
	}
}

func BenchmarkMemoryShardKeyLookup(b *testing.B) {
	for _, length := range []int{32, 120, 121, 1024} {
		id := qviews.ShardID{ReplicaID: 10, VChannel: strings.Repeat("x", length)}
		key := shardKey(id)
		shard := &ShardEntry{id: id}
		var collection CollectionEntry
		collection.shards = collection.shards.set(key, shard)
		var node NodeEntry
		node.contributions = node.contributions.set(key, nodeContribution{shard: id, rows: NodeSizeStats{UpLogicalSize: 100}})
		node.targets = node.targets.set(key, targetContribution{rows: 100})
		for _, hit := range []bool{true, false} {
			query, state, size := id, "hit", int64(100)
			if !hit {
				query.ReplicaID++
				state, size = "miss", 0
			}
			b.Run(fmt.Sprintf("channel%d/%s", length, state), func(b *testing.B) {
				b.Run("shard", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if (collection.GetShard(query) != nil) != hit {
							b.Fatal("unexpected shard")
						}
					}
				})
				b.Run("actual", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if node.Contribution(query).UpLogicalSize != size {
							b.Fatal("unexpected actual contribution")
						}
					}
				})
				b.Run("target", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if node.TargetContribution(query) != size {
							b.Fatal("unexpected target contribution")
						}
					}
				})
			})
		}
	}
}

func TestFixedKeyPublicationRetainsSnapshots(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	for n := int64(1); n <= 3; n++ {
		c.PublishNode(n, &api.NodeInfo{NodeID: n, Alive: true, ResourceGroup: "rg1"})
	}
	base := coordview.ResourceKey{PartitionID: 1, SegmentID: 1000, DataVersion: qviews.DataVersion{StreamingVersion: 2, CompactVersion: 3}, LoadInfoVersion: 4}
	keys := []coordview.ResourceKey{base, base, base, base, base, base}
	keys[1].PartitionID++
	keys[2].SegmentID++
	keys[3].DataVersion.StreamingVersion++
	keys[4].DataVersion.CompactVersion++
	keys[5].LoadInfoVersion++
	resources := make(map[int64]map[coordview.ResourceKey]struct{})
	for i, key := range keys {
		node := int64(i%3 + 1)
		if resources[node] == nil {
			resources[node] = make(map[coordview.ResourceKey]struct{})
		}
		resources[node][key] = struct{}{}
	}
	c.PublishShard(id, &coordview.ShardStats{
		UpNodes: []int64{1, 2, 3}, Resources: resources,
		UpPlacement: &coordview.ViewPlacement{PartitionSizes: map[int64]map[int64]int64{1: {1: 10, 2: 20, 3: 30}}},
	})
	before := c.GetCollection(1)
	// Delete and republish with different encoded keys. The previous index paths
	// must keep the arrays retained by the original insertions.
	c.PublishShard(id, nil)
	nextKey := base
	nextKey.LoadInfoVersion = 99
	c.PublishShard(id, &coordview.ShardStats{
		UpNodes: []int64{2}, Resources: map[int64]map[coordview.ResourceKey]struct{}{2: {nextKey: {}}},
		UpPlacement: &coordview.ViewPlacement{PartitionSizes: map[int64]map[int64]int64{1: {2: 200}}},
	})
	after := c.GetCollection(1)
	for n := int64(1); n <= 3; n++ {
		node := before.PlacementNode(n)
		require.NotNil(t, node)
		require.Equal(t, n*10, before.ReplicaSize(10, n))
		for i, key := range keys {
			require.Equal(t, int64(i%3+1) == n, node.HasResource(key), "all resource identity fields must remain distinct")
		}
		require.False(t, node.HasResource(nextKey))
	}
	require.Nil(t, after.PlacementNode(1))
	require.Nil(t, after.PlacementNode(3))
	require.True(t, after.PlacementNode(2).HasResource(nextKey))
	require.False(t, after.PlacementNode(2).HasResource(base))
	require.Equal(t, int64(200), after.ReplicaSize(10, 2))
	require.Zero(t, after.ReplicaSize(10, 1))
}

func BenchmarkMemoryFixedKeyLookup(b *testing.B) {
	c := New(nil)
	id := cacheShard(1, 10)
	key := coordview.ResourceKey{PartitionID: 1, SegmentID: 1000, DataVersion: qviews.DataVersion{StreamingVersion: 2, CompactVersion: 3}, LoadInfoVersion: 4}
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	c.PublishShard(id, &coordview.ShardStats{
		UpNodes:     []int64{1},
		UpPlacement: &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 1}, PartitionSizes: map[int64]map[int64]int64{1: {1: 100}}},
		Resources:   map[int64]map[coordview.ResourceKey]struct{}{1: {key: {}}},
	})
	collection := c.GetCollection(1)
	node := collection.PlacementNode(1)
	for _, hit := range []bool{true, false} {
		name, nodeID, expectedSize := "hit", int64(1), int64(100)
		resource := key
		if !hit {
			name, nodeID, expectedSize = "miss", 2, 0
			resource.LoadInfoVersion++
		}
		b.Run("placement/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if (collection.PlacementNode(nodeID) != nil) != hit {
					b.Fatal("unexpected placement lookup")
				}
			}
		})
		b.Run("resource/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if node.HasResource(resource) != hit {
					b.Fatal("unexpected resource lookup")
				}
			}
		})
		b.Run("replica/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if collection.ReplicaSize(10, nodeID) != expectedSize {
					b.Fatal("unexpected replica load")
				}
			}
		})
	}
}
