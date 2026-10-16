package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func cacheShard(collection, replica int64) qviews.ShardID {
	return qviews.ShardID{ReplicaID: replica, VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection)}
}

func cacheData(collection int64, channel string, rows ...int64) *api.CollectionDataView {
	segments := make([]*api.SegmentDataView, 0, len(rows))
	for i, row := range rows {
		segments = append(segments, &api.SegmentDataView{SegmentID: collection*1000 + int64(i), PartitionID: 1, RowNum: row})
	}
	return api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: collection, DataVersion: qviews.DataVersion{StreamingVersion: 1}, Shards: []*api.ShardDataView{{VChannel: channel, Partitions: []*api.PartitionDataView{{PartitionID: 1, Segments: segments}}}}})
}

func cacheStats(segment, node, rows int64, state coordview.SegmentState, known bool) *coordview.ShardStats {
	return &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{segment: {SegmentID: segment, PartitionID: 1, RowNum: rows, HasRowNum: known, Nodes: map[int64]coordview.SegmentState{node: state}}}}
}

func TestCacheImmutablePublicationAndFallback(t *testing.T) {
	c := New(nil)
	shard := cacheShard(1, 10)
	cfg := cacheConfig(1, 10)
	c.PublishLoadConfig(1, cfg, 1)
	data := cacheData(1, shard.VChannel, 100, 0)
	c.PublishDataView(1, data)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: cfg.Replicas[0].ResourceGroup})
	stats := cacheStats(1000, 1, 0, coordview.SegmentStateUp, false)
	c.PublishShard(shard, stats)
	before := c.GetCollection(1)
	node := c.GetNode(1)
	require.Equal(t, int64(100), node.Info().UpRowCount)
	require.Equal(t, int64(100), node.Contribution(shard).UpRowCount)
	require.Equal(t, int64(100), data.Shards[0].TotalRows)
	require.Equal(t, 2, data.Shards[0].SegmentCount)
	c.PublishShard(shard, stats)
	require.Same(t, node, c.GetNode(1), "duplicate publication preserves the immutable object")
	c.PublishShard(shard, cacheStats(1000, 1, 0, coordview.SegmentStateReady, true))
	require.Zero(t, c.GetNode(1).Info().PendingRowCount, "published zero must not use historical fallback")
	require.Equal(t, int64(100), node.Info().UpRowCount, "retained readers remain immutable")
	require.Same(t, data, c.GetCollection(1).DataView())
	require.Same(t, stats, before.GetShard(shard).Stats())
	c.PublishShard(shard, cacheStats(1000, 1, 0, coordview.SegmentStatePreparing, false))
	c.PublishDataView(1, cacheData(1, shard.VChannel))
	require.Equal(t, int64(100), c.GetNode(1).Info().PendingRowCount, "resident old membership retains fallback")
	c.PublishLoadConfig(1, nil, 2)
	require.NotNil(t, c.GetCollection(1).GetShard(shard), "desired removal preserves cleanup scope")
	var resident []qviews.ShardID
	c.GetCollection(1).RangeShards(func(s *ShardEntry) bool { resident = append(resident, s.ID()); return true })
	require.Contains(t, resident, shard)
	c.PublishDataView(1, nil)
	c.PublishNode(1, nil)
	require.NotNil(t, c.GetNode(1), "lost node retains placement accounting")
	c.PublishShard(shard, nil)
	require.Nil(t, c.GetCollection(1))
	require.Nil(t, c.GetNode(1))
	_, ok := c.CollectionForReplica(10)
	require.False(t, ok)
}

func TestCacheLateDataViewRefreshesOnlyUnknownFootprints(t *testing.T) {
	c := New(nil)
	shard := cacheShard(1, 10)
	c.PublishShard(shard, cacheStats(1000, 1, 0, coordview.SegmentStateReady, false))
	require.Zero(t, c.GetNode(1).Info().PendingRowCount)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 42))
	require.Equal(t, int64(42), c.GetNode(1).Info().PendingRowCount)
	c.PublishShard(shard, cacheStats(1000, 1, 7, coordview.SegmentStateUp, true))
	c.PublishDataView(1, cacheData(1, shard.VChannel, 99))
	require.Equal(t, int64(7), c.GetNode(1).Info().UpRowCount)
}

func TestCacheConcurrentFieldsAndContributions(t *testing.T) {
	c := New(nil)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	var wg sync.WaitGroup
	for i := int64(1); i <= 16; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			shard := cacheShard(id, id*10)
			for j := 0; j < 30; j++ {
				c.PublishDataView(id, cacheData(id, shard.VChannel, id))
				c.PublishLoadConfig(id, cacheConfig(id, id*10), uint64(j+1))
				c.PublishShard(shard, cacheStats(id*1000, 1, id, coordview.SegmentStateUp, true))
				node := c.GetNode(1)
				var total int64
				node.RangeShards(func(shard qviews.ShardID) bool { total += node.Contribution(shard).UpRowCount; return true })
				if total != node.Info().UpRowCount {
					t.Errorf("inconsistent published node: %d != %d", total, node.Info().UpRowCount)
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, Stopping: i%2 == 0, ResourceGroup: "rg1"})
		}
	}()
	wg.Wait()
	require.Equal(t, int64(136), c.GetNode(1).Info().UpRowCount)
	for i := int64(1); i <= 16; i++ {
		entry := c.GetCollection(i)
		require.NotNil(t, entry.LoadConfig())
		require.NotNil(t, entry.DataView())
		require.NotNil(t, entry.GetShard(cacheShard(i, i*10)))
	}
	// Concurrent writes to different fields of the same collection must merge.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.PublishDataView(1, cacheData(1, cacheShard(1, 10).VChannel, int64(i)))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.PublishLoadConfig(1, cacheConfig(1, 10), uint64(i+100))
		}
	}()
	wg.Wait()
	require.Equal(t, uint64(199), c.GetCollection(1).ConfigVersion())
	require.Equal(t, int64(99), c.GetCollection(1).DataView().Shards[0].TotalRows)
}

func BenchmarkCacheGetCollection(b *testing.B) {
	c := New(nil)
	shard := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 1, 2, 3))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetCollection(1)
	}
}

func TestCacheRetainsReplicaUntilLastResidentShard(t *testing.T) {
	c := New(nil)
	a, b := cacheShard(1, 10), cacheShard(1, 10)
	b.VChannel = "by-dev-rootcoord-dml_0_1v1"
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	c.PublishShard(a, cacheStats(1000, 1, 10, coordview.SegmentStateUp, true))
	c.PublishShard(b, cacheStats(1001, 1, 20, coordview.SegmentStateUp, true))
	c.PublishLoadConfig(1, nil, 2)
	c.PublishShard(a, nil)
	id, ok := c.CollectionForReplica(10)
	require.True(t, ok)
	require.Equal(t, int64(1), id)
	c.PublishShard(b, nil)
	_, ok = c.CollectionForReplica(10)
	require.False(t, ok)
	require.Nil(t, c.GetCollection(1))
	require.Nil(t, c.GetNode(1))
	require.Empty(t, c.groups)
}

func BenchmarkCachePublishShard(b *testing.B) {
	for _, size := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("resident_shards_%d", size), func(b *testing.B) {
			c := New(nil)
			for i := 0; i < size; i++ {
				c.PublishShard(cacheShard(1, int64(i+1)), cacheStats(int64(i), 1, 1, coordview.SegmentStateUp, true))
			}
			id := cacheShard(1, 1)
			a, bStats := cacheStats(0, 1, 1, coordview.SegmentStateUp, true), cacheStats(0, 1, 2, coordview.SegmentStateUp, true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%2 == 0 {
					c.PublishShard(id, a)
				} else {
					c.PublishShard(id, bStats)
				}
			}
		})
	}
}

func TestRetainedCacheVersionsShareUnchangedChildren(t *testing.T) {
	c := New(nil)
	for i := int64(1); i <= 10000; i++ {
		c.PublishShard(cacheShard(1, i), cacheStats(i, 1, 1, coordview.SegmentStateUp, true))
	}
	untouched := cacheShard(1, 9999)
	original := c.GetCollection(1).GetShard(untouched)
	retained := make([]*CollectionEntry, 0, 128)
	for i := int64(1); i <= 128; i++ {
		retained = append(retained, c.GetCollection(1))
		c.PublishShard(cacheShard(1, 1), cacheStats(1, 1, i, coordview.SegmentStateUp, true))
	}
	for _, entry := range retained {
		require.Same(t, original, entry.GetShard(untouched))
	}
	require.Equal(t, int64(1), retained[0].GetShard(cacheShard(1, 1)).Stats().Segments[1].RowNum)
	require.Equal(t, int64(128), c.GetNode(1).Contribution(cacheShard(1, 1)).UpRowCount)
}

func BenchmarkCacheRetainedVersions(b *testing.B) {
	c := New(nil)
	for i := int64(1); i <= 10000; i++ {
		c.PublishShard(cacheShard(1, i), cacheStats(i, 1, 1, coordview.SegmentStateUp, true))
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	retained := make([]*CollectionEntry, 128)
	nodes := make([]*NodeEntry, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for v := range retained {
			retained[v] = c.GetCollection(1)
			nodes[v] = c.GetNode(1)
			c.PublishShard(cacheShard(1, 1), cacheStats(1, 1, int64(v), coordview.SegmentStateUp, true))
		}
	}
	b.StopTimer()
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "retained_heap_B")
	runtime.KeepAlive(retained)
	runtime.KeepAlive(nodes)
	runtime.KeepAlive(c)
}

func cacheConfig(collection, replica int64) *loadmgr.LoadConfig {
	return &loadmgr.LoadConfig{CollectionID: collection, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: replica, ResourceGroup: "rg1"}}}
}
