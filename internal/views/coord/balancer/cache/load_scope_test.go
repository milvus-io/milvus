package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
)

func TestPartitionLoadAccounting(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	data := api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 1, Shards: []*api.ShardDataView{{
		VChannel: id.VChannel, Partitions: []*api.PartitionDataView{
			{PartitionID: 1, Segments: []*api.SegmentDataView{{SegmentID: 101, RowNum: 100}, {SegmentID: 102, RowNum: 25}}},
			{PartitionID: 2, Segments: []*api.SegmentDataView{{SegmentID: 201, RowNum: 900}}},
		},
	}}})
	// DataView replay may precede LoadConfig replay.
	c.PublishDataView(1, data)
	require.Zero(t, c.GetCollection(1).LoadedRows())
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	cfg := cacheConfig(1, 10)
	cfg.PartitionIDs = []int64{1, 2}
	c.PublishLoadConfig(1, cfg, 1)
	stats := &coordview.ShardStats{
		Segments: map[int64]*coordview.SegmentStats{
			101: {SegmentID: 101, PartitionID: 1, RowNum: 100, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}},
			102: {SegmentID: 102, PartitionID: 1, Nodes: map[int64]coordview.SegmentState{2: coordview.SegmentStateUp}},
			201: {SegmentID: 201, PartitionID: 2, RowNum: 900, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}},
		},
		UpPlacement: &coordview.ViewPlacement{
			Assignments: map[int64]int64{101: 1, 102: 2, 201: 1},
			// Preserve this view's exact footprints, even when merged stats or
			// latest DataView report other rows. Known zero must remain zero.
			PartitionRows: map[int64]map[int64]int64{1: {1: 0}, 2: {1: 800}},
			UnknownRows:   []int64{102},
		},
	}
	c.PublishShard(id, stats)
	before := c.GetCollection(1)
	nodeBefore := c.GetNode(1)
	require.Equal(t, int64(1025), before.LoadedRows())
	require.Equal(t, int64(800), nodeBefore.TargetRows())
	require.Equal(t, int64(25), c.GetNode(2).TargetRows())

	partial := cfg.Clone()
	partial.PartitionIDs = []int64{1}
	c.PublishLoadConfig(1, partial, 2)
	loaded := c.GetCollection(1)
	require.Same(t, data, loaded.DataView())
	require.Equal(t, int64(125), loaded.LoadedRows())
	require.Equal(t, int64(125), loaded.ShardLoadedRows(id.VChannel))
	require.Equal(t, int64(125), c.GetResourceGroup("rg1").Demand(2))
	require.Zero(t, c.GetNode(1).TargetRows())
	require.Equal(t, int64(1000), c.GetNode(1).Info().UpRowCount, "release intent does not erase resident resources")
	require.Equal(t, int64(25), loaded.ReplicaRows(10, 2))
	require.Equal(t, int64(1025), before.LoadedRows(), "old cache publications stay immutable")
	require.Equal(t, int64(800), nodeBefore.TargetRows())

	// A later Preparing target uses its own exact footprints, not merged rows.
	preparing := *stats
	preparing.PreparingPlacement = &coordview.ViewPlacement{
		Assignments:   map[int64]int64{101: 2, 201: 2},
		PartitionRows: map[int64]map[int64]int64{1: {2: 7}},
		UnknownRows:   []int64{201},
	}
	preparing.PreparingNodes = []int64{2}
	c.PublishShard(id, &preparing)
	require.Equal(t, int64(7), c.GetNode(2).TargetRows(), "unloaded unknown rows must not enter the fallback")

	empty := cfg.Clone()
	empty.PartitionIDs = nil
	c.PublishLoadConfig(1, empty, 3)
	require.Zero(t, c.GetCollection(1).LoadedRows())
	require.Zero(t, c.GetResourceGroup("rg1").Demand(2))
	require.Zero(t, c.GetNode(2).TargetRows())
	c.PublishLoadConfig(1, cfg, 4)
	require.Equal(t, int64(1025), c.GetCollection(1).LoadedRows())
	require.Equal(t, int64(907), c.GetNode(2).TargetRows())
	c.PublishDataView(1, cacheData(1, id.VChannel, 250))
	require.Equal(t, int64(250), c.GetCollection(1).LoadedRows())
	require.Equal(t, int64(250), c.GetCollection(1).ShardLoadedRows(id.VChannel))
	require.Equal(t, int64(250), c.GetResourceGroup("rg1").Demand(2))
	require.Equal(t, int64(1025), before.LoadedRows())
	c.PublishDataView(1, nil)
	require.Zero(t, c.GetCollection(1).LoadedRows())
	require.Zero(t, c.GetResourceGroup("rg1").Demand(2))
	require.Equal(t, int64(907), c.GetNode(2).TargetRows(), "resident fallback survives DataView removal")
}

func TestFullPartitionLoadFastPathTransitions(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	data := api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 1, Shards: []*api.ShardDataView{
		{VChannel: id.VChannel, Partitions: []*api.PartitionDataView{{PartitionID: 1, Segments: []*api.SegmentDataView{{SegmentID: 101, RowNum: 100}}}}},
		{VChannel: "other", Partitions: []*api.PartitionDataView{{PartitionID: 2, Segments: []*api.SegmentDataView{{SegmentID: 201, RowNum: 900}}}, {PartitionID: 3}}},
	}})
	c.PublishDataView(1, data)
	require.False(t, c.GetCollection(1).LoadsAllPartitions(), "missing config is not full load")
	for i, tc := range []struct {
		partitions []int64
		all        bool
		rows       int64
	}{
		{[]int64{1, 2, 3}, true, 1000},
		{[]int64{1, 2, 4}, false, 1000}, // Same count and rows, but wrong membership.
		{[]int64{1, 2, 3, 4}, true, 1000},
		{[]int64{1}, false, 100},
		{nil, false, 0},
		{[]int64{1, 2, 3}, true, 1000},
	} {
		cfg := cacheConfig(1, 10)
		cfg.PartitionIDs = tc.partitions
		c.PublishLoadConfig(1, cfg, uint64(i+1))
		entry := c.GetCollection(1)
		require.Equal(t, tc.all, entry.LoadsAllPartitions())
		require.Equal(t, tc.rows, entry.LoadedRows())
		require.Equal(t, tc.rows, entry.ShardLoadedRows(id.VChannel)+entry.ShardLoadedRows("other"))
		require.Zero(t, entry.ShardLoadedRows("absent"))
		require.Same(t, data, entry.DataView())
		if tc.all {
			require.Nil(t, entry.loadedShardRows, "full load reuses source summaries without a second row map")
		}
	}
	before := c.GetCollection(1)
	newData := api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 1, Shards: []*api.ShardDataView{{
		VChannel: id.VChannel, Partitions: []*api.PartitionDataView{
			{PartitionID: 1, Segments: []*api.SegmentDataView{{SegmentID: 101, RowNum: 150}}},
			{PartitionID: 4, Segments: []*api.SegmentDataView{{SegmentID: 401, RowNum: 10000}}},
		},
	}}})
	c.PublishDataView(1, newData)
	require.False(t, c.GetCollection(1).LoadsAllPartitions(), "a new unloaded partition invalidates the fast path")
	require.Equal(t, int64(150), c.GetCollection(1).LoadedRows())
	require.True(t, before.LoadsAllPartitions())
	require.Equal(t, int64(1000), before.LoadedRows(), "retained snapshots stay unchanged")
	c.PublishDataView(1, cacheData(1, id.VChannel, 250))
	require.True(t, c.GetCollection(1).LoadsAllPartitions())
	require.Equal(t, int64(250), c.GetCollection(1).LoadedRows())
	c.PublishDataView(1, nil)
	require.False(t, c.GetCollection(1).LoadsAllPartitions())
	require.Zero(t, c.GetCollection(1).LoadedRows())
	c.PublishDataView(1, data)
	c.PublishLoadConfig(1, nil, 7)
	require.False(t, c.GetCollection(1).LoadsAllPartitions())
	require.Zero(t, c.GetCollection(1).LoadedRows())
}

func TestFullCurrentDataViewDoesNotCoverHistoricalPartitions(t *testing.T) {
	c := New(nil)
	id := cacheShard(1, 10)
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishShard(id, &coordview.ShardStats{
		Segments: map[int64]*coordview.SegmentStats{
			1000: {SegmentID: 1000, PartitionID: 1},
			2000: {SegmentID: 2000, PartitionID: 2},
			2001: {SegmentID: 2001, PartitionID: 2},
		},
		UpPlacement: &coordview.ViewPlacement{
			Assignments:   map[int64]int64{1000: 1, 2000: 1, 2001: 1},
			PartitionRows: map[int64]map[int64]int64{1: {1: 100}, 2: {1: 900}},
			UnknownRows:   []int64{2001},
		},
	})
	require.True(t, c.GetCollection(1).LoadsAllPartitions())
	require.False(t, c.GetCollection(1).LoadsPartition(2), "full load applies to the current DataView only")
	require.Equal(t, int64(100), c.GetNode(1).TargetRows(), "historical partitions still require filtering")
}

func BenchmarkLoadedRowSummaries(b *testing.B) {
	for _, partitions := range []int{10, 1000} {
		for _, segments := range []int{1, 100} {
			b.Run(fmt.Sprintf("partitions_%d/segments_per_partition_%d", partitions, segments), func(b *testing.B) {
				cfg := cacheConfig(1, 10)
				cfg.PartitionIDs = nil
				shard := &api.ShardDataView{VChannel: "v0"}
				for p := 0; p < partitions; p++ {
					partition := &api.PartitionDataView{PartitionID: int64(p)}
					if p%2 == 0 {
						cfg.PartitionIDs = append(cfg.PartitionIDs, int64(p))
					}
					for s := 0; s < segments; s++ {
						partition.Segments = append(partition.Segments, &api.SegmentDataView{SegmentID: int64(p*segments + s), RowNum: 100})
					}
					shard.Partitions = append(shard.Partitions, partition)
				}
				entry := &CollectionEntry{config: cfg, data: api.PrepareCollectionDataView(&api.CollectionDataView{Shards: []*api.ShardDataView{shard}})}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entry.refreshLoadRows()
				}
			})
		}
	}
}
