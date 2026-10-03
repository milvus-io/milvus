package balancer

import (
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func newTestCache(cfg *loadmgr.LoadConfig) *balancercache.Cache {
	c := balancercache.New(policyTestConfig())
	c.PublishLoadConfig(cfg.CollectionID, cfg, 1)
	return c
}

func publishTestNodes(c *balancercache.Cache, nodes ...*NodeInfo) {
	for _, node := range nodes {
		c.PublishNode(node.NodeID, node)
	}
}

// publishTestData builds native immutable membership with matching row summaries.
func publishTestData(c *balancercache.Cache, collection int64, version qviews.DataVersion, segments map[int64]*SegmentDataView, shards ...*viewpb.DataViewOfShard) {
	data := &CollectionDataView{CollectionID: collection, DataVersion: version}
	for _, shard := range shards {
		native := &ShardDataView{VChannel: shard.GetVchannel()}
		for _, partition := range shard.GetPartitions() {
			part := &PartitionDataView{PartitionID: partition.GetPartitionId()}
			for _, id := range partition.GetSegmentIds() {
				segment := segments[id]
				if segment == nil {
					segment = &SegmentDataView{SegmentID: id, PartitionID: part.PartitionID}
				}
				part.Segments = append(part.Segments, segment)
			}
			native.Partitions = append(native.Partitions, part)
		}
		data.Shards = append(data.Shards, native)
	}
	c.PublishDataView(collection, api.PrepareCollectionDataView(data))
}

// Background load belongs to a real contribution outside the selected balance scope.
func publishBackgroundRows(c *balancercache.Cache, rows map[int64]int64) {
	cfg := cfgFor(999, 9990, []int64{1}, nil)
	c.PublishLoadConfig(999, cfg, 1)
	segments := make(map[int64]*SegmentDataView)
	var ids []int64
	var placements []testSegmentPlacement
	for node, count := range rows {
		ids = append(ids, node)
		segments[node] = &SegmentDataView{SegmentID: node, PartitionID: 1, RowNum: count}
		placements = append(placements, placement(node, 1, node, coordview.SegmentStateUp))
	}
	version := qviews.DataVersion{StreamingVersion: 1}
	publishTestData(c, 999, version, segments, shardDataView(cacheShard(999, 9990).VChannel, 1, ids...))
	c.PublishShard(cacheShard(999, 9990), withSegmentRows(upStats(version, placements...), rows))
}

// withSegmentRows attaches retained exact-version footprints before publication.
func withSegmentRows(stats *coordview.ShardStats, rows map[int64]int64) *coordview.ShardStats {
	for id, count := range rows {
		stats.Segments[id].RowNum = count
		stats.Segments[id].HasRowNum = true
	}
	for _, view := range []*coordview.ViewPlacement{stats.UpPlacement, stats.PreparingPlacement} {
		if view == nil {
			continue
		}
		view.Rows = make(map[int64]int64)
		view.UnknownRows = nil
		for id, node := range view.Assignments {
			if segment := stats.Segments[id]; segment.HasRowNum {
				view.Rows[node] += segment.RowNum
			} else {
				view.UnknownRows = append(view.UnknownRows, id)
			}
		}
	}
	return stats
}
