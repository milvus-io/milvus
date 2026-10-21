package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
)

func TestCacheRowsDistinguishesUnknownAndPublishedZero(t *testing.T) {
	c := New(nil)
	shard := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 100, 200))
	stats := &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{
		1000: {SegmentID: 1000, RowNum: 0, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}},
		1001: {SegmentID: 1001, Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStatePreparing}},
		3:    {SegmentID: 3, RowNum: 42, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{2: coordview.SegmentStateReady}},
		4:    {SegmentID: 4, RowNum: 999, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{2: coordview.SegmentStateUnrecoverable}},
		5:    {SegmentID: 5, Nodes: map[int64]coordview.SegmentState{2: coordview.SegmentStateUp}},
	}}
	c.PublishShard(shard, stats)
	require.Equal(t, NodeRowStats{PendingRowCount: 200}, c.GetNode(1).Contribution(shard))
	require.Equal(t, NodeRowStats{PendingRowCount: 42}, c.GetNode(2).Contribution(shard))
	replacement := *stats
	c.PublishShard(shard, &replacement)
	require.Equal(t, int64(200), c.GetNode(1).Info().PendingRowCount, "replacing contributions must not double count")
	require.Equal(t, int64(42), c.GetNode(2).Info().PendingRowCount)
	require.Zero(t, c.GetNode(2).Info().UpRowCount, "unknown rows without fallback and Unrecoverable placements do not add load")
	c.PublishShard(shard, nil)
	require.Nil(t, c.GetNode(1))
	require.Nil(t, c.GetNode(2))
}
