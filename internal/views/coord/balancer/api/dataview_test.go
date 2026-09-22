package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPublishedCollectionIndexesAndSummaries(t *testing.T) {
	coll := PrepareCollectionDataView(&CollectionDataView{CollectionID: 1, DataVersion: qviews.DataVersion{StreamingVersion: 2}, Shards: []*ShardDataView{
		nil, {VChannel: "v0", Partitions: []*PartitionDataView{nil, {PartitionID: 10, Segments: []*SegmentDataView{nil, {SegmentID: 100, RowNum: 0}, {SegmentID: 101, RowNum: 42}}}}}, {VChannel: "v1"},
	}})
	shard := coll.Shard("v0")
	require.Equal(t, int64(42), shard.TotalRows)
	require.Equal(t, 2, shard.SegmentCount)
	segment, ok := coll.Segment(100)
	require.True(t, ok)
	require.Equal(t, int64(10), segment.PartitionID)
	require.Nil(t, coll.Shard("absent"))
	require.Same(t, coll, PrepareCollectionDataView(coll))
	require.Equal(t, int64(42), coll.Shard("v0").TotalRows, "sharing must not recompute/double counts")
	require.Same(t, shard, coll.Shard("v0"))
	gotSegment, ok := coll.Segment(100)
	require.True(t, ok)
	require.Same(t, segment, gotSegment)
	_, ok = coll.Segment(999)
	require.False(t, ok)
	require.Empty(t, coll.Shard("v1").Partitions)
}
