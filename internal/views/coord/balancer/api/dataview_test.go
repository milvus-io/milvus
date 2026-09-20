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
	snapshot := NewDataViewSnapshot(7, []*CollectionDataView{nil, coll})
	second := NewDataViewSnapshot(8, []*CollectionDataView{coll})
	require.Same(t, coll, PrepareCollectionDataView(coll))
	require.Equal(t, int64(42), coll.Shard("v0").TotalRows, "sharing must not recompute/double counts")
	require.Equal(t, uint64(7), snapshot.Version())
	version, ok := snapshot.DataVersion(1)
	require.True(t, ok)
	require.Equal(t, coll.DataVersion, version)
	_, ok = snapshot.DataVersion(2)
	require.False(t, ok)
	got, ok := second.ShardView(1, "v0")
	require.True(t, ok)
	require.Same(t, shard, got)
	_, ok = snapshot.ShardView(2, "v0")
	require.False(t, ok)
	gotSegment, ok := snapshot.SegmentInfo(100)
	require.True(t, ok)
	require.Same(t, segment, gotSegment)
	_, ok = snapshot.SegmentInfo(999)
	require.False(t, ok)
	count := 0
	snapshot.RangeCollections(func(view *CollectionDataView) bool { count++; require.Same(t, coll, view); return true })
	require.Equal(t, 1, count)
	snapshot.RangeCollections(func(*CollectionDataView) bool { return false })
	count = 0
	snapshot.RangeShards(1, func(*ShardDataView) bool { count++; return true })
	require.Equal(t, 3, count)
	count = 0
	snapshot.RangeShards(1, func(*ShardDataView) bool { count++; return false })
	require.Equal(t, 1, count)
	snapshot.RangeShards(2, func(*ShardDataView) bool { t.Fatal("missing collection"); return true })
}

func TestEmptySnapshotReads(t *testing.T) {
	var snapshot *DataViewSnapshot
	require.Zero(t, snapshot.Version())
	_, ok := snapshot.DataVersion(1)
	require.False(t, ok)
	_, ok = snapshot.ShardView(1, "v0")
	require.False(t, ok)
	_, ok = snapshot.SegmentInfo(1)
	require.False(t, ok)
	snapshot.RangeCollections(func(*CollectionDataView) bool { t.Fatal("nil snapshot"); return true })
	snapshot.RangeShards(1, func(*ShardDataView) bool { t.Fatal("nil snapshot"); return true })
}
