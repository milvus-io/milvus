package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestNativeDataViewRetainsTransformCoverage(t *testing.T) {
	shard := &ShardDataView{VChannel: "v1", GrowingTransformStartAfterTimetick: 80, CompactedTransformStartAfterTimetick: 50, Partitions: []*PartitionDataView{
		{PartitionID: 1, Segments: []*SegmentDataView{{SegmentID: 10, ManifestVersion: 3, TransformStartAfterTimetick: 70}}},
		{PartitionID: 2, Segments: []*SegmentDataView{{SegmentID: 11, ManifestVersion: 4, TransformStartAfterTimetick: 50}}},
	}}
	view := syntheticDataView(&loadmgr.LoadConfig{CollectionID: 1}, qviews.DataVersion{StreamingVersion: 2}, shard)
	require.Equal(t, uint64(50), shard.CompactedTransformStartAfterTimetick)
	require.Equal(t, uint64(80), shard.GrowingTransformStartAfterTimetick)
	require.Equal(t, uint64(50), qviews.NewQueryViewAtCoordBuilder(1, view, "v1").Build().GetMeta().GetTransformStartAfterTimetick())
	require.Equal(t, []int64{3}, view.Shards[0].Partitions[0].SegmentManifestVersions)
	require.Equal(t, uint64(70), shard.Partitions[0].Segments[0].TransformStartAfterTimetick)
	require.Equal(t, []int64{4}, view.Shards[0].Partitions[1].SegmentManifestVersions)
	require.Equal(t, uint64(50), shard.Partitions[1].Segments[0].TransformStartAfterTimetick)
}
