package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestNativeDataViewRetainsTransformCoverage(t *testing.T) {
	shard := &ShardDataView{VChannel: "v1", TransformStartAfterTimetick: 50, Partitions: []*PartitionDataView{
		{PartitionID: 1, Segments: []*SegmentDataView{{SegmentID: 10, ManifestVersion: 3, TransformStartAfterTimetick: 70}}},
		{PartitionID: 2, Segments: []*SegmentDataView{{SegmentID: 11, ManifestVersion: 4, TransformStartAfterTimetick: 50}}},
	}}
	view := syntheticDataView(&loadmgr.LoadConfig{CollectionID: 1}, qviews.DataVersion{StreamingVersion: 2}, shard)
	require.Equal(t, uint64(50), view.Shards[0].TransformStartAfterTimetick)
	require.Equal(t, []int64{3}, view.Shards[0].Partitions[0].SegmentManifestVersions)
	require.Equal(t, []uint64{70}, view.Shards[0].Partitions[0].SegmentTransformStartAfterTimeticks)
	require.Equal(t, []int64{4}, view.Shards[0].Partitions[1].SegmentManifestVersions)
	require.Equal(t, []uint64{50}, view.Shards[0].Partitions[1].SegmentTransformStartAfterTimeticks)
}
