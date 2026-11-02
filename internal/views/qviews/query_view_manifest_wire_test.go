package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryViewPartitionDecodesCoordinatorManifestVersions(t *testing.T) {
	// Coordinator f50960e1 emits partition 10, segments [1000, 1001], and
	// manifests [7, 9] at tag 4. A local-only round trip would miss tag drift.
	wire := []byte{0x08, 0x0a, 0x12, 0x04, 0xe8, 0x07, 0xe9, 0x07, 0x22, 0x02, 0x07, 0x09}
	var partition viewpb.QueryViewOfPartition
	require.NoError(t, proto.Unmarshal(wire, &partition))
	require.Equal(t, int64(10), partition.GetPartitionId())
	require.Equal(t, []int64{1000, 1001}, partition.GetSegmentIds())
	require.Equal(t, []int64{7, 9}, partition.GetSegmentManifestVersions())
	encoded, err := proto.Marshal(&partition)
	require.NoError(t, err)
	require.Equal(t, wire, encoded)
}
