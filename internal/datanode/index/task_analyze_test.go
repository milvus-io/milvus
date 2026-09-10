package index

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
)

func TestBuildAnalyzeInfo_ManifestShim(t *testing.T) {
	req := &workerpb.AnalyzeRequest{
		ClusterID:     "c1",
		CollectionID:  1,
		PartitionID:   2,
		FieldID:       3,
		FieldType:     schemapb.DataType_FloatVector,
		Dim:           4,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root"},
		SegmentStats: map[int64]*indexpb.SegmentStats{
			// V3: manifest set, no logIDs.
			101: {ID: 101, NumRows: 10, ManifestPath: "{\"base_path\":\"root/segments/101\",\"ver\":3}"},
			// V1: logIDs, no manifest.
			102: {ID: 102, NumRows: 20, LogIDs: []int64{2001, 2002}},
		},
	}

	info := buildAnalyzeInfo(req)

	// V3: forwarded manifest, no insert_files entry.
	assert.Equal(t, "{\"base_path\":\"root/segments/101\",\"ver\":3}", info.GetManifestPaths()[101])
	_, hasV3Files := info.GetInsertFiles()[101]
	assert.False(t, hasV3Files, "V3 segment must not carry insert files")

	// V1: insert-log paths, no manifest entry.
	assert.Len(t, info.GetInsertFiles()[102].GetInsertFiles(), 2)
	_, hasV1Manifest := info.GetManifestPaths()[102]
	assert.False(t, hasV1Manifest)

	assert.Equal(t, int64(10), info.GetNumRows()[101])
}
