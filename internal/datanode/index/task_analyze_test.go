package index

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
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

func TestBuildAnalyzeInfo_V2WithManifestForcesV3StorageVersion(t *testing.T) {
	// When all segments have manifests but the request carries StorageV2,
	// buildAnalyzeInfo must override StorageVersion to V3 so the C++ code
	// reads from manifest_paths (field 17) instead of segment_insert_files
	// (field 19) which is never populated.
	req := &workerpb.AnalyzeRequest{
		ClusterID:      "c1",
		CollectionID:   1,
		PartitionID:    2,
		FieldID:        3,
		FieldType:      schemapb.DataType_FloatVector,
		Dim:            4,
		StorageConfig:  &indexpb.StorageConfig{RootPath: "root"},
		StorageVersion: storage.StorageV2,
		SegmentStats: map[int64]*indexpb.SegmentStats{
			201: {ID: 201, NumRows: 100, ManifestPath: "{\"base_path\":\"root/201\",\"ver\":2}"},
			202: {ID: 202, NumRows: 200, ManifestPath: "{\"base_path\":\"root/202\",\"ver\":2}"},
		},
	}

	info := buildAnalyzeInfo(req)

	// StorageVersion must be overridden to V3 so C++ uses manifest_paths.
	assert.Equal(t, storage.StorageV3, info.GetStorageVersion(),
		"V2 segments with manifests must route through V3 C++ path")

	// All segments should appear in ManifestPaths.
	assert.Equal(t, "{\"base_path\":\"root/201\",\"ver\":2}", info.GetManifestPaths()[201])
	assert.Equal(t, "{\"base_path\":\"root/202\",\"ver\":2}", info.GetManifestPaths()[202])

	// InsertFiles (field 13) should be empty.
	assert.Empty(t, info.GetInsertFiles())

	// NumRows should be populated for all segments.
	assert.Equal(t, int64(100), info.GetNumRows()[201])
	assert.Equal(t, int64(200), info.GetNumRows()[202])
}

func TestBuildAnalyzeInfo_V1KeepsOriginalStorageVersion(t *testing.T) {
	// Pure V1 segments (no manifests) must keep the original storage version.
	req := &workerpb.AnalyzeRequest{
		ClusterID:      "c1",
		CollectionID:   1,
		PartitionID:    2,
		FieldID:        3,
		FieldType:      schemapb.DataType_FloatVector,
		Dim:            4,
		StorageConfig:  &indexpb.StorageConfig{RootPath: "root"},
		StorageVersion: storage.StorageV1,
		SegmentStats: map[int64]*indexpb.SegmentStats{
			301: {ID: 301, NumRows: 50, LogIDs: []int64{4001, 4002}},
		},
	}

	info := buildAnalyzeInfo(req)

	// StorageVersion stays as V1 (0).
	assert.Equal(t, storage.StorageV1, info.GetStorageVersion())

	// InsertFiles must be populated.
	assert.Len(t, info.GetInsertFiles()[301].GetInsertFiles(), 2)

	// ManifestPaths must be empty.
	assert.Empty(t, info.GetManifestPaths())
}
