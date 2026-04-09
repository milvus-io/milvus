// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build dynamic

package packed

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func TestExploreFiles_EmptyColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	// Empty columns should still work
	files, err := ExploreFiles([]string{}, "parquet", "/tmp", "/nonexistent", config)

	// Expect error due to nonexistent directory
	assert.Error(t, err)
	assert.Nil(t, files)
}

func TestExploreFiles_InvalidDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	files, err := ExploreFiles(
		[]string{"col1", "col2"},
		"parquet",
		"/nonexistent/base",
		"/nonexistent/explore",
		config,
	)

	assert.Error(t, err)
	assert.Nil(t, files)
}

func TestGetFileInfo_NonexistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	info, err := GetFileInfo("parquet", "/nonexistent/file.parquet", config)

	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestGetFileInfo_InvalidFormat(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	// Create a temp file
	tmpFile := filepath.Join(tmpDir, "test.txt")
	err := os.WriteFile(tmpFile, []byte("not a parquet file"), 0o600)
	require.NoError(t, err)

	info, err := GetFileInfo("parquet", tmpFile, config)

	// Should fail because it's not a valid parquet file
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestCreateManifestForSegment_EmptyFragments(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	manifestPath, err := CreateManifestForSegment(
		"/test",
		[]string{"col1"},
		"parquet",
		[]Fragment{},
		config,
	)

	assert.Error(t, err)
	assert.Empty(t, manifestPath)
	assert.Contains(t, err.Error(), "fragments cannot be empty")
}

func TestCreateManifestForSegment_InvalidBasePath(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	fragments := []Fragment{
		{
			FragmentID: 0,
			FilePath:   "/data/file.parquet",
			StartRow:   0,
			EndRow:     1000,
			RowCount:   1000,
		},
	}

	// NOTE: C FFI does not validate base path at creation time (paths are virtual in object storage),
	// so this may succeed. We only verify no panic occurs.
	_, _ = CreateManifestForSegment(
		"/test/path",
		[]string{"col1"},
		"parquet",
		fragments,
		config,
	)
}

func TestFileInfo_Struct(t *testing.T) {
	info := FileInfo{
		FilePath: "/data/test.parquet",
		NumRows:  1000000,
	}

	assert.Equal(t, "/data/test.parquet", info.FilePath)
	assert.Equal(t, int64(1000000), info.NumRows)
}

func TestFragment_Struct(t *testing.T) {
	fragment := Fragment{
		FragmentID: 1,
		FilePath:   "/data/large.parquet",
		StartRow:   1000000,
		EndRow:     2000000,
		RowCount:   1000000,
	}

	assert.Equal(t, int64(1), fragment.FragmentID)
	assert.Equal(t, "/data/large.parquet", fragment.FilePath)
	assert.Equal(t, int64(1000000), fragment.StartRow)
	assert.Equal(t, int64(2000000), fragment.EndRow)
	assert.Equal(t, int64(1000000), fragment.RowCount)
}

func TestFragment_RowCountConsistency(t *testing.T) {
	// Test that RowCount should equal EndRow - StartRow
	testCases := []struct {
		startRow int64
		endRow   int64
	}{
		{0, 1000},
		{1000, 2000},
		{0, 1000000},
		{5000000, 10000000},
	}

	for _, tc := range testCases {
		fragment := Fragment{
			FragmentID: 0,
			FilePath:   "/data/test.parquet",
			StartRow:   tc.startRow,
			EndRow:     tc.endRow,
			RowCount:   tc.endRow - tc.startRow,
		}

		assert.Equal(t, fragment.EndRow-fragment.StartRow, fragment.RowCount)
	}
}

func TestNewFragmentIDGenerator(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	assert.Equal(t, int64(0), gen())
	assert.Equal(t, int64(1), gen())
	assert.Equal(t, int64(2), gen())

	gen2 := NewFragmentIDGenerator(100)
	assert.Equal(t, int64(100), gen2())
	assert.Equal(t, int64(101), gen2())
}

func TestSplitFileToFragments_SmallFile(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	fragments := SplitFileToFragments("/data/small.parquet", 500, 1000, gen)

	assert.Len(t, fragments, 1)
	assert.Equal(t, int64(0), fragments[0].FragmentID)
	assert.Equal(t, "/data/small.parquet", fragments[0].FilePath)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(500), fragments[0].EndRow)
	assert.Equal(t, int64(500), fragments[0].RowCount)
}

func TestSplitFileToFragments_ExactLimit(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	fragments := SplitFileToFragments("/data/exact.parquet", 1000, 1000, gen)

	assert.Len(t, fragments, 1)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(1000), fragments[0].EndRow)
}

func TestSplitFileToFragments_LargeFile(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	fragments := SplitFileToFragments("/data/large.parquet", 2500, 1000, gen)

	assert.Len(t, fragments, 3)
	// Fragment 0: [0, 1000)
	assert.Equal(t, int64(0), fragments[0].FragmentID)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(1000), fragments[0].EndRow)
	assert.Equal(t, int64(1000), fragments[0].RowCount)
	// Fragment 1: [1000, 2000)
	assert.Equal(t, int64(1), fragments[1].FragmentID)
	assert.Equal(t, int64(1000), fragments[1].StartRow)
	assert.Equal(t, int64(2000), fragments[1].EndRow)
	// Fragment 2: [2000, 2500) — partial last fragment
	assert.Equal(t, int64(2), fragments[2].FragmentID)
	assert.Equal(t, int64(2000), fragments[2].StartRow)
	assert.Equal(t, int64(2500), fragments[2].EndRow)
	assert.Equal(t, int64(500), fragments[2].RowCount)
}

func TestSplitFileToFragments_FragmentIDContinuity(t *testing.T) {
	gen := NewFragmentIDGenerator(10)
	fragments := SplitFileToFragments("/data/f.parquet", 3000, 1000, gen)

	assert.Len(t, fragments, 3)
	assert.Equal(t, int64(10), fragments[0].FragmentID)
	assert.Equal(t, int64(11), fragments[1].FragmentID)
	assert.Equal(t, int64(12), fragments[2].FragmentID)
}

func TestGetColumnNamesFromSchema_Nil(t *testing.T) {
	assert.Nil(t, GetColumnNamesFromSchema(nil))
}

func TestGetColumnNamesFromSchema_NoExternalField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id"},
			{Name: "vector"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"id", "vector"}, columns)
}

func TestGetColumnNamesFromSchema_WithExternalField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "vector"},
			{Name: "text", ExternalField: "raw_text"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"external_id", "vector", "raw_text"}, columns)
}

func TestGetColumnNamesFromSchema_ExternalCollectionSkipsSystemFields(t *testing.T) {
	// External collection: ExternalSource is set.
	// Fields without ExternalField (like __virtual_pk__) should be skipped.
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/data",
		Fields: []*schemapb.FieldSchema{
			{Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "id_field", ExternalField: "id"},
			{Name: "vec_field", ExternalField: "embedding"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	// Only external field mappings should be returned, __virtual_pk__ is skipped
	assert.Equal(t, []string{"id", "embedding"}, columns)
}

func TestGetColumnNamesFromSchema_ExternalCollectionAllFieldsMapped(t *testing.T) {
	// All user fields have ExternalField mappings
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/data",
		Fields: []*schemapb.FieldSchema{
			{Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "a", ExternalField: "col_a"},
			{Name: "b", ExternalField: "col_b"},
			{Name: "c", ExternalField: "col_c"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"col_a", "col_b", "col_c"}, columns)
}

func TestGetColumnNamesFromSchema_MixedExternalAndNonExternal(t *testing.T) {
	// Non-external collection with some fields having ExternalField set
	// (edge case: ExternalSource not set, but some fields have ExternalField)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64},
			{Name: "ext_field", ExternalField: "ext_col"},
			{Name: "normal_field"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	// Non-external collection: all fields included, external field uses mapping name
	assert.Equal(t, []string{"pk", "ext_col", "normal_field"}, columns)
}

func TestGetColumnNamesFromSchema_EmptyFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Nil(t, columns)
}

func TestBuildCurrentSegmentFragments_NoManifest(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{ID: 1, NumOfRows: 1000},
		{ID: 2, NumOfRows: 2000},
	}
	result, err := BuildCurrentSegmentFragments(segments, nil)
	assert.NoError(t, err)
	assert.Len(t, result, 2)

	// Check virtual fragment for segment 1
	assert.Len(t, result[1], 1)
	assert.Equal(t, int64(1), result[1][0].FragmentID)
	assert.Equal(t, "", result[1][0].FilePath)
	assert.Equal(t, int64(0), result[1][0].StartRow)
	assert.Equal(t, int64(1000), result[1][0].EndRow)
	assert.Equal(t, int64(1000), result[1][0].RowCount)

	// Check virtual fragment for segment 2
	assert.Len(t, result[2], 1)
	assert.Equal(t, int64(2000), result[2][0].RowCount)
}

func TestBuildCurrentSegmentFragments_EmptySegments(t *testing.T) {
	result, err := BuildCurrentSegmentFragments(nil, nil)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestBuildCurrentSegmentFragments_ManifestPathWithNilConfig(t *testing.T) {
	// When storageConfig is nil, manifest path should be skipped (use virtual fragment)
	segments := []*datapb.SegmentInfo{
		{ID: 1, NumOfRows: 500, ManifestPath: "some/path"},
	}
	result, err := BuildCurrentSegmentFragments(segments, nil)
	assert.NoError(t, err)
	assert.Len(t, result[1], 1)
	assert.Equal(t, int64(500), result[1][0].RowCount)
}

func TestMarshalUnmarshalManifestPath(t *testing.T) {
	original := "/base/path/to/manifest"
	version := int64(42)

	marshaled := MarshalManifestPath(original, version)
	assert.NotEmpty(t, marshaled)

	basePath, ver, err := UnmarshalManifestPath(marshaled)
	assert.NoError(t, err)
	assert.Equal(t, original, basePath)
	assert.Equal(t, version, ver)
}

func TestUnmarshalManifestPath_InvalidJSON(t *testing.T) {
	_, _, err := UnmarshalManifestPath("not valid json")
	assert.Error(t, err)
}

func TestUnmarshalManifestPath_EmptyString(t *testing.T) {
	_, _, err := UnmarshalManifestPath("")
	assert.Error(t, err)
}

func TestMarshalManifestPath_EmptyBasePath(t *testing.T) {
	marshaled := MarshalManifestPath("", 0)
	basePath, ver, err := UnmarshalManifestPath(marshaled)
	assert.NoError(t, err)
	assert.Equal(t, "", basePath)
	assert.Equal(t, int64(0), ver)
}

func TestCompareManifestPath(t *testing.T) {
	base := "/base/path/to/segment"

	t.Run("equal paths", func(t *testing.T) {
		a := MarshalManifestPath(base, 5)
		cmp, err := CompareManifestPath(a, a)
		assert.NoError(t, err)
		assert.Equal(t, 0, cmp)
	})

	t.Run("both empty", func(t *testing.T) {
		cmp, err := CompareManifestPath("", "")
		assert.NoError(t, err)
		assert.Equal(t, 0, cmp)
	})

	t.Run("a older than b", func(t *testing.T) {
		a := MarshalManifestPath(base, 1)
		b := MarshalManifestPath(base, 5)
		cmp, err := CompareManifestPath(a, b)
		assert.NoError(t, err)
		assert.Equal(t, -1, cmp)
	})

	t.Run("a newer than b", func(t *testing.T) {
		a := MarshalManifestPath(base, 10)
		b := MarshalManifestPath(base, 3)
		cmp, err := CompareManifestPath(a, b)
		assert.NoError(t, err)
		assert.Equal(t, 1, cmp)
	})

	t.Run("different base paths returns error", func(t *testing.T) {
		a := MarshalManifestPath("/path/a", 1)
		b := MarshalManifestPath("/path/b", 1)
		_, err := CompareManifestPath(a, b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "different base paths")
	})

	t.Run("invalid json a returns error", func(t *testing.T) {
		b := MarshalManifestPath(base, 1)
		_, err := CompareManifestPath("not-json", b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse manifest path")
	})

	t.Run("invalid json b returns error", func(t *testing.T) {
		a := MarshalManifestPath(base, 1)
		_, err := CompareManifestPath(a, "not-json")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse manifest path")
	})

	t.Run("negative version", func(t *testing.T) {
		a := MarshalManifestPath(base, -1)
		b := MarshalManifestPath(base, 0)
		cmp, err := CompareManifestPath(a, b)
		assert.NoError(t, err)
		assert.Equal(t, -1, cmp)
	})
}

func TestCreateSegmentManifest_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := CreateSegmentManifest(ctx, 1, 1, "parquet", []string{"col1"}, nil, nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestCreateSegmentManifestWithBasePath_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := CreateSegmentManifestWithBasePath(ctx, "/base", "parquet", []string{"col1"}, nil, nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestMakePropertiesFromStorageConfig_NilConfig(t *testing.T) {
	_, err := MakePropertiesFromStorageConfig(nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storageConfig is required")
}

func TestMakePropertiesFromStorageConfig_LocalType(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}
	props, err := MakePropertiesFromStorageConfig(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

func TestMakePropertiesFromStorageConfig_AllFields(t *testing.T) {
	config := &indexpb.StorageConfig{
		Address:           "localhost:9000",
		BucketName:        "test-bucket",
		AccessKeyID:       "access-key",
		SecretAccessKey:   "secret-key",
		RootPath:          "/data",
		StorageType:       "minio",
		CloudProvider:     "aws",
		IAMEndpoint:       "http://iam.example.com",
		Region:            "us-east-1",
		SslCACert:         "cert-content",
		GcpCredentialJSON: `{"type":"service_account"}`,
		UseSSL:            true,
		UseIAM:            true,
		UseVirtualHost:    true,
		RequestTimeoutMs:  5000,
	}
	props, err := MakePropertiesFromStorageConfig(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

func TestMakePropertiesFromStorageConfig_WithExtraKVs(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}
	extra := map[string]string{
		"writer.policy": "split",
		"custom.key":    "custom.value",
	}
	props, err := MakePropertiesFromStorageConfig(config, extra)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}
