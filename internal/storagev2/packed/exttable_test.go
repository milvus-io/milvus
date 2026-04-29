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
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestExploreFiles_EmptyColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	// Empty columns should still work
	files, _, err := ExploreFilesReturnManifestPath([]string{}, "parquet", "/tmp", "/nonexistent", config, ExternalSpecContext{})

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

	files, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1", "col2"},
		"parquet",
		"/nonexistent/base",
		"/nonexistent/explore",
		config,
		ExternalSpecContext{},
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

	info, err := GetFileInfo("parquet", "/nonexistent/file.parquet", config, ExternalSpecContext{})

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

	info, err := GetFileInfo("parquet", tmpFile, config, ExternalSpecContext{})

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

func TestCreateSegmentManifestWithBasePath_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := CreateSegmentManifestWithBasePath(ctx, "/base", "parquet", []string{"col1"}, nil, nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestEnsureHTTPScheme(t *testing.T) {
	tests := []struct {
		name    string
		address string
		useSSL  bool
		want    string
	}{
		{
			name:    "no scheme, no SSL → prepend http://",
			address: "localhost:9000",
			useSSL:  false,
			want:    "http://localhost:9000",
		},
		{
			name:    "no scheme, with SSL → prepend https://",
			address: "localhost:9000",
			useSSL:  true,
			want:    "https://localhost:9000",
		},
		{
			name:    "has http:// scheme, no SSL → keep as-is",
			address: "http://localhost:9000",
			useSSL:  false,
			want:    "http://localhost:9000",
		},
		{
			name:    "has https:// scheme, no SSL → keep as-is",
			address: "https://s3.amazonaws.com",
			useSSL:  false,
			want:    "https://s3.amazonaws.com",
		},
		{
			name:    "has https:// scheme, with SSL → keep as-is",
			address: "https://s3.amazonaws.com",
			useSSL:  true,
			want:    "https://s3.amazonaws.com",
		},
		{
			name:    "IP address, no SSL → prepend http://",
			address: "10.0.0.1:9000",
			useSSL:  false,
			want:    "http://10.0.0.1:9000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureHTTPScheme(tt.address, tt.useSSL)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The cgo bridge (loon_properties_inject_external_spec) and Tier-1/2 endpoint
// derivation logic are covered by the C++ gtest suite in test_external_take.cpp.
// Go-side coverage is exercised indirectly through callers that invoke
// injectExternalSpecProperties (GetFileInfo, ExploreFilesReturnManifestPath,
// SampleExternalFieldSizes).

func TestExtfsPrefixForCollection(t *testing.T) {
	assert.Equal(t, "extfs.42.", ExtfsPrefixForCollection(42))
	assert.Equal(t, "extfs.0.", ExtfsPrefixForCollection(0))
}

// NOTE: the Go-side BuildExtfsOverrides / NormalizeExternalSource /
// effectiveSpecAddress helpers were collapsed into the C++ InjectExternalSpecProperties
// pipeline. Tier-1/2 endpoint derivation and AWS-form rewriting are covered by
// the C++ gtest suite in test_external_take.cpp. Go callers exercise the cgo
// bridge indirectly through the public FFI wrappers.

func TestFilterFileInfosByFormat(t *testing.T) {
	files := []FileInfo{
		{FilePath: "data/file1.parquet", NumRows: 100},
		{FilePath: "data/file2.parquet", NumRows: 200},
		{FilePath: "data/_SUCCESS", NumRows: 0},
		{FilePath: "data/metadata.json", NumRows: 0},
		{FilePath: "data/file3.csv", NumRows: 50},
		{FilePath: "data/FILE4.PARQUET", NumRows: 300},
	}

	// Parquet: keep .parquet only (case-insensitive)
	filtered, skipped := filterFileInfosByFormat(files, "parquet")
	assert.Equal(t, 3, len(filtered))
	assert.Equal(t, 3, skipped)
	assert.Equal(t, "data/file1.parquet", filtered[0].FilePath)
	assert.Equal(t, "data/FILE4.PARQUET", filtered[2].FilePath)

	// Vortex: keep .vortex only
	vortexFiles := []FileInfo{
		{FilePath: "a.vortex"},
		{FilePath: "b.parquet"},
		{FilePath: "c.vortex"},
	}
	filtered, skipped = filterFileInfosByFormat(vortexFiles, "vortex")
	assert.Equal(t, 2, len(filtered))
	assert.Equal(t, 1, skipped)

	// Lance-table: keep all (directory-based, no extension filtering)
	filtered, skipped = filterFileInfosByFormat(files, "lance-table")
	assert.Equal(t, len(files), len(filtered))
	assert.Equal(t, 0, skipped)

	// Unknown format: keep all
	filtered, skipped = filterFileInfosByFormat(files, "unknown")
	assert.Equal(t, len(files), len(filtered))
	assert.Equal(t, 0, skipped)

	// Empty list
	filtered, skipped = filterFileInfosByFormat(nil, "parquet")
	assert.Equal(t, 0, len(filtered))
	assert.Equal(t, 0, skipped)
}

// Regression for index-drift bug: DataCoord and DataNode both call
// NormalizeFileInfos on the same raw manifest and MUST produce the
// identical indexed view, regardless of input ordering. Without sort,
// arrow's GetFileInfo can return files in different orders on each
// process and the [fileIndexBegin, fileIndexEnd) slice picks different
// files on the two sides, leading to silent data loss or "Invalid
// parquet magic" failures on Spark `_SUCCESS`/`.crc` strays.
func TestNormalizeFileInfos_StableSortAndFilter(t *testing.T) {
	rawA := []FileInfo{
		{FilePath: "data/_SUCCESS"},
		{FilePath: "data/part-2.parquet"},
		{FilePath: "data/.crc"},
		{FilePath: "data/part-1.parquet"},
		{FilePath: "data/README.md"},
	}
	// Reordered "DataNode side" view (arrow GetFileInfo gives no order).
	rawB := []FileInfo{
		{FilePath: "data/part-1.parquet"},
		{FilePath: "data/README.md"},
		{FilePath: "data/_SUCCESS"},
		{FilePath: "data/part-2.parquet"},
		{FilePath: "data/.crc"},
	}

	gotA, skippedA := NormalizeFileInfos(rawA, "parquet")
	gotB, skippedB := NormalizeFileInfos(rawB, "parquet")

	assert.Equal(t, 2, len(gotA))
	assert.Equal(t, 3, skippedA)
	assert.Equal(t, gotA, gotB, "two views of same manifest must produce identical indexed slice")
	assert.Equal(t, "data/part-1.parquet", gotA[0].FilePath)
	assert.Equal(t, "data/part-2.parquet", gotA[1].FilePath)
	assert.Equal(t, skippedA, skippedB)
}

func TestMakePropertiesFromStorageConfig_ExtraKVsOverride(t *testing.T) {
	// Test that extraKVs can add per-collection extfs properties
	config := &indexpb.StorageConfig{
		StorageType: "minio",
		BucketName:  "original-bucket",
		Address:     "localhost:9000",
		RootPath:    "/data",
	}
	prefix := ExtfsPrefixForCollection(42)
	extra := map[string]string{
		prefix + "bucket_name": "external-bucket",
		prefix + "address":     "http://s3.amazonaws.com",
	}
	props, err := MakePropertiesFromStorageConfig(config, extra)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

func TestNormalizeExternalPathForStorage_UsesInjectedExtfsEndpoint(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	extfs := ExternalSpecContext{
		CollectionID: 42,
		Source:       "s3://liyiyang-test/all_types_v2/",
		Spec:         `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}

	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	require.NoError(t, injectExternalSpecProperties(props, extfs.CollectionID, extfs.Source, extfs.Spec))

	got, err := normalizeExternalPathForStorage(extfs.Source, props, extfs)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/liyiyang-test/all_types_v2/", got)

	got, err = normalizeExternalPathForStorage("s3://liyiyang-test/a/b/file.parquet?versionId=1", props, extfs)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/liyiyang-test/a/b/file.parquet?versionId=1", got)
}

func TestNormalizeExternalPathForStorage_EndpointFormUnchanged(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	source := "s3://s3.us-west-2.amazonaws.com/liyiyang-test/all_types_v2/"
	extfs := ExternalSpecContext{
		CollectionID: 42,
		Source:       source,
		Spec:         `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}

	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	require.NoError(t, injectExternalSpecProperties(props, extfs.CollectionID, extfs.Source, extfs.Spec))

	got, err := normalizeExternalPathForStorage(source, props, extfs)
	require.NoError(t, err)
	assert.Equal(t, source, got)
}

func TestNormalizeExternalPathForStorage_NoRewriteCases(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	prefix := ExtfsPrefixForCollection(42)
	props, err := MakePropertiesFromStorageConfig(config, map[string]string{
		prefix + "address":     "https://s3.us-west-2.amazonaws.com",
		prefix + "bucket_name": "liyiyang-test",
	})
	require.NoError(t, err)
	defer FreeProperties(props)

	path := "s3://liyiyang-test/all_types_v2/"

	got, err := normalizeExternalPathForStorage(path, props, ExternalSpecContext{CollectionID: 42})
	require.NoError(t, err)
	assert.Equal(t, path, got)

	got, err = normalizeExternalPathForStorage(
		path,
		nil,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, path, got)

	got, err = normalizeExternalPathForStorage(
		"relative/path",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "relative/path", got)

	got, err = normalizeExternalPathForStorage(
		"s3://other-bucket/all_types_v2/",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://other-bucket/all_types_v2/", got)
}

func TestNormalizeExternalPathForStorage_BareAddress(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	prefix := ExtfsPrefixForCollection(42)
	props, err := MakePropertiesFromStorageConfig(config, map[string]string{
		prefix + "address":     "s3.us-west-2.amazonaws.com",
		prefix + "bucket_name": "liyiyang-test",
	})
	require.NoError(t, err)
	defer FreeProperties(props)

	got, err := normalizeExternalPathForStorage(
		"s3://liyiyang-test/all_types_v2/",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/liyiyang-test/all_types_v2/", got)
}

// ==================== SampleExternalFieldSizes Tests ====================

func TestSampleExternalFieldSizes_NilStorageConfig(t *testing.T) {
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/tmp","ver":1}`, 100, 42,
		"s3://bucket/data/", `{"format":"parquet"}`,
		nil,
		nil,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storageConfig is required")
}

func TestSampleExternalFieldSizes_EmptyManifestPath(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	result, err := SampleExternalFieldSizes(
		"", 100, 42,
		"", "",
		nil,
		config,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest_path is empty")
}

func TestSampleExternalFieldSizes_InvalidManifestPath(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	// Valid JSON but manifest file doesn't exist
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/nonexistent/path","ver":1}`, 100, 42,
		"", "",
		nil,
		config,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestSampleExternalFieldSizes_WithSpec(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	// Will fail at manifest read (no real file), but validates that
	// properties construction with extfs injection via cgo doesn't panic.
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/nonexistent","ver":1}`, 100, 42,
		"s3://s3.us-west-2.amazonaws.com/ext-bucket/data/",
		`{"format":"parquet","extfs":{"region":"us-west-2","use_ssl":"true"}}`,
		nil,
		config,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
}

// ==================== ExploreFilesReturnManifestPath Tests ====================

func TestExploreFilesReturnManifestPath_InvalidDir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1"}, "parquet", "/nonexistent/base", "/nonexistent/explore", config, ExternalSpecContext{},
	)
	assert.Error(t, err)
}

func TestExploreFilesReturnManifestPath_EmptyColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, _, err := ExploreFilesReturnManifestPath(
		[]string{}, "parquet", "/nonexistent", "/nonexistent", config, ExternalSpecContext{},
	)
	assert.Error(t, err)
}

func TestExploreFilesReturnManifestPath_PropertiesError(t *testing.T) {
	_, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1"}, "parquet", "/base", "/explore", nil, ExternalSpecContext{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties")
}

// ==================== ReadFileInfosFromManifestPath Tests ====================

func TestReadFileInfosFromManifestPath_InvalidPath(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, err := ReadFileInfosFromManifestPath("/nonexistent/manifest.json", config)
	assert.Error(t, err)
}

func TestReadFileInfosFromManifestPath_NilConfig(t *testing.T) {
	_, err := ReadFileInfosFromManifestPath("/manifest.json", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties")
}

// ==================== GetFileInfo Additional Tests ====================

func TestGetFileInfo_PropertiesError(t *testing.T) {
	_, err := GetFileInfo("parquet", "/some/file.parquet", nil, ExternalSpecContext{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties")
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

// ==================== FetchFragmentsFromExternalSourceWithRange Tests ====================

func TestFetchFragmentsFromExternalSourceWithRange_HappyPath(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	// Mock ReadFileInfosFromManifestPath to return 3 files
	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 1000},
		{FilePath: "f2.parquet", NumRows: 2000},
		{FilePath: "f3.parquet", NumRows: 3000},
	}, nil).Build()
	defer mockRead.UnPatch()

	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", []string{"col1"}, "s3://endpoint/bucket/path", config,
		0, 3, "/manifest.json", ExternalFetchOptions{CollectionID: 1, RowLimit: 5000},
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, fragments)
}

func TestFetchFragmentsFromExternalSourceWithRange_EmptyManifest(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 10, "", ExternalFetchOptions{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest path is required")
}

func TestFetchFragmentsFromExternalSourceWithRange_ReadManifestFailed(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).
		Return(nil, fmt.Errorf("manifest read error")).Build()
	defer mockRead.UnPatch()

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 10, "/manifest.json", ExternalFetchOptions{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest")
}

func TestFetchFragmentsFromExternalSourceWithRange_IndexOutOfRange(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 100},
	}, nil).Build()
	defer mockRead.UnPatch()

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		5, 10, "/manifest.json", ExternalFetchOptions{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fileIndexBegin")
}

func TestFetchFragmentsFromExternalSourceWithRange_NeedsFileInfo(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	// File with NumRows=0 needs GetFileInfo
	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 0},
	}, nil).Build()
	defer mockRead.UnPatch()

	mockGetInfo := mockey.Mock(GetFileInfo).Return(&FileInfo{
		FilePath: "f1.parquet", NumRows: 500,
	}, nil).Build()
	defer mockGetInfo.UnPatch()

	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{CollectionID: 1, RowLimit: 1000},
	)
	assert.NoError(t, err)
	assert.Len(t, fragments, 1)
	assert.Equal(t, int64(500), fragments[0].RowCount)
}

func TestFetchFragmentsFromExternalSourceWithRange_GetFileInfoFailed(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 0},
	}, nil).Build()
	defer mockRead.UnPatch()

	mockGetInfo := mockey.Mock(GetFileInfo).
		Return(nil, fmt.Errorf("get file info error")).Build()
	defer mockGetInfo.UnPatch()

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{CollectionID: 1},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get file info")
}

func TestFetchFragmentsFromExternalSourceWithRange_FileIndexEndClamped(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 100},
		{FilePath: "f2.parquet", NumRows: 200},
	}, nil).Build()
	defer mockRead.UnPatch()

	// fileIndexEnd (99) > len(fileInfos) (2), should be clamped
	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 99, "/manifest.json", ExternalFetchOptions{CollectionID: 1, RowLimit: 1000},
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, fragments)
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

// ==================== rowLimitOrDefault Tests ====================

func TestRowLimitOrDefault(t *testing.T) {
	t.Run("zero falls back to default", func(t *testing.T) {
		o := ExternalFetchOptions{RowLimit: 0}
		assert.Equal(t, int64(DefaultFragmentRowLimit), o.rowLimitOrDefault())
	})
	t.Run("negative falls back to default", func(t *testing.T) {
		o := ExternalFetchOptions{RowLimit: -1}
		assert.Equal(t, int64(DefaultFragmentRowLimit), o.rowLimitOrDefault())
	})
	t.Run("positive used as-is", func(t *testing.T) {
		o := ExternalFetchOptions{RowLimit: 1234}
		assert.Equal(t, int64(1234), o.rowLimitOrDefault())
	})
}

// ==================== fetchRowCountsConcurrently Tests ====================

func TestFetchRowCountsConcurrently_AllKnownSkipsFFI(t *testing.T) {
	// All fileInfos already have NumRows > 0 → no GetFileInfo call.
	mockCalled := int32(0)
	m := mockey.Mock(GetFileInfo).To(func(format, path string, cfg *indexpb.StorageConfig, extfs ExternalSpecContext) (*FileInfo, error) {
		atomic.AddInt32(&mockCalled, 1)
		return &FileInfo{FilePath: path, NumRows: 99}, nil
	}).Build()
	defer m.UnPatch()

	fileInfos := []FileInfo{
		{FilePath: "a", NumRows: 10},
		{FilePath: "b", NumRows: 20},
		{FilePath: "c", NumRows: 30},
	}
	got, err := fetchRowCountsConcurrently(context.Background(), "parquet", fileInfos, &indexpb.StorageConfig{}, ExternalSpecContext{})
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 20, 30}, got)
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockCalled))
}

func TestFetchRowCountsConcurrently_MissingFilled(t *testing.T) {
	m := mockey.Mock(GetFileInfo).To(func(format, path string, cfg *indexpb.StorageConfig, extfs ExternalSpecContext) (*FileInfo, error) {
		return &FileInfo{FilePath: path, NumRows: 777}, nil
	}).Build()
	defer m.UnPatch()

	fileInfos := []FileInfo{
		{FilePath: "a", NumRows: 10},
		{FilePath: "b", NumRows: 0},
		{FilePath: "c", NumRows: -1},
	}
	got, err := fetchRowCountsConcurrently(context.Background(), "parquet", fileInfos, &indexpb.StorageConfig{}, ExternalSpecContext{})
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 777, 777}, got)
}

func TestFetchRowCountsConcurrently_WorkerErrorPropagated(t *testing.T) {
	m := mockey.Mock(GetFileInfo).Return(nil, fmt.Errorf("fake ffi failure")).Build()
	defer m.UnPatch()

	_, err := fetchRowCountsConcurrently(context.Background(), "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, ExternalSpecContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get file info")
}

func TestFetchRowCountsConcurrently_CtxPreCancelled(t *testing.T) {
	// Mock returns nil synchronously; ctx is canceled before launching workers.
	m := mockey.Mock(GetFileInfo).Return(&FileInfo{NumRows: 10}, nil).Build()
	defer m.UnPatch()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fetchRowCountsConcurrently(ctx, "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, ExternalSpecContext{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestFetchRowCountsConcurrently_CtxCancelledDuringRun verifies the
// post-AwaitAll ctx.Err() guard: workers ignore ctx and return success, but
// the ctx is canceled mid-run — the function must still return ctx.Err()
// rather than a half-filled rowCounts slice.
func TestFetchRowCountsConcurrently_CtxCancelledDuringRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mockey.Mock(GetFileInfo).To(func(_ string, _ string, _ *indexpb.StorageConfig, _ ExternalSpecContext) (*FileInfo, error) {
		cancel()
		return &FileInfo{NumRows: 10}, nil
	}).Build()
	defer m.UnPatch()

	_, err := fetchRowCountsConcurrently(ctx, "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, ExternalSpecContext{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
