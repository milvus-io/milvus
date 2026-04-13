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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func TestGetFileInfo_NonexistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	info, err := GetFileInfo("parquet", "/nonexistent/file.parquet", config, nil)

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

	info, err := GetFileInfo("parquet", tmpFile, config, nil)

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
			name:    "no scheme, with SSL → keep as-is",
			address: "localhost:9000",
			useSSL:  true,
			want:    "localhost:9000",
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

func TestBuildExtfsOverrides(t *testing.T) {
	baseConfig := &indexpb.StorageConfig{
		Address:    "localhost:9000",
		BucketName: "a-bucket",
		UseSSL:     false,
	}
	sslConfig := &indexpb.StorageConfig{
		Address:    "s3.amazonaws.com",
		BucketName: "a-bucket",
		UseSSL:     true,
	}

	tests := []struct {
		name           string
		externalSource string
		config         *indexpb.StorageConfig
		wantBucket     string
		wantAddress    string
	}{
		{
			name:           "relative path → baseline from storageConfig",
			externalSource: "data/files/",
			config:         baseConfig,
			wantBucket:     "a-bucket",
			wantAddress:    "http://localhost:9000",
		},
		{
			name:           "empty string → baseline from storageConfig",
			externalSource: "",
			config:         baseConfig,
			wantBucket:     "a-bucket",
			wantAddress:    "http://localhost:9000",
		},
		{
			name:           "s3 URI with empty host and bucket",
			externalSource: "s3:///external-bucket/some/prefix",
			config:         baseConfig,
			wantBucket:     "external-bucket",
			wantAddress:    "",
		},
		{
			name:           "s3 URI with host and bucket",
			externalSource: "s3://minio:9000/external-bucket/prefix",
			config:         baseConfig,
			wantBucket:     "external-bucket",
			wantAddress:    "http://minio:9000",
		},
		{
			name:           "s3 URI with host and bucket, SSL enabled",
			externalSource: "s3://s3.amazonaws.com/my-bucket/data",
			config:         sslConfig,
			wantBucket:     "my-bucket",
			wantAddress:    "s3.amazonaws.com",
		},
		{
			name:           "s3 URI with only bucket, no prefix",
			externalSource: "s3:///just-bucket",
			config:         baseConfig,
			wantBucket:     "just-bucket",
			wantAddress:    "",
		},
		{
			name:           "s3 URI with trailing slash",
			externalSource: "s3:///external-bucket/prefix/",
			config:         baseConfig,
			wantBucket:     "external-bucket",
			wantAddress:    "",
		},
		{
			name:           "gs URI scheme",
			externalSource: "gs:///gcs-bucket/data",
			config:         baseConfig,
			wantBucket:     "gcs-bucket",
			wantAddress:    "",
		},
		{
			name:           "host with http:// already in address (no SSL)",
			externalSource: "s3://http://already-has-scheme/bucket/key",
			config:         baseConfig,
			// url.Parse treats "http" as host for s3://http://...
			// but this is a degenerate case; just verify no panic
			wantBucket: "",
		},
	}
	prefix := "extfs.42."
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildExtfsOverrides(tt.externalSource, tt.config, prefix, nil)
			assert.NotNil(t, got)
			if tt.wantBucket != "" {
				assert.Equal(t, tt.wantBucket, got[prefix+"bucket_name"])
			}
			if tt.wantAddress != "" {
				assert.Equal(t, tt.wantAddress, got[prefix+"address"])
			}
		})
	}
}

func TestBuildExtfsOverrides_OverrideKeys(t *testing.T) {
	prefix := ExtfsPrefixForCollection(99)
	config := &indexpb.StorageConfig{UseSSL: false, BucketName: "original"}
	got := BuildExtfsOverrides("s3://host:9000/bucket/path", config, prefix, nil)
	require.NotNil(t, got)

	assert.Contains(t, got, prefix+"bucket_name")
	assert.Contains(t, got, prefix+"address")
	assert.Equal(t, "bucket", got[prefix+"bucket_name"])
	assert.Equal(t, "http://host:9000", got[prefix+"address"])
}

func TestBuildExtfsOverrides_SpecExtfsOverrides(t *testing.T) {
	prefix := ExtfsPrefixForCollection(100)
	config := &indexpb.StorageConfig{
		UseSSL:  false,
		UseIAM:  false,
		Region:  "us-east-1",
		Address: "localhost:9000",
	}
	specExtfs := map[string]string{
		prefix + "use_iam": "true",
		prefix + "region":  "us-west-2",
	}
	got := BuildExtfsOverrides("s3:///ext-bucket/data", config, prefix, specExtfs)
	require.NotNil(t, got)

	// Spec overrides win over storageConfig defaults
	assert.Equal(t, "true", got[prefix+"use_iam"])
	assert.Equal(t, "us-west-2", got[prefix+"region"])
	// URI-derived bucket
	assert.Equal(t, "ext-bucket", got[prefix+"bucket_name"])
}

func TestExtfsPrefixForCollection(t *testing.T) {
	assert.Equal(t, "extfs.42.", ExtfsPrefixForCollection(42))
	assert.Equal(t, "extfs.0.", ExtfsPrefixForCollection(0))
}

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

func TestCopyStorageConfigToExtfs(t *testing.T) {
	cfg := &indexpb.StorageConfig{
		StorageType:       "minio",
		BucketName:        "test-bucket",
		Address:           "localhost:9000",
		RootPath:          "/data",
		AccessKeyID:       "ak123",
		SecretAccessKey:   "sk456",
		CloudProvider:     "aws",
		IAMEndpoint:       "https://sts.amazonaws.com",
		Region:            "us-east-1",
		SslCACert:         "cert-pem",
		GcpCredentialJSON: "",
		UseSSL:            true,
		UseIAM:            false,
		UseVirtualHost:    true,
	}

	prefix := "extfs.42."
	overrides := make(map[string]string)
	copyStorageConfigToExtfs(overrides, prefix, cfg)

	// String fields
	assert.Equal(t, "minio", overrides[prefix+"storage_type"])
	assert.Equal(t, "test-bucket", overrides[prefix+"bucket_name"])
	assert.Equal(t, "localhost:9000", overrides[prefix+"address"]) // useSSL=true, no http:// prepended
	assert.Equal(t, "/data", overrides[prefix+"root_path"])
	assert.Equal(t, "ak123", overrides[prefix+"access_key_id"])
	assert.Equal(t, "sk456", overrides[prefix+"access_key_value"])
	assert.Equal(t, "aws", overrides[prefix+"cloud_provider"])
	assert.Equal(t, "https://sts.amazonaws.com", overrides[prefix+"iam_endpoint"])
	assert.Equal(t, "us-east-1", overrides[prefix+"region"])
	assert.Equal(t, "cert-pem", overrides[prefix+"ssl_ca_cert"])

	// Boolean fields
	assert.Equal(t, "true", overrides[prefix+"use_ssl"])
	assert.Equal(t, "false", overrides[prefix+"use_iam"])
	assert.Equal(t, "true", overrides[prefix+"use_virtual_host"])

	// Empty GcpCredentialJSON should not be in overrides
	_, hasGcp := overrides[prefix+"gcp_credential_json"]
	assert.False(t, hasGcp)
}

func TestCopyStorageConfigToExtfs_NoSSL(t *testing.T) {
	cfg := &indexpb.StorageConfig{
		Address: "localhost:9000",
		UseSSL:  false,
	}
	overrides := make(map[string]string)
	copyStorageConfigToExtfs(overrides, "extfs.2.", cfg)

	// UseSSL=false → ensureHTTPScheme prepends "http://"
	assert.Equal(t, "http://localhost:9000", overrides["extfs.2.address"])
	assert.Equal(t, "false", overrides["extfs.2.use_ssl"])
}

func TestCopyStorageConfigToExtfs_EmptyConfig(t *testing.T) {
	cfg := &indexpb.StorageConfig{}
	overrides := make(map[string]string)
	copyStorageConfigToExtfs(overrides, "extfs.1.", cfg)

	// Boolean fields always present (default false)
	assert.Equal(t, "false", overrides["extfs.1.use_ssl"])
	assert.Equal(t, "false", overrides["extfs.1.use_iam"])
	assert.Equal(t, "false", overrides["extfs.1.use_virtual_host"])

	// Empty string fields should not be present
	_, hasBucket := overrides["extfs.1.bucket_name"]
	assert.False(t, hasBucket)
	_, hasAddr := overrides["extfs.1.address"]
	assert.False(t, hasAddr)
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

// ==================== SampleExternalFieldSizes Tests ====================

func TestSampleExternalFieldSizes_NilStorageConfig(t *testing.T) {
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/tmp","ver":1}`, 100, 42,
		"s3://bucket/data/", `{"format":"parquet"}`,
		nil, nil,
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
		config, nil,
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
		config, nil,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestSampleExternalFieldSizes_WithSpecExtfs(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	specExtfs := map[string]string{
		"extfs.42.region":  "us-west-2",
		"extfs.42.use_ssl": "true",
	}
	// Will fail at manifest read (no real file), but validates that
	// properties construction with specExtfs doesn't panic
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/nonexistent","ver":1}`, 100, 42,
		"s3://s3.us-west-2.amazonaws.com/ext-bucket/data/", `{"format":"parquet"}`,
		config, specExtfs,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
}

// ==================== ExploreFilesReturnManifestPath Tests ====================

func TestExploreFilesReturnManifestPath_InvalidDir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1"}, "parquet", "/nonexistent/base", "/nonexistent/explore", config, nil,
	)
	assert.Error(t, err)
}

func TestExploreFilesReturnManifestPath_EmptyColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, _, err := ExploreFilesReturnManifestPath(
		[]string{}, "parquet", "/nonexistent", "/nonexistent", config, nil,
	)
	assert.Error(t, err)
}

func TestExploreFilesReturnManifestPath_PropertiesError(t *testing.T) {
	_, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1"}, "parquet", "/base", "/explore", nil, nil,
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
	_, err := GetFileInfo("parquet", "/some/file.parquet", nil, nil)
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
		ctx, "parquet", "s3:///bucket/path", config,
		0, 3, "/manifest.json", ExternalFetchOptions{CollectionID: 1},
		5000, // rowLimit
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, fragments)
}

func TestFetchFragmentsFromExternalSourceWithRange_EmptyManifest(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", "", config,
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
		ctx, "parquet", "", config,
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
		ctx, "parquet", "", config,
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
		ctx, "parquet", "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{CollectionID: 1}, 1000,
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
		ctx, "parquet", "", config,
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
		ctx, "parquet", "", config,
		0, 99, "/manifest.json", ExternalFetchOptions{CollectionID: 1}, 1000,
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, fragments)
}

func TestFetchFragmentsFromExternalSourceWithRange_FormatProperties(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 100},
	}, nil).Build()
	defer mockRead.UnPatch()

	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{
			CollectionID:     1,
			FormatProperties: map[string]string{"iceberg.snapshot_id": "42"},
		}, 1000,
	)
	assert.NoError(t, err)
	assert.Len(t, fragments, 1)
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
