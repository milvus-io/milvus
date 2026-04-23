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
	files, _, err := ExploreFilesReturnManifestPath([]string{}, "parquet", "/tmp", "/nonexistent", config, nil)

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
		nil,
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

// TestBuildExtfsOverrides_ZeroInit verifies the Layer-0 invariant: every
// bool-valued extfs field is present with "false" as the zero value. String
// fields are NOT zero-initialized (milvus-storage rejects empty values on
// enum-constrained properties like cloud_provider); missing keys are treated
// as "not set" by loon, which is the intended semantics.
func TestBuildExtfsOverrides_ZeroInit(t *testing.T) {
	prefix := ExtfsPrefixForCollection(1)
	got := BuildExtfsOverrides("s3://minio:9000/bkt/key", nil, prefix)
	require.NotNil(t, got)

	// Bool fields — always zero-initialized to "false" and may be overridden.
	boolFields := []string{"use_ssl", "use_iam", "use_virtual_host"}
	for _, f := range boolFields {
		v, ok := got[prefix+f]
		assert.True(t, ok, "bool field %q must be present", f)
		assert.Contains(t, []string{"true", "false"}, v, "bool field %q must be true/false", f)
	}

	// String fields covered by Layer-1 URI derivation must be present with
	// non-empty values (storage_type, use_ssl handled above, bucket_name,
	// address). Other string fields (credentials etc.) are absent unless
	// spec provides them.
	assert.Equal(t, "remote", got[prefix+"storage_type"])
	assert.Equal(t, "bkt", got[prefix+"bucket_name"])
	assert.NotEmpty(t, got[prefix+"address"])

	// Credential fields absent because spec is nil.
	for _, f := range []string{"access_key_id", "access_key_value", "role_arn"} {
		_, ok := got[prefix+f]
		assert.False(t, ok, "credential field %q must NOT be present when spec is nil", f)
	}

	// All bool fields and URI-derived string fields are the only allowed keys.
	allowed := map[string]bool{}
	for _, f := range boolFields {
		allowed[prefix+f] = true
	}
	for _, f := range []string{"storage_type", "bucket_name", "address"} {
		allowed[prefix+f] = true
	}
	for k := range got {
		assert.True(t, allowed[k], "unexpected key %q in overrides", k)
	}
}

// TestBuildExtfsOverrides_NoBaselineInheritance is the explicit regression
// guard for the `use_iam` leak bug. If the function ever regresses to
// inheriting from storageConfig (fs.*), this test fails. The invariant:
// the output depends ONLY on (externalSource, specExtfs) — no third argument.
func TestBuildExtfsOverrides_NoBaselineInheritance(t *testing.T) {
	prefix := ExtfsPrefixForCollection(2)
	// Run twice with identical source+spec; output must match byte-for-byte.
	a := BuildExtfsOverrides("s3://host/bucket/key", nil, prefix)
	b := BuildExtfsOverrides("s3://host/bucket/key", nil, prefix)
	assert.Equal(t, a, b)

	// Specifically: with no spec credentials, use_iam MUST be "false" —
	// there is no path that can set it to "true" without an explicit spec
	// override. This kills the baseline-MinioCfg-useIAM=true leak.
	assert.Equal(t, "false", a[prefix+"use_iam"])
	assert.Equal(t, "", a[prefix+"access_key_id"])
	assert.Equal(t, "", a[prefix+"access_key_value"])
}

// TestBuildExtfsOverrides_URIDerive_MilvusForm: URI host becomes address,
// path[0] becomes bucket, scheme drives storage_type and use_ssl.
// Note: ensureHTTPScheme is symmetric — it prepends https:// when useSSL=true
// and http:// when useSSL=false, so the address is always self-describing
// and the address scheme / use_ssl flag can never disagree.
func TestBuildExtfsOverrides_URIDerive_MilvusForm(t *testing.T) {
	prefix := ExtfsPrefixForCollection(3)
	got := BuildExtfsOverrides("s3://minio:9000/external-bucket/prefix", nil, prefix)
	assert.Equal(t, "external-bucket", got[prefix+"bucket_name"])
	assert.Equal(t, "https://minio:9000", got[prefix+"address"])
	assert.Equal(t, "remote", got[prefix+"storage_type"])
	assert.Equal(t, "true", got[prefix+"use_ssl"])
}

// TestBuildExtfsOverrides_URIDerive_MinioSchemePlainHTTP: the `minio://`
// scheme defaults to plaintext HTTP because self-hosted MinIO is commonly
// deployed without TLS.
func TestBuildExtfsOverrides_URIDerive_MinioSchemePlainHTTP(t *testing.T) {
	prefix := ExtfsPrefixForCollection(4)
	got := BuildExtfsOverrides("minio://m:9000/bucket/key", nil, prefix)
	assert.Equal(t, "false", got[prefix+"use_ssl"])
	assert.Equal(t, "http://m:9000", got[prefix+"address"])
}

// TestBuildExtfsOverrides_AWSForm_ExplicitAddress: spec.extfs.address
// activates AWS-form interpretation — URI host becomes bucket, path becomes
// key, endpoint comes from spec.
func TestBuildExtfsOverrides_AWSForm_ExplicitAddress(t *testing.T) {
	prefix := ExtfsPrefixForCollection(5)
	spec := map[string]string{
		prefix + "address":          "https://s3.us-west-2.amazonaws.com",
		prefix + "access_key_id":    "AK",
		prefix + "access_key_value": "SK",
	}
	got := BuildExtfsOverrides("s3://user-bucket/data/prefix", spec, prefix)
	assert.Equal(t, "user-bucket", got[prefix+"bucket_name"])
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com", got[prefix+"address"])
	assert.Equal(t, "AK", got[prefix+"access_key_id"])
	assert.Equal(t, "SK", got[prefix+"access_key_value"])
}

// TestBuildExtfsOverrides_AWSForm_Tier2Derived: cloud_provider + region
// derive endpoint via DeriveEndpoint when spec.extfs.address is absent.
func TestBuildExtfsOverrides_AWSForm_Tier2Derived(t *testing.T) {
	prefix := ExtfsPrefixForCollection(6)
	spec := map[string]string{
		prefix + "cloud_provider": "aws",
		prefix + "region":         "us-west-2",
	}
	got := BuildExtfsOverrides("s3://my-bucket/data", spec, prefix)
	assert.Equal(t, "my-bucket", got[prefix+"bucket_name"])
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com", got[prefix+"address"])
}

// TestBuildExtfsOverrides_AWSForm_EqualityGuard: URI host matches the
// derived endpoint string → user wrote Milvus-form redundantly carrying
// provider+region → treat as Milvus form (path[0]=bucket, host=endpoint).
func TestBuildExtfsOverrides_AWSForm_EqualityGuard(t *testing.T) {
	prefix := ExtfsPrefixForCollection(7)
	spec := map[string]string{
		prefix + "cloud_provider": "aws",
		prefix + "region":         "us-west-2",
	}
	got := BuildExtfsOverrides(
		"s3://s3.us-west-2.amazonaws.com/real-bucket/data", spec, prefix)
	assert.Equal(t, "real-bucket", got[prefix+"bucket_name"])
	// Equality-guard branch sends URI host through ensureHTTPScheme(..., useSSL=true);
	// the symmetric form prepends "https://" so the address is always
	// self-describing (address scheme and use_ssl never disagree).
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com", got[prefix+"address"])
}

// TestBuildExtfsOverrides_SpecOverride_Wins: spec values override Layer-1
// derivation. E.g., spec.extfs.bucket_name replaces URI-derived bucket.
func TestBuildExtfsOverrides_SpecOverride_Wins(t *testing.T) {
	prefix := ExtfsPrefixForCollection(8)
	spec := map[string]string{
		prefix + "address":     "https://s3.amazonaws.com",
		prefix + "bucket_name": "explicit-bucket",
		prefix + "use_iam":     "true",
		prefix + "region":      "us-east-1",
	}
	got := BuildExtfsOverrides("s3://from-uri/data", spec, prefix)
	assert.Equal(t, "explicit-bucket", got[prefix+"bucket_name"])
	assert.Equal(t, "true", got[prefix+"use_iam"])
	assert.Equal(t, "us-east-1", got[prefix+"region"])
}

// TestBuildExtfsOverrides_SpecEmpty_DoesNotClobber: present-but-empty
// values in specExtfs must NOT erase Layer-1 derivations. A user who
// serializes an absent field as "" should not accidentally wipe the
// URI-derived address.
func TestBuildExtfsOverrides_SpecEmpty_DoesNotClobber(t *testing.T) {
	prefix := ExtfsPrefixForCollection(9)
	spec := map[string]string{
		prefix + "address":        "", // empty — must be ignored
		prefix + "cloud_provider": "aws",
		prefix + "region":         "us-west-2",
	}
	got := BuildExtfsOverrides("s3://my-bucket/data", spec, prefix)
	assert.Equal(t, "my-bucket", got[prefix+"bucket_name"])
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com", got[prefix+"address"])
}

// TestBuildExtfsOverrides_InvalidSource_Panics: an external_source that
// slipped past ValidateExternalSource (missing scheme/host) is a caller-side
// invariant violation. BuildExtfsOverrides panics so the failure is adjacent
// to the bad input rather than surfacing as a 403 deep inside the storage
// layer. Mirrors the C++ AssertInfo on the same invariant.
func TestBuildExtfsOverrides_InvalidSource_Panics(t *testing.T) {
	prefix := ExtfsPrefixForCollection(10)
	assert.Panics(t, func() {
		BuildExtfsOverrides("not-a-valid-uri", nil, prefix)
	})
	assert.Panics(t, func() {
		BuildExtfsOverrides("s3:///bucket/key", nil, prefix)
	})
	assert.Panics(t, func() {
		BuildExtfsOverrides("", nil, prefix)
	})
}

func TestExtfsPrefixForCollection(t *testing.T) {
	assert.Equal(t, "extfs.42.", ExtfsPrefixForCollection(42))
	assert.Equal(t, "extfs.0.", ExtfsPrefixForCollection(0))
}

func TestNormalizeExternalSource(t *testing.T) {
	prefix := ExtfsPrefixForCollection(1)
	specWithAddr := map[string]string{
		prefix + "address": "https://s3.us-west-2.amazonaws.com",
	}
	specWithBareAddr := map[string]string{
		prefix + "address": "minio:9000",
	}
	tests := []struct {
		name       string
		source     string
		specExtfs  map[string]string
		wantResult string
	}{
		{
			name:       "no spec address keeps source unchanged (Milvus form)",
			source:     "s3://endpoint/bucket/key",
			specExtfs:  nil,
			wantResult: "s3://endpoint/bucket/key",
		},
		{
			name:       "relative path is returned unchanged even with spec address",
			source:     "path/to/data",
			specExtfs:  specWithAddr,
			wantResult: "path/to/data",
		},
		{
			name:       "empty-host URI returned unchanged (already same-endpoint form)",
			source:     "s3:///bucket/key",
			specExtfs:  specWithAddr,
			wantResult: "s3:///bucket/key",
		},
		{
			name:       "AWS-style URI rewritten to Milvus form using spec address (stripped of scheme)",
			source:     "s3://my-bucket/data/v1",
			specExtfs:  specWithAddr,
			wantResult: "s3://s3.us-west-2.amazonaws.com/my-bucket/data/v1",
		},
		{
			name:       "AWS-style URI with bare host:port spec address",
			source:     "s3://my-bucket/data",
			specExtfs:  specWithBareAddr,
			wantResult: "s3://minio:9000/my-bucket/data",
		},
		{
			name:       "AWS-style URI with only bucket (empty path)",
			source:     "s3://just-bucket",
			specExtfs:  specWithAddr,
			wantResult: "s3://s3.us-west-2.amazonaws.com/just-bucket",
		},
		{
			name:       "AWS-style URI with trailing slash",
			source:     "s3://my-bucket/",
			specExtfs:  specWithAddr,
			wantResult: "s3://s3.us-west-2.amazonaws.com/my-bucket/",
		},
		{
			name:       "invalid URL returned unchanged",
			source:     "://bad",
			specExtfs:  specWithAddr,
			wantResult: "://bad",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantResult, NormalizeExternalSource(tt.source, tt.specExtfs, prefix))
		})
	}
}

func TestStripURLScheme(t *testing.T) {
	assert.Equal(t, "host:9000", stripURLScheme("http://host:9000"))
	assert.Equal(t, "s3.amazonaws.com", stripURLScheme("https://s3.amazonaws.com"))
	assert.Equal(t, "minio:9000", stripURLScheme("minio:9000"))
	assert.Equal(t, "", stripURLScheme(""))
	// Non-http schemes must be preserved — stripping them would silently
	// rewrite a user-supplied hostname.
	assert.Equal(t, "s3://x", stripURLScheme("s3://x"))
	assert.Equal(t, "ftp://host", stripURLScheme("ftp://host"))
}

func TestIsSafeURIHost(t *testing.T) {
	assert.True(t, isSafeURIHost("host"))
	assert.True(t, isSafeURIHost("host:9000"))
	assert.True(t, isSafeURIHost("bucket-name.s3.amazonaws.com"))
	assert.False(t, isSafeURIHost(""))
	assert.False(t, isSafeURIHost("user@host"))
	assert.False(t, isSafeURIHost("[::1]"))
	assert.False(t, isSafeURIHost("host with space"))
	assert.False(t, isSafeURIHost("host/path"))
	assert.False(t, isSafeURIHost("host?query"))
	assert.False(t, isSafeURIHost("host#frag"))
}

func TestNormalizeExternalSource_RejectsUnsafeHost(t *testing.T) {
	prefix := ExtfsPrefixForCollection(11)
	spec := map[string]string{prefix + "address": "https://s3.amazonaws.com"}
	// IPv6-bracket form as host is not a valid bucket name and must not be
	// reassembled into a normalized URI — return the source unchanged so the
	// caller can surface a clear error downstream.
	src := "s3://[::1]/key"
	assert.Equal(t, src, NormalizeExternalSource(src, spec, prefix))
}

func TestEffectiveSpecAddress(t *testing.T) {
	prefix := ExtfsPrefixForCollection(12)
	// Tier 1: explicit address wins over derivation.
	assert.Equal(t, "https://custom.example.com",
		effectiveSpecAddress(map[string]string{
			prefix + "address":        "https://custom.example.com",
			prefix + "cloud_provider": "aws",
			prefix + "region":         "us-west-2",
		}, prefix))
	// Tier 2: derive from provider+region when address absent.
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com",
		effectiveSpecAddress(map[string]string{
			prefix + "cloud_provider": "aws",
			prefix + "region":         "us-west-2",
		}, prefix))
	// Tier 2 GCP: region-less derivation.
	assert.Equal(t, "https://storage.googleapis.com",
		effectiveSpecAddress(map[string]string{
			prefix + "cloud_provider": "gcp",
		}, prefix))
	// Tier 3: no tier matches → empty.
	assert.Equal(t, "",
		effectiveSpecAddress(map[string]string{
			prefix + "cloud_provider": "azure", // not derivable
			prefix + "region":         "eastus",
		}, prefix))
	assert.Equal(t, "", effectiveSpecAddress(nil, prefix))
}

// AWS-form derivation and equality-guard cases are now covered by
// TestBuildExtfsOverrides_AWSForm_Tier2Derived and
// TestBuildExtfsOverrides_AWSForm_EqualityGuard above; the older baseline-
// inheritance variants were deleted along with copyStorageConfigToExtfs.

func TestNormalizeExternalSource_Tier2_DerivedEndpoint(t *testing.T) {
	prefix := ExtfsPrefixForCollection(15)
	// Derived AWS endpoint rewrites bucket-style URI.
	spec := map[string]string{
		prefix + "cloud_provider": "aws",
		prefix + "region":         "us-west-2",
	}
	assert.Equal(t,
		"s3://s3.us-west-2.amazonaws.com/my-bucket/key",
		NormalizeExternalSource("s3://my-bucket/key", spec, prefix))

	// Equality guard: URI host already equals the derived endpoint → leave
	// unchanged so the Milvus-form path downstream interprets correctly.
	assert.Equal(t,
		"s3://s3.us-west-2.amazonaws.com/real-bucket/data",
		NormalizeExternalSource(
			"s3://s3.us-west-2.amazonaws.com/real-bucket/data", spec, prefix))

	// Azure → not derivable → no rewrite (Tier 3).
	specAzure := map[string]string{
		prefix + "cloud_provider": "azure",
		prefix + "region":         "eastus",
	}
	assert.Equal(t,
		"s3://my-bucket/key",
		NormalizeExternalSource("s3://my-bucket/key", specAzure, prefix))
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

// TestCopyStorageConfigToExtfs* removed: copyStorageConfigToExtfs no longer
// exists. Baseline inheritance was deleted as part of the extfs credential-
// isolation refactor. See TestBuildExtfsOverrides_ZeroInit /
// TestBuildExtfsOverrides_NoBaselineInheritance for the new contract.

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
		ctx, "parquet", []string{"col1"}, "s3:///bucket/path", config,
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

func TestFetchFragmentsFromExternalSourceWithRange_FormatProperties(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 100},
	}, nil).Build()
	defer mockRead.UnPatch()

	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{
			CollectionID: 1,
			SpecExtfs:    map[string]string{"iceberg.snapshot_id": "42"},
			RowLimit:     1000,
		},
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
	m := mockey.Mock(GetFileInfo).To(func(format, path string, cfg *indexpb.StorageConfig, kv map[string]string) (*FileInfo, error) {
		atomic.AddInt32(&mockCalled, 1)
		return &FileInfo{FilePath: path, NumRows: 99}, nil
	}).Build()
	defer m.UnPatch()

	fileInfos := []FileInfo{
		{FilePath: "a", NumRows: 10},
		{FilePath: "b", NumRows: 20},
		{FilePath: "c", NumRows: 30},
	}
	got, err := fetchRowCountsConcurrently(context.Background(), "parquet", fileInfos, &indexpb.StorageConfig{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 20, 30}, got)
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockCalled))
}

func TestFetchRowCountsConcurrently_MissingFilled(t *testing.T) {
	m := mockey.Mock(GetFileInfo).To(func(format, path string, cfg *indexpb.StorageConfig, kv map[string]string) (*FileInfo, error) {
		return &FileInfo{FilePath: path, NumRows: 777}, nil
	}).Build()
	defer m.UnPatch()

	fileInfos := []FileInfo{
		{FilePath: "a", NumRows: 10},
		{FilePath: "b", NumRows: 0},
		{FilePath: "c", NumRows: -1},
	}
	got, err := fetchRowCountsConcurrently(context.Background(), "parquet", fileInfos, &indexpb.StorageConfig{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 777, 777}, got)
}

func TestFetchRowCountsConcurrently_WorkerErrorPropagated(t *testing.T) {
	m := mockey.Mock(GetFileInfo).Return(nil, fmt.Errorf("fake ffi failure")).Build()
	defer m.UnPatch()

	_, err := fetchRowCountsConcurrently(context.Background(), "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, nil)
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
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, nil)
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

	m := mockey.Mock(GetFileInfo).To(func(_ string, _ string, _ *indexpb.StorageConfig, _ map[string]string) (*FileInfo, error) {
		cancel()
		return &FileInfo{NumRows: 10}, nil
	}).Build()
	defer m.UnPatch()

	_, err := fetchRowCountsConcurrently(ctx, "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
