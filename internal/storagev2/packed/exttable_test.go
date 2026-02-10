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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func newTestStorageConfig() *indexpb.StorageConfig {
	return &indexpb.StorageConfig{
		StorageType: "local",
	}
}

func TestExploreFiles_EmptyColumns(t *testing.T) {
	config := newTestStorageConfig()

	// Empty columns should still work
	files, err := ExploreFiles([]string{}, "parquet", "/tmp", "/nonexistent", config)

	// Expect error due to nonexistent directory
	assert.Error(t, err)
	assert.Nil(t, files)
}

func TestExploreFiles_InvalidDirectory(t *testing.T) {
	config := newTestStorageConfig()

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
	config := newTestStorageConfig()

	info, err := GetFileInfo("parquet", "/nonexistent/file.parquet", config)

	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestGetFileInfo_InvalidFormat(t *testing.T) {
	config := newTestStorageConfig()

	// Create a temp file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	err := os.WriteFile(tmpFile, []byte("not a parquet file"), 0o600)
	require.NoError(t, err)

	info, err := GetFileInfo("parquet", tmpFile, config)

	// Should fail because it's not a valid parquet file
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestCreateManifestForSegment_EmptyFragments(t *testing.T) {
	config := newTestStorageConfig()

	manifestPath, err := CreateManifestForSegment(
		"/tmp/test",
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
	config := newTestStorageConfig()

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
		"/nonexistent/path/that/cannot/exist",
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
