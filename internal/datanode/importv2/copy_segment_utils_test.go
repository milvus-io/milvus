// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"context"
	"fmt"
	"io"
	"math"
	"path"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGenerateTargetPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	tests := []struct {
		name       string
		sourcePath string
		wantPath   string
		wantErr    bool
	}{
		{
			name:       "insert binlog path",
			sourcePath: "files/insert_log/111/222/333/100/log1.log",
			wantPath:   "files/insert_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "delta binlog path",
			sourcePath: "files/delta_log/111/222/333/100/log1.log",
			wantPath:   "files/delta_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "stats binlog path",
			sourcePath: "files/stats_log/111/222/333/100/log1.log",
			wantPath:   "files/stats_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "bm25 binlog path",
			sourcePath: "files/bm25_stats/111/222/333/100/log1.log",
			wantPath:   "files/bm25_stats/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "v3 bm25 stats numeric log id path",
			sourcePath: "files/insert_log/111/222/333/_stats/bm25.100/12345",
			wantPath:   "files/insert_log/444/555/666/_stats/bm25.100/12345",
			wantErr:    false,
		},
		{
			name:       "external component in regular binlog root is not external table",
			sourcePath: "files/external/insert_log/111/222/333/100/log1.log",
			wantPath:   "files/external/insert_log/444/555/666/100/log1.log",
			wantErr:    false,
		},
		{
			name:       "invalid path - no log type",
			sourcePath: "files/111/222/333/100/log1.log",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid path - too short",
			sourcePath: "files/insert_log/111",
			wantPath:   "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := generateTargetPath(tt.sourcePath, source, target)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

func TestGenerateMappingsFromFiles_ExternalTable(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		StorageVersion:       storage.StorageV3,
		ManifestPath:         packed.MarshalManifestPath("files/insert_log/111/222/333", 1),
		IsExternalCollection: true,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}
	files := &SegmentFiles{
		InsertBinlogs: []string{"files/insert_log/111/222/333/_metadata/manifest.json"},
	}

	mappings, err := generateMappingsFromFiles(files, source, target)
	assert.NoError(t, err)
	assert.Equal(t,
		"files/insert_log/444/555/666/_metadata/manifest.json",
		mappings["files/insert_log/111/222/333/_metadata/manifest.json"])
}

func TestGenerateTargetLOBPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	tests := []struct {
		name       string
		sourcePath string
		wantPath   string
		wantErr    bool
	}{
		{
			name:       "standard LOB file path",
			sourcePath: "files/insert_log/111/222/lobs/100/_data/abc123.vx",
			wantPath:   "files/insert_log/444/555/lobs/100/_data/abc123.vx",
			wantErr:    false,
		},
		{
			name:       "LOB file with nested field path",
			sourcePath: "root/data/insert_log/111/222/lobs/200/_data/xyz.vx",
			wantPath:   "root/data/insert_log/444/555/lobs/200/_data/xyz.vx",
			wantErr:    false,
		},
		{
			name:       "invalid path - no insert_log",
			sourcePath: "files/other/111/222/lobs/100/_data/abc.vx",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid path - too short after insert_log",
			sourcePath: "files/insert_log/111",
			wantPath:   "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := generateTargetLOBPath(tt.sourcePath, source, target)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

func TestLobFileInfosToPaths(t *testing.T) {
	tests := []struct {
		name     string
		infos    []packed.LobFileInfo
		expected []string
	}{
		{
			name:     "empty input",
			infos:    nil,
			expected: []string{},
		},
		{
			name: "single LOB file - absolute path preserved",
			infos: []packed.LobFileInfo{
				{Path: "root/insert_log/100/200/lobs/300/_data/abc.vx", FieldID: 300, TotalRows: 1000, ValidRows: 900},
			},
			expected: []string{"root/insert_log/100/200/lobs/300/_data/abc.vx"},
		},
		{
			name: "multiple LOB files from same segment",
			infos: []packed.LobFileInfo{
				{Path: "root/insert_log/100/200/lobs/300/_data/file1.vx", FieldID: 300, TotalRows: 500, ValidRows: 500},
				{Path: "root/insert_log/100/200/lobs/300/_data/file2.vx", FieldID: 300, TotalRows: 500, ValidRows: 400},
				{Path: "root/insert_log/100/200/lobs/301/_data/file3.vx", FieldID: 301, TotalRows: 200, ValidRows: 200},
			},
			expected: []string{
				"root/insert_log/100/200/lobs/300/_data/file1.vx",
				"root/insert_log/100/200/lobs/300/_data/file2.vx",
				"root/insert_log/100/200/lobs/301/_data/file3.vx",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lobFileInfosToPaths(tt.infos)
			if len(tt.expected) == 0 {
				assert.Empty(t, result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestCollectSegmentFiles_LOBFromManifest verifies that LOB file collection
// reads from the segment manifest (not the partition directory), ensuring
// only this segment's LOB files are collected.
func TestCollectSegmentFiles_LOBFromManifest(t *testing.T) {
	// This segment owns only 2 LOB files
	segmentLobFiles := []packed.LobFileInfo{
		{Path: "root/insert_log/100/200/lobs/300/_data/seg1_file1.vx", FieldID: 300, TotalRows: 1000, ValidRows: 1000},
		{Path: "root/insert_log/100/200/lobs/300/_data/seg1_file2.vx", FieldID: 300, TotalRows: 500, ValidRows: 500},
	}

	// Mock CreateStorageConfig to avoid paramtable dependency
	mocker0 := mockey.Mock(compaction.CreateStorageConfig).Return(&indexpb.StorageConfig{}).Build()
	defer mocker0.UnPatch()

	// Mock GetManifestLobFiles to return only this segment's LOB files
	mocker1 := mockey.Mock(packed.GetManifestLobFiles).Return(segmentLobFiles, nil).Build()
	defer mocker1.UnPatch()

	// Mock UnmarshalManifestPath
	mocker2 := mockey.Mock(packed.UnmarshalManifestPath).Return("root/insert_log/100/200/1001", int64(1), nil).Build()
	defer mocker2.UnPatch()

	// Mock listAllFiles for insert binlogs (via WalkWithPrefix)
	mocker3 := mockey.Mock(listAllFiles).Return(
		[]string{"root/insert_log/100/200/1001/_data/cg0.parquet"}, nil).Build()
	defer mocker3.UnPatch()

	source := &datapb.CopySegmentSource{
		SegmentId:      1001,
		CollectionId:   100,
		PartitionId:    200,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"basePath":"root/insert_log/100/200/1001","version":1}`,
	}

	files, err := collectSegmentFiles(context.Background(), nil, source)
	assert.NoError(t, err)
	assert.NotNil(t, files)

	// Verify LOB files come from manifest, not from directory listing.
	// GetManifestLobFiles returns absolute paths (after ToAbsolutePaths in C++),
	// so lobFileInfosToPaths uses them directly.
	assert.Equal(t, 2, len(files.LobFiles))
	assert.Equal(t, "root/insert_log/100/200/lobs/300/_data/seg1_file1.vx", files.LobFiles[0])
	assert.Equal(t, "root/insert_log/100/200/lobs/300/_data/seg1_file2.vx", files.LobFiles[1])

	// Key invariant: only this segment's files are returned.
	// The old code used listAllFiles(partition/lobs/) which would have also
	// returned seg2_file3.vx, seg2_file4.vx from other segments.
}

func TestGenerateTargetIndexPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	tests := []struct {
		name        string
		sourcePath  string
		indexType   string
		pathVersion indexpb.IndexStorePathVersion
		wantPath    string
		wantErr     bool
	}{
		{
			name:        "vector scalar index path v0 format",
			sourcePath:  "files/index_files/1001/1/222/333/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			wantPath:    "files/index_files/1001/1/555/666/scalar_index",
			wantErr:     false,
		},
		{
			name:       "text index path",
			sourcePath: "files/text_log/123/1/111/222/333/100/index_file",
			indexType:  IndexTypeText,
			wantPath:   "files/text_log/123/1/444/555/666/100/index_file",
			wantErr:    false,
		},
		{
			name:       "json key index path",
			sourcePath: "files/json_key_index_log/123/1/111/222/333/100/index_file",
			indexType:  IndexTypeJSONKey,
			wantPath:   "files/json_key_index_log/123/1/444/555/666/100/index_file",
			wantErr:    false,
		},
		{
			name:       "json stats path - shared_key_index",
			sourcePath: "files/json_stats/2/123/1/111/222/333/100/shared_key_index/index_file",
			indexType:  IndexTypeJSONStats,
			wantPath:   "files/json_stats/2/123/1/444/555/666/100/shared_key_index/index_file",
			wantErr:    false,
		},
		{
			name:       "json stats path - shredding_data",
			sourcePath: "files/json_stats/2/123/1/111/222/333/100/shredding_data/data_file",
			indexType:  IndexTypeJSONStats,
			wantPath:   "files/json_stats/2/123/1/444/555/666/100/shredding_data/data_file",
			wantErr:    false,
		},
		{
			name:       "invalid - keyword not found",
			sourcePath: "files/other_index/111/222/333/100/index",
			indexType:  IndexTypeVectorScalarV0,
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid - path too short",
			sourcePath: "files/index_files/111",
			indexType:  IndexTypeVectorScalarV0,
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid - unsupported index type",
			sourcePath: "files/unknown_index/111/222/333/100/index",
			indexType:  "unknown_type",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:        "vector scalar index path v1 format",
			sourcePath:  "files/index_v1/111/222/333/1001/1/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			wantPath:    "files/index_v1/444/555/666/1001/1/scalar_index",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := generateTargetIndexPath(tt.sourcePath, source, target, tt.indexType, tt.pathVersion)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
}

// TestGenerateTargetIndexPath_BuildIDMapping verifies that NewBuildIds mapping
// is applied at the correct offset for each index path format.
// Regression test: v1 VectorScalar places buildID at offset 4 (not 1).
func TestGenerateTargetIndexPath_BuildIDMapping(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds:  map[int64]int64{1001: 2001},
	}

	tests := []struct {
		name        string
		sourcePath  string
		indexType   string
		pathVersion indexpb.IndexStorePathVersion
		wantPath    string
	}{
		{
			name:        "v0 vector scalar maps buildID at offset 1",
			sourcePath:  "files/index_files/1001/1/222/333/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			wantPath:    "files/index_files/2001/1/555/666/scalar_index",
		},
		{
			name:        "v1 vector scalar maps buildID at offset 4",
			sourcePath:  "files/index_v1/111/222/333/1001/1/scalar_index",
			indexType:   IndexTypeVectorScalarV0,
			pathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			wantPath:    "files/index_v1/444/555/666/2001/1/scalar_index",
		},
		{
			name:       "text index maps buildID at offset 1",
			sourcePath: "files/text_log/1001/1/111/222/333/100/idx",
			indexType:  IndexTypeText,
			wantPath:   "files/text_log/2001/1/444/555/666/100/idx",
		},
		{
			name:       "json stats maps buildID at offset 2",
			sourcePath: "files/json_stats/2/1001/1/111/222/333/100/shared_key_index/idx",
			indexType:  IndexTypeJSONStats,
			wantPath:   "files/json_stats/2/2001/1/444/555/666/100/shared_key_index/idx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateTargetIndexPath(tt.sourcePath, source, target, tt.indexType, tt.pathVersion)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantPath, got)
		})
	}
}

func TestGenerateMappingsFromFiles_VectorScalarUsesSourcePathVersion(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				BuildID:               1001,
				IndexVersion:          1,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
				IndexFilePaths:        []string{"files/index_files/1001/1/222/333/v0_file"},
			},
			{
				BuildID:               1002,
				IndexVersion:          1,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				IndexFilePaths:        []string{"files/index_v1/111/222/333/1002/1/v1_file"},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds:  map[int64]int64{1001: 2001, 1002: 2002},
	}
	files := &SegmentFiles{
		VectorScalarIndex: []string{
			"files/index_files/1001/1/222/333/v0_file",
			"files/index_v1/111/222/333/1002/1/v1_file",
		},
	}

	mappings, err := generateMappingsFromFiles(files, source, target)
	assert.NoError(t, err)
	assert.Equal(t, "files/index_files/2001/1/555/666/v0_file", mappings["files/index_files/1001/1/222/333/v0_file"])
	assert.Equal(t, "files/index_v1/444/555/666/2002/1/v1_file", mappings["files/index_v1/111/222/333/1002/1/v1_file"])
}

func TestTransformFieldBinlogs(t *testing.T) {
	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1.log": "files/insert_log/444/555/666/100/log1.log",
		"files/insert_log/111/222/333/101/log2.log": "files/insert_log/444/555/666/101/log2.log",
	}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum:    1000,
					TimestampFrom: 100,
					TimestampTo:   200,
					LogPath:       "files/insert_log/111/222/333/100/log1.log",
					LogSize:       1024,
					MemorySize:    1024,
				},
			},
		},
		{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum:    2000,
					TimestampFrom: 150,
					TimestampTo:   250,
					LogPath:       "files/insert_log/111/222/333/101/log2.log",
					LogSize:       2048,
					MemorySize:    2048,
				},
			},
		},
	}

	t.Run("count rows for insert logs", func(t *testing.T) {
		result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, true, false)
		assert.NoError(t, err)
		assert.Equal(t, int64(3000), totalRows)
		assert.Equal(t, 2, len(result))

		// Verify first field binlog
		assert.Equal(t, int64(100), result[0].FieldID)
		assert.Equal(t, 1, len(result[0].Binlogs))
		assert.Equal(t, int64(1000), result[0].Binlogs[0].EntriesNum)
		assert.Equal(t, "files/insert_log/444/555/666/100/log1.log", result[0].Binlogs[0].LogPath)
		assert.Equal(t, int64(1024), result[0].Binlogs[0].LogSize)
		assert.Equal(t, int64(1024), result[0].Binlogs[0].MemorySize)

		// Verify second field binlog
		assert.Equal(t, int64(101), result[1].FieldID)
		assert.Equal(t, 1, len(result[1].Binlogs))
		assert.Equal(t, int64(2000), result[1].Binlogs[0].EntriesNum)
	})

	t.Run("no row counting for stats logs", func(t *testing.T) {
		result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, false, false)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), totalRows)
		assert.Equal(t, 2, len(result))
	})

	t.Run("skip binlogs with empty path", func(t *testing.T) {
		srcWithEmpty := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 1000,
						LogPath:    "",
					},
				},
			},
		}
		result, _, err := transformFieldBinlogs(srcWithEmpty, mappings, false, false)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(result))
	})

	t.Run("error on missing mapping", func(t *testing.T) {
		srcWithUnmapped := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 1000,
						LogPath:    "files/insert_log/999/888/777/100/unmapped.log",
					},
				},
			},
		}
		result, totalRows, err := transformFieldBinlogs(srcWithUnmapped, mappings, true, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found")
		assert.Nil(t, result)
		assert.Equal(t, int64(0), totalRows)
	})
}

func TestCopySegmentAndIndexFiles(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 1000,
						LogPath:    "files/insert_log/111/222/333/100/12345",
						LogSize:    1024,
					},
				},
			},
		},
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100,
				IndexID:        1001,
				BuildID:        1002,
				IndexFilePaths: []string{"files/index_files/1002/1/222/333/index1"},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("successful copy", func(t *testing.T) {
		mCopy := mockey.Mock(copyFile).Return(nil).Build()
		defer mCopy.UnPatch()

		cm := &struct{ storage.ChunkManager }{}
		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(666), result.SegmentId)
		assert.Equal(t, int64(1000), result.ImportedRows)
		assert.Equal(t, 1, len(result.Binlogs))
		assert.Equal(t, 1, len(result.IndexInfos))
		assert.Len(t, copiedFiles, 2)
	})

	t.Run("copy failure", func(t *testing.T) {
		mCopy := mockey.Mock(copyFile).Return(errors.New("copy failed")).Build()
		defer mCopy.UnPatch()

		cm := &struct{ storage.ChunkManager }{}
		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to copy file")
		assert.Empty(t, copiedFiles)
	})
}

func TestGenerateSegmentInfoFromSource(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum:    1000,
						TimestampFrom: 100,
						TimestampTo:   200,
						LogPath:       "files/insert_log/111/222/333/100/log1.log",
						LogSize:       1024,
					},
				},
			},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{
						LogPath: "files/stats_log/111/222/333/100/stats1.log",
					},
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1.log":  "files/insert_log/444/555/666/100/log1.log",
		"files/stats_log/111/222/333/100/stats1.log": "files/stats_log/444/555/666/100/stats1.log",
	}

	t.Run("generate segment info", func(t *testing.T) {
		segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)

		assert.NoError(t, err)
		assert.NotNil(t, segmentInfo)
		assert.Equal(t, int64(666), segmentInfo.SegmentID)
		assert.Equal(t, int64(1000), segmentInfo.ImportedRows)
		assert.Equal(t, 1, len(segmentInfo.Binlogs))
		assert.Equal(t, 1, len(segmentInfo.Statslogs))
		assert.Equal(t, "files/insert_log/444/555/666/100/log1.log", segmentInfo.Binlogs[0].Binlogs[0].LogPath)
	})
}

func TestBuildIndexInfoFromSource(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100,
				IndexID:        1001,
				BuildID:        1002,
				IndexFilePaths: []string{"files/index_files/1002/1/222/333/index1"},
				SerializedSize: 5000,
			},
		},
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			100: {
				FieldID:    100,
				Version:    1,
				BuildID:    2001,
				Files:      []string{"files/text_log/123/1/111/222/333/100/text1"},
				LogSize:    2048,
				MemorySize: 4096,
			},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			101: {
				FieldID:                101,
				Version:                1,
				BuildID:                3001,
				JsonKeyStatsDataFormat: 1, // Legacy format
				Files:                  []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
				MemorySize:             3072,
			},
			102: {
				FieldID:                102,
				Version:                1,
				BuildID:                3002,
				JsonKeyStatsDataFormat: 2, // New format
				Files: []string{
					"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/index1",
					"files/json_stats/2/3002/1/111/222/333/102/shredding_data/data1",
				},
				MemorySize: 4096,
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/index_files/1002/1/222/333/index1":                           "files/index_files/1002/1/555/666/index1",
		"files/text_log/123/1/111/222/333/100/text1":                        "files/text_log/123/1/444/555/666/100/text1",
		"files/json_key_index_log/123/1/111/222/333/101/json1":              "files/json_key_index_log/123/1/444/555/666/101/json1",
		"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/index1": "files/json_stats/2/3002/1/444/555/666/102/shared_key_index/index1",
		"files/json_stats/2/3002/1/111/222/333/102/shredding_data/data1":    "files/json_stats/2/3002/1/444/555/666/102/shredding_data/data1",
	}

	t.Run("error on missing vector index mapping", func(t *testing.T) {
		emptyMappings := map[string]string{}
		_, _, _, err := buildIndexInfoFromSource(source, target, emptyMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for index file")
	})

	t.Run("error on missing text index mapping", func(t *testing.T) {
		partialMappings := map[string]string{
			"files/index_files/1002/1/222/333/index1": "files/index_files/1002/1/555/666/index1",
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, partialMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for text index file")
	})

	t.Run("error on missing json index mapping", func(t *testing.T) {
		partialMappings := map[string]string{
			"files/index_files/1002/1/222/333/index1":    "files/index_files/1002/1/555/666/index1",
			"files/text_log/123/1/111/222/333/100/text1": "files/text_log/123/1/444/555/666/100/text1",
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, partialMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for JSON index file")
	})

	t.Run("build all index info", func(t *testing.T) {
		indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, mappings)
		assert.NoError(t, err)

		// Verify vector/scalar index info (keyed by buildID, not fieldID)
		assert.Equal(t, 1, len(indexInfos))
		assert.NotNil(t, indexInfos[1002])
		assert.Equal(t, int64(100), indexInfos[1002].FieldId)
		assert.Equal(t, int64(1001), indexInfos[1002].IndexId)
		assert.Equal(t, int64(1002), indexInfos[1002].BuildId)
		assert.Equal(t, int64(5000), indexInfos[1002].IndexSize)
		assert.Equal(t, "files/index_files/1002/1/555/666/index1", indexInfos[1002].IndexFilePaths[0])

		// Verify text index info
		assert.Equal(t, 1, len(textIndexInfos))
		assert.NotNil(t, textIndexInfos[100])
		assert.Equal(t, int64(100), textIndexInfos[100].FieldID)
		assert.Equal(t, int64(2001), textIndexInfos[100].BuildID)
		assert.Equal(t, "files/text_log/123/1/444/555/666/100/text1", textIndexInfos[100].Files[0])

		// Verify JSON key index info (legacy and new formats)
		assert.Equal(t, 2, len(jsonKeyIndexInfos))

		// Legacy format (data_format = 1)
		assert.NotNil(t, jsonKeyIndexInfos[101])
		assert.Equal(t, int64(101), jsonKeyIndexInfos[101].FieldID)
		assert.Equal(t, int64(3001), jsonKeyIndexInfos[101].BuildID)
		assert.Equal(t, int64(1), jsonKeyIndexInfos[101].JsonKeyStatsDataFormat)
		assert.Equal(t, "files/json_key_index_log/123/1/444/555/666/101/json1", jsonKeyIndexInfos[101].Files[0])

		// New format (data_format = 2)
		assert.NotNil(t, jsonKeyIndexInfos[102])
		assert.Equal(t, int64(102), jsonKeyIndexInfos[102].FieldID)
		assert.Equal(t, int64(3002), jsonKeyIndexInfos[102].BuildID)
		assert.Equal(t, int64(2), jsonKeyIndexInfos[102].JsonKeyStatsDataFormat)
		assert.Equal(t, 2, len(jsonKeyIndexInfos[102].Files))
		assert.Equal(t, "files/json_stats/2/3002/1/444/555/666/102/shared_key_index/index1", jsonKeyIndexInfos[102].Files[0])
		assert.Equal(t, "files/json_stats/2/3002/1/444/555/666/102/shredding_data/data1", jsonKeyIndexInfos[102].Files[1])
	})
}

func TestBuildIndexInfoFromSource_PreservesIndexStorePathVersion(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:               100,
				IndexID:               10,
				BuildID:               1001,
				IndexName:             "idx_v0",
				IndexVersion:          1,
				SerializedSize:        1024,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
				IndexFilePaths:        []string{"files/index_files/1001/1/222/333/v0_file"},
			},
			{
				FieldID:               101,
				IndexID:               11,
				BuildID:               1002,
				IndexName:             "idx_v1",
				IndexVersion:          1,
				SerializedSize:        2048,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				IndexFilePaths:        []string{"files/index_v1/111/222/333/1002/1/v1_file"},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds:  map[int64]int64{1001: 2001, 1002: 2002},
	}
	mappings := map[string]string{
		"files/index_files/1001/1/222/333/v0_file":  "files/index_files/2001/1/555/666/v0_file",
		"files/index_v1/111/222/333/1002/1/v1_file": "files/index_v1/444/555/666/2002/1/v1_file",
	}

	indexInfos, _, _, err := buildIndexInfoFromSource(source, target, mappings)
	assert.NoError(t, err)
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, indexInfos[2001].GetIndexStorePathVersion())
	assert.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, indexInfos[2002].GetIndexStorePathVersion())
}

func TestCopySegmentAndIndexFiles_ReturnsFileList(t *testing.T) {
	t.Run("success returns all copied files", func(t *testing.T) {
		mCopy := mockey.Mock(copyFile).Return(nil).Build()
		defer mCopy.UnPatch()
		cm := &struct{ storage.ChunkManager }{}

		source := &datapb.CopySegmentSource{
			CollectionId: 111,
			PartitionId:  222,
			SegmentId:    333,
			InsertBinlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogPath: "files/insert_log/111/222/333/1/10001", LogSize: 100},
						{LogPath: "files/insert_log/111/222/333/1/10002", LogSize: 200},
					},
				},
			},
		}
		target := &datapb.CopySegmentTarget{
			CollectionId: 444,
			PartitionId:  555,
			SegmentId:    666,
		}

		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, copiedFiles, 2)
		assert.Contains(t, copiedFiles, "files/insert_log/444/555/666/1/10001")
		assert.Contains(t, copiedFiles, "files/insert_log/444/555/666/1/10002")
	})

	t.Run("failure returns partial file list", func(t *testing.T) {
		callCount := 0
		mCopy := mockey.Mock(copyFile).
			To(func(_ context.Context, _ storage.ChunkManager, src, dst string) error {
				callCount++
				if callCount > 1 {
					return errors.New("copy failed")
				}
				return nil
			}).Build()
		defer mCopy.UnPatch()
		cm := &struct{ storage.ChunkManager }{}

		source := &datapb.CopySegmentSource{
			CollectionId: 111,
			PartitionId:  222,
			SegmentId:    333,
			InsertBinlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{LogPath: "files/insert_log/111/222/333/1/10001", LogSize: 100},
						{LogPath: "files/insert_log/111/222/333/1/10002", LogSize: 200},
					},
				},
			},
		}
		target := &datapb.CopySegmentTarget{
			CollectionId: 444,
			PartitionId:  555,
			SegmentId:    666,
		}

		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.True(t, len(copiedFiles) <= 1, "should return files copied before failure")
	})
}

func TestGenerateTargetIndexPath_PathTooShort(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	tests := []struct {
		name      string
		path      string
		indexType string
	}{
		{
			name:      "text index path too short",
			path:      "files/text_log/123/1/111",
			indexType: IndexTypeText,
		},
		{
			name:      "json key index path too short",
			path:      "files/json_key_index_log/123",
			indexType: IndexTypeJSONKey,
		},
		{
			name:      "json stats path too short",
			path:      "files/json_stats/2/123/1/111",
			indexType: IndexTypeJSONStats,
		},
		{
			name:      "vector scalar path exactly at keyword",
			path:      "files/index_files",
			indexType: IndexTypeVectorScalarV0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := generateTargetIndexPath(tt.path, source, target, tt.indexType, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid")
		})
	}
}

func TestGenerateTargetIndexPath_VectorScalarPreservesIDs(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	// Verify buildID (1001) and indexVersion (1) are preserved, NOT replaced
	// partitionID (222->555) and segmentID (333->666) ARE replaced
	// collectionID is NOT in path at all
	result, err := generateTargetIndexPath("files/index_files/1001/1/222/333/HNSW_SQ_3", source, target, IndexTypeVectorScalarV0, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED)
	assert.NoError(t, err)
	assert.Equal(t, "files/index_files/1001/1/555/666/HNSW_SQ_3", result)

	// Verify collectionID (111) does NOT appear in target path
	assert.NotContains(t, result, "111")
	assert.NotContains(t, result, "444") // collectionID should NOT be injected
}

func TestGenerateTargetPath_AllConstants(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	// Verify all constants match the correct storage path names
	tests := []struct {
		name     string
		logType  string
		expected string
	}{
		{"insert uses insert_log", BinlogTypeInsert, "insert_log"},
		{"delta uses delta_log", BinlogTypeDelta, "delta_log"},
		{"stats uses stats_log", BinlogTypeStats, "stats_log"},
		{"bm25 uses bm25_stats", BinlogTypeBM25, "bm25_stats"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("files/%s/111/222/333/100/log1", tt.expected)
			result, err := generateTargetPath(path, source, target)
			assert.NoError(t, err)
			assert.Contains(t, result, tt.expected)
			assert.Contains(t, result, "444")
			assert.Contains(t, result, "555")
			assert.Contains(t, result, "666")
		})
	}
}

func TestGenerateSegmentInfoFromSource_AllBinlogTypes(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 500, LogPath: "files/insert_log/111/222/333/100/log1"},
				},
			},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/stats_log/111/222/333/100/stats1"},
				},
			},
		},
		DeltaBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 10, LogPath: "files/delta_log/111/222/333/100/delta1"},
				},
			},
		},
		Bm25Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/bm25_stats/111/222/333/100/bm25_1"},
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1":   "files/insert_log/444/555/666/100/log1",
		"files/stats_log/111/222/333/100/stats1":  "files/stats_log/444/555/666/100/stats1",
		"files/delta_log/111/222/333/100/delta1":  "files/delta_log/444/555/666/100/delta1",
		"files/bm25_stats/111/222/333/100/bm25_1": "files/bm25_stats/444/555/666/100/bm25_1",
	}

	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	assert.NoError(t, err)
	assert.NotNil(t, segmentInfo)
	assert.Equal(t, int64(666), segmentInfo.SegmentID)
	assert.Equal(t, int64(500), segmentInfo.ImportedRows) // Only insert rows counted
	assert.Equal(t, 1, len(segmentInfo.Binlogs))
	assert.Equal(t, 1, len(segmentInfo.Statslogs))
	assert.Equal(t, 1, len(segmentInfo.Deltalogs))
	assert.Equal(t, 1, len(segmentInfo.Bm25Logs))
	assert.Equal(t, "files/delta_log/444/555/666/100/delta1", segmentInfo.Deltalogs[0].Binlogs[0].LogPath)
	assert.Equal(t, "files/bm25_stats/444/555/666/100/bm25_1", segmentInfo.Bm25Logs[0].Binlogs[0].LogPath)
}

func TestGenerateSegmentInfoFromSource_EmptySource(t *testing.T) {
	source := &datapb.CopySegmentSource{}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	segmentInfo, err := generateSegmentInfoFromSource(source, target, map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, segmentInfo)
	assert.Equal(t, int64(666), segmentInfo.SegmentID)
	assert.Equal(t, int64(0), segmentInfo.ImportedRows)
	assert.Equal(t, 0, len(segmentInfo.Binlogs))
	assert.Equal(t, 0, len(segmentInfo.Statslogs))
	assert.Equal(t, 0, len(segmentInfo.Deltalogs))
	assert.Equal(t, 0, len(segmentInfo.Bm25Logs))
}

// TestBuildIndexInfoFromSource_MultipleIndexesPerField verifies that multiple indexes
// on the same field (e.g., JSON path indexes) are all preserved in the result map.
// This is the core scenario for the fix: using buildID as map key instead of fieldID.
func TestBuildIndexInfoFromSource_MultipleIndexesPerField(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100, // Same field
				IndexID:        1001,
				BuildID:        2001,
				IndexName:      "idx_category",
				IndexFilePaths: []string{"files/index_files/2001/1/222/333/index1"},
				SerializedSize: 3000,
			},
			{
				FieldID:        100, // Same field, different path index
				IndexID:        1002,
				BuildID:        2002,
				IndexName:      "idx_price",
				IndexFilePaths: []string{"files/index_files/2002/1/222/333/index2"},
				SerializedSize: 4000,
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds: map[int64]int64{
			2001: 3001, // old buildID -> new buildID
			2002: 3002,
		},
	}

	mappings := map[string]string{
		"files/index_files/2001/1/222/333/index1": "files/index_files/3001/1/555/666/index1",
		"files/index_files/2002/1/222/333/index2": "files/index_files/3002/1/555/666/index2",
	}

	indexInfos, _, _, err := buildIndexInfoFromSource(source, target, mappings)
	assert.NoError(t, err)

	// Both indexes should be present (keyed by new buildID)
	assert.Equal(t, 2, len(indexInfos), "Both JSON path indexes should be in the result")

	// Verify first index (idx_category)
	assert.NotNil(t, indexInfos[3001])
	assert.Equal(t, int64(100), indexInfos[3001].FieldId)
	assert.Equal(t, int64(1001), indexInfos[3001].IndexId)
	assert.Equal(t, int64(3001), indexInfos[3001].BuildId)
	assert.Equal(t, "idx_category", indexInfos[3001].IndexName)

	// Verify second index (idx_price)
	assert.NotNil(t, indexInfos[3002])
	assert.Equal(t, int64(100), indexInfos[3002].FieldId)
	assert.Equal(t, int64(1002), indexInfos[3002].IndexId)
	assert.Equal(t, int64(3002), indexInfos[3002].BuildId)
	assert.Equal(t, "idx_price", indexInfos[3002].IndexName)
}

func TestBuildIndexInfoFromSource_EmptySource(t *testing.T) {
	source := &datapb.CopySegmentSource{}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, map[string]string{})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(indexInfos))
	assert.Equal(t, 0, len(textIndexInfos))
	assert.Equal(t, 0, len(jsonKeyIndexInfos))
}

func TestBuildIndexInfoFromSource_UnmappedPaths(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("unmapped vector index path returns error", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			IndexFiles: []*indexpb.IndexFilePathInfo{
				{
					FieldID:        100,
					IndexID:        1001,
					BuildID:        1002,
					IndexFilePaths: []string{"unmapped/path"},
					SerializedSize: 5000,
				},
			},
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for index file")
	})

	t.Run("unmapped text index path returns error", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			TextIndexFiles: map[int64]*datapb.TextIndexStats{
				200: {
					FieldID: 200,
					Files:   []string{"unmapped/text/path"},
				},
			},
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for text index file")
	})

	t.Run("unmapped json index path returns error", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				300: {
					FieldID: 300,
					Files:   []string{"unmapped/json/path"},
				},
			},
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for JSON index file")
	})
}

func TestTransformFieldBinlogs_NilInput(t *testing.T) {
	result, totalRows, err := transformFieldBinlogs(nil, map[string]string{}, true, false)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), totalRows)
	assert.Equal(t, 0, len(result))
}

func TestTransformFieldBinlogs_MultipleBinlogsPerField(t *testing.T) {
	mappings := map[string]string{
		"files/insert_log/111/222/333/100/log1": "files/insert_log/444/555/666/100/log1",
		"files/insert_log/111/222/333/100/log2": "files/insert_log/444/555/666/100/log2",
		"files/insert_log/111/222/333/100/log3": "files/insert_log/444/555/666/100/log3",
	}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 100, LogPath: "files/insert_log/111/222/333/100/log1", LogSize: 512},
				{EntriesNum: 200, LogPath: "files/insert_log/111/222/333/100/log2", LogSize: 1024},
				{EntriesNum: 300, LogPath: "files/insert_log/111/222/333/100/log3", LogSize: 2048},
			},
		},
	}

	result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, true, false)
	assert.NoError(t, err)
	assert.Equal(t, int64(600), totalRows)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 3, len(result[0].Binlogs))
	assert.Equal(t, "files/insert_log/444/555/666/100/log1", result[0].Binlogs[0].LogPath)
	assert.Equal(t, "files/insert_log/444/555/666/100/log2", result[0].Binlogs[1].LogPath)
	assert.Equal(t, "files/insert_log/444/555/666/100/log3", result[0].Binlogs[2].LogPath)
}

func TestTransformFieldBinlogs_UnmappedPath(t *testing.T) {
	// Path not in mappings -> returns error (fail-fast)
	mappings := map[string]string{}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 100, LogPath: "files/insert_log/111/222/333/100/log1"},
			},
		},
	}

	_, _, err := transformFieldBinlogs(srcFieldBinlogs, mappings, true, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no mapping found for source path")
}

func TestCopySegmentAndIndexFiles_CreateFileMappingsError(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	// Source with invalid path that will cause generateMappingsFromFiles to fail
	source := &datapb.CopySegmentSource{
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "invalid/path/without/log/type"},
				},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), mockCM, source, target, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Nil(t, copiedFiles)
	assert.Contains(t, err.Error(), "failed to generate file mappings")
}

func TestShortenIndexFilePaths(t *testing.T) {
	tests := []struct {
		name      string
		fullPaths []string
		expected  []string
	}{
		{
			name: "vector/scalar index paths",
			fullPaths: []string{
				"files/index_files/1001/1/555/666/scalar_index",
				"files/index_files/1001/1/555/666/vector_index",
			},
			expected: []string{"scalar_index", "vector_index"},
		},
		{
			name:      "empty path list",
			fullPaths: []string{},
			expected:  []string{},
		},
		{
			name: "single file name",
			fullPaths: []string{
				"index_file",
			},
			expected: []string{"index_file"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shortenIndexFilePaths(tt.fullPaths)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShortenSingleJsonStatsPath(t *testing.T) {
	tests := []struct {
		name         string
		inputPath    string
		expectedPath string
	}{
		{
			name:         "shared_key_index path",
			inputPath:    "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0",
			expectedPath: "shared_key_index/inverted_index_0",
		},
		{
			name:         "shredding_data path",
			inputPath:    "files/json_stats/2/123/1/444/555/666/100/shredding_data/parquet_data_0",
			expectedPath: "shredding_data/parquet_data_0",
		},
		{
			name:         "already shortened - shared_key_index",
			inputPath:    "shared_key_index/inverted_index_0",
			expectedPath: "shared_key_index/inverted_index_0",
		},
		{
			name:         "already shortened - shredding_data",
			inputPath:    "shredding_data/parquet_data_0",
			expectedPath: "shredding_data/parquet_data_0",
		},
		{
			name:         "no keyword - keep as-is",
			inputPath:    "files/other/path/file.json",
			expectedPath: "files/other/path/file.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shortenSingleJSONStatsPath(tt.inputPath)
			assert.Equal(t, tt.expectedPath, result)
		})
	}
}

func TestShortenJsonStatsPath(t *testing.T) {
	jsonStats := map[int64]*datapb.JsonKeyStats{
		100: {
			FieldID: 100,
			Version: 1,
			BuildID: 123,
			Files: []string{
				"files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0",
				"files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_1",
			},
			JsonKeyStatsDataFormat: 2,
			MemorySize:             1024,
			LogSize:                2048,
		},
		200: {
			FieldID: 200,
			Version: 1,
			BuildID: 456,
			Files: []string{
				"files/json_stats/2/456/1/444/555/666/200/shredding_data/parquet_data_0",
			},
			JsonKeyStatsDataFormat: 2,
			MemorySize:             512,
		},
	}

	result := shortenJSONStatsPath(jsonStats)

	assert.Equal(t, 2, len(result))

	// Check field 100
	assert.NotNil(t, result[100])
	assert.Equal(t, int64(100), result[100].FieldID)
	assert.Equal(t, int64(1), result[100].Version)
	assert.Equal(t, int64(123), result[100].BuildID)
	assert.Equal(t, int64(2), result[100].JsonKeyStatsDataFormat)
	assert.Equal(t, int64(1024), result[100].MemorySize)
	assert.Equal(t, int64(2048), result[100].LogSize)
	assert.Equal(t, 2, len(result[100].Files))
	assert.Equal(t, "shared_key_index/inverted_index_0", result[100].Files[0])
	assert.Equal(t, "shared_key_index/inverted_index_1", result[100].Files[1])

	// Check field 200
	assert.NotNil(t, result[200])
	assert.Equal(t, int64(200), result[200].FieldID)
	assert.Equal(t, int64(1), result[200].Version)
	assert.Equal(t, int64(456), result[200].BuildID)
	assert.Equal(t, int64(2), result[200].JsonKeyStatsDataFormat)
	assert.Equal(t, int64(512), result[200].MemorySize)
	assert.Equal(t, 1, len(result[200].Files))
	assert.Equal(t, "shredding_data/parquet_data_0", result[200].Files[0])
}

func TestShortenJsonStatsPath_MetaJson(t *testing.T) {
	jsonStats := map[int64]*datapb.JsonKeyStats{
		102: {
			FieldID: 102,
			Version: 1,
			BuildID: 462930163709949539,
			Files: []string{
				"files/json_stats/2/462930163709949539/1/462930163710600038/462930163710600039/462930163710600046/102/meta.json",
			},
			JsonKeyStatsDataFormat: 2,
			MemorySize:             256,
		},
	}

	result := shortenJSONStatsPath(jsonStats)

	assert.Equal(t, 1, len(result))
	assert.NotNil(t, result[102])
	assert.Equal(t, int64(102), result[102].FieldID)
	assert.Equal(t, 1, len(result[102].Files))
	assert.Equal(t, "meta.json", result[102].Files[0])
}

func TestShortenSingleJsonStatsPath_EdgeCases(t *testing.T) {
	t.Run("already_shortened_meta", func(t *testing.T) {
		result := shortenSingleJSONStatsPath("meta.json")
		assert.Equal(t, "meta.json", result)
	})

	t.Run("already_shortened_shared_key", func(t *testing.T) {
		result := shortenSingleJSONStatsPath("shared_key_index/inverted_index_0")
		assert.Equal(t, "shared_key_index/inverted_index_0", result)
	})

	t.Run("full_path_meta_json", func(t *testing.T) {
		fullPath := "files/json_stats/2/123/1/444/555/666/100/meta.json"
		result := shortenSingleJSONStatsPath(fullPath)
		assert.Equal(t, "meta.json", result)
	})

	t.Run("full_path_nested_file", func(t *testing.T) {
		fullPath := "files/json_stats/2/123/1/444/555/666/100/subdir/file.dat"
		result := shortenSingleJSONStatsPath(fullPath)
		assert.Equal(t, "subdir/file.dat", result)
	})
}

func TestTransformManifestPath(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		SegmentId:    2001,
		CollectionId: 111,
		PartitionId:  222,
	}

	sourceManifest := packed.MarshalManifestPath(
		"files/insert_log/449104612037410004/449104621518610066/449104621518610065",
		2,
	)

	source := &datapb.CopySegmentSource{}
	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.NoError(t, err)

	basePath, version, err := packed.UnmarshalManifestPath(targetManifest)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.Contains(t, basePath, "111")
	assert.Contains(t, basePath, "222")
	assert.Contains(t, basePath, "2001")
}

func TestTransformManifestPath_ExternalTable(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		SegmentId:    666,
		CollectionId: 444,
		PartitionId:  555,
	}

	sourceManifest := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)
	source := &datapb.CopySegmentSource{IsExternalCollection: true}
	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.NoError(t, err)

	basePath, version, err := packed.UnmarshalManifestPath(targetManifest)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.Equal(t, "files/insert_log/444/555/666", basePath)
}

func TestTransformManifestPath_LegacyExternalTableLayoutUnsupported(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		SegmentId:    666,
		CollectionId: 444,
		PartitionId:  555,
	}

	sourceManifest := packed.MarshalManifestPath("external/111/segments/333", 2)
	source := &datapb.CopySegmentSource{IsExternalCollection: true}
	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.Error(t, err)
	assert.Empty(t, targetManifest)
	assert.Contains(t, err.Error(), "invalid binlog path structure")
}

func TestTransformFieldBinlogs_SkipsPathMappingForExternalTable(t *testing.T) {
	src := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{
					LogID:      10,
					EntriesNum: 123,
					MemorySize: 456,
					LogSize:    456,
				},
			},
		},
	}
	got, totalRows, err := transformFieldBinlogs(src, nil, true, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), totalRows)
	assert.Len(t, got, 1)
	assert.Equal(t, int64(100), got[0].GetFieldID())
	assert.Len(t, got[0].GetBinlogs(), 1)
	assert.Empty(t, got[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, int64(10), got[0].GetBinlogs()[0].GetLogID())
	assert.Equal(t, int64(123), got[0].GetBinlogs()[0].GetEntriesNum())
	assert.Equal(t, int64(456), got[0].GetBinlogs()[0].GetMemorySize())
}

func TestTransformFieldBinlogs_SkipsEmptyPathForInternalTable(t *testing.T) {
	src := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogID: 10, EntriesNum: 123},
			},
		},
	}

	got, totalRows, err := transformFieldBinlogs(src, nil, false, false)
	assert.NoError(t, err)
	assert.Zero(t, totalRows)
	assert.Empty(t, got)
}

func TestGenerateSegmentInfoFromSource_PreservesExternalTableBinlogMetadata(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		StorageVersion:       storage.StorageV3,
		ManifestPath:         packed.MarshalManifestPath("files/insert_log/111/222/333", 2),
		IsExternalCollection: true,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs: []*datapb.Binlog{
					{LogID: 10, EntriesNum: 123, MemorySize: 456},
				},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	segmentInfo, err := generateSegmentInfoFromSource(source, target, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), segmentInfo.GetImportedRows())
	assert.Len(t, segmentInfo.GetBinlogs(), 1)
	assert.Empty(t, segmentInfo.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestCopySegmentAndIndexFiles_ExternalTable(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:         111,
		PartitionId:          222,
		SegmentId:            333,
		StorageVersion:       storage.StorageV3,
		ManifestPath:         packed.MarshalManifestPath("files/insert_log/111/222/333", 2),
		IsExternalCollection: true,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs: []*datapb.Binlog{
					{LogID: 10, EntriesNum: 123, MemorySize: 456},
				},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mList := mockey.Mock(listAllFiles).Return([]string{
		"files/insert_log/111/222/333/_data/0",
		"files/insert_log/111/222/333/_metadata/manifest.json",
	}, nil).Build()
	defer mList.UnPatch()

	copied := make(map[string]string)
	mCopy := mockey.Mock(copyFile).
		To(func(_ context.Context, _ storage.ChunkManager, src, dst string) error {
			copied[src] = dst
			return nil
		}).Build()
	defer mCopy.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), result.GetImportedRows())
	assert.Equal(t, packed.MarshalManifestPath("files/insert_log/444/555/666", 2), result.GetManifestPath())
	assert.Len(t, result.GetBinlogs(), 1)
	assert.Empty(t, result.GetBinlogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "files/insert_log/444/555/666/_data/0", copied["files/insert_log/111/222/333/_data/0"])
	assert.Equal(t, "files/insert_log/444/555/666/_metadata/manifest.json", copied["files/insert_log/111/222/333/_metadata/manifest.json"])
	assert.ElementsMatch(t, []string{
		"files/insert_log/444/555/666/_data/0",
		"files/insert_log/444/555/666/_metadata/manifest.json",
	}, copiedFiles)
}

func TestCollectSegmentFiles_WithManifest(t *testing.T) {
	ctx := context.Background()

	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	mList := mockey.Mock(listAllFiles).Return([]string{
		"files/insert_log/111/222/333/_data/100/file1.log",
		"files/insert_log/111/222/333/_data/101/file2.log",
		"files/insert_log/111/222/333/_metadata/manifest.json",
	}, nil).Build()
	defer mList.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		InsertBinlogs:  []*datapb.FieldBinlog{},
	}

	files, err := collectSegmentFiles(ctx, cm, source)

	assert.NoError(t, err)
	assert.Len(t, files.InsertBinlogs, 3)
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/_data/100/file1.log")
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/_data/101/file2.log")
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/_metadata/manifest.json")
}

func TestCollectSegmentFiles_V3MissingManifestPath(t *testing.T) {
	ctx := context.Background()

	cm := &struct{ storage.ChunkManager }{}

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   "", // V3 but no manifest
	}

	files, err := collectSegmentFiles(ctx, cm, source)

	assert.Error(t, err)
	assert.Nil(t, files)
	assert.Contains(t, err.Error(), "requires manifest_path but it is empty")
}

func TestCollectSegmentFiles_ManifestUnmarshalFailure(t *testing.T) {
	ctx := context.Background()

	cm := &struct{ storage.ChunkManager }{}

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   "invalid-json-not-a-manifest",
	}

	files, err := collectSegmentFiles(ctx, cm, source)

	assert.Error(t, err)
	assert.Nil(t, files)
	assert.Contains(t, err.Error(), "failed to unmarshal manifest path")
}

func TestCollectSegmentFiles_ManifestListFailure(t *testing.T) {
	ctx := context.Background()

	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	mList := mockey.Mock(listAllFiles).Return(nil, errors.New("storage unavailable")).Build()
	defer mList.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
	}

	files, err := collectSegmentFiles(ctx, cm, source)

	assert.Error(t, err)
	assert.Nil(t, files)
	assert.Contains(t, err.Error(), "failed to list files from manifest base path")
}

func TestCollectSegmentFiles_ManifestEmptyFiles(t *testing.T) {
	ctx := context.Background()

	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	mList := mockey.Mock(listAllFiles).Return([]string{}, nil).Build()
	defer mList.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		InsertBinlogs:  []*datapb.FieldBinlog{}, // empty pb — should NOT be used
	}

	// Empty file list is OK for V3 — segment may have only deltas
	files, err := collectSegmentFiles(ctx, cm, source)

	assert.NoError(t, err)
	assert.NotNil(t, files)
	assert.Empty(t, files.InsertBinlogs)
}

func TestCollectSegmentFiles_NoManifest(t *testing.T) {
	ctx := context.Background()

	cm := &struct{ storage.ChunkManager }{}

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV1, // V1: use pb paths
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/insert_log/111/222/333/100/file1.log"},
					{LogPath: "files/insert_log/111/222/333/100/file2.log"},
				},
			},
		},
	}

	files, err := collectSegmentFiles(ctx, cm, source)

	assert.NoError(t, err)
	assert.Len(t, files.InsertBinlogs, 2)
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/100/file1.log")
	assert.Contains(t, files.InsertBinlogs, "files/insert_log/111/222/333/100/file2.log")
}

func TestExtractFromPb(t *testing.T) {
	t.Run("extract paths from field binlogs", func(t *testing.T) {
		fieldBinlogs := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/a"},
					{LogPath: "path/b"},
				},
			},
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/c"},
				},
			},
		}
		paths := extractFromPb(fieldBinlogs)
		assert.Equal(t, []string{"path/a", "path/b", "path/c"}, paths)
	})

	t.Run("skip empty paths", func(t *testing.T) {
		fieldBinlogs := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/a"},
					{LogPath: ""},
				},
			},
		}
		paths := extractFromPb(fieldBinlogs)
		assert.Equal(t, []string{"path/a"}, paths)
	})

	t.Run("empty input", func(t *testing.T) {
		paths := extractFromPb(nil)
		assert.Empty(t, paths)
	})
}

func TestExtractIndexFiles(t *testing.T) {
	t.Run("extract index file paths", func(t *testing.T) {
		indexInfos := []*indexpb.IndexFilePathInfo{
			{IndexFilePaths: []string{"idx/a", "idx/b"}},
			{IndexFilePaths: []string{"idx/c"}},
		}
		paths := extractIndexFiles(indexInfos)
		assert.Equal(t, []string{"idx/a", "idx/b", "idx/c"}, paths)
	})

	t.Run("empty input", func(t *testing.T) {
		paths := extractIndexFiles(nil)
		assert.Empty(t, paths)
	})
}

func TestExtractTextIndexFiles(t *testing.T) {
	t.Run("extract text index file paths", func(t *testing.T) {
		textInfos := map[int64]*datapb.TextIndexStats{
			100: {Files: []string{"text/a", "text/b"}},
		}
		paths := extractTextIndexFiles(textInfos)
		assert.Equal(t, []string{"text/a", "text/b"}, paths)
	})

	t.Run("empty input", func(t *testing.T) {
		paths := extractTextIndexFiles(nil)
		assert.Empty(t, paths)
	})
}

func TestExtractJsonFiles(t *testing.T) {
	t.Run("separate by data format", func(t *testing.T) {
		jsonInfos := map[int64]*datapb.JsonKeyStats{
			100: {
				JsonKeyStatsDataFormat: 1,
				Files:                  []string{"legacy/a"},
			},
			101: {
				JsonKeyStatsDataFormat: 2,
				Files:                  []string{"new/b", "new/c"},
			},
		}
		jsonKeyFiles, jsonStatsFiles := extractJSONFiles(jsonInfos)
		assert.Equal(t, []string{"legacy/a"}, jsonKeyFiles)
		assert.ElementsMatch(t, []string{"new/b", "new/c"}, jsonStatsFiles)
	})

	t.Run("empty input", func(t *testing.T) {
		jsonKeyFiles, jsonStatsFiles := extractJSONFiles(nil)
		assert.Empty(t, jsonKeyFiles)
		assert.Empty(t, jsonStatsFiles)
	})
}

func TestGenerateMappingsFromFiles(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("generate mappings for all file types", func(t *testing.T) {
		files := &SegmentFiles{
			InsertBinlogs:     []string{"files/insert_log/111/222/333/100/log1"},
			DeltaBinlogs:      []string{"files/delta_log/111/222/333/100/delta1"},
			StatsBinlogs:      []string{"files/stats_log/111/222/333/100/stats1"},
			Bm25Binlogs:       []string{"files/bm25_stats/111/222/333/100/bm25_1"},
			VectorScalarIndex: []string{"files/index_files/1002/1/222/333/idx1"},
			TextIndex:         []string{"files/text_log/123/1/111/222/333/100/text1"},
			JSONKeyIndex:      []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
			JSONStats:         []string{"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/idx1"},
		}

		mappings, err := generateMappingsFromFiles(files, source, target)
		assert.NoError(t, err)
		assert.Len(t, mappings, 8)

		assert.Equal(t, "files/insert_log/444/555/666/100/log1",
			mappings["files/insert_log/111/222/333/100/log1"])
		assert.Equal(t, "files/delta_log/444/555/666/100/delta1",
			mappings["files/delta_log/111/222/333/100/delta1"])
		assert.Equal(t, "files/bm25_stats/444/555/666/100/bm25_1",
			mappings["files/bm25_stats/111/222/333/100/bm25_1"])
		assert.Equal(t, "files/index_files/1002/1/555/666/idx1",
			mappings["files/index_files/1002/1/222/333/idx1"])
	})

	t.Run("empty files", func(t *testing.T) {
		files := &SegmentFiles{}
		mappings, err := generateMappingsFromFiles(files, source, target)
		assert.NoError(t, err)
		assert.Empty(t, mappings)
	})

	t.Run("invalid path returns error", func(t *testing.T) {
		files := &SegmentFiles{
			InsertBinlogs: []string{"invalid/path/no/log_type"},
		}
		_, err := generateMappingsFromFiles(files, source, target)
		assert.Error(t, err)
	})
}

// TestCopySegmentAndIndexFiles_WithManifest tests the complete copy flow for StorageV3 segments.
// When storage_version >= StorageV3, insert binlogs are stored in packed format.
// Physical files (under _data/_metadata) differ from logical paths in protobuf.
// Step 3.5 must add logical pb path mappings AFTER file copying for metadata generation.
func TestCopySegmentAndIndexFiles_WithManifest(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 500, LogPath: "files/insert_log/111/222/333/100/10001", LogSize: 1024},
				},
			},
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 500, LogPath: "files/insert_log/111/222/333/101/10002", LogSize: 2048},
				},
			},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/stats_log/111/222/333/100/20001"},
				},
			},
		},
		DeltaBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 0,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/delta_log/111/222/333/0/30001"},
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("successful copy with manifest", func(t *testing.T) {
		mList := mockey.Mock(listAllFiles).To(func(_ context.Context, _ storage.ChunkManager, basePath string) ([]string, error) {
			return []string{
				"files/insert_log/111/222/333/_data/0",
				"files/insert_log/111/222/333/_metadata/manifest.json",
			}, nil
		}).Build()
		defer mList.UnPatch()

		copiedSrcPaths := make([]string, 0)
		mCopy := mockey.Mock(copyFile).
			To(func(_ context.Context, _ storage.ChunkManager, src, dst string) error {
				copiedSrcPaths = append(copiedSrcPaths, src)
				return nil
			}).Build()
		defer mCopy.UnPatch()

		cm := &struct{ storage.ChunkManager }{}

		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		assert.Equal(t, int64(666), result.SegmentId)
		assert.Equal(t, int64(1000), result.ImportedRows)

		assert.Equal(t, 2, len(result.Binlogs))
		assert.Equal(t, 1, len(result.Statslogs))
		assert.Equal(t, 1, len(result.Deltalogs))

		expectedManifestPath := packed.MarshalManifestPath("files/insert_log/444/555/666", 2)
		assert.Equal(t, expectedManifestPath, result.ManifestPath)

		assert.Len(t, copiedFiles, 4)

		for _, src := range copiedSrcPaths {
			assert.NotEqual(t, "files/insert_log/111/222/333/100/10001", src)
			assert.NotEqual(t, "files/insert_log/111/222/333/101/10002", src)
		}

		assert.Contains(t, copiedSrcPaths, "files/insert_log/111/222/333/_data/0")
		assert.Contains(t, copiedSrcPaths, "files/insert_log/111/222/333/_metadata/manifest.json")
		assert.Contains(t, copiedSrcPaths, "files/stats_log/111/222/333/100/20001")
		assert.Contains(t, copiedSrcPaths, "files/delta_log/111/222/333/0/30001")
	})

	t.Run("copy failure on physical file", func(t *testing.T) {
		mList := mockey.Mock(listAllFiles).To(func(_ context.Context, _ storage.ChunkManager, basePath string) ([]string, error) {
			return []string{"files/insert_log/111/222/333/_data/0"}, nil
		}).Build()
		defer mList.UnPatch()

		mCopy := mockey.Mock(copyFile).Return(errors.New("storage unavailable")).Build()
		defer mCopy.UnPatch()

		cm := &struct{ storage.ChunkManager }{}
		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to copy file")
		assert.Empty(t, copiedFiles)
	})

	t.Run("step 3.5 fails on invalid pb path format", func(t *testing.T) {
		badPbSource := &datapb.CopySegmentSource{
			CollectionId:   111,
			PartitionId:    222,
			SegmentId:      333,
			StorageVersion: storage.StorageV3,
			ManifestPath:   manifestPath,
			InsertBinlogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 500, LogPath: "invalid/path/no/log_type/10001", LogSize: 1024},
					},
				},
			},
		}

		mList := mockey.Mock(listAllFiles).Return([]string{"files/insert_log/111/222/333/_data/0"}, nil).Build()
		defer mList.UnPatch()

		mCopy := mockey.Mock(copyFile).Return(nil).Build()
		defer mCopy.UnPatch()

		cm := &struct{ storage.ChunkManager }{}
		result, _, err := CopySegmentAndIndexFiles(context.Background(), cm, badPbSource, target, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to generate target path for pb insert binlog")
	})
}

// TestCopySegmentAndIndexFiles_WithManifest_NoPbBinlogs tests StorageV3 segment with no pb insert binlogs.
// When InsertBinlogs in pb is empty but manifest has files, Step 3.5 has nothing to add (no logical paths).
func TestCopySegmentAndIndexFiles_WithManifest_NoPbBinlogs(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		InsertBinlogs:  []*datapb.FieldBinlog{},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mList := mockey.Mock(listAllFiles).Return([]string{"files/insert_log/111/222/333/_data/0"}, nil).Build()
	defer mList.UnPatch()

	mCopy := mockey.Mock(copyFile).Return(nil).Build()
	defer mCopy.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(0), result.ImportedRows)
	assert.Len(t, copiedFiles, 1)
	assert.NotEmpty(t, result.ManifestPath)
}

// TestCopySegmentAndIndexFiles_WithoutManifest_Unchanged verifies that StorageV1 segments
// use pb paths and do NOT trigger manifest-related logic (Step 3.5, Step 8).
func TestCopySegmentAndIndexFiles_WithoutManifest_Unchanged(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV1,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 1000, LogPath: "files/insert_log/111/222/333/100/10001", LogSize: 1024},
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mCopy := mockey.Mock(copyFile).Return(nil).Build()
	defer mCopy.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1000), result.ImportedRows)
	assert.Equal(t, int64(666), result.SegmentId)
	assert.Len(t, copiedFiles, 1)
	assert.Empty(t, result.ManifestPath)
}

func TestListAllFiles(t *testing.T) {
	t.Run("list files successfully", func(t *testing.T) {
		mWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).
			To(func(_ *storage.LocalChunkManager, _ context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
				walkFunc(&storage.ChunkObjectInfo{FilePath: "base/path/file1"})
				walkFunc(&storage.ChunkObjectInfo{FilePath: "base/path/file2"})
				return nil
			}).Build()
		defer mWalk.UnPatch()

		cm := &storage.LocalChunkManager{}
		files, err := listAllFiles(context.Background(), cm, "base/path")
		assert.NoError(t, err)
		assert.Equal(t, []string{"base/path/file1", "base/path/file2"}, files)
	})

	t.Run("walk error returns error", func(t *testing.T) {
		mWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).
			Return(errors.New("walk failed")).Build()
		defer mWalk.UnPatch()

		cm := &storage.LocalChunkManager{}
		files, err := listAllFiles(context.Background(), cm, "bad/path")
		assert.Error(t, err)
		assert.Nil(t, files)
	})
}

// TestCopySegmentAndIndexFiles_V3WithTextAndJsonStats verifies that V3 segments
// with text index and JSON key stats succeed during copy. Before the fix, V3
// segments had wrong-format paths in TextIndexFiles/JsonKeyIndexFiles (etcd
// metadata), causing "no mapping found" errors during buildIndexInfoFromSource.
// The fix skips pb path extraction for V3 (files already copied via manifest
// basePath/_stats/) and passes metadata as placeholders.
func TestCopySegmentAndIndexFiles_V3WithTextAndJsonStats(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/111/222/333", 2)

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 500, LogPath: "files/insert_log/111/222/333/100/10001", LogSize: 1024},
				},
			},
		},
		// Text index with relative paths (as stored in etcd after dual-write fix)
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			101: {
				FieldID: 101,
				BuildID: 7000,
				Version: 1,
				Files:   []string{"tokenizer.json", "index.data"},
			},
		},
		// JSON key stats with relative paths (as stored in etcd after dual-write fix)
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			102: {
				FieldID:                102,
				BuildID:                8000,
				Version:                1,
				Files:                  []string{"shared_key_index/.managed.json_0", "shared_key_index/.managed.json_1"},
				JsonKeyStatsDataFormat: 3,
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
		NewBuildIds:  map[int64]int64{7000: 7777, 8000: 9000},
	}

	mList := mockey.Mock(listAllFiles).To(func(_ context.Context, _ storage.ChunkManager, basePath string) ([]string, error) {
		// Simulate listing all files under basePath including _stats/
		return []string{
			"files/insert_log/111/222/333/_data/0",
			"files/insert_log/111/222/333/_metadata/manifest.json",
			"files/insert_log/111/222/333/_stats/text_index.101/tokenizer.json",
			"files/insert_log/111/222/333/_stats/text_index.101/index.data",
			"files/insert_log/111/222/333/_stats/json_stats.102/shared_key_index/.managed.json_0",
			"files/insert_log/111/222/333/_stats/json_stats.102/shared_key_index/.managed.json_1",
		}, nil
	}).Build()
	defer mList.UnPatch()

	mCopy := mockey.Mock(copyFile).Return(nil).Build()
	defer mCopy.UnPatch()

	cm := &struct{ storage.ChunkManager }{}
	result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(666), result.SegmentId)

	// Verify text index metadata is passed through as placeholder
	assert.Len(t, result.TextIndexInfos, 1)
	assert.Contains(t, result.TextIndexInfos, int64(101))
	// Files are passed through as-is (placeholder, not mapped)
	assert.Equal(t, []string{"tokenizer.json", "index.data"}, result.TextIndexInfos[101].GetFiles())
	assert.Equal(t, int64(7777), result.TextIndexInfos[101].GetBuildID(), "text index buildID should be remapped")

	// Verify JSON key stats metadata is passed through as placeholder with new buildID
	assert.Len(t, result.JsonKeyIndexInfos, 1)
	assert.Contains(t, result.JsonKeyIndexInfos, int64(102))
	assert.Equal(t, int64(9000), result.JsonKeyIndexInfos[102].GetBuildID(), "buildID should be remapped")
	// Files are passed through as-is (placeholder, not mapped)
	assert.Equal(t, []string{"shared_key_index/.managed.json_0", "shared_key_index/.managed.json_1"},
		result.JsonKeyIndexInfos[102].GetFiles())

	// Verify manifest path is transformed
	expectedManifestPath := packed.MarshalManifestPath("files/insert_log/444/555/666", 2)
	assert.Equal(t, expectedManifestPath, result.ManifestPath)

	// Verify _stats files are included in the copy (from listAllFiles, part of InsertBinlogs)
	assert.True(t, len(copiedFiles) >= 6, "should copy all files including _stats/")
}

func TestScanSourceDeltaLogPathsForCutoff_V2FallsBackToPackedReader(t *testing.T) {
	source := &datapb.CopySegmentSource{
		SegmentId:      333,
		CutoffTs:       100,
		StorageVersion: storage.StorageV2,
		DeltaBinlogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{
				{LogPath: "files/delta_log/111/222/333/31"},
				{LogPath: "files/delta_log/111/222/333/32"},
				{LogPath: "files/delta_log/111/222/333/31"},
			},
		}},
	}

	expectedPaths := []string{
		"files/delta_log/111/222/333/31",
		"files/delta_log/111/222/333/32",
	}
	callCount := 0
	mockScan := mockey.Mock(scanDeltaLogPathsForCutoff).To(
		func(paths []string, pkType schemapb.DataType, cutoffTs uint64, options ...storage.RwOption) ([]cutoffDeleteEntry, error) {
			callCount++
			assert.Equal(t, expectedPaths, paths)
			assert.Equal(t, schemapb.DataType_Int64, pkType)
			assert.Equal(t, uint64(100), cutoffTs)
			if callCount == 1 {
				return nil, errors.New("legacy delta read failed")
			}
			return []cutoffDeleteEntry{
				{pk: storage.NewInt64PrimaryKey(10), ts: 90},
				{pk: storage.NewInt64PrimaryKey(11), ts: 91},
			}, nil
		},
	).Build()
	defer mockScan.UnPatch()

	deletes, err := scanSourceDeltaLogPathsForCutoff(
		&struct{ storage.ChunkManager }{},
		source,
		&schemapb.CollectionSchema{Name: "test"},
		schemapb.DataType_Int64,
		100,
		&indexpb.StorageConfig{},
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, callCount)
	assert.Len(t, deletes, 2)
	assert.Equal(t, int64(10), deletes[0].pk.GetValue())
	assert.Equal(t, uint64(90), deletes[0].ts)
	assert.Equal(t, int64(11), deletes[1].pk.GetValue())
	assert.Equal(t, uint64(91), deletes[1].ts)
}

func TestScanInsertBinlogsForCutoff_V2ReadsSystemTimestampWithUserSchema(t *testing.T) {
	paramtable.Init()

	const dim = 2
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	schemaWithSystemFields := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	userSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	insertData := &storage.InsertData{Data: map[storage.FieldID]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: []int64{1, 2, 3}},
		common.TimeStampField: &storage.Int64FieldData{Data: []int64{90, 110, 120}},
		100:                   &storage.Int64FieldData{Data: []int64{1, 2, 3}},
		101:                   &storage.FloatVectorFieldData{Data: []float32{0.1, 0.2, 1.1, 1.2, 2.1, 2.2}, Dim: dim},
	}}

	arrowSchema, err := storage.ConvertToArrowSchema(schemaWithSystemFields, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, insertData, schemaWithSystemFields))
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		101:                   3,
	})
	defer rec.Release()

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		111,
		222,
		333,
		schemaWithSystemFields,
		allocator.NewLocalAllocator(10, math.MaxInt64),
		1024,
		1000,
		storage.WithVersion(storage.StorageV2),
		storage.WithBufferSize(1024*1024),
		storage.WithMultiPartUploadSize(0),
		storage.WithColumnGroups([]storagecommon.ColumnGroup{
			{GroupID: storagecommon.DefaultShortColumnGroupID, Columns: []int{0, 1, 2}, Fields: []int64{common.RowIDField, common.TimeStampField, 100}},
			{GroupID: 101, Columns: []int{3}, Fields: []int64{101}},
		}),
		storage.WithStorageConfig(storageConfig),
		storage.WithUploader(func(context.Context, map[string][]byte) error { return nil }),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())
	fieldBinlogs, _, _, _, _ := writer.GetLogs()

	pkField := userSchema.GetFields()[0]
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		InsertBinlogs:  storage.SortFieldBinlogs(fieldBinlogs),
		StorageVersion: storage.StorageV2,
	}
	preCutoff, postCutoff, err := scanInsertBinlogsForCutoff(
		ctx,
		source,
		userSchema,
		pkField,
		100,
		storageConfig,
	)
	require.NoError(t, err)

	assert.Contains(t, preCutoff, cutoffPK{int64PK: 1})
	assert.NotContains(t, preCutoff, cutoffPK{int64PK: 2})
	assert.Len(t, postCutoff, 2)
	assert.Equal(t, uint64(110), postCutoff[cutoffPK{int64PK: 2}].ts)
	assert.Equal(t, uint64(120), postCutoff[cutoffPK{int64PK: 3}].ts)
}

type cutoffFakeRecordReader struct {
	records []storage.Record
	idx     int
}

func (r *cutoffFakeRecordReader) Next() (storage.Record, error) {
	if r.idx >= len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.idx]
	r.idx++
	return record, nil
}

func (r *cutoffFakeRecordReader) Close() error {
	return nil
}

type cutoffFakeBinlogRecordWriter struct {
	fieldBinlogs map[storage.FieldID]*datapb.FieldBinlog
	statsLog     *datapb.FieldBinlog
	bm25StatsLog map[storage.FieldID]*datapb.FieldBinlog
	manifest     string
	schema       *schemapb.CollectionSchema
}

func (w *cutoffFakeBinlogRecordWriter) Write(storage.Record) error {
	return nil
}

func (w *cutoffFakeBinlogRecordWriter) GetWrittenUncompressed() uint64 {
	return 0
}

func (w *cutoffFakeBinlogRecordWriter) Close() error {
	return nil
}

func (w *cutoffFakeBinlogRecordWriter) GetLogs() (
	map[storage.FieldID]*datapb.FieldBinlog,
	*datapb.FieldBinlog,
	map[storage.FieldID]*datapb.FieldBinlog,
	string,
	[]int64,
) {
	return w.fieldBinlogs, w.statsLog, w.bm25StatsLog, w.manifest, nil
}

func (w *cutoffFakeBinlogRecordWriter) GetRowNum() int64 {
	return 0
}

func (w *cutoffFakeBinlogRecordWriter) FlushChunk() error {
	return nil
}

func (w *cutoffFakeBinlogRecordWriter) GetBufferUncompressed() uint64 {
	return 0
}

func (w *cutoffFakeBinlogRecordWriter) Schema() *schemapb.CollectionSchema {
	return w.schema
}

func TestScanInsertManifestForCutoff_ReadsManifestPKs(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{90, 110, 120}, nil)
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		100:                   0,
		common.TimeStampField: 1,
	})

	mockReader := mockey.Mock(storage.NewManifestRecordReader).To(
		func(
			ctx context.Context,
			manifest string,
			schema *schemapb.CollectionSchema,
			options ...storage.RwOption,
		) (storage.RecordReader, error) {
			assert.Equal(t, "manifest", manifest)
			require.Len(t, schema.GetFields(), 2)
			assert.Equal(t, "7", func() string {
				for _, property := range schema.GetProperties() {
					if property.GetKey() == common.EncryptionEzIDKey {
						return property.GetValue()
					}
				}
				return ""
			}())
			assert.Equal(t, int64(100), schema.GetFields()[0].GetFieldID())
			assert.Equal(t, int64(common.TimeStampField), schema.GetFields()[1].GetFieldID())
			return &cutoffFakeRecordReader{records: []storage.Record{rec}}, nil
		},
	).Build()
	defer mockReader.UnPatch()

	source := &datapb.CopySegmentSource{
		CollectionId:   19530,
		ManifestPath:   "manifest",
		StorageVersion: storage.StorageV3,
	}
	preCutoff, postCutoff, err := scanInsertManifestForCutoff(
		context.Background(),
		source,
		&schemapb.CollectionSchema{
			Name: "test",
			Properties: []*commonpb.KeyValuePair{
				{Key: common.EncryptionEzIDKey, Value: "7"},
			},
		},
		&schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		100,
		&indexpb.StorageConfig{},
	)

	require.NoError(t, err)
	assert.Contains(t, preCutoff, cutoffPK{int64PK: 1})
	assert.Len(t, postCutoff, 2)
	assert.Equal(t, uint64(110), postCutoff[cutoffPK{int64PK: 2}].ts)
	assert.Equal(t, uint64(120), postCutoff[cutoffPK{int64PK: 3}].ts)
}

func TestBuildCutoffTextColumnConfigs_UsesRewriteMode(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "varchar", DataType: schemapb.DataType_VarChar},
		},
	}

	configs := buildCutoffTextColumnConfigs(schema, "files/insert_log/444/555")

	require.Len(t, configs, 1)
	assert.Equal(t, int64(101), configs[0].FieldID)
	assert.Equal(t, "files/insert_log/444/555/lobs/101", configs[0].LobBasePath)
	assert.True(t, configs[0].RewriteMode)
	assert.Equal(t, paramtable.Get().DataNodeCfg.TextInlineThreshold.GetAsInt64(), configs[0].InlineThreshold)
	assert.Equal(t, paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(), configs[0].MaxLobFileBytes)
	assert.Equal(t, paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(), configs[0].FlushThresholdBytes)
}

func TestRewriteV2InsertBinlogsForCutoff_RewritesCrossingBatch(t *testing.T) {
	paramtable.Init()

	const dim = 2
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	schemaWithSystemFields := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	userSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	insertData := &storage.InsertData{Data: map[storage.FieldID]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: []int64{1, 2, 3}},
		common.TimeStampField: &storage.Int64FieldData{Data: []int64{90, 110, 120}},
		100:                   &storage.Int64FieldData{Data: []int64{1, 2, 3}},
		101:                   &storage.FloatVectorFieldData{Data: []float32{0.1, 0.2, 1.1, 1.2, 2.1, 2.2}, Dim: dim},
	}}

	arrowSchema, err := storage.ConvertToArrowSchema(schemaWithSystemFields, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, insertData, schemaWithSystemFields))
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		101:                   3,
	})
	defer rec.Release()

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		111,
		222,
		333,
		schemaWithSystemFields,
		allocator.NewLocalAllocator(10, math.MaxInt64),
		1024,
		1000,
		storage.WithVersion(storage.StorageV2),
		storage.WithBufferSize(1024*1024),
		storage.WithMultiPartUploadSize(0),
		storage.WithColumnGroups([]storagecommon.ColumnGroup{
			{GroupID: storagecommon.DefaultShortColumnGroupID, Columns: []int{0, 1, 2}, Fields: []int64{common.RowIDField, common.TimeStampField, 100}},
			{GroupID: 101, Columns: []int{3}, Fields: []int64{101}},
		}),
		storage.WithStorageConfig(storageConfig),
		storage.WithUploader(func(context.Context, map[string][]byte) error { return nil }),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())
	fieldBinlogs, _, _, _, _ := writer.GetLogs()

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		InsertBinlogs:  storage.SortFieldBinlogs(fieldBinlogs),
		CutoffTs:       100,
		StorageVersion: storage.StorageV2,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666}
	copyInsert, metaInsert, mappings, copiedFiles, mutated, err := rewriteV2InsertBinlogsForCutoff(
		ctx, source, target, userSchema, storageConfig)
	require.NoError(t, err)
	assert.True(t, mutated)
	assert.Empty(t, copyInsert)
	require.Len(t, metaInsert, 2)
	assert.Len(t, mappings, 2)
	assert.Len(t, copiedFiles, 2)
	for _, field := range metaInsert {
		require.Len(t, field.GetBinlogs(), 1)
		assert.Equal(t, int64(1), field.GetBinlogs()[0].GetEntriesNum())
		sourcePath := field.GetBinlogs()[0].GetLogPath()
		targetPath := mappings[sourcePath]
		require.NotEmpty(t, targetPath)
		field.GetBinlogs()[0].LogPath = targetPath
	}

	rewriteSchema := buildCutoffRewriteSchema(userSchema)
	reader, err := storage.NewBinlogRecordReader(ctx, metaInsert, rewriteSchema,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()))
	require.NoError(t, err)
	defer reader.Close()
	got, err := reader.Next()
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, 1, got.Len())
	assert.Equal(t, int64(90), got.Column(common.TimeStampField).(*array.Int64).Value(0))
	assert.Equal(t, int64(1), got.Column(100).(*array.Int64).Value(0))
	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestRewriteManifestSegmentForCutoff_FiltersRows(t *testing.T) {
	paramtable.Init()

	const dim = 2
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	schemaWithSystemFields := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	userSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	insertData := &storage.InsertData{Data: map[storage.FieldID]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: []int64{1, 2}},
		common.TimeStampField: &storage.Int64FieldData{Data: []int64{90, 110}},
		100:                   &storage.Int64FieldData{Data: []int64{1, 2}},
		101:                   &storage.FloatVectorFieldData{Data: []float32{0.1, 0.2, 1.1, 1.2}, Dim: dim},
	}}

	arrowSchema, err := storage.ConvertToArrowSchema(schemaWithSystemFields, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, insertData, schemaWithSystemFields))
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		101:                   3,
	})
	defer rec.Release()

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		111,
		222,
		333,
		schemaWithSystemFields,
		allocator.NewLocalAllocator(10, math.MaxInt64),
		1024,
		1000,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(1024*1024),
		storage.WithMultiPartUploadSize(0),
		storage.WithCollectionID(111),
		storage.WithCollectionProperties(schemaWithSystemFields.GetProperties()),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())
	_, _, _, manifestPath, _ := writer.GetLogs()
	require.NotEmpty(t, manifestPath)

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		CutoffTs:       100,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666}

	result, copiedFiles, err := rewriteManifestSegmentForCutoff(ctx, source, target, userSchema, storageConfig)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.GetImportedRows())
	assert.True(t, result.GetUpdateNumRows())
	targetBasePath, _, err := packed.UnmarshalManifestPath(result.GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, "444/555/666"), targetBasePath)
	assert.NotEmpty(t, copiedFiles)

	rewriteSchema := buildCutoffRewriteSchema(userSchema)
	reader, err := storage.NewManifestRecordReader(ctx, result.GetManifestPath(), rewriteSchema,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(packed.DefaultReadBufferSize),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()))
	require.NoError(t, err)
	defer reader.Close()
	got, err := reader.Next()
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, 1, got.Len())
	assert.Equal(t, int64(90), got.Column(common.TimeStampField).(*array.Int64).Value(0))
	assert.Equal(t, int64(1), got.Column(100).(*array.Int64).Value(0))
	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestRewriteManifestSegmentForCutoff_AddsWriterStatsToManifest(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath("files/insert_log/111/222/333", 1),
		CutoffTs:       100,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "bm25", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
		},
	}

	targetBasePath := path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, "444/555/666")
	targetManifest := packed.MarshalManifestPath(targetBasePath, 1)
	statsLog := &datapb.FieldBinlog{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{{
			LogPath:    path.Join(targetBasePath, "_stats/bloom_filter.100/1"),
			MemorySize: 64,
		}},
	}
	bm25StatsLog := &datapb.FieldBinlog{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{
			LogPath:    path.Join(targetBasePath, "_stats/bm25.102/2"),
			MemorySize: 32,
		}},
	}
	writer := &cutoffFakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			storagecommon.DefaultShortColumnGroupID: {
				FieldID: storagecommon.DefaultShortColumnGroupID,
				Binlogs: []*datapb.Binlog{{
					LogPath:    path.Join(targetBasePath, "_data/0"),
					EntriesNum: 1,
				}},
			},
		},
		statsLog:     statsLog,
		bm25StatsLog: map[storage.FieldID]*datapb.FieldBinlog{102: bm25StatsLog},
		manifest:     targetManifest,
		schema:       buildCutoffRewriteSchema(schema),
	}

	readerPatch := mockey.Mock(storage.NewManifestRecordReader).Return(&cutoffFakeRecordReader{}, nil).Build()
	defer readerPatch.UnPatch()
	writerPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer writerPatch.UnPatch()
	sortPatch := mockey.Mock(storage.Sort).Return(1, &storage.SortTimings{}, nil).Build()
	defer sortPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(nil, nil).Build()
	defer deltaPatch.UnPatch()

	var gotStats []packed.StatEntry
	addStatsPatch := mockey.Mock(packed.AddStatsToManifest).To(
		func(manifestPath string, _ *indexpb.StorageConfig, stats []packed.StatEntry) (string, error) {
			require.Equal(t, targetManifest, manifestPath)
			gotStats = append(gotStats, stats...)
			return packed.MarshalManifestPath(targetBasePath, 2), nil
		}).Build()
	defer addStatsPatch.UnPatch()

	result, copiedFiles, err := rewriteManifestSegmentForCutoff(ctx, source, target, schema, storageConfig)

	require.NoError(t, err)
	assert.Equal(t, packed.MarshalManifestPath(targetBasePath, 2), result.GetManifestPath())
	require.Len(t, gotStats, 2)
	assert.Equal(t, "bloom_filter.100", gotStats[0].Key)
	assert.Equal(t, statsLog.GetBinlogs()[0].GetLogPath(), gotStats[0].Files[0])
	assert.Equal(t, "bm25.102", gotStats[1].Key)
	assert.Equal(t, bm25StatsLog.GetBinlogs()[0].GetLogPath(), gotStats[1].Files[0])
	assert.Contains(t, copiedFiles, statsLog.GetBinlogs()[0].GetLogPath())
	assert.Contains(t, copiedFiles, bm25StatsLog.GetBinlogs()[0].GetLogPath())
}

func TestScanSourceDeltaLogsForCutoff_ReadsManifestDeltas(t *testing.T) {
	source := &datapb.CopySegmentSource{CollectionId: 111, ManifestPath: "manifest"}
	schema := &schemapb.CollectionSchema{Name: "test"}
	expectedDeletes := []cutoffDeleteEntry{{pk: storage.NewInt64PrimaryKey(1), ts: 90}}

	mockManifest := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string{"delta-1", "delta-2"}, nil).Build()
	defer mockManifest.UnPatch()
	mockScan := mockey.Mock(scanDeltaLogPathsForCutoff).To(
		func(paths []string, pkType schemapb.DataType, cutoffTs uint64, options ...storage.RwOption) ([]cutoffDeleteEntry, error) {
			assert.Equal(t, []string{"delta-1", "delta-2"}, paths)
			assert.Equal(t, schemapb.DataType_Int64, pkType)
			assert.Equal(t, uint64(100), cutoffTs)
			return expectedDeletes, nil
		},
	).Build()
	defer mockScan.UnPatch()

	deletes, err := scanSourceDeltaLogsForCutoff(source, schema, schemapb.DataType_Int64, 100, &indexpb.StorageConfig{})

	require.NoError(t, err)
	assert.Equal(t, expectedDeletes, deletes)
}

func TestWriteAndScanDeltaLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(dir))
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}
	deltaPath := fmt.Sprintf("%s/delta_log/444/555/666/900", dir)
	deletes := []cutoffDeleteEntry{
		{pk: storage.NewInt64PrimaryKey(1), ts: 90},
		{pk: storage.NewInt64PrimaryKey(2), ts: 110},
	}

	deltaLog, err := writeDeltaLog(ctx, cm, target, &schemapb.CollectionSchema{Name: "test"}, schemapb.DataType_Int64, 900, deltaPath, deletes, storage.StorageV1, &indexpb.StorageConfig{})
	require.NoError(t, err)
	require.Len(t, deltaLog.GetBinlogs(), 1)
	assert.Equal(t, int64(2), deltaLog.GetBinlogs()[0].GetEntriesNum())

	retained, err := scanDeltaLogPathsForCutoff(
		[]string{deltaPath},
		schemapb.DataType_Int64,
		100,
		storage.WithVersion(storage.StorageV1),
		storage.WithDownloader(cm.MultiRead),
	)
	require.NoError(t, err)
	require.Len(t, retained, 1)
	assert.Equal(t, int64(1), retained[0].pk.GetValue())
	assert.Equal(t, uint64(90), retained[0].ts)
}

func TestCutoffHelpers(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/1/2/3", 9)
	assert.Equal(t, "files/insert_log/1/2/3/_metadata/manifest-9.avro", manifestPhysicalPath(manifestPath))
	assert.Empty(t, manifestPhysicalPath("not-json"))
	assert.True(t, isManifestControlFile("files/insert_log/1/2/3", "files/insert_log/1/2/3/_metadata/manifest-1.avro"))
	assert.True(t, isManifestControlFile("files/insert_log/1/2/3", "files/insert_log/1/2/3/_delta/99"))
	assert.False(t, isManifestControlFile("files/insert_log/1/2/3", "files/insert_log/1/2/3/_data/part"))

	_, err := findPrimaryKeyField(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64},
	}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key field not found")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "pk", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(120)
	builder.Field(1).(*array.StringBuilder).Append("pk-a")
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.TimeStampField: 0,
		100:                   1,
	})
	defer rec.Release()

	visited := false
	err = visitPKTimestampRecord(rec, 100, schemapb.DataType_VarChar, func(key cutoffPK, pk storage.PrimaryKey, ts uint64) error {
		visited = true
		assert.Equal(t, cutoffPK{strPK: "pk-a"}, key)
		assert.Equal(t, "pk-a", pk.GetValue())
		assert.Equal(t, uint64(120), ts)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, visited)

	err = visitPKTimestampRecord(rec, 100, schemapb.DataType_Bool, func(cutoffPK, storage.PrimaryKey, uint64) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported primary key type")
}

func TestCopySegmentAndIndexFiles_V2WithCutoff(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		},
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		CutoffTs:       100,
		StorageVersion: storage.StorageV2,
		InsertBinlogs:  []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/insert_log/111/222/333/0/11", EntriesNum: 3}}}},
		StatsBinlogs:   []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/stats_log/111/222/333/100/21"}}}},
		DeltaBinlogs:   []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/delta_log/111/222/333/31"}}}},
		ManifestPath:   "",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}
	copySource := proto.Clone(source).(*datapb.CopySegmentSource)
	copySource.DeltaBinlogs = nil
	metaSource := proto.Clone(copySource).(*datapb.CopySegmentSource)
	metaSource.DeltaBinlogs = []*datapb.FieldBinlog{{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{{
			LogPath:    "files/delta_log/111/222/333/31",
			EntriesNum: 1,
		}},
	}}
	mockPrepare := mockey.Mock(prepareNonManifestRestoreCutoff).Return(&restoreCutoffPlan{
		copySource:    copySource,
		metaSource:    metaSource,
		mappings:      map[string]string{"files/delta_log/111/222/333/31": "files/delta_log/444/555/666/31"},
		copiedFiles:   []string{"files/delta_log/444/555/666/31"},
		updateNumRows: true,
	}, nil).Build()
	defer mockPrepare.UnPatch()

	copiedSrcPaths := make([]string, 0)
	mockCopy := mockey.Mock(copyFile).To(func(_ context.Context, _ storage.ChunkManager, src, dst string) error {
		copiedSrcPaths = append(copiedSrcPaths, src)
		return nil
	}).Build()
	defer mockCopy.UnPatch()

	result, copiedFiles, err := CopySegmentAndIndexFiles(
		context.Background(),
		&struct{ storage.ChunkManager }{},
		source,
		target,
		nil,
		CopySegmentFileOptions{
			Schema:        schema,
			StorageConfig: &indexpb.StorageConfig{RootPath: "files"},
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, int64(3), result.GetImportedRows())
	assert.True(t, result.GetUpdateNumRows())
	assert.Len(t, result.GetDeltalogs(), 1)
	assert.Equal(t, int64(100), result.GetDeltalogs()[0].GetFieldID())
	assert.Equal(t, int64(1), result.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())
	assert.Contains(t, copiedSrcPaths, "files/insert_log/111/222/333/0/11")
	assert.Contains(t, copiedSrcPaths, "files/stats_log/111/222/333/100/21")
	assert.NotContains(t, copiedSrcPaths, "files/delta_log/111/222/333/31")
	assert.Contains(t, copiedFiles, "files/delta_log/444/555/666/31")
}

func TestCopySegmentAndIndexFiles_L0WithCutoff(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    -1,
		SegmentId:      333,
		CutoffTs:       100,
		DeltaBinlogs:   []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "files/delta_log/111/-1/333/1"}}}},
		StorageVersion: storage.StorageV3,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  -1,
		SegmentId:    666,
	}

	t.Run("rewrite retained deletes", func(t *testing.T) {
		copySource := proto.Clone(source).(*datapb.CopySegmentSource)
		copySource.DeltaBinlogs = nil
		metaSource := proto.Clone(copySource).(*datapb.CopySegmentSource)
		metaSource.DeltaBinlogs = []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    "files/delta_log/111/-1/333/1",
				EntriesNum: 2,
			}},
		}}
		mockPrepare := mockey.Mock(prepareNonManifestRestoreCutoff).Return(&restoreCutoffPlan{
			copySource:    copySource,
			metaSource:    metaSource,
			mappings:      map[string]string{"files/delta_log/111/-1/333/1": "files/delta_log/444/-1/666/1"},
			copiedFiles:   []string{"files/delta_log/444/-1/666/1"},
			updateNumRows: true,
			importedRows:  2,
		}, nil).Build()
		defer mockPrepare.UnPatch()

		result, copiedFiles, err := CopySegmentAndIndexFiles(
			context.Background(),
			&struct{ storage.ChunkManager }{},
			source,
			target,
			nil,
			CopySegmentFileOptions{
				Schema:        schema,
				StorageConfig: &indexpb.StorageConfig{RootPath: "files"},
			},
		)

		assert.NoError(t, err)
		assert.True(t, result.GetUpdateNumRows())
		assert.Equal(t, int64(2), result.GetImportedRows())
		assert.Len(t, result.GetDeltalogs(), 1)
		assert.Equal(t, []string{"files/delta_log/444/-1/666/1"}, copiedFiles)
	})

	t.Run("all deletes after cutoff", func(t *testing.T) {
		copySource := proto.Clone(source).(*datapb.CopySegmentSource)
		copySource.DeltaBinlogs = nil
		metaSource := proto.Clone(copySource).(*datapb.CopySegmentSource)
		mockPrepare := mockey.Mock(prepareNonManifestRestoreCutoff).Return(&restoreCutoffPlan{
			copySource:    copySource,
			metaSource:    metaSource,
			mappings:      map[string]string{},
			updateNumRows: true,
			importedRows:  0,
		}, nil).Build()
		defer mockPrepare.UnPatch()

		result, copiedFiles, err := CopySegmentAndIndexFiles(
			context.Background(),
			&struct{ storage.ChunkManager }{},
			source,
			target,
			nil,
			CopySegmentFileOptions{
				Schema:        schema,
				StorageConfig: &indexpb.StorageConfig{RootPath: "files"},
			},
		)

		assert.NoError(t, err)
		assert.True(t, result.GetUpdateNumRows())
		assert.Equal(t, int64(0), result.GetImportedRows())
		assert.Empty(t, result.GetDeltalogs())
		assert.Empty(t, copiedFiles)
	})
}
