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
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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
		name       string
		sourcePath string
		indexType  string
		wantPath   string
		wantErr    bool
	}{
		{
			name:       "vector scalar index path",
			sourcePath: "files/index_files/111/222/333/100/1001/1002/scalar_index",
			indexType:  IndexTypeVectorScalar,
			wantPath:   "files/index_files/444/555/666/100/1001/1002/scalar_index",
			wantErr:    false,
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
			indexType:  IndexTypeVectorScalar,
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "invalid - path too short",
			sourcePath: "files/index_files/111",
			indexType:  IndexTypeVectorScalar,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := generateTargetIndexPath(tt.sourcePath, source, target, tt.indexType)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantPath, gotPath)
			}
		})
	}
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
		result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, true)
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
		result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, false)
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
		result, _, err := transformFieldBinlogs(srcWithEmpty, mappings, false)
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
		result, totalRows, err := transformFieldBinlogs(srcWithUnmapped, mappings, true)
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
				IndexFilePaths: []string{"files/index_files/111/222/333/100/1001/1002/index1"},
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
				IndexFilePaths: []string{"files/index_files/111/222/333/100/1001/1002/index1"},
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
		"files/index_files/111/222/333/100/1001/1002/index1":                "files/index_files/444/555/666/100/1001/1002/index1",
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
			"files/index_files/111/222/333/100/1001/1002/index1": "files/index_files/444/555/666/100/1001/1002/index1",
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, partialMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for text index file")
	})

	t.Run("error on missing json index mapping", func(t *testing.T) {
		partialMappings := map[string]string{
			"files/index_files/111/222/333/100/1001/1002/index1": "files/index_files/444/555/666/100/1001/1002/index1",
			"files/text_log/123/1/111/222/333/100/text1":         "files/text_log/123/1/444/555/666/100/text1",
		}
		_, _, _, err := buildIndexInfoFromSource(source, target, partialMappings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping found for JSON index file")
	})

	t.Run("build all index info", func(t *testing.T) {
		indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, mappings)
		assert.NoError(t, err)

		// Verify vector/scalar index info
		assert.Equal(t, 1, len(indexInfos))
		assert.NotNil(t, indexInfos[100])
		assert.Equal(t, int64(100), indexInfos[100].FieldId)
		assert.Equal(t, int64(1001), indexInfos[100].IndexId)
		assert.Equal(t, int64(1002), indexInfos[100].BuildId)
		assert.Equal(t, int64(5000), indexInfos[100].IndexSize)
		assert.Equal(t, "files/index_files/444/555/666/100/1001/1002/index1", indexInfos[100].IndexFilePaths[0])

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

func TestShortenIndexFilePaths(t *testing.T) {
	tests := []struct {
		name      string
		fullPaths []string
		expected  []string
	}{
		{
			name: "vector/scalar index paths",
			fullPaths: []string{
				"files/index_files/444/555/666/100/1001/1002/scalar_index",
				"files/index_files/444/555/666/100/1001/1002/vector_index",
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
			result := shortenSingleJsonStatsPath(tt.inputPath)
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

	result := shortenJsonStatsPath(jsonStats)

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

	result := shortenJsonStatsPath(jsonStats)

	assert.Equal(t, 1, len(result))
	assert.NotNil(t, result[102])
	assert.Equal(t, int64(102), result[102].FieldID)
	assert.Equal(t, 1, len(result[102].Files))
	assert.Equal(t, "meta.json", result[102].Files[0])
}

func TestShortenSingleJsonStatsPath_EdgeCases(t *testing.T) {
	t.Run("already_shortened_meta", func(t *testing.T) {
		result := shortenSingleJsonStatsPath("meta.json")
		assert.Equal(t, "meta.json", result)
	})

	t.Run("already_shortened_shared_key", func(t *testing.T) {
		result := shortenSingleJsonStatsPath("shared_key_index/inverted_index_0")
		assert.Equal(t, "shared_key_index/inverted_index_0", result)
	})

	t.Run("full_path_meta_json", func(t *testing.T) {
		fullPath := "files/json_stats/2/123/1/444/555/666/100/meta.json"
		result := shortenSingleJsonStatsPath(fullPath)
		assert.Equal(t, "meta.json", result)
	})

	t.Run("full_path_nested_file", func(t *testing.T) {
		fullPath := "files/json_stats/2/123/1/444/555/666/100/subdir/file.dat"
		result := shortenSingleJsonStatsPath(fullPath)
		assert.Equal(t, "subdir/file.dat", result)
	})
}

func TestTransformManifestPath(t *testing.T) {
	source := &datapb.CopySegmentSource{
		SegmentId:    449104621518610065,
		CollectionId: 449104612037410004,
		PartitionId:  449104621518610066,
	}

	target := &datapb.CopySegmentTarget{
		SegmentId:    2001,
		CollectionId: 111,
		PartitionId:  222,
	}

	sourceManifest := packed.MarshalManifestPath(
		"files/insert_log/449104612037410004/449104621518610066/449104621518610065",
		2,
	)

	targetManifest, err := transformManifestPath(sourceManifest, source, target)
	assert.NoError(t, err)

	basePath, version, err := packed.UnmarshalManfestPath(targetManifest)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.Contains(t, basePath, "111")
	assert.Contains(t, basePath, "222")
	assert.Contains(t, basePath, "2001")
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
		jsonKeyFiles, jsonStatsFiles := extractJsonFiles(jsonInfos)
		assert.Equal(t, []string{"legacy/a"}, jsonKeyFiles)
		assert.ElementsMatch(t, []string{"new/b", "new/c"}, jsonStatsFiles)
	})

	t.Run("empty input", func(t *testing.T) {
		jsonKeyFiles, jsonStatsFiles := extractJsonFiles(nil)
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
			VectorScalarIndex: []string{"files/index_files/111/222/333/100/1001/1002/idx1"},
			TextIndex:         []string{"files/text_log/123/1/111/222/333/100/text1"},
			JsonKeyIndex:      []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
			JsonStats:         []string{"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/idx1"},
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
		assert.Equal(t, "files/index_files/444/555/666/100/1001/1002/idx1",
			mappings["files/index_files/111/222/333/100/1001/1002/idx1"])
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
