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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
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
			sourcePath: "files/index_files/1001/1/222/333/scalar_index",
			indexType:  IndexTypeVectorScalar,
			wantPath:   "files/index_files/1001/1/555/666/scalar_index",
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
}

func TestCreateFileMappings(t *testing.T) {
	source := &datapb.CopySegmentSource{
		CollectionId: 111,
		PartitionId:  222,
		SegmentId:    333,
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/insert_log/111/222/333/100/log1.log"},
				},
			},
		},
		DeltaBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/delta_log/111/222/333/100/delta1.log"},
				},
			},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/stats_log/111/222/333/100/stats1.log"},
				},
			},
		},
		Bm25Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "files/bm25_stats/111/222/333/100/bm25_1.log"},
				},
			},
		},
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100,
				IndexFilePaths: []string{"files/index_files/1001/1/222/333/index1"},
			},
		},
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			100: {
				FieldID: 100,
				Files:   []string{"files/text_log/123/1/111/222/333/100/text1"},
			},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			101: {
				FieldID:                101,
				JsonKeyStatsDataFormat: 1, // Legacy format
				Files:                  []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
			},
			102: {
				FieldID:                102,
				BuildID:                3002,
				Version:                1,
				JsonKeyStatsDataFormat: 2, // New format
				Files: []string{
					"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/index1",
					"files/json_stats/2/3002/1/111/222/333/102/shredding_data/data1",
				},
			},
		},
	}

	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("create all file mappings", func(t *testing.T) {
		mappings, err := createFileMappings(source, target)
		assert.NoError(t, err)
		assert.Equal(t, 9, len(mappings)) // Updated: 7 + 2 JSON Stats files

		// Verify insert binlog mapping
		assert.Equal(t, "files/insert_log/444/555/666/100/log1.log",
			mappings["files/insert_log/111/222/333/100/log1.log"])

		// Verify delta binlog mapping
		assert.Equal(t, "files/delta_log/444/555/666/100/delta1.log",
			mappings["files/delta_log/111/222/333/100/delta1.log"])

		// Verify stats binlog mapping
		assert.Equal(t, "files/stats_log/444/555/666/100/stats1.log",
			mappings["files/stats_log/111/222/333/100/stats1.log"])

		// Verify BM25 binlog mapping
		assert.Equal(t, "files/bm25_stats/444/555/666/100/bm25_1.log",
			mappings["files/bm25_stats/111/222/333/100/bm25_1.log"])

		// Verify vector/scalar index mapping
		assert.Equal(t, "files/index_files/1001/1/555/666/index1",
			mappings["files/index_files/1001/1/222/333/index1"])

		// Verify text index mapping
		assert.Equal(t, "files/text_log/123/1/444/555/666/100/text1",
			mappings["files/text_log/123/1/111/222/333/100/text1"])

		// Verify JSON key index mapping (legacy format)
		assert.Equal(t, "files/json_key_index_log/123/1/444/555/666/101/json1",
			mappings["files/json_key_index_log/123/1/111/222/333/101/json1"])

		// Verify JSON Stats mapping (new format)
		assert.Equal(t, "files/json_stats/2/3002/1/444/555/666/102/shared_key_index/index1",
			mappings["files/json_stats/2/3002/1/111/222/333/102/shared_key_index/index1"])
		assert.Equal(t, "files/json_stats/2/3002/1/444/555/666/102/shredding_data/data1",
			mappings["files/json_stats/2/3002/1/111/222/333/102/shredding_data/data1"])
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
		mockCM := mocks.NewChunkManager(t)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)

		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), mockCM, source, target, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(666), result.SegmentId)
		assert.Equal(t, int64(1000), result.ImportedRows)
		assert.Equal(t, 1, len(result.Binlogs))
		assert.Equal(t, 1, len(result.IndexInfos))
		assert.Len(t, copiedFiles, 2)
	})

	t.Run("copy failure", func(t *testing.T) {
		mockCM := mocks.NewChunkManager(t)
		mockCM.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("copy failed")).Once()

		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), mockCM, source, target, nil)

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

	t.Run("build all index info", func(t *testing.T) {
		indexInfos, textIndexInfos, jsonKeyIndexInfos := buildIndexInfoFromSource(source, target, mappings)

		// Verify vector/scalar index info
		assert.Equal(t, 1, len(indexInfos))
		assert.NotNil(t, indexInfos[100])
		assert.Equal(t, int64(100), indexInfos[100].FieldId)
		assert.Equal(t, int64(1001), indexInfos[100].IndexId)
		assert.Equal(t, int64(1002), indexInfos[100].BuildId)
		assert.Equal(t, int64(5000), indexInfos[100].IndexSize)
		assert.Equal(t, "files/index_files/1002/1/555/666/index1", indexInfos[100].IndexFilePaths[0])

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
		cm := mocks.NewChunkManager(t)
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

		// Mock successful copies
		cm.EXPECT().Copy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)

		result, copiedFiles, err := CopySegmentAndIndexFiles(context.Background(), cm, source, target, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, copiedFiles, 2)
		assert.Contains(t, copiedFiles, "files/insert_log/444/555/666/1/10001")
		assert.Contains(t, copiedFiles, "files/insert_log/444/555/666/1/10002")
	})

	t.Run("failure returns partial file list", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
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

		// First copy succeeds, second fails
		cm.EXPECT().Copy(mock.Anything, "files/insert_log/111/222/333/1/10001", "files/insert_log/444/555/666/1/10001").Return(nil).Maybe()
		cm.EXPECT().Copy(mock.Anything, "files/insert_log/111/222/333/1/10002", "files/insert_log/444/555/666/1/10002").Return(errors.New("copy failed")).Maybe()

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
			indexType: IndexTypeVectorScalar,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := generateTargetIndexPath(tt.path, source, target, tt.indexType)
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
	result, err := generateTargetIndexPath("files/index_files/1001/1/222/333/HNSW_SQ_3", source, target, IndexTypeVectorScalar)
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

func TestCreateFileMappings_ErrorPaths(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("insert binlog invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			InsertBinlogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "invalid/path"}}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), BinlogTypeInsert)
	})

	t.Run("delta binlog invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			DeltaBinlogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "invalid/path"}}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), BinlogTypeDelta)
	})

	t.Run("stats binlog invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			StatsBinlogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "invalid/path"}}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), BinlogTypeStats)
	})

	t.Run("bm25 binlog invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			Bm25Binlogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "invalid/path"}}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), BinlogTypeBM25)
	})

	t.Run("vector scalar index invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			IndexFiles: []*indexpb.IndexFilePathInfo{
				{FieldID: 100, IndexFilePaths: []string{"invalid/path"}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), IndexTypeVectorScalar)
	})

	t.Run("text index invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			TextIndexFiles: map[int64]*datapb.TextIndexStats{
				100: {FieldID: 100, Files: []string{"invalid/path"}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), IndexTypeText)
	})

	t.Run("json key index invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				101: {FieldID: 101, JsonKeyStatsDataFormat: 1, Files: []string{"invalid/path"}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), IndexTypeJSONKey)
	})

	t.Run("json stats invalid path", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				102: {FieldID: 102, JsonKeyStatsDataFormat: 2, Files: []string{"invalid/path"}},
			},
		}
		_, err := createFileMappings(source, target)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), IndexTypeJSONStats)
	})
}

func TestCreateFileMappings_EmptySource(t *testing.T) {
	source := &datapb.CopySegmentSource{}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings, err := createFileMappings(source, target)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mappings))
}

func TestCreateFileMappings_SkipsEmptyPaths(t *testing.T) {
	source := &datapb.CopySegmentSource{
		InsertBinlogs: []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: ""},
					{LogPath: "files/insert_log/111/222/333/100/log1"},
				},
			},
		},
		DeltaBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: ""}}},
		},
		StatsBinlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: ""}}},
		},
		Bm25Binlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: ""}}},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings, err := createFileMappings(source, target)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mappings)) // Only the non-empty insert path
}

func TestCreateFileMappings_JsonKeyFormatRouting(t *testing.T) {
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	t.Run("legacy format uses json_key_index_log", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				101: {
					FieldID:                101,
					JsonKeyStatsDataFormat: 1,
					Files:                  []string{"files/json_key_index_log/123/1/111/222/333/101/json1"},
				},
			},
		}
		mappings, err := createFileMappings(source, target)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(mappings))
		assert.Contains(t, mappings["files/json_key_index_log/123/1/111/222/333/101/json1"], "json_key_index_log")
	})

	t.Run("new format uses json_stats", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:                102,
					JsonKeyStatsDataFormat: 2,
					Files:                  []string{"files/json_stats/2/3002/1/111/222/333/102/shared_key_index/idx1"},
				},
			},
		}
		mappings, err := createFileMappings(source, target)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(mappings))
		assert.Contains(t, mappings["files/json_stats/2/3002/1/111/222/333/102/shared_key_index/idx1"], "json_stats")
	})

	t.Run("format 0 treated as legacy", func(t *testing.T) {
		source := &datapb.CopySegmentSource{
			JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
				103: {
					FieldID:                103,
					JsonKeyStatsDataFormat: 0,
					Files:                  []string{"files/json_key_index_log/123/1/111/222/333/103/json1"},
				},
			},
		}
		mappings, err := createFileMappings(source, target)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(mappings))
	})
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

func TestBuildIndexInfoFromSource_EmptySource(t *testing.T) {
	source := &datapb.CopySegmentSource{}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	indexInfos, textIndexInfos, jsonKeyIndexInfos := buildIndexInfoFromSource(source, target, map[string]string{})
	assert.Equal(t, 0, len(indexInfos))
	assert.Equal(t, 0, len(textIndexInfos))
	assert.Equal(t, 0, len(jsonKeyIndexInfos))
}

func TestBuildIndexInfoFromSource_UnmappedPaths(t *testing.T) {
	source := &datapb.CopySegmentSource{
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{
				FieldID:        100,
				IndexID:        1001,
				BuildID:        1002,
				IndexFilePaths: []string{"files/index_files/1002/1/222/333/index1", "unmapped/path"},
				SerializedSize: 5000,
			},
		},
		TextIndexFiles: map[int64]*datapb.TextIndexStats{
			200: {
				FieldID: 200,
				Files:   []string{"unmapped/text/path"},
			},
		},
		JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
			300: {
				FieldID: 300,
				Files:   []string{"unmapped/json/path"},
			},
		},
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}

	mappings := map[string]string{
		"files/index_files/1002/1/222/333/index1": "files/index_files/1002/1/555/666/index1",
	}

	indexInfos, textIndexInfos, jsonKeyIndexInfos := buildIndexInfoFromSource(source, target, mappings)

	// Vector/scalar: only mapped path included
	assert.Equal(t, 1, len(indexInfos))
	assert.Equal(t, 1, len(indexInfos[100].IndexFilePaths))
	assert.Equal(t, "files/index_files/1002/1/555/666/index1", indexInfos[100].IndexFilePaths[0])

	// Text: no mapped paths -> empty files list
	assert.Equal(t, 1, len(textIndexInfos))
	assert.Equal(t, 0, len(textIndexInfos[200].Files))

	// JSON: no mapped paths -> empty files list
	assert.Equal(t, 1, len(jsonKeyIndexInfos))
	assert.Equal(t, 0, len(jsonKeyIndexInfos[300].Files))
}

func TestTransformFieldBinlogs_NilInput(t *testing.T) {
	result, totalRows, err := transformFieldBinlogs(nil, map[string]string{}, true)
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

	result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(600), totalRows)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 3, len(result[0].Binlogs))
	assert.Equal(t, "files/insert_log/444/555/666/100/log1", result[0].Binlogs[0].LogPath)
	assert.Equal(t, "files/insert_log/444/555/666/100/log2", result[0].Binlogs[1].LogPath)
	assert.Equal(t, "files/insert_log/444/555/666/100/log3", result[0].Binlogs[2].LogPath)
}

func TestTransformFieldBinlogs_UnmappedPath(t *testing.T) {
	// Path not in mappings -> LogPath becomes empty string (zero value)
	mappings := map[string]string{}

	srcFieldBinlogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 100, LogPath: "files/insert_log/111/222/333/100/log1"},
			},
		},
	}

	result, totalRows, err := transformFieldBinlogs(srcFieldBinlogs, mappings, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), totalRows) // Still counts rows
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "", result[0].Binlogs[0].LogPath) // Unmapped -> empty
}

func TestCopySegmentAndIndexFiles_CreateFileMappingsError(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	// Source with invalid path that will cause createFileMappings to fail
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
	assert.Contains(t, err.Error(), "failed to collect copy tasks")
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
	// Test shortening meta.json path (file directly under fieldID directory)
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
	// Test already shortened path
	t.Run("already_shortened_meta", func(t *testing.T) {
		result := shortenSingleJsonStatsPath("meta.json")
		assert.Equal(t, "meta.json", result)
	})

	// Test already shortened shared_key_index path
	t.Run("already_shortened_shared_key", func(t *testing.T) {
		result := shortenSingleJsonStatsPath("shared_key_index/inverted_index_0")
		assert.Equal(t, "shared_key_index/inverted_index_0", result)
	})

	// Test full path with meta.json
	t.Run("full_path_meta_json", func(t *testing.T) {
		fullPath := "files/json_stats/2/123/1/444/555/666/100/meta.json"
		result := shortenSingleJsonStatsPath(fullPath)
		assert.Equal(t, "meta.json", result)
	})

	// Test full path with nested file under fieldID
	t.Run("full_path_nested_file", func(t *testing.T) {
		fullPath := "files/json_stats/2/123/1/444/555/666/100/subdir/file.dat"
		result := shortenSingleJsonStatsPath(fullPath)
		assert.Equal(t, "subdir/file.dat", result)
	})
}
