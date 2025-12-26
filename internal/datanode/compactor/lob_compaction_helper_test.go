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

package compactor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func init() {
	paramtable.Init()
}

func TestNewLOBCompactionHelper(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "minio",
		BucketName:  "test-bucket",
		RootPath:    "test-root",
	}

	helper := NewLOBCompactionHelper(storageConfig)
	require.NotNil(t, helper)
	assert.Equal(t, storageConfig, helper.storageConfig)
}

func TestLOBCompactionHelper_GetStorageConfig(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		StorageType: "minio",
		BucketName:  "test-bucket",
	}

	helper := NewLOBCompactionHelper(storageConfig)
	assert.Equal(t, storageConfig, helper.GetStorageConfig())
}

func TestLOBCompactionHelper_ConvertProtoLOBMetadata(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)

	t.Run("empty proto metadata", func(t *testing.T) {
		result := helper.ConvertProtoLOBMetadata(nil, 1, 2, 3)
		assert.Nil(t, result)

		result = helper.ConvertProtoLOBMetadata(make(map[int64]*datapb.LOBFieldMetadata), 1, 2, 3)
		assert.Nil(t, result)
	})

	t.Run("valid proto metadata with LOBFile structs", func(t *testing.T) {
		protoMeta := map[int64]*datapb.LOBFieldMetadata{
			101: {
				FieldId: 101,
				LobFiles: []*datapb.LOBFile{
					{FilePath: "file1", LobFileId: 1001, RowCount: 50, ValidRecordCount: 45},
					{FilePath: "file2", LobFileId: 1002, RowCount: 50, ValidRecordCount: 40},
				},
				SizeThreshold: 65536,
				RecordCount:   100,
				TotalBytes:    1024000,
			},
			102: {
				FieldId: 102,
				LobFiles: []*datapb.LOBFile{
					{FilePath: "file3", LobFileId: 1003, RowCount: 50, ValidRecordCount: 50},
				},
				SizeThreshold: 32768,
				RecordCount:   50,
				TotalBytes:    512000,
			},
		}

		result := helper.ConvertProtoLOBMetadata(protoMeta, 1, 2, 3)
		require.NotNil(t, result)

		assert.Equal(t, int64(1), result.SegmentID)
		assert.Equal(t, int64(2), result.CollectionID)
		assert.Equal(t, int64(3), result.PartitionID)
		assert.Len(t, result.LOBFields, 2)

		field101 := result.LOBFields[101]
		require.NotNil(t, field101)
		assert.Equal(t, int64(101), field101.FieldID)
		assert.Len(t, field101.LOBFiles, 2)
		assert.Equal(t, "file1", field101.LOBFiles[0].FilePath)
		assert.Equal(t, int64(1001), field101.LOBFiles[0].LobFileID)
		assert.Equal(t, int64(50), field101.LOBFiles[0].RowCount)
		assert.Equal(t, int64(45), field101.LOBFiles[0].ValidRecordCount)
		assert.Equal(t, int64(65536), field101.SizeThreshold)
		assert.Equal(t, int64(100), field101.RecordCount)
		assert.Equal(t, int64(1024000), field101.TotalBytes)

		field102 := result.LOBFields[102]
		require.NotNil(t, field102)
		assert.Equal(t, int64(102), field102.FieldID)
		assert.Len(t, field102.LOBFiles, 1)

		assert.Equal(t, 3, result.TotalLOBFiles)
		assert.Equal(t, int64(150), result.TotalLOBRecords)
		assert.Equal(t, int64(1536000), result.TotalLOBBytes)
	})

	t.Run("backward compatibility - ValidRecordCount defaults to RowCount when zero", func(t *testing.T) {
		protoMeta := map[int64]*datapb.LOBFieldMetadata{
			101: {
				FieldId: 101,
				LobFiles: []*datapb.LOBFile{
					{FilePath: "file1", LobFileId: 1001, RowCount: 100, ValidRecordCount: 0}, // old data without valid_record_count
				},
				SizeThreshold: 65536,
				RecordCount:   100,
				TotalBytes:    1024000,
			},
		}

		result := helper.ConvertProtoLOBMetadata(protoMeta, 1, 2, 3)
		require.NotNil(t, result)

		// ValidRecordCount should be 0 as stored (backward compatibility handled at calculation time)
		field101 := result.LOBFields[101]
		assert.Equal(t, int64(0), field101.LOBFiles[0].ValidRecordCount)
	})
}

func TestLOBCompactionHelper_DecideCompactionMode(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)

	t.Run("mix compaction with high delete ratio triggers SmartRewrite", func(t *testing.T) {
		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, 0.8, 2)
		assert.Equal(t, CompactionLOBModeSmartRewrite, mode)
	})

	t.Run("mix compaction with low delete ratio uses ReferenceOnly", func(t *testing.T) {
		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, 0.1, 2)
		assert.Equal(t, CompactionLOBModeReferenceOnly, mode)
	})

	t.Run("mix compaction at threshold boundary", func(t *testing.T) {
		threshold := getLOBCompactionSmartRewriteThreshold()
		// at threshold should trigger SmartRewrite
		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, threshold, 2)
		assert.Equal(t, CompactionLOBModeSmartRewrite, mode)

		// below threshold should use ReferenceOnly
		mode = helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, threshold-0.01, 2)
		assert.Equal(t, CompactionLOBModeReferenceOnly, mode)
	})

	t.Run("clustering compaction always uses ReferenceOnly", func(t *testing.T) {
		mode := helper.DecideCompactionMode(datapb.CompactionType_ClusteringCompaction, 0.9, 3)
		assert.Equal(t, CompactionLOBModeReferenceOnly, mode)
	})

	t.Run("sort compaction always uses ReferenceOnly", func(t *testing.T) {
		mode := helper.DecideCompactionMode(datapb.CompactionType_SortCompaction, 0.9, 2)
		assert.Equal(t, CompactionLOBModeReferenceOnly, mode)
	})

	t.Run("L0 delete compaction skips LOB processing", func(t *testing.T) {
		mode := helper.DecideCompactionMode(datapb.CompactionType_Level0DeleteCompaction, 0.5, 1)
		assert.Equal(t, CompactionLOBModeSkip, mode)
	})

	t.Run("unknown compaction type skips LOB processing", func(t *testing.T) {
		mode := helper.DecideCompactionMode(datapb.CompactionType(999), 0.5, 1)
		assert.Equal(t, CompactionLOBModeSkip, mode)
	})
}

func TestLOBCompactionHelper_ConvertToProtoLOBMetadata(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)

	t.Run("nil metadata", func(t *testing.T) {
		result := helper.ConvertToProtoLOBMetadata(nil)
		assert.Nil(t, result)
	})

	t.Run("empty metadata", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		result := helper.ConvertToProtoLOBMetadata(meta)
		assert.Nil(t, result)
	})

	t.Run("valid metadata with LOBFileInfo", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
				{FilePath: "file2", LobFileID: 1002, RowCount: 50, ValidRecordCount: 45},
			},
			SizeThreshold: 65536,
			RecordCount:   150,
			TotalBytes:    1024000,
		}

		result := helper.ConvertToProtoLOBMetadata(meta)
		require.NotNil(t, result)
		assert.Len(t, result, 1)

		protoField := result[101]
		require.NotNil(t, protoField)
		assert.Equal(t, int64(101), protoField.FieldId)
		assert.Len(t, protoField.LobFiles, 2)
		assert.Equal(t, "file1", protoField.LobFiles[0].FilePath)
		assert.Equal(t, int64(1001), protoField.LobFiles[0].LobFileId)
		assert.Equal(t, int64(100), protoField.LobFiles[0].RowCount)
		assert.Equal(t, int64(80), protoField.LobFiles[0].ValidRecordCount)
		assert.Equal(t, int64(65536), protoField.SizeThreshold)
		assert.Equal(t, int64(150), protoField.RecordCount)
		assert.Equal(t, int64(1024000), protoField.TotalBytes)
	})
}

func TestLOBCompactionHelper_CalculateLOBGarbageRatio(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)

	t.Run("empty metadata returns zero", func(t *testing.T) {
		ratio := helper.CalculateLOBGarbageRatio(nil)
		assert.Equal(t, float64(0), ratio)

		ratio = helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{})
		assert.Equal(t, float64(0), ratio)
	})

	t.Run("all records valid returns zero garbage", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
			},
		}

		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.Equal(t, float64(0), ratio)
	})

	t.Run("50% garbage ratio", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 50},
			},
		}

		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.Equal(t, 0.5, ratio)
	})

	t.Run("multiple files with different garbage ratios", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80}, // 20% garbage
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 60}, // 40% garbage
			},
		}

		// total: 200 rows, 140 valid -> 30% garbage
		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.InDelta(t, 0.3, ratio, 0.0001)
	})

	t.Run("multiple segments", func(t *testing.T) {
		meta1 := storage.NewLOBSegmentMetadata()
		meta1.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100}, // 0% garbage
			},
		}

		meta2 := storage.NewLOBSegmentMetadata()
		meta2.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 0}, // 100% garbage
			},
		}

		// total: 200 rows, 100 valid -> 50% garbage
		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta1, meta2})
		assert.Equal(t, 0.5, ratio)
	})

	t.Run("multiple fields", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
		}
		meta.LOBFields[102] = &storage.LOBFieldMetadata{
			FieldID: 102,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 2001, RowCount: 100, ValidRecordCount: 60},
			},
		}

		// total: 200 rows, 140 valid -> 30% garbage
		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.InDelta(t, 0.3, ratio, 0.0001)
	})
}

func TestMergeLOBMetadata(t *testing.T) {
	t.Run("nil source segments", func(t *testing.T) {
		result := MergeLOBMetadata(nil)
		require.NotNil(t, result)
		assert.False(t, result.HasLOBFields())
	})

	t.Run("empty source segments", func(t *testing.T) {
		result := MergeLOBMetadata([]*storage.LOBSegmentMetadata{})
		require.NotNil(t, result)
		assert.False(t, result.HasLOBFields())
	})

	t.Run("single source segment with LOBFileInfo", func(t *testing.T) {
		source := storage.NewLOBSegmentMetadata()
		source.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 50, ValidRecordCount: 45},
				{FilePath: "file2", LobFileID: 1002, RowCount: 50, ValidRecordCount: 40},
			},
			SizeThreshold: 65536,
			RecordCount:   100,
			TotalBytes:    1024000,
		}

		result := MergeLOBMetadata([]*storage.LOBSegmentMetadata{source})
		require.NotNil(t, result)
		assert.True(t, result.HasLOBFields())
		assert.Len(t, result.LOBFields, 1)
		assert.Len(t, result.LOBFields[101].LOBFiles, 2)
		assert.Equal(t, int64(45), result.LOBFields[101].LOBFiles[0].ValidRecordCount)
		assert.Equal(t, int64(100), result.LOBFields[101].RecordCount)
		assert.Equal(t, int64(1024000), result.LOBFields[101].TotalBytes)
	})

	t.Run("multiple source segments same field - preserves ValidRecordCount", func(t *testing.T) {
		source1 := storage.NewLOBSegmentMetadata()
		source1.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
			SizeThreshold: 65536,
			RecordCount:   100,
			TotalBytes:    500000,
		}

		source2 := storage.NewLOBSegmentMetadata()
		source2.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 60},
				{FilePath: "file3", LobFileID: 1003, RowCount: 50, ValidRecordCount: 50},
			},
			SizeThreshold: 65536,
			RecordCount:   150,
			TotalBytes:    1000000,
		}

		result := MergeLOBMetadata([]*storage.LOBSegmentMetadata{source1, source2})
		require.NotNil(t, result)
		assert.True(t, result.HasLOBFields())

		field := result.LOBFields[101]
		require.NotNil(t, field)
		assert.Len(t, field.LOBFiles, 3)

		// verify ValidRecordCount is preserved for each file
		assert.Equal(t, int64(80), field.LOBFiles[0].ValidRecordCount)
		// note: order may vary due to append, check by file ID
		var file2Found, file3Found bool
		for _, f := range field.LOBFiles[1:] {
			if f.LobFileID == 1002 {
				assert.Equal(t, int64(60), f.ValidRecordCount)
				file2Found = true
			}
			if f.LobFileID == 1003 {
				assert.Equal(t, int64(50), f.ValidRecordCount)
				file3Found = true
			}
		}
		assert.True(t, file2Found)
		assert.True(t, file3Found)

		assert.Equal(t, int64(250), field.RecordCount)
		assert.Equal(t, int64(1500000), field.TotalBytes)

		assert.Equal(t, 3, result.TotalLOBFiles)
		assert.Equal(t, int64(250), result.TotalLOBRecords)
		assert.Equal(t, int64(1500000), result.TotalLOBBytes)
	})

	t.Run("multiple source segments different fields", func(t *testing.T) {
		source1 := storage.NewLOBSegmentMetadata()
		source1.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 50, ValidRecordCount: 40},
			},
			SizeThreshold: 65536,
			RecordCount:   50,
			TotalBytes:    500000,
		}

		source2 := storage.NewLOBSegmentMetadata()
		source2.LOBFields[102] = &storage.LOBFieldMetadata{
			FieldID: 102,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 2001, RowCount: 100, ValidRecordCount: 90},
			},
			SizeThreshold: 32768,
			RecordCount:   100,
			TotalBytes:    1000000,
		}

		result := MergeLOBMetadata([]*storage.LOBSegmentMetadata{source1, source2})
		require.NotNil(t, result)
		assert.True(t, result.HasLOBFields())
		assert.Len(t, result.LOBFields, 2)

		assert.NotNil(t, result.LOBFields[101])
		assert.NotNil(t, result.LOBFields[102])
		assert.Equal(t, int64(40), result.LOBFields[101].LOBFiles[0].ValidRecordCount)
		assert.Equal(t, int64(90), result.LOBFields[102].LOBFiles[0].ValidRecordCount)

		assert.Equal(t, 2, result.TotalLOBFiles)
		assert.Equal(t, int64(150), result.TotalLOBRecords)
		assert.Equal(t, int64(1500000), result.TotalLOBBytes)
	})

	t.Run("with nil source in list", func(t *testing.T) {
		source := storage.NewLOBSegmentMetadata()
		source.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 50, ValidRecordCount: 45},
			},
			SizeThreshold: 65536,
			RecordCount:   50,
			TotalBytes:    500000,
		}

		result := MergeLOBMetadata([]*storage.LOBSegmentMetadata{nil, source, nil})
		require.NotNil(t, result)
		assert.True(t, result.HasLOBFields())
		assert.Len(t, result.LOBFields, 1)
	})
}

func TestFilterLOBMetadataByUsedFiles(t *testing.T) {
	t.Run("nil metadata returns nil", func(t *testing.T) {
		result := FilterLOBMetadataByUsedFiles(nil, nil)
		assert.Nil(t, result)
	})

	t.Run("empty used files returns nil", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		result := FilterLOBMetadataByUsedFiles(meta, nil)
		assert.Nil(t, result)

		result = FilterLOBMetadataByUsedFiles(meta, make(map[int64]map[int64]int64))
		assert.Nil(t, result)
	})

	t.Run("filter keeps only used files with correct ValidRecordCount", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.SegmentID = 1
		meta.CollectionID = 100
		meta.PartitionID = 10
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 60},
				{FilePath: "file3", LobFileID: 1003, RowCount: 100, ValidRecordCount: 50},
			},
			RecordCount: 300,
			TotalBytes:  3000000,
		}

		// only use file1 (30 refs) and file3 (20 refs), skip file2
		usedFiles := map[int64]map[int64]int64{
			101: {
				1001: 30, // file1: 30 references
				1003: 20, // file3: 20 references
			},
		}

		result := FilterLOBMetadataByUsedFiles(meta, usedFiles)
		require.NotNil(t, result)

		assert.Equal(t, int64(1), result.SegmentID)
		assert.Equal(t, int64(100), result.CollectionID)
		assert.Equal(t, int64(10), result.PartitionID)

		field := result.LOBFields[101]
		require.NotNil(t, field)
		assert.Len(t, field.LOBFiles, 2)

		// check that ValidRecordCount is set to actual reference count
		var file1Found, file3Found bool
		for _, f := range field.LOBFiles {
			if f.LobFileID == 1001 {
				assert.Equal(t, int64(100), f.RowCount)        // original row count preserved
				assert.Equal(t, int64(30), f.ValidRecordCount) // set to actual ref count
				file1Found = true
			}
			if f.LobFileID == 1003 {
				assert.Equal(t, int64(100), f.RowCount)
				assert.Equal(t, int64(20), f.ValidRecordCount)
				file3Found = true
			}
		}
		assert.True(t, file1Found)
		assert.True(t, file3Found)

		// RecordCount should be sum of original RowCounts of included files
		assert.Equal(t, int64(200), field.RecordCount) // 100 + 100

		assert.Equal(t, 2, result.TotalLOBFiles)
		assert.Equal(t, int64(200), result.TotalLOBRecords)
	})

	t.Run("filter multiple fields", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "f101_1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
				{FilePath: "f101_2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 60},
			},
			RecordCount: 200,
			TotalBytes:  2000000,
		}
		meta.LOBFields[102] = &storage.LOBFieldMetadata{
			FieldID: 102,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "f102_1", LobFileID: 2001, RowCount: 50, ValidRecordCount: 50},
			},
			RecordCount: 50,
			TotalBytes:  500000,
		}

		usedFiles := map[int64]map[int64]int64{
			101: {1001: 25}, // only file1 from field 101
			102: {2001: 40}, // file1 from field 102
		}

		result := FilterLOBMetadataByUsedFiles(meta, usedFiles)
		require.NotNil(t, result)
		assert.Len(t, result.LOBFields, 2)

		assert.Len(t, result.LOBFields[101].LOBFiles, 1)
		assert.Equal(t, int64(25), result.LOBFields[101].LOBFiles[0].ValidRecordCount)

		assert.Len(t, result.LOBFields[102].LOBFiles, 1)
		assert.Equal(t, int64(40), result.LOBFields[102].LOBFiles[0].ValidRecordCount)
	})

	t.Run("field with no used files is excluded", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}
		meta.LOBFields[102] = &storage.LOBFieldMetadata{
			FieldID: 102,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 2001, RowCount: 50, ValidRecordCount: 50},
			},
			RecordCount: 50,
			TotalBytes:  500000,
		}

		// only use files from field 101
		usedFiles := map[int64]map[int64]int64{
			101: {1001: 50},
		}

		result := FilterLOBMetadataByUsedFiles(meta, usedFiles)
		require.NotNil(t, result)
		assert.Len(t, result.LOBFields, 1)
		assert.NotNil(t, result.LOBFields[101])
		assert.Nil(t, result.LOBFields[102])
	})

	t.Run("all files filtered out returns nil", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		// use a file that doesn't exist
		usedFiles := map[int64]map[int64]int64{
			101: {9999: 50}, // non-existent file
		}

		result := FilterLOBMetadataByUsedFiles(meta, usedFiles)
		assert.Nil(t, result)
	})
}

func TestFilterLOBMetadataByUsedFiles_GarbageRatioCalculation(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)

	t.Run("garbage ratio calculated correctly after filtering", func(t *testing.T) {
		// scenario: clustering compaction splits 1 segment into 2
		// source segment has 2 LOB files: file1 (100 rows), file2 (100 rows)
		// output segment A uses: file1 (30 refs), file2 (20 refs)
		// output segment B uses: file1 (50 refs), file2 (60 refs)

		sourceMeta := storage.NewLOBSegmentMetadata()
		sourceMeta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 100},
			},
			RecordCount: 200,
			TotalBytes:  2000000,
		}

		// simulate output segment A
		usedFilesA := map[int64]map[int64]int64{
			101: {1001: 30, 1002: 20},
		}
		filteredA := FilterLOBMetadataByUsedFiles(sourceMeta, usedFilesA)
		require.NotNil(t, filteredA)

		// garbage ratio for segment A: 1 - (30+20)/(100+100) = 1 - 50/200 = 0.75
		ratioA := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{filteredA})
		assert.Equal(t, 0.75, ratioA)

		// simulate output segment B
		usedFilesB := map[int64]map[int64]int64{
			101: {1001: 50, 1002: 60},
		}
		filteredB := FilterLOBMetadataByUsedFiles(sourceMeta, usedFilesB)
		require.NotNil(t, filteredB)

		// garbage ratio for segment B: 1 - (50+60)/(100+100) = 1 - 110/200 = 0.45
		ratioB := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{filteredB})
		assert.InDelta(t, 0.45, ratioB, 0.0001)
	})

	t.Run("mix compaction reference-only preserves garbage tracking", func(t *testing.T) {
		// scenario: mix compaction merges 2 segments
		// segment1: file1 (100 rows, 80 valid)
		// segment2: file2 (100 rows, 60 valid)
		// after compaction: some rows deleted, result references:
		// file1: 70 refs, file2: 50 refs

		source1 := storage.NewLOBSegmentMetadata()
		source1.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		source2 := storage.NewLOBSegmentMetadata()
		source2.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 60},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		// merge source metadata
		merged := MergeLOBMetadata([]*storage.LOBSegmentMetadata{source1, source2})
		require.NotNil(t, merged)

		// filter with actual reference counts from compaction
		usedFiles := map[int64]map[int64]int64{
			101: {1001: 70, 1002: 50},
		}
		filtered := FilterLOBMetadataByUsedFiles(merged, usedFiles)
		require.NotNil(t, filtered)

		// verify ValidRecordCount is updated to actual refs
		field := filtered.LOBFields[101]
		for _, f := range field.LOBFiles {
			if f.LobFileID == 1001 {
				assert.Equal(t, int64(100), f.RowCount)
				assert.Equal(t, int64(70), f.ValidRecordCount)
			}
			if f.LobFileID == 1002 {
				assert.Equal(t, int64(100), f.RowCount)
				assert.Equal(t, int64(50), f.ValidRecordCount)
			}
		}

		// garbage ratio: 1 - (70+50)/(100+100) = 1 - 120/200 = 0.4
		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{filtered})
		assert.Equal(t, 0.4, ratio)
	})
}

func TestClusterBuffer_LOBReferenceTracking(t *testing.T) {
	t.Run("track LOB file references with counts", func(t *testing.T) {
		buffer := &ClusterBuffer{
			textFieldIDs:   map[int64]struct{}{101: {}},
			usedLOBFileIDs: make(map[int64]map[int64]int64),
		}

		// simulate writing multiple rows that reference different LOB files
		// row 1: references file 1001
		ref1 := storage.NewLOBReference(1001, 0)
		refBytes1 := storage.EncodeLOBReference(ref1)

		// row 2: references file 1001 again
		ref2 := storage.NewLOBReference(1001, 1)
		refBytes2 := storage.EncodeLOBReference(ref2)

		// row 3: references file 1002
		ref3 := storage.NewLOBReference(1002, 0)
		refBytes3 := storage.EncodeLOBReference(ref3)

		// simulate trackLOBFileIDs being called for each row
		// this simulates internal tracking logic
		for _, refBytes := range [][]byte{refBytes1, refBytes2, refBytes3} {
			if storage.IsLOBReference(refBytes) {
				ref, err := storage.DecodeLOBReference(refBytes)
				require.NoError(t, err)

				if buffer.usedLOBFileIDs[101] == nil {
					buffer.usedLOBFileIDs[101] = make(map[int64]int64)
				}
				buffer.usedLOBFileIDs[101][int64(ref.LobFileID)]++
			}
		}

		// verify reference counts
		usedFiles := buffer.GetUsedLOBFileIDs()
		require.NotNil(t, usedFiles)
		require.NotNil(t, usedFiles[101])

		assert.Equal(t, int64(2), usedFiles[101][1001]) // file 1001 referenced twice
		assert.Equal(t, int64(1), usedFiles[101][1002]) // file 1002 referenced once
	})

	t.Run("empty text fields returns no tracking", func(t *testing.T) {
		buffer := &ClusterBuffer{
			textFieldIDs:   map[int64]struct{}{}, // no TEXT fields
			usedLOBFileIDs: nil,
		}

		usedFiles := buffer.GetUsedLOBFileIDs()
		assert.Nil(t, usedFiles)
	})

	t.Run("multiple TEXT fields tracked separately", func(t *testing.T) {
		buffer := &ClusterBuffer{
			textFieldIDs:   map[int64]struct{}{101: {}, 102: {}},
			usedLOBFileIDs: make(map[int64]map[int64]int64),
		}

		// field 101: file 1001 referenced 3 times
		buffer.usedLOBFileIDs[101] = map[int64]int64{1001: 3}

		// field 102: file 2001 referenced 5 times
		buffer.usedLOBFileIDs[102] = map[int64]int64{2001: 5}

		usedFiles := buffer.GetUsedLOBFileIDs()
		assert.Equal(t, int64(3), usedFiles[101][1001])
		assert.Equal(t, int64(5), usedFiles[102][2001])
	})
}

func TestEndToEnd_LOBGarbageAccumulation(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)

	t.Run("garbage accumulates across multiple compactions", func(t *testing.T) {
		// Initial state: Segment A with 1 LOB file (100 rows, all valid)
		segmentA := storage.NewLOBSegmentMetadata()
		segmentA.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		// Initial garbage ratio: 0%
		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{segmentA})
		assert.Equal(t, 0.0, ratio)

		// Compaction 1: 20 rows deleted, 80 rows remaining
		// Filter metadata with actual reference count
		usedFiles1 := map[int64]map[int64]int64{
			101: {1001: 80},
		}
		afterCompaction1 := FilterLOBMetadataByUsedFiles(segmentA, usedFiles1)
		require.NotNil(t, afterCompaction1)

		// Verify: RowCount preserved, ValidRecordCount updated
		assert.Equal(t, int64(100), afterCompaction1.LOBFields[101].LOBFiles[0].RowCount)
		assert.Equal(t, int64(80), afterCompaction1.LOBFields[101].LOBFiles[0].ValidRecordCount)

		// Garbage ratio: 20%
		ratio = helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{afterCompaction1})
		assert.InDelta(t, 0.2, ratio, 0.0001)

		// Compaction 2: another 30 rows deleted (50 remaining out of original 100)
		usedFiles2 := map[int64]map[int64]int64{
			101: {1001: 50},
		}
		afterCompaction2 := FilterLOBMetadataByUsedFiles(afterCompaction1, usedFiles2)
		require.NotNil(t, afterCompaction2)

		// Garbage ratio: 50%
		ratio = helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{afterCompaction2})
		assert.InDelta(t, 0.5, ratio, 0.0001)

		// Compaction 3: another 20 rows deleted (30 remaining)
		usedFiles3 := map[int64]map[int64]int64{
			101: {1001: 30},
		}
		afterCompaction3 := FilterLOBMetadataByUsedFiles(afterCompaction2, usedFiles3)
		require.NotNil(t, afterCompaction3)

		// Garbage ratio: 70%
		ratio = helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{afterCompaction3})
		assert.InDelta(t, 0.7, ratio, 0.0001)

		// At this point, garbage ratio exceeds threshold, should trigger SmartRewrite
		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, ratio, 1)
		assert.Equal(t, CompactionLOBModeSmartRewrite, mode)
	})

	t.Run("clustering compaction splits segment correctly", func(t *testing.T) {
		// Source segment: 2 LOB files, total 200 rows
		source := storage.NewLOBSegmentMetadata()
		source.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 100},
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 100},
			},
			RecordCount: 200,
			TotalBytes:  2000000,
		}

		// Clustering compaction splits into 2 output segments
		// Output A: references file1(40), file2(30)
		usedA := map[int64]map[int64]int64{
			101: {1001: 40, 1002: 30},
		}
		outputA := FilterLOBMetadataByUsedFiles(source, usedA)
		require.NotNil(t, outputA)

		// Output B: references file1(60), file2(70)
		usedB := map[int64]map[int64]int64{
			101: {1001: 60, 1002: 70},
		}
		outputB := FilterLOBMetadataByUsedFiles(source, usedB)
		require.NotNil(t, outputB)

		// Verify output A
		ratioA := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{outputA})
		// A: (100+100) total rows, (40+30) valid = 70/200 valid, 65% garbage
		assert.InDelta(t, 0.65, ratioA, 0.0001)

		// Verify output B
		ratioB := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{outputB})
		// B: (100+100) total rows, (60+70) valid = 130/200 valid, 35% garbage
		assert.InDelta(t, 0.35, ratioB, 0.0001)

		// Total valid count should equal original: 40+60=100, 30+70=100 -> 200 total
		totalValid := int64(0)
		for _, f := range outputA.LOBFields[101].LOBFiles {
			totalValid += f.ValidRecordCount
		}
		for _, f := range outputB.LOBFields[101].LOBFiles {
			totalValid += f.ValidRecordCount
		}
		assert.Equal(t, int64(200), totalValid)
	})

	t.Run("mix compaction merges segments correctly", func(t *testing.T) {
		// Source segment 1
		source1 := storage.NewLOBSegmentMetadata()
		source1.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 80},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		// Source segment 2
		source2 := storage.NewLOBSegmentMetadata()
		source2.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file2", LobFileID: 1002, RowCount: 100, ValidRecordCount: 60},
			},
			RecordCount: 100,
			TotalBytes:  1000000,
		}

		// Merge metadata
		merged := MergeLOBMetadata([]*storage.LOBSegmentMetadata{source1, source2})
		require.NotNil(t, merged)

		// Initial garbage ratio from merged metadata: (80+60)/200 = 140/200 valid, 30% garbage
		initialRatio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{merged})
		assert.InDelta(t, 0.3, initialRatio, 0.0001)

		// After mix compaction (ReferenceOnly): some rows deleted
		// Compaction keeps file1(70), file2(50) references
		usedFiles := map[int64]map[int64]int64{
			101: {1001: 70, 1002: 50},
		}
		filtered := FilterLOBMetadataByUsedFiles(merged, usedFiles)
		require.NotNil(t, filtered)

		// Final garbage ratio: (70+50)/200 = 120/200 valid, 40% garbage
		finalRatio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{filtered})
		assert.Equal(t, 0.4, finalRatio)
	})
}

func TestCompactionModeDecisionWithGarbageRatio(t *testing.T) {
	helper := NewLOBCompactionHelper(nil)
	threshold := getLOBCompactionSmartRewriteThreshold()

	t.Run("low garbage triggers ReferenceOnly mode", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 90}, // 10% garbage
			},
		}

		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.Less(t, ratio, threshold)

		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, ratio, 1)
		assert.Equal(t, CompactionLOBModeReferenceOnly, mode)
	})

	t.Run("high garbage triggers SmartRewrite mode", func(t *testing.T) {
		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 20}, // 80% garbage
			},
		}

		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.GreaterOrEqual(t, ratio, threshold)

		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, ratio, 1)
		assert.Equal(t, CompactionLOBModeSmartRewrite, mode)
	})

	t.Run("accumulated garbage over multiple compactions", func(t *testing.T) {
		// simulate scenario where garbage accumulates over multiple compactions
		// initial: 100 rows all valid
		// after compaction 1: 80 valid (20% garbage)
		// after compaction 2: 50 valid (50% garbage)
		// after compaction 3: 25 valid (75% garbage) -> triggers SmartRewrite

		meta := storage.NewLOBSegmentMetadata()
		meta.LOBFields[101] = &storage.LOBFieldMetadata{
			FieldID: 101,
			LOBFiles: []*storage.LOBFileInfo{
				{FilePath: "file1", LobFileID: 1001, RowCount: 100, ValidRecordCount: 25},
			},
		}

		ratio := helper.CalculateLOBGarbageRatio([]*storage.LOBSegmentMetadata{meta})
		assert.Equal(t, 0.75, ratio)
		assert.GreaterOrEqual(t, ratio, threshold)

		mode := helper.DecideCompactionMode(datapb.CompactionType_MixCompaction, ratio, 1)
		assert.Equal(t, CompactionLOBModeSmartRewrite, mode)
	})
}
