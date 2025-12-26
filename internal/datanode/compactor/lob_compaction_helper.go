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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func getLOBCompactionSmartRewriteThreshold() float64 {
	return paramtable.Get().CommonCfg.LOBCompactionSmartRewriteThreshold.GetAsFloat()
}

// CompactionLOBMode defines how LOB files are handled during compaction.
//
// LOB compaction modes:
//   - ReferenceOnly: LOB files stay in place, only segment metadata is updated.
//     used for low delete ratio scenarios where copying LOB data is wasteful.
//   - SmartRewrite: LOB files are read, filtered, and written to new files.
//     used for high delete ratio scenarios to reclaim space.
//   - Skip: no LOB processing needed (no LOB data in source segments).
type CompactionLOBMode int

const (
	CompactionLOBModeReferenceOnly CompactionLOBMode = iota

	CompactionLOBModeSmartRewrite

	CompactionLOBModeSkip
)

// LOBCompactionHelper handles LOB file operations during compaction.
//
// responsibilities:
//   - convert proto LOB metadata to internal storage structure
//   - decide compaction mode based on delete ratio and compaction type
//   - provide storage config for LOB readers/writers
type LOBCompactionHelper struct {
	storageConfig *indexpb.StorageConfig
	logger        *log.MLogger
}

func NewLOBCompactionHelper(storageConfig *indexpb.StorageConfig) *LOBCompactionHelper {
	return &LOBCompactionHelper{
		storageConfig: storageConfig,
		logger:        log.With(zap.String("component", "LOBCompactionHelper")),
	}
}

// ConvertProtoLOBMetadata converts proto LOB metadata (from CompactionSegmentBinlogs)
// to internal storage.LOBSegmentMetadata structure.
func (h *LOBCompactionHelper) ConvertProtoLOBMetadata(
	protoMeta map[int64]*datapb.LOBFieldMetadata,
	segmentID int64,
	collectionID int64,
	partitionID int64,
) *storage.LOBSegmentMetadata {
	if len(protoMeta) == 0 {
		return nil
	}

	metadata := storage.NewLOBSegmentMetadata()
	metadata.SegmentID = segmentID
	metadata.CollectionID = collectionID
	metadata.PartitionID = partitionID

	for fieldID, protoFieldMeta := range protoMeta {
		lobFiles := make([]*storage.LOBFileInfo, 0, len(protoFieldMeta.LobFiles))
		for _, protoLobFile := range protoFieldMeta.LobFiles {
			validCount := protoLobFile.GetValidRecordCount()
			lobFiles = append(lobFiles, &storage.LOBFileInfo{
				FilePath:         protoLobFile.GetFilePath(),
				LobFileID:        protoLobFile.GetLobFileId(),
				RowCount:         protoLobFile.GetRowCount(),
				ValidRecordCount: validCount,
			})
		}
		fieldMeta := &storage.LOBFieldMetadata{
			FieldID:       fieldID,
			LOBFiles:      lobFiles,
			SizeThreshold: protoFieldMeta.SizeThreshold,
			RecordCount:   protoFieldMeta.RecordCount,
			TotalBytes:    protoFieldMeta.TotalBytes,
		}
		metadata.LOBFields[fieldID] = fieldMeta
		metadata.TotalLOBFiles += len(fieldMeta.LOBFiles)
		metadata.TotalLOBRecords += fieldMeta.RecordCount
		metadata.TotalLOBBytes += fieldMeta.TotalBytes
	}

	if !metadata.HasLOBFields() {
		return nil
	}

	h.logger.Info("converted LOB metadata from proto",
		zap.Int64("segmentID", segmentID),
		zap.Int("lobFiles", metadata.TotalLOBFiles),
		zap.Int("fields", len(metadata.LOBFields)))

	return metadata
}

// DecideCompactionMode decides which LOB compaction mode to use based on:
//   - compaction type (Mix, Clustering, Sort, L0 Delete)
//   - lobGarbageRatio(for Mix compaction)
//
// decision logic:
//   - MixCompaction: SmartRewrite if high lobGarbageRatio, otherwise ReferenceOnly
//   - ClusteringCompaction: ReferenceOnly (only reorder references)
//   - SortCompaction: ReferenceOnly (only reorder references)
//   - L0DeleteCompaction: Skip (no LOB changes needed)
func (h *LOBCompactionHelper) DecideCompactionMode(
	compactionType datapb.CompactionType,
	lobGarbageRatio float64,
	sourceSegmentCount int,
) CompactionLOBMode {
	smartRewriteThreshold := getLOBCompactionSmartRewriteThreshold()

	switch compactionType {
	case datapb.CompactionType_MixCompaction:
		if lobGarbageRatio >= smartRewriteThreshold {
			h.logger.Info("using SmartRewrite mode for mix compaction",
				zap.Float64("lobGarbageRatio", lobGarbageRatio),
				zap.Float64("smartRewriteThreshold", smartRewriteThreshold),
				zap.String("reason", "high delete ratio, filter unused rows to save space"))
			return CompactionLOBModeSmartRewrite
		} else {
			h.logger.Info("using Reference-Only mode for mix compaction",
				zap.Float64("lobGarbageRatio", lobGarbageRatio),
				zap.Float64("smartRewriteThreshold", smartRewriteThreshold),
				zap.String("reason", "low delete ratio, only add references to existing LOB files"))
			return CompactionLOBModeReferenceOnly
		}

	case datapb.CompactionType_ClusteringCompaction:
		h.logger.Info("using Reference-Only mode for clustering compaction",
			zap.String("reason", "only reorder reference column, LOB files stay in place"))
		return CompactionLOBModeReferenceOnly

	case datapb.CompactionType_SortCompaction:
		h.logger.Info("using Reference-Only mode for sort compaction",
			zap.String("reason", "order change only, LOB files stay in place"))
		return CompactionLOBModeReferenceOnly

	case datapb.CompactionType_Level0DeleteCompaction:
		h.logger.Info("skipping LOB processing for L0 delete compaction",
			zap.String("reason", "L0 compaction only applies deletes, no LOB file changes needed"))
		return CompactionLOBModeSkip

	default:
		h.logger.Warn("unknown compaction type, skipping LOB processing",
			zap.String("type", compactionType.String()))
		return CompactionLOBModeSkip
	}
}

// GetStorageConfig returns the storage configuration for creating LOB readers/writers
func (h *LOBCompactionHelper) GetStorageConfig() *indexpb.StorageConfig {
	return h.storageConfig
}

// CalculateLOBGarbageRatio calculates the garbage ratio from LOB metadata.
// garbage ratio = 1 - (sum of valid_record_count) / (sum of row_count)
// returns 0 if no LOB metadata or all records are valid.
func (h *LOBCompactionHelper) CalculateLOBGarbageRatio(lobMetadata []*storage.LOBSegmentMetadata) float64 {
	if len(lobMetadata) == 0 {
		return 0
	}

	var totalRowCount int64
	var totalValidCount int64

	for _, meta := range lobMetadata {
		if meta == nil || !meta.HasLOBFields() {
			continue
		}

		for _, fieldMeta := range meta.LOBFields {
			for _, lobFile := range fieldMeta.LOBFiles {
				totalRowCount += lobFile.RowCount
				validCount := lobFile.ValidRecordCount
				totalValidCount += validCount
			}
		}
	}

	if totalRowCount == 0 {
		return 0
	}

	garbageRatio := 1.0 - float64(totalValidCount)/float64(totalRowCount)
	if garbageRatio < 0 {
		garbageRatio = 0
	}

	h.logger.Info("calculated LOB garbage ratio",
		zap.Int64("totalRowCount", totalRowCount),
		zap.Int64("totalValidCount", totalValidCount),
		zap.Float64("garbageRatio", garbageRatio))

	return garbageRatio
}

// ConvertToProtoLOBMetadata converts internal storage.LOBSegmentMetadata to proto field metadata map.
// this is the reverse of ConvertProtoLOBMetadata.
func (h *LOBCompactionHelper) ConvertToProtoLOBMetadata(meta *storage.LOBSegmentMetadata) map[int64]*datapb.LOBFieldMetadata {
	if meta == nil || !meta.HasLOBFields() {
		return nil
	}

	protoFieldMap := make(map[int64]*datapb.LOBFieldMetadata)

	for fieldID, fieldMeta := range meta.LOBFields {
		// convert storage LOBFileInfo to proto LOBFile
		protoLobFiles := make([]*datapb.LOBFile, 0, len(fieldMeta.LOBFiles))
		for _, lobFile := range fieldMeta.LOBFiles {
			protoLobFiles = append(protoLobFiles, &datapb.LOBFile{
				FilePath:         lobFile.FilePath,
				LobFileId:        lobFile.LobFileID,
				RowCount:         lobFile.RowCount,
				ValidRecordCount: lobFile.ValidRecordCount,
			})
		}
		protoFieldMap[fieldID] = &datapb.LOBFieldMetadata{
			FieldId:       fieldMeta.FieldID,
			LobFiles:      protoLobFiles,
			SizeThreshold: fieldMeta.SizeThreshold,
			RecordCount:   fieldMeta.RecordCount,
			TotalBytes:    fieldMeta.TotalBytes,
		}
	}

	return protoFieldMap
}

// MergeLOBMetadata merges LOB metadata from multiple source segments into one.
// used in ReferenceOnly compaction mode where LOB files stay in place.
func MergeLOBMetadata(sourceSegments []*storage.LOBSegmentMetadata) *storage.LOBSegmentMetadata {
	merged := storage.NewLOBSegmentMetadata()

	for _, sourceMeta := range sourceSegments {
		if sourceMeta == nil || !sourceMeta.HasLOBFields() {
			continue
		}

		for fieldID, fieldMeta := range sourceMeta.LOBFields {
			targetFieldMeta, exists := merged.LOBFields[fieldID]
			if !exists {
				lobFilesCopy := make([]*storage.LOBFileInfo, len(fieldMeta.LOBFiles))
				for i, f := range fieldMeta.LOBFiles {
					lobFilesCopy[i] = &storage.LOBFileInfo{
						FilePath:         f.FilePath,
						LobFileID:        f.LobFileID,
						RowCount:         f.RowCount,
						ValidRecordCount: f.ValidRecordCount,
					}
				}
				targetFieldMeta = &storage.LOBFieldMetadata{
					FieldID:       fieldMeta.FieldID,
					LOBFiles:      lobFilesCopy,
					SizeThreshold: fieldMeta.SizeThreshold,
					RecordCount:   fieldMeta.RecordCount,
					TotalBytes:    fieldMeta.TotalBytes,
				}
				merged.LOBFields[fieldID] = targetFieldMeta
			} else {
				targetFieldMeta.LOBFiles = append(targetFieldMeta.LOBFiles, fieldMeta.LOBFiles...)
				targetFieldMeta.RecordCount += fieldMeta.RecordCount
				targetFieldMeta.TotalBytes += fieldMeta.TotalBytes
			}
		}
	}

	// calculate totals
	for _, fieldMeta := range merged.LOBFields {
		merged.TotalLOBFiles += len(fieldMeta.LOBFiles)
		merged.TotalLOBRecords += fieldMeta.RecordCount
		merged.TotalLOBBytes += fieldMeta.TotalBytes
	}

	return merged
}

// FilterLOBMetadataByUsedFiles filters LOB metadata to only keep files that are actually used.
// usedLOBFileIDs is a map of fieldID -> map of lob_file_id -> reference count.
// the reference count is used to update valid_record_count for each LOB file.
func FilterLOBMetadataByUsedFiles(
	fullMeta *storage.LOBSegmentMetadata,
	usedLOBFileIDs map[int64]map[int64]int64,
) *storage.LOBSegmentMetadata {
	if fullMeta == nil || !fullMeta.HasLOBFields() {
		return nil
	}

	filtered := storage.NewLOBSegmentMetadata()
	filtered.SegmentID = fullMeta.SegmentID
	filtered.CollectionID = fullMeta.CollectionID
	filtered.PartitionID = fullMeta.PartitionID

	for fieldID, fieldMeta := range fullMeta.LOBFields {
		usedFileIDsWithCount, exists := usedLOBFileIDs[fieldID]
		if !exists || len(usedFileIDsWithCount) == 0 {
			// no LOB files used for this field in this segment
			continue
		}

		// filter LOB files to only keep used ones and update valid_record_count
		var filteredFiles []*storage.LOBFileInfo
		var totalRowCount int64
		var totalBytes int64

		for _, lobFile := range fieldMeta.LOBFiles {
			if refCount, used := usedFileIDsWithCount[lobFile.LobFileID]; used {
				filteredFiles = append(filteredFiles, &storage.LOBFileInfo{
					FilePath:         lobFile.FilePath,
					LobFileID:        lobFile.LobFileID,
					RowCount:         lobFile.RowCount, // keep original row count
					ValidRecordCount: refCount,         // actual reference count in this segment
				})
				// RecordCount and TotalBytes should reflect original file sizes
				// so garbage ratio can be calculated correctly
				totalRowCount += lobFile.RowCount
				// estimate bytes based on proportion of this file's rows
				if fieldMeta.RecordCount > 0 {
					totalBytes += fieldMeta.TotalBytes * lobFile.RowCount / fieldMeta.RecordCount
				}
			}
		}

		if len(filteredFiles) > 0 {
			filtered.LOBFields[fieldID] = &storage.LOBFieldMetadata{
				FieldID:       fieldMeta.FieldID,
				LOBFiles:      filteredFiles,
				SizeThreshold: fieldMeta.SizeThreshold,
				RecordCount:   totalRowCount, // total original rows in included files
				TotalBytes:    totalBytes,    // total original bytes in included files
			}
			filtered.TotalLOBFiles += len(filteredFiles)
			filtered.TotalLOBRecords += totalRowCount
			filtered.TotalLOBBytes += totalBytes
		}
	}

	if !filtered.HasLOBFields() {
		return nil
	}

	return filtered
}
