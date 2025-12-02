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
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func getLOBCompactionSmartRewriteThreshold() float64 {
	return paramtable.Get().CommonCfg.LOBCompactionSmartRewriteThreshold.GetAsFloat()
}

type CompactionLOBMode int

const (
	// CompactionLOBModeReferenceOnly only copies references, doesn't move LOB files
	CompactionLOBModeReferenceOnly CompactionLOBMode = iota

	// CompactionLOBModeMove moves LOB files to new location (fast, no filtering)
	CompactionLOBModeMove

	// CompactionLOBModeSmartRewrite moves and filters LOB files (slower but saves space)
	CompactionLOBModeSmartRewrite

	// CompactionLOBModeSkip skips LOB processing
	CompactionLOBModeSkip
)

type LOBCompactionHelper struct {
	binlogIO      io.BinlogIO
	helper        *storage.LOBCompactionHelper
	storageConfig *indexpb.StorageConfig
	logger        *log.MLogger
}

func NewLOBCompactionHelper(binlogIO io.BinlogIO, storageConfig *indexpb.StorageConfig) *LOBCompactionHelper {
	chunkManager := binlogIO.(storage.ChunkManager)

	return &LOBCompactionHelper{
		binlogIO:      binlogIO,
		helper:        storage.NewLOBCompactionHelper(chunkManager, storageConfig),
		storageConfig: storageConfig,
		logger:        log.With(zap.String("component", "LOBCompactionHelper")),
	}
}

func (h *LOBCompactionHelper) ExtractLOBMetadataFromSegment(
	ctx context.Context,
	segmentID int64,
	collectionID int64,
	partitionID int64,
) (*storage.LOBSegmentMetadata, error) {
	metadata := storage.NewLOBSegmentMetadata()
	chunkManager := h.binlogIO.(storage.ChunkManager)
	rootPath := chunkManager.RootPath()

	// construct LOB file prefix for this segment
	// Path pattern: {root}/insert_log/{coll}/{part}/{seg}/{field}/lob/*.parquet
	lobPrefix := fmt.Sprintf("%s/insert_log/%d/%d/%d",
		rootPath, collectionID, partitionID, segmentID)

	// walk through the segment directory to find LOB files
	err := chunkManager.WalkWithPrefix(ctx, lobPrefix, true,
		func(chunkInfo *storage.ChunkObjectInfo) bool {
			if !strings.Contains(chunkInfo.FilePath, "/lob/") {
				return true // continue
			}

			fieldID, _, ok := metautil.ParseLOBFilePath(chunkInfo.FilePath)
			if !ok || fieldID == 0 {
				h.logger.Warn("failed to parse LOB file path",
					zap.String("path", chunkInfo.FilePath))
				return true // continue
			}

			// get or create field metadata
			fieldMeta, exists := metadata.LOBFields[fieldID]
			if !exists {
				fieldMeta = &storage.LOBFieldMetadata{
					FieldID:  fieldID,
					LOBFiles: make([]string, 0),
				}
				metadata.LOBFields[fieldID] = fieldMeta
			}

			// add LOB file (store relative path)
			relativePath := strings.TrimPrefix(chunkInfo.FilePath, rootPath+"/")
			fieldMeta.LOBFiles = append(fieldMeta.LOBFiles, relativePath)
			metadata.TotalLOBFiles++

			return true // continue
		})

	if err != nil {
		return nil, err
	}

	if !metadata.HasLOBFields() {
		return nil, nil
	}

	h.logger.Info("extracted LOB metadata from segment",
		zap.Int64("segmentID", segmentID),
		zap.Int("lobFiles", metadata.TotalLOBFiles),
		zap.Int("fields", len(metadata.LOBFields)))

	return metadata, nil
}

// DecideCompactionMode decides which LOB compaction mode to use
func (h *LOBCompactionHelper) DecideCompactionMode(
	compactionType datapb.CompactionType,
	deleteRatio float64,
	sourceSegmentCount int,
) CompactionLOBMode {
	smartRewriteThreshold := getLOBCompactionSmartRewriteThreshold()

	switch compactionType {
	case datapb.CompactionType_MixCompaction:
		if deleteRatio >= smartRewriteThreshold {
			h.logger.Info("using SmartRewrite mode for mix compaction",
				zap.Float64("deleteRatio", deleteRatio),
				zap.Float64("smartRewriteThreshold", smartRewriteThreshold),
				zap.String("reason", "high delete ratio, filter unused rows to save space"))
			return CompactionLOBModeSmartRewrite
		} else {
			h.logger.Info("using Move mode for mix compaction",
				zap.Float64("deleteRatio", deleteRatio),
				zap.Float64("smartRewriteThreshold", smartRewriteThreshold),
				zap.String("reason", "low delete ratio, simply copy LOB files"))
			return CompactionLOBModeMove
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
		// For unknown compaction types, skip LOB processing
		h.logger.Warn("unknown compaction type, skipping LOB processing",
			zap.String("type", compactionType.String()))
		return CompactionLOBModeSkip
	}
}

// CalculateDeleteRatio calculates the delete ratio from source segments
func CalculateDeleteRatio(plan *datapb.CompactionPlan, resultSegments []*datapb.CompactionSegment) float64 {
	var totalInputRows, totalOutputRows int64

	for _, segBinlog := range plan.GetSegmentBinlogs() {
		if len(segBinlog.GetFieldBinlogs()) > 0 {
			for _, binlog := range segBinlog.GetFieldBinlogs()[0].GetBinlogs() {
				totalInputRows += binlog.GetEntriesNum()
			}
		}
	}

	for _, seg := range resultSegments {
		totalOutputRows += seg.GetNumOfRows()
	}

	if totalInputRows == 0 {
		return 0
	}

	deletedRows := totalInputRows - totalOutputRows
	if deletedRows < 0 {
		deletedRows = 0
	}

	return float64(deletedRows) / float64(totalInputRows)
}

// CompactLOBFiles handles LOB file compaction based on mode
// Returns LOBCompactionResult which includes metadata and reference mapping
// lobFileIDAlloc: allocator for generating new LOB file IDs (required for SmartRewrite mode, can be nil for other modes)
func (h *LOBCompactionHelper) CompactLOBFiles(
	ctx context.Context,
	sourceMetadatas []*storage.LOBSegmentMetadata,
	targetSegmentID int64,
	partitionID int64,
	collectionID int64,
	mode CompactionLOBMode,
	lobFileIDAlloc allocator.Interface,
) (*storage.LOBCompactionResult, error) {
	switch mode {
	case CompactionLOBModeReferenceOnly:
		metadata, err := h.compactReferenceOnly(sourceMetadatas)
		if err != nil {
			return nil, err
		}
		return &storage.LOBCompactionResult{
			Metadata:         metadata,
			ReferenceMapping: storage.NewLOBReferenceMapping(),
		}, nil

	case CompactionLOBModeMove:
		// use Relocate mode - files are copied but NOT deleted
		// source deletion happens in GC phase after compaction is committed
		chunkManager := h.binlogIO.(storage.ChunkManager)
		return h.helper.CompactLOBFiles(
			ctx,
			sourceMetadatas,
			targetSegmentID,
			partitionID,
			collectionID,
			chunkManager.RootPath(),
			storage.CompactionModeRelocate,
			nil, // no reference set for simple relocate
			nil, // allocator not needed for Relocate mode
		)

	case CompactionLOBModeSmartRewrite:
		// SmartRewrite mode should never reach here - it's processed in-flight
		// LOB processing happens during batch loop in mix_compactor.go (lines 720-757)
		// If we reach here, it indicates a programming error
		return nil, fmt.Errorf("SmartRewrite mode should not call CompactLOBFiles - LOB processing is done in-flight")

	case CompactionLOBModeSkip:
		return &storage.LOBCompactionResult{
			Metadata:         storage.NewLOBSegmentMetadata(),
			ReferenceMapping: storage.NewLOBReferenceMapping(),
		}, nil

	default:
		return nil, fmt.Errorf("unknown LOB compaction mode: %d", mode)
	}
}

// compactReferenceOnly implements reference-only compaction
// In this mode, LOB files are not moved, only metadata is merged
func (h *LOBCompactionHelper) compactReferenceOnly(
	sourceMetadatas []*storage.LOBSegmentMetadata,
) (*storage.LOBSegmentMetadata, error) {
	h.logger.Info("compacting LOB files in Reference-Only mode",
		zap.Int("sourceSegments", len(sourceMetadatas)))

	// Simply merge metadata from all source segments
	// LOB files remain in their original locations
	targetMetadata := storage.MergeLOBMetadata(sourceMetadatas)

	h.logger.Info("Reference-Only compaction completed",
		zap.Int("totalLOBFiles", targetMetadata.TotalLOBFiles),
		zap.Int64("totalLOBRecords", targetMetadata.TotalLOBRecords),
		zap.Int64("totalLOBBytes", targetMetadata.TotalLOBBytes))

	return targetMetadata, nil
}

// GetStorageConfig returns the storage configuration for creating LOB readers
func (h *LOBCompactionHelper) GetStorageConfig() *indexpb.StorageConfig {
	return h.storageConfig
}
