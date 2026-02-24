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
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// Copy Mode Implementation for Snapshot/Backup Import
//
// This file implements high-performance segment import by copying files directly
// instead of reading, parsing, and rewriting data. This is specifically designed
// for snapshot restore and backup import scenarios where data format is identical.
//
// IMPLEMENTATION APPROACH:
// 1. Pre-calculate all file path mappings (source -> target) in one pass
// 2. Copy files sequentially using ChunkManager.Copy()
// 3. Preserve all binlog metadata (EntriesNum, Timestamps, LogSize) from source
// 4. Build complete index metadata (vector/scalar, text, JSON) from source
// 5. Generate complete segment metadata with accurate row counts
//
// SUPPORTED FILE TYPES:
// - Binlogs: Insert (required), Delta, Stats, BM25
// - Indexes: Vector/Scalar indexes, Text indexes, JSON Key indexes
//
// WHY THIS APPROACH:
// - Direct file copying is 10-100x faster than data parsing/rewriting
// - Snapshot/backup scenarios guarantee data format compatibility
// - All metadata is preserved from source (binlogs, indexes, row counts, timestamps)
// - Simplified error handling - any copy failure aborts the entire operation
//
// SAFETY:
// - All file operations are validated and logged
// - Copy failures are properly detected and reported with full context
// - Fail-fast behavior prevents partial/inconsistent imports
//
// # CopySegmentAndIndexFiles copies all segment files and index files sequentially
//
// This function is the main entry point for copying segment data from source to target paths.
// It handles all types of segment files (insert, stats, delta, BM25 binlogs) and index files
// (vector/scalar, text, JSON key indexes).
//
// Process flow:
// 1. Validate input - ensure source has insert binlogs
// 2. Generate all file path mappings (source -> target) by replacing collection/partition/segment IDs
// 3. Execute all file copy operations sequentially via ChunkManager
// 4. Build segment metadata preserving source binlog information (row counts, timestamps, etc.)
// 5. Return segment info and index info (currently nil - to be implemented)
//
// Parameters:
//   - ctx: Context for cancellation and logging
//   - cm: ChunkManager for file operations (S3, MinIO, local storage, etc.)
//   - source: Source segment information containing all file paths and metadata
//   - target: Target collection/partition/segment IDs for path transformation
//   - logFields: Additional zap fields for contextual logging
//
// Returns:
//   - result: Complete CopySegmentResult with segment binlogs and index metadata
//   - copiedFiles: List of successfully copied target file paths (for cleanup on failure)
//   - error: First encountered copy error, or nil if all operations succeed
func CopySegmentAndIndexFiles(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	logFields []zap.Field,
) (*datapb.CopySegmentResult, []string, error) {
	log.Info("start copying segment and index files")

	// Step 1: Collect all copy tasks (both segment binlogs and index files)
	mappings, err := createFileMappings(source, target)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to collect copy tasks: %w", err)
	}

	// Step 2: Execute all copy operations and track successfully copied files
	copiedFiles := make([]string, 0, len(mappings))
	for src, dst := range mappings {
		log.Info("execute copy file",
			zap.String("sourcePath", src),
			zap.String("targetPath", dst))
		if err := cm.Copy(ctx, src, dst); err != nil {
			log.Warn("failed to copy file", append(logFields,
				zap.String("sourcePath", src),
				zap.String("targetPath", dst),
				zap.Error(err))...)
			// Return the list of files that were successfully copied before this failure
			return nil, copiedFiles, fmt.Errorf("failed to copy file from %s to %s: %w", src, dst, err)
		}
		// Track successfully copied file
		copiedFiles = append(copiedFiles, dst)
	}

	log.Info("all files copied successfully", append(logFields,
		zap.Int("fileCount", len(mappings)))...)

	// Step 3: Build index metadata from source
	indexInfos, textIndexInfos, jsonKeyIndexInfos := buildIndexInfoFromSource(source, target, mappings)

	// Step 4: Generate segment metadata with path mappings
	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, copiedFiles, fmt.Errorf("failed to generate segment info: %v", err)
	}

	// Step 5: Compress all paths before returning to DataCoord to reduce RPC size
	// This optimization moves compression from DataCoord to DataNode, reducing network overhead

	// Compress binlog paths (insert, stats, delta, bm25) using existing binlog package
	err = binlog.CompressBinLogs(segmentInfo.GetBinlogs(), segmentInfo.GetStatslogs(),
		segmentInfo.GetDeltalogs(), segmentInfo.GetBm25Logs())
	if err != nil {
		return nil, copiedFiles, fmt.Errorf("failed to compress binlog paths: %w", err)
	}

	// Compress vector/scalar index paths to only keep base filenames
	for _, indexInfo := range indexInfos {
		indexInfo.IndexFilePaths = shortenIndexFilePaths(indexInfo.IndexFilePaths)
	}

	// Compress JSON stats paths to keep only key suffix
	jsonKeyIndexInfos = shortenJsonStatsPath(jsonKeyIndexInfos)

	log.Info("path compression completed",
		zap.Int("binlogFields", len(segmentInfo.GetBinlogs())),
		zap.Int("indexCount", len(indexInfos)),
		zap.Int("jsonStatsCount", len(jsonKeyIndexInfos)))

	// Step 6: Build complete result combining segment info and index metadata
	result := &datapb.CopySegmentResult{
		SegmentId:         segmentInfo.GetSegmentID(),
		ImportedRows:      segmentInfo.GetImportedRows(),
		Binlogs:           segmentInfo.GetBinlogs(),
		Statslogs:         segmentInfo.GetStatslogs(),
		Deltalogs:         segmentInfo.GetDeltalogs(),
		Bm25Logs:          segmentInfo.GetBm25Logs(),
		IndexInfos:        indexInfos,
		TextIndexInfos:    textIndexInfos,
		JsonKeyIndexInfos: jsonKeyIndexInfos,
	}

	log.Info("copy segment and index files completed successfully",
		zap.Int64("importedRows", result.ImportedRows))
	return result, copiedFiles, nil
}

// transformFieldBinlogs transforms source FieldBinlog list to destination by replacing paths
// using the pre-calculated mappings, while preserving all other metadata.
//
// This function is used to build the segment metadata that DataCoord needs for tracking
// the imported segment. All source binlog metadata is preserved except for the file paths,
// which are replaced using the mappings generated during the copy operation.
//
// Parameters:
//   - srcFieldBinlogs: Source field binlogs with original paths
//   - mappings: Pre-calculated map of source path -> target path
//   - countRows: If true, accumulate total row count from EntriesNum (for insert logs only)
//
// Returns:
//   - []*datapb.FieldBinlog: Transformed binlog list with target paths
//   - int64: Total row count (sum of EntriesNum from all binlogs if countRows=true, 0 otherwise)
//   - error: Always returns nil in current implementation
func transformFieldBinlogs(
	srcFieldBinlogs []*datapb.FieldBinlog,
	mappings map[string]string,
	countRows bool, // true for insert logs to count total rows
) ([]*datapb.FieldBinlog, int64, error) {
	result := make([]*datapb.FieldBinlog, 0, len(srcFieldBinlogs))
	var totalRows int64

	for _, srcFieldBinlog := range srcFieldBinlogs {
		dstFieldBinlog := proto.Clone(srcFieldBinlog).(*datapb.FieldBinlog)
		dstFieldBinlog.Binlogs = make([]*datapb.Binlog, 0, len(srcFieldBinlog.GetBinlogs()))

		for _, srcBinlog := range srcFieldBinlog.GetBinlogs() {
			if srcPath := srcBinlog.GetLogPath(); srcPath != "" {
				dstBinlog := proto.Clone(srcBinlog).(*datapb.Binlog)
				dstBinlog.LogPath = mappings[srcPath]
				dstFieldBinlog.Binlogs = append(dstFieldBinlog.Binlogs, dstBinlog)

				if countRows {
					totalRows += srcBinlog.GetEntriesNum()
				}
			}
		}

		if len(dstFieldBinlog.Binlogs) > 0 {
			result = append(result, dstFieldBinlog)
		}
	}

	return result, totalRows, nil
}

// generateSegmentInfoFromSource generates ImportSegmentInfo from CopySegmentSource
// by transforming all binlog paths and preserving metadata.
//
// This function constructs the complete segment metadata that DataCoord uses to track
// the imported segment. It processes all four types of binlogs:
//   - Insert binlogs (required): Contains row data, row count is summed for ImportedRows
//   - Stats binlogs (optional): Contains statistics like min/max values
//   - Delta binlogs (optional): Contains delete operations
//   - BM25 binlogs (optional): Contains BM25 index data
//
// All source binlog metadata (EntriesNum, TimestampFrom, TimestampTo, LogSize) is preserved
// to maintain data integrity and enable proper query/compaction operations.
//
// Parameters:
//   - source: Source segment with original binlog paths and metadata
//   - target: Target IDs (collection/partition/segment) for segment identification
//   - mappings: Pre-calculated path mappings (source -> target)
//
// Returns:
//   - *datapb.ImportSegmentInfo: Complete segment metadata with target paths and row counts
//   - error: Error if any binlog transformation fails
func generateSegmentInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (*datapb.ImportSegmentInfo, error) {
	segmentInfo := &datapb.ImportSegmentInfo{
		SegmentID:    target.GetSegmentId(),
		ImportedRows: 0,
		Binlogs:      []*datapb.FieldBinlog{},
		Statslogs:    []*datapb.FieldBinlog{},
		Deltalogs:    []*datapb.FieldBinlog{},
		Bm25Logs:     []*datapb.FieldBinlog{},
	}

	// Process insert binlogs (count rows)
	binlogs, totalRows, err := transformFieldBinlogs(source.GetInsertBinlogs(), mappings, true)
	if err != nil {
		return nil, fmt.Errorf("failed to transform insert binlogs: %w", err)
	}
	segmentInfo.Binlogs = binlogs
	segmentInfo.ImportedRows = totalRows

	// Process stats binlogs (no row counting)
	statslogs, _, err := transformFieldBinlogs(source.GetStatsBinlogs(), mappings, false)
	if err != nil {
		return nil, fmt.Errorf("failed to transform stats binlogs: %w", err)
	}
	segmentInfo.Statslogs = statslogs

	// Process delta binlogs (no row counting)
	deltalogs, _, err := transformFieldBinlogs(source.GetDeltaBinlogs(), mappings, false)
	if err != nil {
		return nil, fmt.Errorf("failed to transform delta binlogs: %w", err)
	}
	segmentInfo.Deltalogs = deltalogs

	// Process BM25 binlogs (no row counting)
	bm25logs, _, err := transformFieldBinlogs(source.GetBm25Binlogs(), mappings, false)
	if err != nil {
		return nil, fmt.Errorf("failed to transform BM25 binlogs: %w", err)
	}
	segmentInfo.Bm25Logs = bm25logs

	return segmentInfo, nil
}

// generateTargetPath converts source file path to target path by replacing collection/partition/segment IDs
// Binlog path format: {rootPath}/{log_type}/{collectionID}/{partitionID}/{segmentID}/{fieldID}/{logID}
// Example: files/insert_log/111/222/333/444/555.log -> files/insert_log/aaa/bbb/ccc/444/555.log
func generateTargetPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	// Convert IDs to strings for replacement
	targetCollectionIDStr := strconv.FormatInt(target.GetCollectionId(), 10)
	targetPartitionIDStr := strconv.FormatInt(target.GetPartitionId(), 10)
	targetSegmentIDStr := strconv.FormatInt(target.GetSegmentId(), 10)

	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Find the log type index (insert_log, delta_log, stats_log, bm25_stats)
	// Path structure: .../log_type/collectionID/partitionID/segmentID/...
	logTypeIndex := -1
	for i, part := range parts {
		if part == BinlogTypeInsert || part == BinlogTypeDelta || part == BinlogTypeStats || part == BinlogTypeBM25 {
			logTypeIndex = i
			break
		}
	}

	if logTypeIndex == -1 || logTypeIndex+3 >= len(parts) {
		return "", fmt.Errorf("invalid binlog path structure: %s (expected log_type at a valid position)", sourcePath)
	}

	// Replace IDs in order: collectionID, partitionID, segmentID
	// log_type is at index logTypeIndex
	// collectionID is at index logTypeIndex + 1
	// partitionID is at index logTypeIndex + 2
	// segmentID is at index logTypeIndex + 3
	parts[logTypeIndex+1] = targetCollectionIDStr
	parts[logTypeIndex+2] = targetPartitionIDStr
	parts[logTypeIndex+3] = targetSegmentIDStr

	return strings.Join(parts, "/"), nil
}

// createFileMappings generates path mappings for all segment files in a single pass.
//
// This function iterates through all file types (binlogs and indexes) and generates
// target paths by replacing collection/partition/segment IDs. The resulting mappings
// are used for both file copying and metadata generation.
//
// Supported file types:
//   - Binlog types: Insert (required), Delta, Stats, BM25
//   - Index types: Vector/Scalar indexes, Text indexes, JSON Key indexes
//
// Path transformation:
//   - Binlogs: {rootPath}/log_type/coll/part/seg/... -> {rootPath}/log_type/NEW_coll/NEW_part/NEW_seg/...
//   - Indexes: Similar ID replacement based on index type path structure
//
// Parameters:
//   - source: Source segment with original file paths
//   - target: Target IDs (collection/partition/segment) for path transformation
//
// Returns:
//   - map[string]string: Source path -> target path for all files
//   - error: Error if path generation fails for any file
func createFileMappings(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (map[string]string, error) {
	mappings := make(map[string]string)

	fileTypeList := []string{BinlogTypeInsert, BinlogTypeDelta, BinlogTypeStats, BinlogTypeBM25, IndexTypeVectorScalar, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats}
	for _, fileType := range fileTypeList {
		switch fileType {
		case BinlogTypeInsert:
			for _, fieldBinlog := range source.GetInsertBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case BinlogTypeDelta:
			for _, fieldBinlog := range source.GetDeltaBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case BinlogTypeStats:
			for _, fieldBinlog := range source.GetStatsBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case BinlogTypeBM25:
			for _, fieldBinlog := range source.GetBm25Binlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case IndexTypeVectorScalar:
			for _, indexInfo := range source.GetIndexFiles() {
				for _, sourcePath := range indexInfo.GetIndexFilePaths() {
					targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
					if err != nil {
						return nil, fmt.Errorf("failed to generate target %s index path for %s: %w", fileType, sourcePath, err)
					}
					mappings[sourcePath] = targetPath
				}
			}

		case IndexTypeText:
			for _, indexInfo := range source.GetTextIndexFiles() {
				for _, sourcePath := range indexInfo.GetFiles() {
					targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
					if err != nil {
						return nil, fmt.Errorf("failed to generate target %s index path for %s: %w", fileType, sourcePath, err)
					}
					mappings[sourcePath] = targetPath
				}
			}

		case IndexTypeJSONKey:
			for _, indexInfo := range source.GetJsonKeyIndexFiles() {
				// Only process legacy format JSON Key Index (data_format < 2)
				// New format (data_format >= 2) is handled by IndexTypeJSONStats case
				if indexInfo.GetJsonKeyStatsDataFormat() < 2 {
					for _, sourcePath := range indexInfo.GetFiles() {
						targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target %s index path for %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}

		case IndexTypeJSONStats:
			for _, indexInfo := range source.GetJsonKeyIndexFiles() {
				// Only process new format JSON Stats (data_format >= 2)
				// Legacy format (data_format < 2) is handled by IndexTypeJSONKey case
				if indexInfo.GetJsonKeyStatsDataFormat() >= 2 {
					for _, sourcePath := range indexInfo.GetFiles() {
						targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target %s path for %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}

		default:
			return nil, fmt.Errorf("unsupported index type: %s", fileType)
		}
	}

	return mappings, nil
}

// buildIndexInfoFromSource builds complete index metadata from source information.
//
// This function extracts and transforms all index metadata (vector/scalar, text, JSON)
// from the source segment, converting file paths to target paths using the provided mappings.
//
// Parameters:
//   - source: Source segment with index file information
//   - target: Target IDs for the segment
//   - mappings: Pre-calculated source->target path mappings
//
// Returns:
//   - Vector/Scalar index metadata (fieldID -> VectorScalarIndexInfo)
//   - Text index metadata (fieldID -> TextIndexStats)
//   - JSON Key index metadata (fieldID -> JsonKeyStats)
func buildIndexInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (
	map[int64]*datapb.VectorScalarIndexInfo,
	map[int64]*datapb.TextIndexStats,
	map[int64]*datapb.JsonKeyStats,
) {
	// Process vector/scalar indexes
	indexInfos := make(map[int64]*datapb.VectorScalarIndexInfo)
	for _, srcIndex := range source.GetIndexFiles() {
		// Transform index file paths using mappings
		targetPaths := make([]string, 0, len(srcIndex.GetIndexFilePaths()))
		for _, srcPath := range srcIndex.GetIndexFilePaths() {
			if targetPath, ok := mappings[srcPath]; ok {
				targetPaths = append(targetPaths, targetPath)
			}
		}

		indexInfos[srcIndex.GetFieldID()] = &datapb.VectorScalarIndexInfo{
			FieldId:                   srcIndex.GetFieldID(),
			IndexId:                   srcIndex.GetIndexID(),
			BuildId:                   srcIndex.GetBuildID(),
			Version:                   srcIndex.GetIndexVersion(),
			IndexFilePaths:            targetPaths,
			IndexSize:                 int64(srcIndex.GetSerializedSize()),
			CurrentIndexVersion:       srcIndex.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: srcIndex.GetCurrentScalarIndexVersion(),
		}
	}

	// Process text indexes - transform file paths
	textIndexInfos := make(map[int64]*datapb.TextIndexStats)
	for fieldID, srcText := range source.GetTextIndexFiles() {
		// Transform text index file paths using mappings
		targetFiles := make([]string, 0, len(srcText.GetFiles()))
		for _, srcFile := range srcText.GetFiles() {
			if targetFile, ok := mappings[srcFile]; ok {
				targetFiles = append(targetFiles, targetFile)
			}
		}

		dstText := proto.Clone(srcText).(*datapb.TextIndexStats)
		dstText.Files = targetFiles
		textIndexInfos[fieldID] = dstText
	}

	// Process JSON Key indexes - transform file paths
	jsonKeyIndexInfos := make(map[int64]*datapb.JsonKeyStats)
	for fieldID, srcJson := range source.GetJsonKeyIndexFiles() {
		// Transform JSON index file paths using mappings
		targetFiles := make([]string, 0, len(srcJson.GetFiles()))
		for _, srcFile := range srcJson.GetFiles() {
			if targetFile, ok := mappings[srcFile]; ok {
				targetFiles = append(targetFiles, targetFile)
			}
		}

		dstJson := proto.Clone(srcJson).(*datapb.JsonKeyStats)
		dstJson.Files = targetFiles
		jsonKeyIndexInfos[fieldID] = dstJson
	}

	return indexInfos, textIndexInfos, jsonKeyIndexInfos
}

// ============================================================================
// File Type Constants
// ============================================================================

// File type constants used for path identification and generation.
// These constants match the directory names in Milvus storage paths.
const (
	BinlogTypeInsert      = "insert_log"
	BinlogTypeStats       = "stats_log"
	BinlogTypeDelta       = "delta_log"
	BinlogTypeBM25        = "bm25_stats"
	IndexTypeVectorScalar = "index_files"
	IndexTypeText         = "text_log"
	IndexTypeJSONKey      = "json_key_index_log" // Legacy: JSON Key Inverted Index
	IndexTypeJSONStats    = "json_stats"         // New: JSON Stats with Shredding Design
)

// generateTargetIndexPath is the unified function for generating target paths for all index types
// The indexType parameter specifies which type of index path to generate
//
// Supported index types (use constants):
//   - IndexTypeVectorScalar: Vector/Scalar Index path format (from BuildSegmentIndexFilePaths)
//     {rootPath}/index_files/{build_id}/{index_version}/{partition_id}/{segment_id}/file
//     Note: collectionID is NOT in the path, only partitionID and segmentID are replaced
//   - IndexTypeText: Text Index path format
//     {rootPath}/text_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONKey: JSON Key Index path format (legacy)
//     {rootPath}/json_key_index_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONStats: JSON Stats path format (new, data_format >= 2)
//     {rootPath}/json_stats/{data_format_version}/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/(shared_key_index|shredding_data)/...
//
// Examples:
// generateTargetIndexPath(..., IndexTypeVectorScalar):
//
//	files/index_files/1001/1/222/333/scalar_index -> files/index_files/1001/1/bbb/ccc/scalar_index
//
// generateTargetIndexPath(..., IndexTypeText):
//
//	files/text_log/123/1/111/222/333/444/index_file -> files/text_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONKey):
//
//	files/json_key_index_log/123/1/111/222/333/444/index_file -> files/json_key_index_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONStats):
//
//	files/json_stats/2/123/1/111/222/333/444/shared_key_index/file -> files/json_stats/2/123/1/aaa/bbb/ccc/444/shared_key_index/file
func generateTargetIndexPath(
	sourcePath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	indexType string,
) (string, error) {
	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Determine keyword and offsets based on index type
	var keywordIdx int
	var collectionOffset, partitionOffset, segmentOffset int

	// Find the keyword position in the path
	keywordIdx = -1
	for i, part := range parts {
		if part == indexType {
			keywordIdx = i
			break
		}
	}

	if keywordIdx == -1 {
		return "", fmt.Errorf("keyword '%s' not found in path: %s", indexType, sourcePath)
	}

	// Set offsets based on index type
	// collectionOffset = -1 means collectionID is not present in the path
	switch indexType {
	case IndexTypeVectorScalar:
		// Vector/Scalar index: index_files/{buildID}/{indexVersion}/{partID}/{segID}/{fileKey}
		// Path generated by BuildSegmentIndexFilePaths - no collectionID in path
		collectionOffset = -1
		partitionOffset = 3
		segmentOffset = 4
	case IndexTypeText, IndexTypeJSONKey:
		// Text/JSON index: text_log|json_key_index_log/build/ver/coll/part/seg/field
		collectionOffset = 3
		partitionOffset = 4
		segmentOffset = 5
	case IndexTypeJSONStats:
		// JSON Stats: json_stats/data_format_ver/build/ver/coll/part/seg/field/(shared_key_index|shredding_data)/...
		collectionOffset = 4 // One more level than legacy (data_format_version)
		partitionOffset = 5
		segmentOffset = 6
	default:
		return "", fmt.Errorf("unsupported index type: %s (expected '%s', '%s', '%s', or '%s')",
			indexType, IndexTypeVectorScalar, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats)
	}

	// Validate path structure has enough components
	if keywordIdx+segmentOffset >= len(parts) {
		return "", fmt.Errorf("invalid %s path structure: %s (expected '%s' with at least %d components after it)",
			indexType, sourcePath, indexType, segmentOffset+1)
	}

	// Replace IDs at specified offsets
	// collectionOffset = -1 means collectionID is not present in the path (e.g., vector/scalar index)
	if collectionOffset >= 0 {
		parts[keywordIdx+collectionOffset] = strconv.FormatInt(target.GetCollectionId(), 10)
	}
	parts[keywordIdx+partitionOffset] = strconv.FormatInt(target.GetPartitionId(), 10)
	parts[keywordIdx+segmentOffset] = strconv.FormatInt(target.GetSegmentId(), 10)

	return strings.Join(parts, "/"), nil
}

// ============================================================================
// Path Compression Utilities
// ============================================================================
// These functions compress file paths before returning to DataCoord to reduce
// RPC response size and network transmission overhead.
// The implementations are copied from internal/datacoord/copy_segment_task.go
// to maintain consistency with DataCoord's compression logic.

const (
	jsonStatsSharedIndexPath   = "shared_key_index"
	jsonStatsShreddingDataPath = "shredding_data"
)

// shortenIndexFilePaths shortens vector/scalar index file paths to only keep the base filename.
//
// In normal index building flow, only the base filename (last path segment) is stored in IndexFileKeys.
// In copy segment flow, DataNode returns full paths after file copying.
// This function extracts the base filename to match the format expected by QueryNode loading.
//
// Path transformation:
//   - Input:  "files/index_files/444/555/666/100/1001/1002/scalar_index"
//   - Output: "scalar_index"
//
// Why only base filename:
// - DataCoord rebuilds full paths using BuildSegmentIndexFilePaths when needed
// - Storing full paths would cause duplicate path concatenation
// - Matches the convention from normal index building
//
// Parameters:
//   - fullPaths: List of full index file paths
//
// Returns:
//   - List of base filenames (last segment of each path)
func shortenIndexFilePaths(fullPaths []string) []string {
	result := make([]string, 0, len(fullPaths))
	for _, fullPath := range fullPaths {
		// Extract base filename (last segment after final '/')
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			result = append(result, parts[len(parts)-1])
		}
	}
	return result
}

// shortenJsonStatsPath shortens JSON stats file paths to only keep the last 2+ segments.
//
// In normal import flow, the C++ core returns already-shortened paths (e.g., "shared_key_index/file").
// In copy segment flow, DataNode returns full paths after file copying.
// This function normalizes the paths to match the format expected by query nodes.
//
// Path transformation:
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0"
//   - Output: "shared_key_index/inverted_index_0"
//
// Parameters:
//   - jsonStats: Map of field ID to JsonKeyStats with full paths
//
// Returns:
//   - Map of field ID to JsonKeyStats with shortened paths
func shortenJsonStatsPath(jsonStats map[int64]*datapb.JsonKeyStats) map[int64]*datapb.JsonKeyStats {
	result := make(map[int64]*datapb.JsonKeyStats)
	for fieldID, stats := range jsonStats {
		shortenedFiles := make([]string, 0, len(stats.GetFiles()))
		for _, file := range stats.GetFiles() {
			shortenedFiles = append(shortenedFiles, shortenSingleJsonStatsPath(file))
		}

		result[fieldID] = &datapb.JsonKeyStats{
			FieldID:                stats.GetFieldID(),
			Version:                stats.GetVersion(),
			BuildID:                stats.GetBuildID(),
			Files:                  shortenedFiles,
			JsonKeyStatsDataFormat: stats.GetJsonKeyStatsDataFormat(),
			MemorySize:             stats.GetMemorySize(),
			LogSize:                stats.GetLogSize(),
		}
	}
	return result
}

// shortenSingleJsonStatsPath shortens a single JSON stats file path.
//
// This function extracts the relative path from a full JSON stats file path by:
//  1. Finding "shared_key_index" or "shredding_data" keywords and extracting from that position
//  2. For files directly under fieldID directory (e.g., meta.json), extracting everything after
//     the 7 path components following "json_stats"
//
// Path format: {root}/json_stats/{dataFormat}/{buildID}/{version}/{collID}/{partID}/{segID}/{fieldID}/...
//
// Path examples:
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shared_key_index/inverted_index_0"
//     Output: "shared_key_index/inverted_index_0"
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/shredding_data/parquet_data_0"
//     Output: "shredding_data/parquet_data_0"
//   - Input:  "files/json_stats/2/123/1/444/555/666/100/meta.json"
//     Output: "meta.json"
//   - Input:  "shared_key_index/inverted_index_0" (already shortened)
//     Output: "shared_key_index/inverted_index_0" (idempotent)
//   - Input:  "meta.json" (already shortened)
//     Output: "meta.json" (idempotent)
//
// Parameters:
//   - fullPath: Full or partial JSON stats file path
//
// Returns:
//   - Shortened path relative to fieldID directory
func shortenSingleJsonStatsPath(fullPath string) string {
	// Find "shared_key_index" in path
	if idx := strings.Index(fullPath, jsonStatsSharedIndexPath); idx != -1 {
		return fullPath[idx:]
	}
	// Find "shredding_data" in path
	if idx := strings.Index(fullPath, jsonStatsShreddingDataPath); idx != -1 {
		return fullPath[idx:]
	}

	// Handle files directly under fieldID directory (e.g., meta.json)
	// Path format: .../json_stats/{dataFormat}/{build}/{ver}/{coll}/{part}/{seg}/{field}/filename
	// json_stats is followed by 7 components, the 8th onwards is the file path
	parts := strings.Split(fullPath, "/")
	for i, part := range parts {
		if part == common.JSONStatsPath && i+8 < len(parts) {
			return strings.Join(parts[i+8:], "/")
		}
	}

	// If already shortened or no json_stats found, return as-is
	return fullPath
}
