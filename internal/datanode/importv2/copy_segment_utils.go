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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// SegmentFiles organizes source files by type for copy operations.
// InsertBinlogs come from manifest (when storage_version >= StorageV3) or pb (otherwise).
// Other types are always from pb.
type SegmentFiles struct {
	// From manifest (when storage_version >= StorageV3) or pb (when < StorageV3)
	InsertBinlogs []string

	// Always from pb
	DeltaBinlogs      []string
	StatsBinlogs      []string
	Bm25Binlogs       []string
	VectorScalarIndex []string
	TextIndex         []string
	JsonKeyIndex      []string
	JsonStats         []string
}

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
// CopySegmentAndIndexFiles copies all segment files and index files sequentially.
//
// Process flow:
// 1. Collect all source files (from manifest or pb)
// 2. Generate src->dst path mappings
// 3. Execute file copy operations
// 3.5. For manifest segments (StorageV3+), add logical pb path mappings after physical
//      files are copied (needed for metadata generation which expects pb paths)
// 4. Build index metadata from source
// 5. Generate segment metadata with path mappings
// 6. Compress paths for RPC efficiency
// 7. Build result with all metadata
// 8. Transform manifest path if present

// transformManifestPath replaces source IDs in manifest path with target IDs.
//
// Manifest path is a JSON string: {"ver": 2, "base_path": "files/insert_log/coll/part/seg"}
//
// Process:
// 1. Unmarshal JSON to get base_path and version
// 2. Replace collection/partition/segment IDs in base_path using generateTargetPath
// 3. Marshal back to JSON
func transformManifestPath(
	manifestPath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (string, error) {
	basePath, version, err := packed.UnmarshalManfestPath(manifestPath)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal manifest path: %w", err)
	}

	targetBasePath, err := generateTargetPath(basePath, source, target)
	if err != nil {
		return "", fmt.Errorf("failed to generate target base path: %w", err)
	}

	targetManifestPath := packed.MarshalManifestPath(targetBasePath, version)
	return targetManifestPath, nil
}

// listAllFiles recursively lists all files under the given path using WalkWithPrefix.
// Returns (nil, error) if the walk fails.
func listAllFiles(ctx context.Context, cm storage.ChunkManager, basePath string) ([]string, error) {
	var files []string
	err := cm.WalkWithPrefix(ctx, basePath, true, func(info *storage.ChunkObjectInfo) bool {
		files = append(files, info.FilePath)
		return true
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// copyFile copies a single file from src to dst using the chunk manager.
func copyFile(ctx context.Context, cm storage.ChunkManager, src, dst string) error {
	return cm.Copy(ctx, src, dst)
}

// extractFromPb extracts file paths from FieldBinlog list (insert/delta/stats/bm25).
func extractFromPb(fieldBinlogs []*datapb.FieldBinlog) []string {
	var paths []string
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if path := binlog.GetLogPath(); path != "" {
				paths = append(paths, path)
			}
		}
	}
	return paths
}

// extractIndexFiles extracts vector/scalar index file paths.
func extractIndexFiles(indexInfos []*indexpb.IndexFilePathInfo) []string {
	var paths []string
	for _, info := range indexInfos {
		paths = append(paths, info.GetIndexFilePaths()...)
	}
	return paths
}

// extractTextIndexFiles extracts text index file paths.
func extractTextIndexFiles(textIndexInfos map[int64]*datapb.TextIndexStats) []string {
	var paths []string
	for _, info := range textIndexInfos {
		paths = append(paths, info.GetFiles()...)
	}
	return paths
}

// extractJsonFiles extracts JSON index files, separated by data format version.
// Returns (jsonKeyFiles, jsonStatsFiles).
func extractJsonFiles(jsonIndexInfos map[int64]*datapb.JsonKeyStats) ([]string, []string) {
	var jsonKeyFiles []string
	var jsonStatsFiles []string

	for _, info := range jsonIndexInfos {
		dataFormat := info.GetJsonKeyStatsDataFormat()
		files := info.GetFiles()

		if dataFormat < 2 {
			// Legacy format (< v2) -> JSON Key Index
			jsonKeyFiles = append(jsonKeyFiles, files...)
		} else {
			// New format (>= v2) -> JSON Stats
			jsonStatsFiles = append(jsonStatsFiles, files...)
		}
	}

	return jsonKeyFiles, jsonStatsFiles
}

// collectSegmentFiles collects all files to copy, organized by type.
//
// For InsertBinlogs, the decision is based on storage_version:
//   - storage_version >= StorageV3 (3): MUST resolve from manifest_path.
//     manifest_path missing → error. Listing fails → error. Empty file list → OK (no binlogs).
//   - storage_version < StorageV3: use pb paths (traditional non-packed format).
//
// For other 7 types: always from pb (not yet in manifest).
func collectSegmentFiles(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
) (*SegmentFiles, error) {
	files := &SegmentFiles{}

	if source.GetStorageVersion() >= storage.StorageV3 {
		// StorageV3+: binlog paths MUST come from manifest
		manifestPath := source.GetManifestPath()
		if manifestPath == "" {
			return nil, fmt.Errorf("storage_version=%d requires manifest_path but it is empty (segmentID=%d)",
				source.GetStorageVersion(), source.GetSegmentId())
		}

		basePath, _, err := packed.UnmarshalManfestPath(manifestPath)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal manifest path %q for segment %d: %w", manifestPath, source.GetSegmentId(), err)
		}

		allFiles, listErr := listAllFiles(ctx, cm, basePath)
		if listErr != nil {
			return nil, fmt.Errorf("failed to list files from manifest base path %q for segment %d: %w", basePath, source.GetSegmentId(), listErr)
		}

		// Empty file list is OK for V3 — segment may have only deltas and no insert binlogs
		files.InsertBinlogs = allFiles
		log.Info("collected InsertBinlogs from manifest",
			zap.String("basePath", basePath),
			zap.Int("fileCount", len(allFiles)),
			zap.Int64("storageVersion", source.GetStorageVersion()))
	} else {
		// StorageV1/V2: use pb paths (traditional non-packed format)
		files.InsertBinlogs = extractFromPb(source.GetInsertBinlogs())
		log.Info("using InsertBinlogs from pb",
			zap.Int("fileCount", len(files.InsertBinlogs)),
			zap.Int64("storageVersion", source.GetStorageVersion()))
	}

	// Other types always from pb (not yet in manifest)
	files.DeltaBinlogs = extractFromPb(source.GetDeltaBinlogs())
	files.StatsBinlogs = extractFromPb(source.GetStatsBinlogs())
	files.Bm25Binlogs = extractFromPb(source.GetBm25Binlogs())
	files.VectorScalarIndex = extractIndexFiles(source.GetIndexFiles())
	files.TextIndex = extractTextIndexFiles(source.GetTextIndexFiles())
	files.JsonKeyIndex, files.JsonStats = extractJsonFiles(source.GetJsonKeyIndexFiles())

	return files, nil
}

// generateMappingsFromFiles generates file copy mappings from SegmentFiles.
// Each source file path is transformed to target path by replacing collection/partition/segment IDs.
func generateMappingsFromFiles(
	files *SegmentFiles,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (map[string]string, error) {
	mappings := make(map[string]string)

	// Helper to add mappings with error handling
	addMappings := func(srcPaths []string, fileType string) error {
		for _, srcPath := range srcPaths {
			var dstPath string
			var err error

			// Determine path generation logic based on file type
			switch fileType {
			case IndexTypeVectorScalar, IndexTypeText, IndexTypeJSONKey, IndexTypeJSONStats:
				dstPath, err = generateTargetIndexPath(srcPath, source, target, fileType)
			default:
				dstPath, err = generateTargetPath(srcPath, source, target)
			}

			if err != nil {
				return fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, srcPath, err)
			}
			mappings[srcPath] = dstPath
		}
		return nil
	}

	// Generate mappings for all file types
	if err := addMappings(files.InsertBinlogs, BinlogTypeInsert); err != nil {
		return nil, err
	}
	if err := addMappings(files.DeltaBinlogs, BinlogTypeDelta); err != nil {
		return nil, err
	}
	if err := addMappings(files.StatsBinlogs, BinlogTypeStats); err != nil {
		return nil, err
	}
	if err := addMappings(files.Bm25Binlogs, BinlogTypeBM25); err != nil {
		return nil, err
	}
	if err := addMappings(files.VectorScalarIndex, IndexTypeVectorScalar); err != nil {
		return nil, err
	}
	if err := addMappings(files.TextIndex, IndexTypeText); err != nil {
		return nil, err
	}
	if err := addMappings(files.JsonKeyIndex, IndexTypeJSONKey); err != nil {
		return nil, err
	}
	if err := addMappings(files.JsonStats, IndexTypeJSONStats); err != nil {
		return nil, err
	}

	return mappings, nil
}

func CopySegmentAndIndexFiles(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	logFields []zap.Field,
) (*datapb.CopySegmentResult, []string, error) {
	segmentID := source.GetSegmentId()
	useManifest := source.GetStorageVersion() >= storage.StorageV3

	log.Info("start copying segment and index files",
		zap.Int64("sourceSegmentID", segmentID),
		zap.Int64("storageVersion", source.GetStorageVersion()),
		zap.Bool("useManifest", useManifest))

	// Step 1: Collect all files to copy
	files, err := collectSegmentFiles(ctx, cm, source)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to collect segment files: %w", err)
	}

	// Step 2: Generate src->dst mappings for file copying
	mappings, err := generateMappingsFromFiles(files, source, target)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate file mappings: %w", err)
	}

	// Step 3: Execute all copy operations
	copiedFiles := make([]string, 0, len(mappings))
	for src, dst := range mappings {
		log.Debug("copying file",
			zap.String("src", src),
			zap.String("dst", dst))

		if err := copyFile(ctx, cm, src, dst); err != nil {
			fields := make([]zap.Field, 0, len(logFields)+3)
			fields = append(fields, logFields...)
			fields = append(fields, zap.String("src", src), zap.String("dst", dst), zap.Error(err))
			log.Warn("failed to copy file", fields...)
			return nil, copiedFiles, fmt.Errorf("failed to copy file from %s to %s: %w", src, dst, err)
		}
		copiedFiles = append(copiedFiles, dst)
	}

	log.Info("all files copied successfully",
		zap.Int("fileCount", len(mappings)))

	// Step 3.5: When manifest is used (StorageV3+), InsertBinlogs were collected from manifest
	// (actual file paths under base_path including _data/ and _metadata/), but
	// generateSegmentInfoFromSource needs mappings for the protobuf logical paths too.
	// Add these "logical-only" mappings AFTER file copying so they don't trigger actual copy operations.
	if useManifest {
		pbInsertPaths := extractFromPb(source.GetInsertBinlogs())
		for _, srcPath := range pbInsertPaths {
			if _, exists := mappings[srcPath]; !exists {
				dstPath, pathErr := generateTargetPath(srcPath, source, target)
				if pathErr != nil {
					return nil, copiedFiles, fmt.Errorf("failed to generate target path for pb insert binlog %s: %w", srcPath, pathErr)
				}
				mappings[srcPath] = dstPath
			}
		}
		log.Info("added logical insert binlog mappings for manifest segment",
			zap.Int("pbPathCount", len(pbInsertPaths)))
	}

	// Step 4: Build index metadata from source
	indexInfos, textIndexInfos, jsonKeyIndexInfos, err := buildIndexInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, copiedFiles, fmt.Errorf("failed to build index info: %w", err)
	}

	// Step 5: Generate segment metadata with path mappings
	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, copiedFiles, fmt.Errorf("failed to generate segment info: %w", err)
	}

	// Step 6: Compress paths
	err = binlog.CompressBinLogs(segmentInfo.GetBinlogs(), segmentInfo.GetStatslogs(),
		segmentInfo.GetDeltalogs(), segmentInfo.GetBm25Logs())
	if err != nil {
		return nil, copiedFiles, fmt.Errorf("failed to compress binlog paths: %w", err)
	}

	for _, indexInfo := range indexInfos {
		indexInfo.IndexFilePaths = shortenIndexFilePaths(indexInfo.IndexFilePaths)
	}

	jsonKeyIndexInfos = shortenJsonStatsPath(jsonKeyIndexInfos)

	log.Info("path compression completed",
		zap.Int("binlogFields", len(segmentInfo.GetBinlogs())),
		zap.Int("indexCount", len(indexInfos)),
		zap.Int("jsonStatsCount", len(jsonKeyIndexInfos)))

	// Step 7: Build result
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

	// Step 8: Transform and propagate manifest_path for StorageV3+ segments
	if useManifest {
		targetManifestPath, err := transformManifestPath(source.GetManifestPath(), source, target)
		if err != nil {
			return nil, copiedFiles, fmt.Errorf("failed to transform manifest path: %w", err)
		}
		result.ManifestPath = targetManifestPath
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
//   - error: Non-nil if any source path has no mapping (fail-fast on missing mappings)
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
				dstPath, ok := mappings[srcPath]
				if !ok {
					return nil, 0, fmt.Errorf("no mapping found for source path: %s", srcPath)
				}
				dstBinlog := proto.Clone(srcBinlog).(*datapb.Binlog)
				dstBinlog.LogPath = dstPath
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
		if part == "insert_log" || part == "delta_log" || part == "stats_log" || part == "bm25_stats" {
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
//   - error: Non-nil if any index file path has no mapping (fail-fast on missing mappings)
func buildIndexInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (
	map[int64]*datapb.VectorScalarIndexInfo,
	map[int64]*datapb.TextIndexStats,
	map[int64]*datapb.JsonKeyStats,
	error,
) {
	// Process vector/scalar indexes
	indexInfos := make(map[int64]*datapb.VectorScalarIndexInfo)
	for _, srcIndex := range source.GetIndexFiles() {
		// Transform index file paths using mappings
		targetPaths := make([]string, 0, len(srcIndex.GetIndexFilePaths()))
		for _, srcPath := range srcIndex.GetIndexFilePaths() {
			targetPath, ok := mappings[srcPath]
			if !ok {
				return nil, nil, nil, fmt.Errorf("no mapping found for index file: %s", srcPath)
			}
			targetPaths = append(targetPaths, targetPath)
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
			targetFile, ok := mappings[srcFile]
			if !ok {
				return nil, nil, nil, fmt.Errorf("no mapping found for text index file: %s", srcFile)
			}
			targetFiles = append(targetFiles, targetFile)
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
			targetFile, ok := mappings[srcFile]
			if !ok {
				return nil, nil, nil, fmt.Errorf("no mapping found for JSON index file: %s", srcFile)
			}
			targetFiles = append(targetFiles, targetFile)
		}

		dstJson := proto.Clone(srcJson).(*datapb.JsonKeyStats)
		dstJson.Files = targetFiles
		jsonKeyIndexInfos[fieldID] = dstJson
	}

	return indexInfos, textIndexInfos, jsonKeyIndexInfos, nil
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
//   - IndexTypeVectorScalar: Vector/Scalar Index path format
//     {rootPath}/index_files/{collection_id}/{partition_id}/{segment_id}/{field_id}/{index_id}/{build_id}/file
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
//	files/index_files/111/222/333/444/555/666/scalar_index -> files/index_files/aaa/bbb/ccc/444/555/666/scalar_index
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
	switch indexType {
	case IndexTypeVectorScalar:
		// Vector/Scalar index: index_files/coll/part/seg/field/index/build
		collectionOffset = 1
		partitionOffset = 2
		segmentOffset = 3
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
	parts[keywordIdx+collectionOffset] = strconv.FormatInt(target.GetCollectionId(), 10)
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
