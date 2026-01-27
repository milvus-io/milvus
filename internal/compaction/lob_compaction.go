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

package compaction

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type LOBCompactionStrategy int32

const (
	LOBStrategyReuseAll LOBCompactionStrategy = iota

	LOBStrategyRewriteAll

	LOBStrategySkip
)

// GetForcedStrategy returns forced strategy for specific compaction types.
// returns (strategy, forced) where forced=true means skip hole ratio calculation.
//
// forced strategies:
//   - clustering compaction: always REWRITE_ALL (data is repartitioned)
//   - sort compaction: always REUSE_ALL (row order changes but same data)
//   - mix compaction with split (1->N): always REWRITE_ALL (data is redistributed)
//   - L0 delete compaction: always SKIP (only applies delete logs, segment LOB refs unchanged)
func GetForcedStrategy(compactionType datapb.CompactionType, sourceSegmentCount, targetSegmentCount int) (LOBCompactionStrategy, bool) {
	switch compactionType {
	case datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_ClusteringPartitionKeySortCompaction:
		return LOBStrategyRewriteAll, true

	case datapb.CompactionType_SortCompaction,
		datapb.CompactionType_PartitionKeySortCompaction:
		return LOBStrategyReuseAll, true

	case datapb.CompactionType_MixCompaction:
		if sourceSegmentCount == 1 && targetSegmentCount > 1 {
			return LOBStrategyRewriteAll, true
		}
		return LOBStrategyReuseAll, false

	case datapb.CompactionType_Level0DeleteCompaction:
		return LOBStrategySkip, true

	default:
		return LOBStrategyReuseAll, false
	}
}

type LOBCompactionDecision struct {
	FieldID          int64
	Strategy         LOBCompactionStrategy
	OverallHoleRatio float64
	TotalValidRows   int64
	TotalRows        int64
}

const DefaultLOBHoleRatioThreshold = 0.30

func GetLOBHoleRatioThreshold() float64 {
	threshold := paramtable.Get().DataNodeCfg.LOBHoleRatioThreshold.GetAsFloat()
	if threshold <= 0 || threshold > 1 {
		return DefaultLOBHoleRatioThreshold
	}
	return threshold
}

const (
	DefaultTextInlineThreshold     = 65536
	DefaultTextMaxLobFileBytes     = 64 * 1024 * 1024
	DefaultTextFlushThresholdBytes = 16 * 1024 * 1024
)

func getTextInlineThreshold() int64 {
	threshold := paramtable.Get().DataNodeCfg.TextInlineThreshold.GetAsInt64()
	if threshold <= 0 {
		return DefaultTextInlineThreshold
	}
	return threshold
}

func getTextMaxLobFileBytes() int64 {
	maxSize := paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.GetAsInt64()
	if maxSize <= 0 {
		return DefaultTextMaxLobFileBytes
	}
	return maxSize
}

func getTextFlushThresholdBytes() int64 {
	threshold := paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.GetAsInt64()
	if threshold <= 0 {
		return DefaultTextFlushThresholdBytes
	}
	return threshold
}

// DecideLOBStrategyFromManifest calculates hole ratio and decides compaction strategy for a TEXT field
// using LOB file information from manifest.
//
// hole ratio formula:
//
//	hole_ratio = 1 - Σ(valid_rows) / Σ(total_rows)
//
// where:
//   - valid_rows: number of valid (non-deleted) rows in each LOB file
//   - total_rows: total rows in each LOB file
//
// both values are tracked per LOB file in the manifest and are deduplicated by file path.
func DecideLOBStrategyFromManifest(lobFiles []packed.LobFileInfo, fieldID int64, threshold float64) LOBCompactionDecision {
	var totalValidRows int64
	seenFiles := make(map[string]int64) // filePath -> totalRows (deduplicated)

	for _, file := range lobFiles {
		if file.FieldID != fieldID {
			continue
		}

		totalValidRows += file.ValidRows

		// only count totalRows once per unique file
		if _, seen := seenFiles[file.Path]; !seen {
			seenFiles[file.Path] = file.TotalRows
		}
	}

	var totalRows int64
	for _, rowCount := range seenFiles {
		totalRows += rowCount
	}

	if totalRows == 0 {
		return LOBCompactionDecision{
			FieldID:          fieldID,
			Strategy:         LOBStrategyReuseAll,
			OverallHoleRatio: 0,
			TotalValidRows:   0,
			TotalRows:        0,
		}
	}

	holeRatio := 1.0 - float64(totalValidRows)/float64(totalRows)

	strategy := LOBStrategyReuseAll
	if holeRatio >= threshold {
		strategy = LOBStrategyRewriteAll
	}

	return LOBCompactionDecision{
		FieldID:          fieldID,
		Strategy:         strategy,
		OverallHoleRatio: holeRatio,
		TotalValidRows:   totalValidRows,
		TotalRows:        totalRows,
	}
}

// MergeLOBFiles merges LOB file information from multiple manifests (source segments)
// used in REUSE_ALL mode where TEXT column bytes are directly copied.
//
// the merge operation:
//   - for each unique file path: validRows are accumulated from all segments
//   - totalRows is kept as-is (should be same across segments)
//
// note: in REUSE_ALL mode, the LOB files themselves are not modified.
// we track references to determine when a LOB file can be garbage collected.
func MergeLOBFiles(allLobFiles [][]packed.LobFileInfo, fieldID int64) []packed.LobFileInfo {
	fileRefMap := make(map[string]*packed.LobFileInfo)

	for _, lobFiles := range allLobFiles {
		for _, file := range lobFiles {
			if file.FieldID != fieldID {
				continue
			}

			if existing, ok := fileRefMap[file.Path]; ok {
				// accumulate validRows (represents references from this segment)
				existing.ValidRows += file.ValidRows
			} else {
				// first occurrence of this file
				fileCopy := file
				fileRefMap[file.Path] = &fileCopy
			}
		}
	}

	// convert map to slice
	result := make([]packed.LobFileInfo, 0, len(fileRefMap))
	for _, file := range fileRefMap {
		result = append(result, *file)
	}

	return result
}

// GetTEXTFieldIDsFromLOBFiles returns field IDs of TEXT type fields from LOB files.
func GetTEXTFieldIDsFromLOBFiles(lobFiles []packed.LobFileInfo) []int64 {
	fieldIDs := make(map[int64]bool)

	for _, file := range lobFiles {
		fieldIDs[file.FieldID] = true
	}

	result := make([]int64, 0, len(fieldIDs))
	for fieldID := range fieldIDs {
		result = append(result, fieldID)
	}
	return result
}

// GetTEXTFieldIDsFromSchema returns field IDs of TEXT type fields from schema.
func GetTEXTFieldIDsFromSchema(schema *schemapb.CollectionSchema) []int64 {
	result := make([]int64, 0)
	for _, field := range schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_Text {
			result = append(result, field.GetFieldID())
		}
	}
	return result
}

// LOBCompactionContext holds compaction strategy decisions for all TEXT fields
// used by compactors to determine how to handle TEXT columns during compaction
type LOBCompactionContext struct {
	// decisions maps field ID to its compaction decision
	Decisions map[int64]LOBCompactionDecision
	// allLobFiles holds LOB files from all source segments, indexed by segment ID
	AllLobFiles map[int64][]packed.LobFileInfo
	// mergedLobFiles holds merged LOB files for REUSE_ALL fields
	MergedLobFiles map[int64][]packed.LobFileInfo
	// segmentRowStats tracks row statistics per segment for updating LOB file valid_rows
	// key: segmentID, value: SegmentRowStats
	SegmentRowStats map[int64]*SegmentRowStats
	// compactionType indicates the type of compaction operation
	CompactionType datapb.CompactionType
	// forcedStrategy is the forced strategy when applicable (set by SetForcedStrategy)
	ForcedStrategy LOBCompactionStrategy
	// isForced indicates whether strategy is forced (skip hole ratio calculation)
	IsForced bool
}

// SegmentRowStats tracks row statistics for a segment during compaction
type SegmentRowStats struct {
	TotalRows   int64 // total rows in segment before compaction
	DeletedRows int64 // rows deleted during compaction
}

// NewLOBCompactionContext creates a new LOBCompactionContext.
func NewLOBCompactionContext() *LOBCompactionContext {
	return &LOBCompactionContext{
		Decisions:       make(map[int64]LOBCompactionDecision),
		AllLobFiles:     make(map[int64][]packed.LobFileInfo),
		MergedLobFiles:  make(map[int64][]packed.LobFileInfo),
		SegmentRowStats: make(map[int64]*SegmentRowStats),
	}
}

// AddSegmentLobFiles adds LOB files from a source segment.
func (ctx *LOBCompactionContext) AddSegmentLobFiles(segmentID int64, lobFiles []packed.LobFileInfo) {
	ctx.AllLobFiles[segmentID] = lobFiles
}

// SetCompactionType sets the compaction type and checks for forced strategy.
// sourceSegmentCount: number of source segments
// targetSegmentCount: expected number of output segments (0 if unknown)
func (ctx *LOBCompactionContext) SetCompactionType(compactionType datapb.CompactionType, sourceSegmentCount, targetSegmentCount int) {
	ctx.CompactionType = compactionType
	strategy, forced := GetForcedStrategy(compactionType, sourceSegmentCount, targetSegmentCount)
	if forced {
		ctx.ForcedStrategy = strategy
		ctx.IsForced = true
	}
}

// SetSegmentRowStats sets the row statistics for a segment.
// this should be called during compaction to track how many rows were deleted from each segment.
// totalRows: total rows in segment before compaction
// deletedRows: rows deleted/filtered during compaction
func (ctx *LOBCompactionContext) SetSegmentRowStats(segmentID int64, totalRows, deletedRows int64) {
	ctx.SegmentRowStats[segmentID] = &SegmentRowStats{
		TotalRows:   totalRows,
		DeletedRows: deletedRows,
	}
}

// IncrementSegmentDeletedRows increments the deleted row count for a segment.
// this can be called during compaction iteration when a row is filtered out.
func (ctx *LOBCompactionContext) IncrementSegmentDeletedRows(segmentID int64, count int64) {
	if stats, ok := ctx.SegmentRowStats[segmentID]; ok {
		stats.DeletedRows += count
	}
}

// ComputeStrategies calculates compaction strategies for all TEXT fields.
func (ctx *LOBCompactionContext) ComputeStrategies(textFieldIDs []int64, threshold float64) {
	if ctx.IsForced {
		// SKIP means no LOB processing needed at all — don't create any decisions
		if ctx.ForcedStrategy == LOBStrategySkip {
			return
		}
		for _, fieldID := range textFieldIDs {
			ctx.Decisions[fieldID] = LOBCompactionDecision{
				FieldID:          fieldID,
				Strategy:         ctx.ForcedStrategy,
				OverallHoleRatio: 0, // not calculated
				TotalValidRows:   0,
				TotalRows:        0,
			}
		}
		return
	}

	var allFiles []packed.LobFileInfo
	for _, files := range ctx.AllLobFiles {
		allFiles = append(allFiles, files...)
	}

	for _, fieldID := range textFieldIDs {
		decision := DecideLOBStrategyFromManifest(allFiles, fieldID, threshold)
		ctx.Decisions[fieldID] = decision
	}
}

// GetStrategy returns the strategy for a specific field.
func (ctx *LOBCompactionContext) GetStrategy(fieldID int64) LOBCompactionStrategy {
	if decision, ok := ctx.Decisions[fieldID]; ok {
		return decision.Strategy
	}
	return LOBStrategyReuseAll // default to REUSE_ALL if no decision
}

// IsReuseAll returns true if the field should use REUSE_ALL strategy.
func (ctx *LOBCompactionContext) IsReuseAll(fieldID int64) bool {
	return ctx.GetStrategy(fieldID) == LOBStrategyReuseAll
}

// GetMergedLobFiles returns LOB files for a specific field from all segments.
// used in REUSE_ALL mode.
func (ctx *LOBCompactionContext) GetMergedLobFiles(fieldID int64) []packed.LobFileInfo {
	var result []packed.LobFileInfo
	for _, files := range ctx.AllLobFiles {
		for _, file := range files {
			if file.FieldID == fieldID {
				result = append(result, file)
			}
		}
	}
	return result
}

// GetAllMergedLobFilesWithUpdatedStats returns all LOB files from all segments
// with updated valid_rows based on per-segment deletion statistics.
//
// in REUSE_ALL mode, LOB files are not rewritten, but their valid_rows need to be updated
// to reflect the actual number of valid rows after compaction.
//
// the update logic uses proportional allocation:
//   - each LOB file belongs to exactly one segment
//   - a segment may have multiple LOB files (when file size exceeds max_lob_file_bytes)
//   - for each segment, we calculate: survival_ratio = 1 - (deleted_rows / total_rows)
//   - each LOB file's valid_rows is updated: new_valid_rows = original_valid_rows * survival_ratio
//
// this proportional approach assumes deleted rows are evenly distributed across LOB files,
// which is a reasonable approximation when precise per-file tracking is not available.
//
// NOTE: SetSegmentRowStats() must be called for each segment before calling this function.
func (ctx *LOBCompactionContext) GetAllMergedLobFilesWithUpdatedStats() []packed.LobFileInfo {
	var result []packed.LobFileInfo

	for segmentID, lobFiles := range ctx.AllLobFiles {
		stats := ctx.SegmentRowStats[segmentID]
		if stats == nil {
			// no stats available, use original values
			result = append(result, lobFiles...)
			continue
		}

		// calculate survival ratio for this segment
		// survival_ratio = 1 - (deleted_rows / total_rows)
		var survivalRatio float64 = 1.0
		if stats.TotalRows > 0 {
			survivalRatio = 1.0 - float64(stats.DeletedRows)/float64(stats.TotalRows)
			if survivalRatio < 0 {
				survivalRatio = 0
			}
		}

		for _, file := range lobFiles {
			updatedFile := file
			// update valid_rows based on total_rows and survival ratio
			// new_valid_rows = total_rows * survival_ratio
			// this reflects the actual number of valid rows after compaction
			updatedFile.ValidRows = int64(float64(file.TotalRows) * survivalRatio)
			// ensure valid_rows doesn't go negative
			if updatedFile.ValidRows < 0 {
				updatedFile.ValidRows = 0
			}
			result = append(result, updatedFile)
		}
	}

	return result
}

func (ctx *LOBCompactionContext) GetAllMergedLobFiles() []packed.LobFileInfo {
	var result []packed.LobFileInfo
	for _, files := range ctx.AllLobFiles {
		result = append(result, files...)
	}
	return result
}

func (ctx *LOBCompactionContext) HasTEXTFields() bool {
	return len(ctx.Decisions) > 0
}

func (ctx *LOBCompactionContext) ShouldRewriteAnyField() bool {
	for _, decision := range ctx.Decisions {
		if decision.Strategy == LOBStrategyRewriteAll {
			return true
		}
	}
	return false
}

func CollectLobFilesFromManifests(
	manifests map[int64]string, // segmentID -> manifestPath
	storageConfig *indexpb.StorageConfig,
) (map[int64][]packed.LobFileInfo, error) {
	result := make(map[int64][]packed.LobFileInfo)

	for segmentID, manifestPath := range manifests {
		if manifestPath == "" {
			continue
		}
		lobFiles, err := packed.GetManifestLobFiles(manifestPath, storageConfig)
		if err != nil {
			return nil, err
		}
		result[segmentID] = lobFiles
	}

	return result, nil
}

// WriteLobFilesToManifest writes merged LOB files to the output segment manifest.
// this is called after compaction completes for REUSE_ALL TEXT fields.
// manifestPath: the output segment's manifest path (JSON format with basePath and version)
// lobFiles: the merged LOB files to write
// storageConfig: storage configuration
// returns the new manifestPath with updated version after commit
func WriteLobFilesToManifest(
	manifestPath string,
	lobFiles []packed.LobFileInfo,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	if len(lobFiles) == 0 || manifestPath == "" {
		return manifestPath, nil
	}

	basePath, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return "", err
	}

	newVersion, err := packed.AddLobFilesToTransaction(basePath, version, storageConfig, lobFiles)
	if err != nil {
		return "", err
	}

	return packed.MarshalManifestPath(basePath, newVersion), nil
}

// ApplyLobCompactionToManifests updates output segment manifests with merged LOB files.
// this should be called after compaction completes for TEXT fields using REUSE_ALL strategy.
//
// IMPORTANT: SetSegmentRowStats() must be called before this function to ensure
// LOB file valid_rows are updated correctly based on actual compacted row count.
func ApplyLobCompactionToManifests(
	ctx *LOBCompactionContext,
	outputManifests map[int64]string,
	storageConfig *indexpb.StorageConfig,
) (map[int64]string, error) {
	if ctx == nil || !ctx.HasTEXTFields() {
		return outputManifests, nil
	}

	if !ctx.HasReuseAllFields() {
		return outputManifests, nil
	}

	// get LOB files for REUSE_ALL fields (with updated valid_rows)
	mergedFiles := ctx.GetReuseAllLobFilesWithUpdatedStats()
	if len(mergedFiles) == 0 {
		return outputManifests, nil
	}

	// write merged LOB files to each output segment's manifest
	updatedManifests := make(map[int64]string, len(outputManifests))
	for segmentID, manifestPath := range outputManifests {
		newManifestPath, err := WriteLobFilesToManifest(manifestPath, mergedFiles, storageConfig)
		if err != nil {
			return nil, err
		}
		updatedManifests[segmentID] = newManifestPath
	}

	return updatedManifests, nil
}

// GetRewriteAllFieldIDs returns field IDs that require REWRITE_ALL strategy.
func (ctx *LOBCompactionContext) GetRewriteAllFieldIDs() []int64 {
	if ctx == nil {
		return nil
	}
	var result []int64
	for fieldID, decision := range ctx.Decisions {
		if decision.Strategy == LOBStrategyRewriteAll {
			result = append(result, fieldID)
		}
	}
	return result
}

// GetReuseAllFieldIDs returns field IDs that require REUSE_ALL strategy.
func (ctx *LOBCompactionContext) GetReuseAllFieldIDs() []int64 {
	if ctx == nil {
		return nil
	}
	var result []int64
	for fieldID, decision := range ctx.Decisions {
		if decision.Strategy == LOBStrategyReuseAll {
			result = append(result, fieldID)
		}
	}
	return result
}

// HasReuseAllFields returns true if any field requires REUSE_ALL strategy.
func (ctx *LOBCompactionContext) HasReuseAllFields() bool {
	for _, decision := range ctx.Decisions {
		if decision.Strategy == LOBStrategyReuseAll {
			return true
		}
	}
	return false
}

// GetReuseAllLobFilesWithUpdatedStats returns LOB files for REUSE_ALL fields only,
// with updated valid_rows based on per-segment deletion statistics.
func (ctx *LOBCompactionContext) GetReuseAllLobFilesWithUpdatedStats() []packed.LobFileInfo {
	if ctx == nil {
		return nil
	}

	reuseAllFieldIDs := make(map[int64]bool)
	for fieldID, decision := range ctx.Decisions {
		if decision.Strategy == LOBStrategyReuseAll {
			reuseAllFieldIDs[fieldID] = true
		}
	}

	if len(reuseAllFieldIDs) == 0 {
		return nil
	}

	var result []packed.LobFileInfo

	for segmentID, lobFiles := range ctx.AllLobFiles {
		stats := ctx.SegmentRowStats[segmentID]
		if stats == nil {
			// no stats available, use original values but filter by field
			for _, file := range lobFiles {
				if reuseAllFieldIDs[file.FieldID] {
					result = append(result, file)
				}
			}
			continue
		}

		// calculate survival ratio for this segment
		var survivalRatio float64 = 1.0
		if stats.TotalRows > 0 {
			survivalRatio = 1.0 - float64(stats.DeletedRows)/float64(stats.TotalRows)
		}

		for _, file := range lobFiles {
			if !reuseAllFieldIDs[file.FieldID] {
				continue
			}

			updatedFile := file
			// new_valid_rows = total_rows * survival_ratio
			updatedFile.ValidRows = int64(float64(file.TotalRows) * survivalRatio)
			if updatedFile.ValidRows < 0 {
				updatedFile.ValidRows = 0
			}
			result = append(result, updatedFile)
		}
	}

	return result
}

// GetTextColumnConfigs returns TEXT column configurations for REWRITE_ALL fields.
// this is used to configure the segment writer for TEXT column rewriting.
// lobBasePath: base path for LOB files (typically segment's LOB path)
// inlineThreshold, maxLobFileBytes, flushThresholdBytes: TEXT column config parameters
// passed from DataCoord via compaction params
func (ctx *LOBCompactionContext) GetTextColumnConfigs(lobBasePath string, inlineThreshold, maxLobFileBytes, flushThresholdBytes int64) []packed.TextColumnConfig {
	if ctx == nil {
		return nil
	}

	rewriteFieldIDs := ctx.GetRewriteAllFieldIDs()
	if len(rewriteFieldIDs) == 0 {
		return nil
	}

	configs := make([]packed.TextColumnConfig, 0, len(rewriteFieldIDs))
	for _, fieldID := range rewriteFieldIDs {
		configs = append(configs, packed.TextColumnConfig{
			FieldID:             fieldID,
			LobBasePath:         lobBasePath,
			InlineThreshold:     inlineThreshold,
			MaxLobFileBytes:     maxLobFileBytes,
			FlushThresholdBytes: flushThresholdBytes,
		})
	}

	return configs
}
