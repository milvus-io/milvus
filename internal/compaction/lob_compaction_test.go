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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestDecideLOBStrategyFromManifest_ReuseAll(t *testing.T) {
	// Test case: hole ratio < 30%, should return REUSE_ALL
	// file_1001: total_rows=1000, valid_rows=900 (shared by two segments)
	// hole_ratio = 1 - 900/1000 = 10% -> REUSE_ALL

	fieldID := int64(101)
	lobFiles := []packed.LobFileInfo{
		{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 500},
		{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 400}, // same file, different segment
	}

	decision := DecideLOBStrategyFromManifest(lobFiles, fieldID, DefaultLOBHoleRatioThreshold)

	assert.Equal(t, fieldID, decision.FieldID)
	assert.Equal(t, LOBStrategyReuseAll, decision.Strategy)
	assert.Equal(t, int64(900), decision.TotalValidRows)
	assert.Equal(t, int64(1000), decision.TotalRows) // file counted once
	assert.InDelta(t, 0.1, decision.OverallHoleRatio, 0.001)
}

func TestDecideLOBStrategyFromManifest_RewriteAll(t *testing.T) {
	// Test case: hole ratio >= 30%, should return REWRITE_ALL
	// file_1001: total_rows=1000, valid_rows=700 (from two segments: 500+200)
	// file_1002: total_rows=800, valid_rows=300
	// file_1003: total_rows=500, valid_rows=100
	// total_valid_rows = 700 + 300 + 100 = 1100
	// total_rows = 1000 + 800 + 500 = 2300
	// hole_ratio = 1 - 1100/2300 = 52.2% -> REWRITE_ALL

	fieldID := int64(101)
	lobFiles := []packed.LobFileInfo{
		{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 500},
		{Path: "/lob/file_1002.vortex", FieldID: fieldID, TotalRows: 800, ValidRows: 300},
		{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 200}, // same file
		{Path: "/lob/file_1003.vortex", FieldID: fieldID, TotalRows: 500, ValidRows: 100},
	}

	decision := DecideLOBStrategyFromManifest(lobFiles, fieldID, DefaultLOBHoleRatioThreshold)

	assert.Equal(t, fieldID, decision.FieldID)
	assert.Equal(t, LOBStrategyRewriteAll, decision.Strategy)
	assert.Equal(t, int64(1100), decision.TotalValidRows)
	assert.Equal(t, int64(2300), decision.TotalRows)
	assert.InDelta(t, 0.522, decision.OverallHoleRatio, 0.01)
}

func TestDecideLOBStrategyFromManifest_EmptyFiles(t *testing.T) {
	fieldID := int64(101)

	// Test with empty LOB files
	decision := DecideLOBStrategyFromManifest([]packed.LobFileInfo{}, fieldID, DefaultLOBHoleRatioThreshold)
	assert.Equal(t, LOBStrategyReuseAll, decision.Strategy)
	assert.Equal(t, int64(0), decision.TotalValidRows)
	assert.Equal(t, int64(0), decision.TotalRows)
	assert.Equal(t, float64(0), decision.OverallHoleRatio)
}

func TestDecideLOBStrategyFromManifest_NoMatchingField(t *testing.T) {
	fieldID := int64(101)

	// Test with LOB files for different field
	lobFiles := []packed.LobFileInfo{
		{Path: "/lob/file_1001.vortex", FieldID: 999, TotalRows: 1000, ValidRows: 500},
	}

	decision := DecideLOBStrategyFromManifest(lobFiles, fieldID, DefaultLOBHoleRatioThreshold)
	assert.Equal(t, LOBStrategyReuseAll, decision.Strategy)
	assert.Equal(t, int64(0), decision.TotalValidRows)
	assert.Equal(t, int64(0), decision.TotalRows)
}

func TestDecideLOBStrategyFromManifest_CustomThreshold(t *testing.T) {
	fieldID := int64(101)
	lobFiles := []packed.LobFileInfo{
		{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 850}, // 15% hole
	}

	// With default threshold (30%), should be REUSE_ALL
	decision := DecideLOBStrategyFromManifest(lobFiles, fieldID, DefaultLOBHoleRatioThreshold)
	assert.Equal(t, LOBStrategyReuseAll, decision.Strategy)

	// With lower threshold (10%), should be REWRITE_ALL
	decision = DecideLOBStrategyFromManifest(lobFiles, fieldID, 0.10)
	assert.Equal(t, LOBStrategyRewriteAll, decision.Strategy)
}

func TestMergeLOBFiles_Basic(t *testing.T) {
	fieldID := int64(101)
	allLobFiles := [][]packed.LobFileInfo{
		{
			{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 500},
			{Path: "/lob/file_1002.vortex", FieldID: fieldID, TotalRows: 800, ValidRows: 300},
		},
		{
			{Path: "/lob/file_1001.vortex", FieldID: fieldID, TotalRows: 1000, ValidRows: 200},
			{Path: "/lob/file_1003.vortex", FieldID: fieldID, TotalRows: 500, ValidRows: 100},
		},
	}

	merged := MergeLOBFiles(allLobFiles, fieldID)

	assert.Len(t, merged, 3)

	// Build a map for easier assertion
	fileMap := make(map[string]packed.LobFileInfo)
	for _, f := range merged {
		fileMap[f.Path] = f
	}

	// file_1001: valid_rows should be accumulated (500 + 200 = 700)
	assert.Equal(t, int64(1000), fileMap["/lob/file_1001.vortex"].TotalRows)
	assert.Equal(t, int64(700), fileMap["/lob/file_1001.vortex"].ValidRows)

	// file_1002: valid_rows = 300
	assert.Equal(t, int64(800), fileMap["/lob/file_1002.vortex"].TotalRows)
	assert.Equal(t, int64(300), fileMap["/lob/file_1002.vortex"].ValidRows)

	// file_1003: valid_rows = 100
	assert.Equal(t, int64(500), fileMap["/lob/file_1003.vortex"].TotalRows)
	assert.Equal(t, int64(100), fileMap["/lob/file_1003.vortex"].ValidRows)
}

func TestMergeLOBFiles_EmptySlices(t *testing.T) {
	fieldID := int64(101)

	merged := MergeLOBFiles([][]packed.LobFileInfo{}, fieldID)
	assert.Len(t, merged, 0)

	merged = MergeLOBFiles([][]packed.LobFileInfo{{}, {}}, fieldID)
	assert.Len(t, merged, 0)
}

func TestMergeLOBFiles_NoMatchingField(t *testing.T) {
	fieldID := int64(101)
	allLobFiles := [][]packed.LobFileInfo{
		{
			{Path: "/lob/file_1001.vortex", FieldID: 999, TotalRows: 1000, ValidRows: 500},
		},
	}

	merged := MergeLOBFiles(allLobFiles, fieldID)
	assert.Len(t, merged, 0)
}

func TestGetTEXTFieldIDsFromLOBFiles(t *testing.T) {
	lobFiles := []packed.LobFileInfo{
		{Path: "/lob/file_1.vortex", FieldID: 101, TotalRows: 100, ValidRows: 100},
		{Path: "/lob/file_2.vortex", FieldID: 102, TotalRows: 100, ValidRows: 100},
		{Path: "/lob/file_3.vortex", FieldID: 101, TotalRows: 100, ValidRows: 100}, // same field
		{Path: "/lob/file_4.vortex", FieldID: 103, TotalRows: 100, ValidRows: 100},
	}

	fieldIDs := GetTEXTFieldIDsFromLOBFiles(lobFiles)

	assert.Len(t, fieldIDs, 3)
	fieldIDMap := make(map[int64]bool)
	for _, id := range fieldIDs {
		fieldIDMap[id] = true
	}
	assert.True(t, fieldIDMap[101])
	assert.True(t, fieldIDMap[102])
	assert.True(t, fieldIDMap[103])
}

func TestGetTEXTFieldIDsFromLOBFiles_Empty(t *testing.T) {
	fieldIDs := GetTEXTFieldIDsFromLOBFiles([]packed.LobFileInfo{})
	assert.Len(t, fieldIDs, 0)
}

func TestGetTEXTFieldIDsFromSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text1", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "varchar", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "text2", DataType: schemapb.DataType_Text},
		},
	}

	fieldIDs := GetTEXTFieldIDsFromSchema(schema)
	assert.Len(t, fieldIDs, 2)
	assert.Contains(t, fieldIDs, int64(101))
	assert.Contains(t, fieldIDs, int64(103))
}

func TestGetTEXTFieldIDsFromSchema_NoTextFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "varchar", DataType: schemapb.DataType_VarChar},
		},
	}

	fieldIDs := GetTEXTFieldIDsFromSchema(schema)
	assert.Len(t, fieldIDs, 0)
}

func TestLOBCompactionContext_Basic(t *testing.T) {
	ctx := NewLOBCompactionContext()
	assert.NotNil(t, ctx)
	assert.NotNil(t, ctx.Decisions)
	assert.NotNil(t, ctx.AllLobFiles)
	assert.NotNil(t, ctx.MergedLobFiles)
}

func TestLOBCompactionContext_AddSegmentLobFiles(t *testing.T) {
	ctx := NewLOBCompactionContext()

	files1 := []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 900},
	}
	files2 := []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 100},
	}

	ctx.AddSegmentLobFiles(1, files1)
	ctx.AddSegmentLobFiles(2, files2)

	assert.Len(t, ctx.AllLobFiles, 2)
	assert.Equal(t, files1, ctx.AllLobFiles[1])
	assert.Equal(t, files2, ctx.AllLobFiles[2])
}

func TestLOBCompactionContext_ComputeStrategies_ReuseAll(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Segment 1: 900 valid rows from file
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 900},
	})

	// hole_ratio = 1 - 900/1000 = 10% < 30% -> REUSE_ALL
	ctx.ComputeStrategies([]int64{101}, DefaultLOBHoleRatioThreshold)

	assert.True(t, ctx.HasTEXTFields())
	assert.True(t, ctx.IsReuseAll(101))
	assert.False(t, ctx.ShouldRewriteAnyField())

	merged := ctx.GetMergedLobFiles(101)
	assert.Len(t, merged, 1)
	assert.Equal(t, int64(900), merged[0].ValidRows)
}

func TestLOBCompactionContext_ComputeStrategies_RewriteAll(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Segment 1: 200 valid rows
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 200},
	})

	// hole_ratio = 1 - 200/1000 = 80% >= 30% -> REWRITE_ALL
	ctx.ComputeStrategies([]int64{101}, DefaultLOBHoleRatioThreshold)

	assert.True(t, ctx.HasTEXTFields())
	assert.False(t, ctx.IsReuseAll(101))
	assert.Equal(t, LOBStrategyRewriteAll, ctx.GetStrategy(101))
	assert.True(t, ctx.ShouldRewriteAnyField())

	// For REWRITE_ALL, GetReuseAllLobFilesWithUpdatedStats should return empty
	// because there are no REUSE_ALL fields
	reuseAllFiles := ctx.GetReuseAllLobFilesWithUpdatedStats()
	assert.Len(t, reuseAllFiles, 0)

	// GetMergedLobFiles still returns all files (used for reference)
	merged := ctx.GetMergedLobFiles(101)
	assert.Len(t, merged, 1)
}

func TestLOBCompactionContext_ComputeStrategies_MultipleFields(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Field 101: 90% valid -> REUSE_ALL
	// Field 102: 50% valid -> REWRITE_ALL
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101_file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 900},
		{Path: "/lob/field102_file1.vortex", FieldID: 102, TotalRows: 1000, ValidRows: 500},
	})

	ctx.ComputeStrategies([]int64{101, 102}, DefaultLOBHoleRatioThreshold)

	assert.True(t, ctx.IsReuseAll(101))
	assert.False(t, ctx.IsReuseAll(102))
	assert.True(t, ctx.ShouldRewriteAnyField()) // field 102 needs rewrite
}

func TestLOBCompactionContext_GetAllMergedLobFiles(t *testing.T) {
	ctx := NewLOBCompactionContext()

	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101_file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 900},
		{Path: "/lob/field102_file1.vortex", FieldID: 102, TotalRows: 1000, ValidRows: 800},
	})

	ctx.ComputeStrategies([]int64{101, 102}, DefaultLOBHoleRatioThreshold)

	allMerged := ctx.GetAllMergedLobFiles()
	assert.Len(t, allMerged, 2) // both fields are REUSE_ALL
}

// Tests for GetForcedStrategy
func TestGetForcedStrategy_ClusterCompaction(t *testing.T) {
	// Cluster compaction always forces REWRITE_ALL
	strategy, forced := GetForcedStrategy(datapb.CompactionType_ClusteringCompaction, 3, 5)
	assert.True(t, forced)
	assert.Equal(t, LOBStrategyRewriteAll, strategy)

	// ClusteringPartitionKeySortCompaction also forces REWRITE_ALL
	strategy, forced = GetForcedStrategy(datapb.CompactionType_ClusteringPartitionKeySortCompaction, 3, 5)
	assert.True(t, forced)
	assert.Equal(t, LOBStrategyRewriteAll, strategy)
}

func TestGetForcedStrategy_SortCompaction(t *testing.T) {
	// Sort compaction always forces REUSE_ALL
	strategy, forced := GetForcedStrategy(datapb.CompactionType_SortCompaction, 1, 1)
	assert.True(t, forced)
	assert.Equal(t, LOBStrategyReuseAll, strategy)

	// PartitionKeySortCompaction also forces REUSE_ALL
	strategy, forced = GetForcedStrategy(datapb.CompactionType_PartitionKeySortCompaction, 1, 1)
	assert.True(t, forced)
	assert.Equal(t, LOBStrategyReuseAll, strategy)
}

func TestGetForcedStrategy_MixCompaction_Split(t *testing.T) {
	// Mix compaction with split (1->N) forces REWRITE_ALL
	strategy, forced := GetForcedStrategy(datapb.CompactionType_MixCompaction, 1, 3)
	assert.True(t, forced)
	assert.Equal(t, LOBStrategyRewriteAll, strategy)
}

func TestGetForcedStrategy_MixCompaction_Normal(t *testing.T) {
	// Normal mix compaction (N->1 or N->M where N>1) uses hole ratio
	strategy, forced := GetForcedStrategy(datapb.CompactionType_MixCompaction, 3, 1)
	assert.False(t, forced)
	assert.Equal(t, LOBStrategyReuseAll, strategy) // default when not forced
}

func TestGetForcedStrategy_Level0DeleteCompaction(t *testing.T) {
	// L0 delete compaction forces SKIP (only applies delete logs, segment LOB refs unchanged)
	strategy, forced := GetForcedStrategy(datapb.CompactionType_Level0DeleteCompaction, 1, 1)
	assert.True(t, forced)
	assert.Equal(t, LOBStrategySkip, strategy)
}

func TestLOBCompactionContext_ComputeStrategies_Skip(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Add LOB files
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950},
	})

	// Set L0 delete compaction which forces SKIP
	ctx.SetCompactionType(datapb.CompactionType_Level0DeleteCompaction, 1, 1)

	// Compute strategies - should result in no decisions (SKIP)
	ctx.ComputeStrategies([]int64{101}, DefaultLOBHoleRatioThreshold)

	// SKIP means no LOB processing: no decisions, no TEXT fields to handle
	assert.False(t, ctx.HasTEXTFields())
	assert.False(t, ctx.ShouldRewriteAnyField())
	assert.False(t, ctx.HasReuseAllFields())
}

// Tests for SetCompactionType
func TestLOBCompactionContext_SetCompactionType_Forced(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Cluster compaction should set forced strategy
	ctx.SetCompactionType(datapb.CompactionType_ClusteringCompaction, 3, 5)

	assert.True(t, ctx.IsForced)
	assert.Equal(t, LOBStrategyRewriteAll, ctx.ForcedStrategy)
	assert.Equal(t, datapb.CompactionType_ClusteringCompaction, ctx.CompactionType)
}

func TestLOBCompactionContext_SetCompactionType_NotForced(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Normal mix compaction should not set forced strategy
	ctx.SetCompactionType(datapb.CompactionType_MixCompaction, 3, 1)

	assert.False(t, ctx.IsForced)
	assert.Equal(t, datapb.CompactionType_MixCompaction, ctx.CompactionType)
}

func TestLOBCompactionContext_ComputeStrategies_WithForcedStrategy(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Add LOB files with high valid ratio (would normally be REUSE_ALL)
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950},
	})

	// Set cluster compaction which forces REWRITE_ALL
	ctx.SetCompactionType(datapb.CompactionType_ClusteringCompaction, 1, 3)

	// Compute strategies - should use forced strategy
	ctx.ComputeStrategies([]int64{101}, DefaultLOBHoleRatioThreshold)

	// Even with high valid ratio, strategy should be REWRITE_ALL due to forced
	assert.Equal(t, LOBStrategyRewriteAll, ctx.GetStrategy(101))
	assert.True(t, ctx.ShouldRewriteAnyField())
}

// Tests for SegmentRowStats
func TestLOBCompactionContext_SetSegmentRowStats(t *testing.T) {
	ctx := NewLOBCompactionContext()

	ctx.SetSegmentRowStats(1, 1000, 100)
	ctx.SetSegmentRowStats(2, 2000, 300)

	assert.Equal(t, int64(1000), ctx.SegmentRowStats[1].TotalRows)
	assert.Equal(t, int64(100), ctx.SegmentRowStats[1].DeletedRows)
	assert.Equal(t, int64(2000), ctx.SegmentRowStats[2].TotalRows)
	assert.Equal(t, int64(300), ctx.SegmentRowStats[2].DeletedRows)
}

func TestLOBCompactionContext_IncrementSegmentDeletedRows(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Initialize stats first
	ctx.SetSegmentRowStats(1, 1000, 0)

	// Increment deleted rows
	ctx.IncrementSegmentDeletedRows(1, 50)
	ctx.IncrementSegmentDeletedRows(1, 30)

	assert.Equal(t, int64(80), ctx.SegmentRowStats[1].DeletedRows)

	// Incrementing non-existent segment should not panic
	ctx.IncrementSegmentDeletedRows(999, 10) // no-op
}

// Tests for GetAllMergedLobFilesWithUpdatedStats
func TestLOBCompactionContext_GetAllMergedLobFilesWithUpdatedStats(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Segment 1: 1000 total, 200 deleted -> survival ratio = 0.8
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/seg1_file1.vortex", FieldID: 101, TotalRows: 500, ValidRows: 500},
		{Path: "/lob/seg1_file2.vortex", FieldID: 101, TotalRows: 500, ValidRows: 500},
	})
	ctx.SetSegmentRowStats(1, 1000, 200)

	// Segment 2: 800 total, 400 deleted -> survival ratio = 0.5
	ctx.AddSegmentLobFiles(2, []packed.LobFileInfo{
		{Path: "/lob/seg2_file1.vortex", FieldID: 101, TotalRows: 800, ValidRows: 800},
	})
	ctx.SetSegmentRowStats(2, 800, 400)

	files := ctx.GetAllMergedLobFilesWithUpdatedStats()

	assert.Len(t, files, 3)

	// Build map for easier assertions
	fileMap := make(map[string]packed.LobFileInfo)
	for _, f := range files {
		fileMap[f.Path] = f
	}

	// Segment 1 files: new_valid_rows = total_rows * 0.8
	assert.Equal(t, int64(400), fileMap["/lob/seg1_file1.vortex"].ValidRows) // 500 * 0.8
	assert.Equal(t, int64(400), fileMap["/lob/seg1_file2.vortex"].ValidRows) // 500 * 0.8

	// Segment 2 file: new_valid_rows = total_rows * 0.5
	assert.Equal(t, int64(400), fileMap["/lob/seg2_file1.vortex"].ValidRows) // 800 * 0.5
}

func TestLOBCompactionContext_GetAllMergedLobFilesWithUpdatedStats_NoStats(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Add files without setting row stats
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 900},
	})

	files := ctx.GetAllMergedLobFilesWithUpdatedStats()

	// Should return original values when no stats
	assert.Len(t, files, 1)
	assert.Equal(t, int64(900), files[0].ValidRows)
}

func TestLOBCompactionContext_GetAllMergedLobFilesWithUpdatedStats_AllDeleted(t *testing.T) {
	ctx := NewLOBCompactionContext()

	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 1000},
	})
	// All rows deleted -> survival ratio = 0
	ctx.SetSegmentRowStats(1, 1000, 1000)

	files := ctx.GetAllMergedLobFilesWithUpdatedStats()

	assert.Len(t, files, 1)
	assert.Equal(t, int64(0), files[0].ValidRows)
}

// Tests for GetReuseAllLobFilesWithUpdatedStats (mixed strategy)
func TestLOBCompactionContext_GetReuseAllLobFilesWithUpdatedStats_MixedStrategy(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Field 101: high valid ratio -> REUSE_ALL
	// Field 102: low valid ratio -> REWRITE_ALL
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101_file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950},
		{Path: "/lob/field102_file1.vortex", FieldID: 102, TotalRows: 1000, ValidRows: 200},
	})
	ctx.SetSegmentRowStats(1, 2000, 400) // 20% deleted -> survival ratio = 0.8

	ctx.ComputeStrategies([]int64{101, 102}, DefaultLOBHoleRatioThreshold)

	// Verify mixed strategies
	assert.True(t, ctx.IsReuseAll(101))
	assert.False(t, ctx.IsReuseAll(102))
	assert.True(t, ctx.ShouldRewriteAnyField())
	assert.True(t, ctx.HasReuseAllFields())

	// Get REUSE_ALL files only
	files := ctx.GetReuseAllLobFilesWithUpdatedStats()

	// Should only contain field 101 files
	assert.Len(t, files, 1)
	assert.Equal(t, int64(101), files[0].FieldID)
	assert.Equal(t, "/lob/field101_file1.vortex", files[0].Path)
	// valid_rows = total_rows * survival_ratio = 1000 * 0.8 = 800
	assert.Equal(t, int64(800), files[0].ValidRows)
}

func TestLOBCompactionContext_GetReuseAllLobFilesWithUpdatedStats_AllRewrite(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// All fields have low valid ratio -> all REWRITE_ALL
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101_file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 200},
		{Path: "/lob/field102_file1.vortex", FieldID: 102, TotalRows: 1000, ValidRows: 300},
	})

	ctx.ComputeStrategies([]int64{101, 102}, DefaultLOBHoleRatioThreshold)

	assert.False(t, ctx.HasReuseAllFields())

	// Should return nil when no REUSE_ALL fields
	files := ctx.GetReuseAllLobFilesWithUpdatedStats()
	assert.Nil(t, files)
}

func TestLOBCompactionContext_GetReuseAllLobFilesWithUpdatedStats_NilContext(t *testing.T) {
	var ctx *LOBCompactionContext = nil
	files := ctx.GetReuseAllLobFilesWithUpdatedStats()
	assert.Nil(t, files)
}

// Tests for GetRewriteAllFieldIDs and GetReuseAllFieldIDs
func TestLOBCompactionContext_GetFieldIDsByStrategy(t *testing.T) {
	ctx := NewLOBCompactionContext()

	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950}, // REUSE_ALL
		{Path: "/lob/field102.vortex", FieldID: 102, TotalRows: 1000, ValidRows: 200}, // REWRITE_ALL
		{Path: "/lob/field103.vortex", FieldID: 103, TotalRows: 1000, ValidRows: 900}, // REUSE_ALL
	})

	ctx.ComputeStrategies([]int64{101, 102, 103}, DefaultLOBHoleRatioThreshold)

	rewriteIDs := ctx.GetRewriteAllFieldIDs()
	reuseIDs := ctx.GetReuseAllFieldIDs()

	assert.Len(t, rewriteIDs, 1)
	assert.Contains(t, rewriteIDs, int64(102))

	assert.Len(t, reuseIDs, 2)
	assert.Contains(t, reuseIDs, int64(101))
	assert.Contains(t, reuseIDs, int64(103))
}

func TestLOBCompactionContext_GetFieldIDsByStrategy_NilContext(t *testing.T) {
	var ctx *LOBCompactionContext = nil

	assert.Nil(t, ctx.GetRewriteAllFieldIDs())
	assert.Nil(t, ctx.GetReuseAllFieldIDs())
}

// Tests for HasReuseAllFields
func TestLOBCompactionContext_HasReuseAllFields(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Before computing strategies
	assert.False(t, ctx.HasReuseAllFields())

	// Add REUSE_ALL field
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950},
	})
	ctx.ComputeStrategies([]int64{101}, DefaultLOBHoleRatioThreshold)

	assert.True(t, ctx.HasReuseAllFields())
}

// Tests for GetTextColumnConfigs
func TestLOBCompactionContext_GetTextColumnConfigs(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Field 101: REUSE_ALL (should not be in configs)
	// Field 102: REWRITE_ALL (should be in configs)
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950},
		{Path: "/lob/field102.vortex", FieldID: 102, TotalRows: 1000, ValidRows: 200},
	})
	ctx.ComputeStrategies([]int64{101, 102}, DefaultLOBHoleRatioThreshold)

	configs := ctx.GetTextColumnConfigs("/lob/base/path", DefaultTextInlineThreshold, DefaultTextMaxLobFileBytes, DefaultTextFlushThresholdBytes)

	// Should only contain REWRITE_ALL field
	assert.Len(t, configs, 1)
	assert.Equal(t, int64(102), configs[0].FieldID)
	assert.Equal(t, "/lob/base/path", configs[0].LobBasePath)
	assert.Equal(t, int64(DefaultTextInlineThreshold), configs[0].InlineThreshold)
	assert.Equal(t, int64(DefaultTextMaxLobFileBytes), configs[0].MaxLobFileBytes)
	assert.Equal(t, int64(DefaultTextFlushThresholdBytes), configs[0].FlushThresholdBytes)
}

func TestLOBCompactionContext_GetTextColumnConfigs_NoRewriteFields(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// All fields are REUSE_ALL
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/field101.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 950},
	})
	ctx.ComputeStrategies([]int64{101}, DefaultLOBHoleRatioThreshold)

	configs := ctx.GetTextColumnConfigs("/lob/base/path", DefaultTextInlineThreshold, DefaultTextMaxLobFileBytes, DefaultTextFlushThresholdBytes)

	assert.Nil(t, configs)
}

func TestLOBCompactionContext_GetTextColumnConfigs_NilContext(t *testing.T) {
	var ctx *LOBCompactionContext = nil
	configs := ctx.GetTextColumnConfigs("/lob/base/path", DefaultTextInlineThreshold, DefaultTextMaxLobFileBytes, DefaultTextFlushThresholdBytes)
	assert.Nil(t, configs)
}

// Tests for edge cases with proportional valid_rows calculation
func TestLOBCompactionContext_ProportionalValidRows_MultipleFilesPerSegment(t *testing.T) {
	ctx := NewLOBCompactionContext()

	// Single segment with multiple LOB files (simulating large data scenario)
	// Each file has different total_rows
	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 1000},
		{Path: "/lob/file2.vortex", FieldID: 101, TotalRows: 500, ValidRows: 500},
		{Path: "/lob/file3.vortex", FieldID: 101, TotalRows: 300, ValidRows: 300},
	})

	// 10% of rows deleted from segment
	ctx.SetSegmentRowStats(1, 1800, 180) // survival ratio = 0.9

	files := ctx.GetAllMergedLobFilesWithUpdatedStats()
	assert.Len(t, files, 3)

	// Each file's valid_rows should be updated proportionally
	fileMap := make(map[string]packed.LobFileInfo)
	for _, f := range files {
		fileMap[f.Path] = f
	}

	// valid_rows = total_rows * 0.9
	assert.Equal(t, int64(900), fileMap["/lob/file1.vortex"].ValidRows) // 1000 * 0.9
	assert.Equal(t, int64(450), fileMap["/lob/file2.vortex"].ValidRows) // 500 * 0.9
	assert.Equal(t, int64(270), fileMap["/lob/file3.vortex"].ValidRows) // 300 * 0.9
}

func TestLOBCompactionContext_ProportionalValidRows_ZeroTotalRows(t *testing.T) {
	ctx := NewLOBCompactionContext()

	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 1000},
	})

	// Edge case: total rows is 0 (should not happen but handle gracefully)
	ctx.SetSegmentRowStats(1, 0, 0)

	files := ctx.GetAllMergedLobFilesWithUpdatedStats()
	assert.Len(t, files, 1)
	// With 0 total rows, survival ratio defaults to 1.0
	assert.Equal(t, int64(1000), files[0].ValidRows)
}

func TestLOBCompactionContext_ProportionalValidRows_MoreDeletedThanTotal(t *testing.T) {
	ctx := NewLOBCompactionContext()

	ctx.AddSegmentLobFiles(1, []packed.LobFileInfo{
		{Path: "/lob/file1.vortex", FieldID: 101, TotalRows: 1000, ValidRows: 1000},
	})

	// Edge case: deleted > total (data inconsistency, handle gracefully)
	ctx.SetSegmentRowStats(1, 1000, 1500)

	files := ctx.GetAllMergedLobFilesWithUpdatedStats()
	assert.Len(t, files, 1)
	// Survival ratio clamped to 0
	assert.Equal(t, int64(0), files[0].ValidRows)
}
