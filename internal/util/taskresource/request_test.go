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

package taskresource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ---------------------------------------------------------------------------
// RequirementForCompaction
// ---------------------------------------------------------------------------

// Case B of the compatibility table: an old DataCoord sends no resource
// information, but the request itself carries enough to recompute. Because
// DataNode upgrades first, this is the path that protects the node before
// the coordinator knows anything about two-dimensional resources.
func TestRequirementForCompactionRecomputesWithoutCoordHelp(t *testing.T) {
	paramtable.Init()

	plan := &datapb.CompactionPlan{
		Type: datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				StorageVersion: 3,
				FieldBinlogs: []*datapb.FieldBinlog{
					{Binlogs: []*datapb.Binlog{{MemorySize: 4 << 30}}},
				},
			},
		},
	}

	got := RequirementForCompaction(plan)
	assert.GreaterOrEqual(t, got.Memory, int64(4)<<30)

	// exact: storage version must be read off the segment (CompactionPlan
	// itself carries no top-level storage_version field) and drive the v3
	// formula.
	want := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: 4 << 30,
	})
	assert.Equal(t, want, got)
}

// Case D: nothing usable at all. Fall back to a positive, conservative
// value rather than admitting the task for free.
func TestRequirementForCompactionEmptyPlanIsStillCharged(t *testing.T) {
	paramtable.Init()

	got := RequirementForCompaction(&datapb.CompactionPlan{Type: datapb.CompactionType_MixCompaction})
	assert.Greater(t, got.Memory, int64(0))
	assert.Greater(t, got.CPU, float64(0))
}

// Cross-task contract check (CompactionInput.MaxSegmentDeleteBytes doc
// comment): the streaming compactors load one segment's deltalogs at a time,
// so the requirement must reflect the largest single segment's delete
// payload, not the sum across segments.
func TestRequirementForCompactionStreamingTakesMaxDeleteAcrossSegments(t *testing.T) {
	paramtable.Init()

	plan := &datapb.CompactionPlan{
		Type: datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 1 << 20}}}}},
			{Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 5 << 20}}}}},
			{Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 2 << 20}}}}},
		},
	}

	got := RequirementForCompaction(plan)
	want := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_MixCompaction,
		MaxSegmentDeleteBytes: 5 << 20, // the max, not 1+5+2=8MiB
	})
	assert.Equal(t, want, got)
}

// Cross-task contract check, the other direction: L0's
// ComposeDeleteDataFromSegments loads every segment's deltalogs at once, so
// the requirement must sum across segments, not take the max.
func TestRequirementForCompactionL0SumsDeleteAcrossSegments(t *testing.T) {
	paramtable.Init()

	plan := &datapb.CompactionPlan{
		Type: datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 1 << 20}}}}},
			{Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 5 << 20}}}}},
			{Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 2 << 20}}}}},
		},
	}

	got := RequirementForCompaction(plan)
	want := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_Level0DeleteCompaction,
		MaxSegmentDeleteBytes: 8 << 20, // the sum, not the max of 5MiB
	})
	assert.Equal(t, want, got)
}

// Regression for a bug where totalRows was accumulated inside the per-field
// binlog loop, so a segment with N fields had its row count counted N times.
// estimateSortCompaction feeds TotalRows*rowIndexBytes directly, so a
// 10-field, 1M-row segment was charged as if it had 10M rows.
func TestRequirementForCompactionCountsRowsOncePerSegment(t *testing.T) {
	paramtable.Init()

	plan := &datapb.CompactionPlan{
		Type: datapb.CompactionType_SortCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				FieldBinlogs: []*datapb.FieldBinlog{
					{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 1_000_000}}},
					{FieldID: 101, Binlogs: []*datapb.Binlog{{EntriesNum: 1_000_000}}},
					{FieldID: 102, Binlogs: []*datapb.Binlog{{EntriesNum: 1_000_000}}},
				},
			},
		},
	}

	got := RequirementForCompaction(plan)
	want := EstimateCompaction(CompactionInput{
		Type:      datapb.CompactionType_SortCompaction,
		TotalRows: 1_000_000, // once, not x3 for the three fields
	})
	assert.Equal(t, want, got)
}

func TestSegmentRowCountSkipsFieldsWithNoBinlogs(t *testing.T) {
	fieldBinlogs := []*datapb.FieldBinlog{
		{FieldID: 100}, // e.g. an all-null column with nothing written
		{FieldID: 101, Binlogs: []*datapb.Binlog{{EntriesNum: 42}}},
	}
	assert.Equal(t, int64(42), segmentRowCount(fieldBinlogs))
}

// ---------------------------------------------------------------------------
// RequirementForIndex
// ---------------------------------------------------------------------------

func TestRequirementForIndexUsesFieldSizeAndIndexType(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateJobRequest{
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
		Dim:         768,
		NumRows:     1_163_739,
		Field:       &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector},
	}

	got := RequirementForIndex(req)
	// exact: 768*1_163_739*4 bytes raw * HNSW factor 2.0
	rawSize := int64(768) * 1_163_739 * 4
	want := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: rawSize})
	assert.Equal(t, want, got)
	// sanity bounds from the brief, kept as an extra guardrail
	assert.GreaterOrEqual(t, got.Memory, int64(3)<<30)
	assert.LessOrEqual(t, got.Memory, int64(16)<<30)
}

// A request shaped like the pre-2.4 wire format has no Dim/NumRows-derived
// closed form (e.g. ArrayOfVector), so sizing must fall back to the actual
// binlog bytes of the target field only -- not every field's binlogs summed
// together.
func TestRequirementForIndexFallsBackToBinlogsForFieldsWithoutClosedForm(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateJobRequest{
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}},
		Field:       &schemapb.FieldSchema{FieldID: 5, DataType: schemapb.DataType_ArrayOfVector},
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 5, Binlogs: []*datapb.Binlog{{MemorySize: 10 << 20}}},
			{FieldID: 6, Binlogs: []*datapb.Binlog{{MemorySize: 999 << 20}}}, // must not be counted
		},
	}

	got := RequirementForIndex(req)
	want := EstimateIndexBuild(IndexInput{IndexType: "INVERTED", FieldMemorySize: 10 << 20})
	assert.Equal(t, want, got)
}

// The field-size formula must match task_index.go's DataType switch for
// every branch, not just FloatVector: BinaryVector packs 8 dims/byte and
// Float16Vector uses 2 bytes/dim. Wrong branches here would silently
// under-size the requirement for these index builds.
// Dim/NumRows are deliberately large enough here that the raw byte size
// clears the 64MiB EstimateIndexBuild floor: at small sizes BinaryVector's
// dim/8*rows and FloatVector's dim*rows*4 formulas both collapse to the same
// floored 64MiB result and a wrong branch would go undetected.
func TestRequirementForIndexClosedFormCoversBinaryAndFloat16(t *testing.T) {
	paramtable.Init()

	binaryReq := &workerpb.CreateJobRequest{
		Dim:     8192,
		NumRows: 200_000,
		Field:   &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector},
	}
	gotBinary := RequirementForIndex(binaryReq)
	wantBinary := EstimateIndexBuild(IndexInput{FieldMemorySize: 8192 / 8 * 200_000})
	assert.Equal(t, wantBinary, gotBinary)
	assert.Greater(t, gotBinary.Memory, int64(64)<<20, "must clear the floor to actually exercise the formula")

	f16Req := &workerpb.CreateJobRequest{
		Dim:     4096,
		NumRows: 200_000,
		Field:   &schemapb.FieldSchema{DataType: schemapb.DataType_Float16Vector},
	}
	gotF16 := RequirementForIndex(f16Req)
	wantF16 := EstimateIndexBuild(IndexInput{FieldMemorySize: 4096 * 200_000 * 2})
	assert.Equal(t, wantF16, gotF16)
	assert.Greater(t, gotF16.Memory, int64(64)<<20, "must clear the floor to actually exercise the formula")
}

// A pre-2.4 wire request never fills Field, only the legacy scalar FieldID
// and FieldType; the field-size and field-lookup logic must fall back to
// those, not just to Field. The binlog size is large enough that a lookup
// bug (falling back to fieldID 0, which matches nothing) produces a result
// that clears the 64MiB floor differently from the correct one, instead of
// both collapsing to the same floored value.
func TestRequirementForIndexFallsBackToLegacyFieldIDAndFieldType(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateJobRequest{
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}},
		FieldID:     7,
		FieldType:   schemapb.DataType_ArrayOfVector,
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 7, Binlogs: []*datapb.Binlog{{MemorySize: 150 << 20}}},
		},
	}

	got := RequirementForIndex(req)
	want := EstimateIndexBuild(IndexInput{IndexType: "INVERTED", FieldMemorySize: 150 << 20})
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20, "must clear the floor: 0 (fieldID lookup miss) and 150MiB*0.8 both floor to the same value otherwise")
}

// A field with no closed form and no matching binlog at all must still be
// charged the index-build floor, not silently priced at zero.
func TestRequirementForIndexUnmatchedFieldStillCharged(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateJobRequest{
		Field: &schemapb.FieldSchema{FieldID: 9, DataType: schemapb.DataType_ArrayOfVector},
	}

	got := RequirementForIndex(req)
	assert.Greater(t, got.Memory, int64(0))
}

// An index request with no recognized index_type key must still fall back
// to the configured default factor rather than treating the field as free.
// Dim/NumRows are large enough that the default factor (1.5) and, say, an
// accidental HNSW factor (2.0) would produce visibly different un-floored
// results, so this actually pins which factor got used.
func TestRequirementForIndexMissingIndexTypeUsesDefaultFactor(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateJobRequest{
		Dim:     1024,
		NumRows: 200_000,
		Field:   &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector},
	}

	got := RequirementForIndex(req)
	want := EstimateIndexBuild(IndexInput{IndexType: "", FieldMemorySize: 1024 * 200_000 * 4})
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20)
}

// ---------------------------------------------------------------------------
// RequirementForStats
// ---------------------------------------------------------------------------

func schemaWithFields(fields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: fields}
}

// Only the field(s) the sub-job actually touches should be charged: a
// text-index job must not be charged for an unrelated field's binlogs even
// though the coord's CreateStatsRequest carries the whole segment's insert
// logs. The touched field's binlog is large enough (>32MiB, so that x2
// textFactor clears the 64MiB EstimateStats floor) that summing the wrong
// field(s) would produce a visibly different, non-floored result rather than
// both landing on the same floored value.
func TestRequirementForStatsTextIndexJobChargesOnlyMatchingField(t *testing.T) {
	paramtable.Init()

	textField := &schemapb.FieldSchema{
		FieldID:  100,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "enable_match", Value: "true"},
		},
	}
	plainField := &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_VarChar}

	req := &workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_TextIndexJob,
		Schema:     schemaWithFields(textField, plainField),
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 100 << 20}}},
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 800 << 20}}}, // must not be counted
		},
	}

	got := RequirementForStats(req)
	want := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_TextIndexJob, FieldMemorySize: 100 << 20})
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20, "must clear the floor to actually exercise field selection")
}

func TestRequirementForStatsJSONKeyIndexJobChargesOnlyJSONField(t *testing.T) {
	paramtable.Init()

	jsonField := &schemapb.FieldSchema{FieldID: 200, DataType: schemapb.DataType_JSON}
	intField := &schemapb.FieldSchema{FieldID: 201, DataType: schemapb.DataType_Int64}

	req := &workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		Schema:     schemaWithFields(jsonField, intField),
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 200, Binlogs: []*datapb.Binlog{{MemorySize: 100 << 20}}},
			{FieldID: 201, Binlogs: []*datapb.Binlog{{MemorySize: 800 << 20}}}, // must not be counted
		},
	}

	got := RequirementForStats(req)
	want := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob, FieldMemorySize: 100 << 20})
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20, "must clear the floor to actually exercise field selection")
}

// IsFunctionOutput must be set on the output field for
// typeutil.IsBM25FunctionOutputField to recognize it -- a schema missing that
// flag (as a real DataCoord-populated schema never would) makes the matcher
// return false for every field, which would zero the touched set entirely.
func TestRequirementForStatsBM25JobChargesOnlyFunctionOutputField(t *testing.T) {
	paramtable.Init()

	inputField := &schemapb.FieldSchema{FieldID: 300, DataType: schemapb.DataType_VarChar}
	outputField := &schemapb.FieldSchema{
		FieldID:          301,
		Name:             "sparse",
		DataType:         schemapb.DataType_SparseFloatVector,
		IsFunctionOutput: true,
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{inputField, outputField},
		Functions: []*schemapb.FunctionSchema{
			{
				Type:             schemapb.FunctionType_BM25,
				InputFieldIds:    []int64{300},
				OutputFieldIds:   []int64{301},
				OutputFieldNames: []string{"sparse"},
			},
		},
	}

	req := &workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_BM25Job,
		Schema:     schema,
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 300, Binlogs: []*datapb.Binlog{{MemorySize: 800 << 20}}}, // must not be counted
			{FieldID: 301, Binlogs: []*datapb.Binlog{{MemorySize: 100 << 20}}},
		},
	}

	got := RequirementForStats(req)
	want := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_BM25Job, FieldMemorySize: 100 << 20})
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20, "must clear the floor to actually exercise field selection")
}

// EstimateStats's contract (see estimate_index.go) is that Sort must never
// reach it: sort is priced as a compaction. A stats request carrying the
// Sort sub-job (a legacy/deprecated shape) must be routed to
// EstimateCompaction(SortCompaction) instead of being silently priced as a
// text index.
func TestRequirementForStatsSortRoutesToSortCompactionNotTextIndex(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateStatsRequest{
		SubJobType:     indexpb.StatsSubJob_Sort,
		StorageVersion: 2,
		NumRows:        1000,
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 1, Binlogs: []*datapb.Binlog{{MemorySize: 10 << 20}}},
		},
		DeltaLogs: []*datapb.FieldBinlog{
			{FieldID: 1, Binlogs: []*datapb.Binlog{{MemorySize: 2 << 20}}},
		},
	}

	got := RequirementForStats(req)
	want := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_SortCompaction,
		StorageVersion:        2,
		TotalMemorySize:       10 << 20,
		TotalRows:             1000,
		MaxSegmentDeleteBytes: 2 << 20,
	})
	assert.Equal(t, want, got)

	// It must differ from what a (wrong) text-index pricing would produce,
	// so this test would fail if Sort silently fell through to the text
	// factor.
	wrongIfTreatedAsText := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_TextIndexJob, FieldMemorySize: 10 << 20})
	assert.NotEqual(t, wrongIfTreatedAsText, got)
}

// An unrecognized/future sub-job (or the zero value) has no known field
// subset, so it must charge conservatively for the whole segment rather
// than guessing -- under-charging is the dangerous direction. Each field is
// individually small but their sum clears the 64MiB floor, so a bug that
// only counted one of the two fields (rather than the whole segment) would
// produce a visibly different, non-floored result.
func TestRequirementForStatsUnknownSubJobChargesWholeSegment(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_None,
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 1, Binlogs: []*datapb.Binlog{{MemorySize: 60 << 20}}},
			{FieldID: 2, Binlogs: []*datapb.Binlog{{MemorySize: 60 << 20}}},
		},
	}

	got := RequirementForStats(req)
	want := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_None, FieldMemorySize: 120 << 20})
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20, "must clear the floor to actually exercise summation")

	// A bug that counted only one field would produce a different,
	// non-floored number, not the same floored value.
	onlyOneField := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_None, FieldMemorySize: 60 << 20})
	assert.NotEqual(t, onlyOneField, got)
}

// ---------------------------------------------------------------------------
// RequirementForAnalyze
// ---------------------------------------------------------------------------

// Dim/row counts are large enough that using only the first segment's rows
// (a plausible "forgot to sum the map" bug) would clear the 64MiB
// EstimateAnalyze floor at a visibly different value than summing both.
func TestRequirementForAnalyzeSumsRowsAcrossSegments(t *testing.T) {
	paramtable.Init()

	req := &workerpb.AnalyzeRequest{
		Dim:   2048,
		Field: &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector},
		SegmentStats: map[int64]*indexpb.SegmentStats{
			10: {ID: 10, NumRows: 100_000},
			11: {ID: 11, NumRows: 50_000},
		},
	}

	got := RequirementForAnalyze(req)
	want := EstimateAnalyze(int64(2048)*150_000*4, req.GetMaxTrainSizeRatio())
	assert.Equal(t, want, got)
	assert.Greater(t, got.Memory, int64(64)<<20, "must clear the floor to actually exercise row summation")

	onlyFirstSegment := EstimateAnalyze(int64(2048)*100_000*4, req.GetMaxTrainSizeRatio())
	assert.NotEqual(t, onlyFirstSegment, got)
}

// Case D equivalent for analyze: a request with no segments/dim must still
// be charged a positive amount, never zero.
func TestRequirementForAnalyzeEmptyRequestIsStillCharged(t *testing.T) {
	paramtable.Init()

	got := RequirementForAnalyze(&workerpb.AnalyzeRequest{})
	assert.Greater(t, got.Memory, int64(0))
	assert.Greater(t, got.CPU, float64(0))
}

// ---------------------------------------------------------------------------
// LegacySlotToRequirement
// ---------------------------------------------------------------------------

// Case C: legacy slot is the last resort, and it must never produce zero.
func TestLegacySlotToRequirement(t *testing.T) {
	paramtable.Init()

	cfg := &paramtable.Get().DataNodeCfg
	perSlot := LegacyMemoryPerSlot()
	unit := cfg.WorkerSlotUnit.GetAsFloat()

	got := LegacySlotToRequirement(4)
	assert.Equal(t, Requirement{CPU: 4 / unit, Memory: 4 * perSlot}, got)

	// An absent slot (0) must not mean free: it floors to one slot, exactly
	// like a non-positive slot does.
	zero := LegacySlotToRequirement(0)
	want := Requirement{CPU: 1 / unit, Memory: perSlot}
	assert.Equal(t, want, zero)
	assert.Greater(t, zero.Memory, int64(0), "an absent slot must not mean free")

	negative := LegacySlotToRequirement(-5)
	assert.Equal(t, want, negative, "a negative slot must floor the same as zero")
}

// ---------------------------------------------------------------------------
// Storage v2/v3 column-group binlogs
// ---------------------------------------------------------------------------

// columnGroupInsertLogs is the storage v2/v3 layout, which is what this whole
// effort targets: FieldBinlog entries are keyed by COLUMN GROUP id -- a
// sequential counter from 0, see storagecommon.SplitBySchema -- and the real
// field ids (>= 100) live in ChildFields. A lookup that matches only the
// top-level FieldID finds nothing here at all.
func columnGroupInsertLogs() []*datapb.FieldBinlog {
	return []*datapb.FieldBinlog{
		{
			FieldID:     0,
			ChildFields: []int64{100, 101},
			Binlogs:     []*datapb.Binlog{{MemorySize: 3 << 30, EntriesNum: 1_000_000}},
		},
		{
			FieldID:     1,
			ChildFields: []int64{102},
			Binlogs:     []*datapb.Binlog{{MemorySize: 512 << 20, EntriesNum: 1_000_000}},
		},
	}
}

// A scalar index build over a multi-GiB varchar column stored in a v3 column
// group used to price at the 64MiB floor, because the per-field lookup never
// matched. Returning the whole column group over-estimates, which is the safe
// direction and is what DataCoord's getFieldBinlogSize does.
func TestRequirementForIndexFindsFieldInsideAColumnGroup(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateJobRequest{
		BuildID:        1,
		FieldID:        100,
		Field:          &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_VarChar},
		StorageVersion: 3,
		InsertLogs:     columnGroupInsertLogs(),
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "INVERTED"},
		},
	}

	got := RequirementForIndex(req)

	want := EstimateIndexBuild(IndexInput{
		IndexType:       "INVERTED",
		FieldMemorySize: 3 << 30,
		StorageVersion:  3,
	})
	assert.Equal(t, want, got)
	// 3GiB x 0.8 (INVERTED) + the v3 decode window: nowhere near the floor,
	// which is exactly the point -- 64MiB is what this used to be charged.
	assert.Greater(t, got.Memory, int64(2)<<30)
	assert.NotEqual(t, int64(64)<<20, got.Memory)
}

// The stats sub-jobs select their fields by schema and then look those field
// ids up in the binlogs, so they hit the same wall. All three sub-jobs
// DataCoord actually submits go through one helper; json-key stands for them.
func TestRequirementForStatsFindsFieldsInsideColumnGroups(t *testing.T) {
	paramtable.Init()

	req := &workerpb.CreateStatsRequest{
		TaskID:         2,
		SubJobType:     indexpb.StatsSubJob_JsonKeyIndexJob,
		StorageVersion: 3,
		InsertLogs:     columnGroupInsertLogs(),
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_VarChar},
				{FieldID: 101, DataType: schemapb.DataType_Int64},
				{FieldID: 102, DataType: schemapb.DataType_JSON},
			},
		},
	}

	got := RequirementForStats(req)

	factor := paramtable.Get().DataCoordCfg.ResourceJSONKeyIndexFactor.GetAsFloat()
	want := int64(float64(512<<20) * factor)
	require.Greater(t, want, int64(64)<<20, "setup: the expected value must not be the floor")
	assert.Equal(t, want, got.Memory)
}

// A column group is one set of binlogs. When two of its member fields are both
// selected, its bytes must be counted once -- adding it per matching child
// would multiply the same bytes by the number of fields sharing the group.
func TestStatsCountsAColumnGroupOnceForAllItsMatchingFields(t *testing.T) {
	paramtable.Init()

	logs := columnGroupInsertLogs()
	req := &workerpb.CreateStatsRequest{
		TaskID:     3,
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		InsertLogs: logs,
		Schema: &schemapb.CollectionSchema{
			// Both members of group 0 are JSON, so both are selected.
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_JSON},
				{FieldID: 101, DataType: schemapb.DataType_JSON},
			},
		},
	}

	got := RequirementForStats(req)

	factor := paramtable.Get().DataCoordCfg.ResourceJSONKeyIndexFactor.GetAsFloat()
	want := int64(float64(3<<30) * factor)
	assert.Equal(t, want, got.Memory, "group 0's bytes must be counted once, not twice")
}

// A field that cannot be isolated must not be charged the floor: that reads as
// "this task is free" and switches admission control off for it. DataCoord's
// getFieldBinlogSize makes the same choice -- <= 0 falls back to the whole
// segment -- and over-charging is the direction that does not OOM the node.
func TestUnresolvableFieldFallsBackToTheWholeSegment(t *testing.T) {
	paramtable.Init()

	logs := columnGroupInsertLogs()
	wholeSegment := int64(3<<30) + int64(512<<20)

	t.Run("index build", func(t *testing.T) {
		req := &workerpb.CreateJobRequest{
			BuildID:    1,
			FieldID:    9999, // not carried by any group
			Field:      &schemapb.FieldSchema{FieldID: 9999, DataType: schemapb.DataType_VarChar},
			InsertLogs: logs,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "INVERTED"},
			},
		}

		got := RequirementForIndex(req)
		want := EstimateIndexBuild(IndexInput{IndexType: "INVERTED", FieldMemorySize: wholeSegment})
		assert.Equal(t, want, got)
		assert.Greater(t, got.Memory, int64(64)<<20, "must not fall to the estimator floor")
	})

	t.Run("stats", func(t *testing.T) {
		req := &workerpb.CreateStatsRequest{
			TaskID:     2,
			SubJobType: indexpb.StatsSubJob_TextIndexJob,
			InsertLogs: logs,
			// No field in this schema enables match, so the touched set is empty.
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64}},
			},
		}

		got := RequirementForStats(req)
		factor := paramtable.Get().DataCoordCfg.ResourceTextIndexFactor.GetAsFloat()
		assert.Equal(t, int64(float64(wholeSegment)*factor), got.Memory)
		assert.Greater(t, got.Memory, int64(64)<<20, "must not fall to the estimator floor")
	})
}

// TestCompactionMemoryCountsDeltaAndStatsLogs pins the F5 contract on the
// DataNode side: TotalMemorySize means insert + delta + stats, the same three
// terms SegmentInfo.getSegmentSize sums, because DataCoord feeds exactly that
// into the same estimator for the same task.
func TestCompactionMemoryCountsDeltaAndStatsLogs(t *testing.T) {
	paramtable.Init()

	plan := &datapb.CompactionPlan{
		Type: datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				StorageVersion:      3,
				FieldBinlogs:        []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 4 << 30, EntriesNum: 100}}}},
				Deltalogs:           []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 700 << 20}}}},
				Field2StatslogPaths: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 300 << 20}}}},
			},
		},
	}

	got := RequirementForCompaction(plan)

	// v3: factor x (insert + delta + stats) + the delete payload again.
	factor := paramtable.Get().DataCoordCfg.ResourceMixCompactionV3Factor.GetAsFloat()
	total := int64(4<<30) + int64(700<<20) + int64(300<<20)
	want := int64(float64(total)*factor) + int64(700<<20)
	assert.Equal(t, want, got.Memory)

	// Anti-vacuity: insert-only would be a full GiB smaller.
	insertOnly := int64(float64(4<<30)*factor) + int64(700<<20)
	require.Greater(t, got.Memory, insertOnly)
}
