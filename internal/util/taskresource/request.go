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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The DataNode always recomputes a task's requirement from the request itself
// rather than trusting a number handed down by the coordinator.
//
// The upgrade order is DataNode first, DataCoord second, so for a whole
// upgrade window this node is driven by a coordinator that knows nothing about
// resource requirements. Recomputing locally is what makes the memory
// protection effective during that window instead of after it.
//
// Only when a request carries too little information to recompute does the
// legacy slot get folded in, and even then never to zero.

// RequirementForCompaction derives a compaction task's requirement straight
// from the plan's binlogs, independent of anything the coordinator might have
// attached to the request.
func RequirementForCompaction(plan *datapb.CompactionPlan) Requirement {
	var totalMemory, totalRows, maxDelete, storageVersion int64

	for _, seg := range plan.GetSegmentBinlogs() {
		// CompactionPlan carries no plan-wide storage_version field; each
		// segment carries its own. A plan's segments are being merged
		// together and are expected to share one version, but take the max
		// so a mixed plan prices at least as conservatively as its newest
		// segment.
		if v := seg.GetStorageVersion(); v > storageVersion {
			storageVersion = v
		}

		totalMemory += sumFieldBinlogMemory(seg.GetFieldBinlogs())
		totalRows += segmentRowCount(seg.GetFieldBinlogs())

		segDelete := sumFieldBinlogMemory(seg.GetDeltalogs())
		if plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
			// L0's ComposeDeleteDataFromSegments loads every segment's
			// deletes at once: sum across segments.
			maxDelete += segDelete
		} else if segDelete > maxDelete {
			// The streaming compactors' ComposeDeleteFromDeltalogs loads one
			// segment's deltalogs at a time: the largest single segment
			// bounds the peak, not the sum.
			maxDelete = segDelete
		}
	}

	return EstimateCompaction(CompactionInput{
		Type:                  plan.GetType(),
		StorageVersion:        storageVersion,
		TotalMemorySize:       totalMemory,
		TotalRows:             totalRows,
		MaxSegmentDeleteBytes: maxDelete,
	})
}

// segmentRowCount returns a compaction input segment's row count from a
// single field's binlogs. Every field in a segment covers the same rows, so
// summing EntriesNum across all of them (as an earlier version of
// RequirementForCompaction did) multiplies the row count by the field count
// instead of counting it once -- for a 10-field, 1M-row segment that is 10M,
// not 1M, feeding directly into estimateSortCompaction's
// TotalRows*rowIndexBytes term. CompactionSegmentBinlogs carries no row-count
// field of its own (checked pkg/proto/datapb/data_coord.pb.go), so this reads
// one field instead of guessing at a dedicated count. It skips over any field
// with zero binlogs (e.g. an all-null column) to avoid returning 0 when a
// later field would report the real count.
func segmentRowCount(fieldBinlogs []*datapb.FieldBinlog) int64 {
	for _, fb := range fieldBinlogs {
		var rows int64
		for _, b := range fb.GetBinlogs() {
			rows += b.GetEntriesNum()
		}
		if rows > 0 {
			return rows
		}
	}
	return 0
}

// vectorFieldByteSize mirrors the DataType switch in
// internal/datanode/index/task_index.go's Execute, because that is the sizing
// the actual index-build path applies: BinaryVector packs 8 dims/byte,
// Float16/BFloat16 use 2 bytes/dim, FloatVector uses 4 bytes/dim. The two
// must stay in step -- internal/datanode/index is cgo-bound so it cannot be
// imported here to share the code directly.
func vectorFieldByteSize(dataType schemapb.DataType, dim, numRows int64) int64 {
	switch dataType {
	case schemapb.DataType_BinaryVector:
		return dim / 8 * numRows
	case schemapb.DataType_FloatVector:
		return dim * numRows * 4
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return dim * numRows * 2
	default:
		return 0
	}
}

// sumFieldBinlogMemory sums Binlog.MemorySize across every field in logs.
func sumFieldBinlogMemory(logs []*datapb.FieldBinlog) int64 {
	var total int64
	for _, fb := range logs {
		for _, b := range fb.GetBinlogs() {
			total += b.GetMemorySize()
		}
	}
	return total
}

// sumFieldBinlogMemoryForField sums Binlog.MemorySize for the first
// FieldBinlog matching fieldID, mirroring getFieldDataSizeFromBinlogs in
// internal/datanode/index/util.go (which also stops at the first match).
func sumFieldBinlogMemoryForField(logs []*datapb.FieldBinlog, fieldID int64) int64 {
	for _, fb := range logs {
		if fb.GetFieldID() != fieldID {
			continue
		}
		var total int64
		for _, b := range fb.GetBinlogs() {
			total += b.GetMemorySize()
		}
		return total
	}
	return 0
}

// estimateFieldMemorySize sizes the field an index build targets. For
// vector types with a closed-form size (see vectorFieldByteSize) it uses
// Dim x NumRows directly, exactly as the build path does; for everything
// else (e.g. ArrayOfVector, or a legacy request that never filled Dim) it
// falls back to the field's own binlog bytes, matching
// getFieldDataSizeFromBinlogs in internal/datanode/index/util.go.
func estimateFieldMemorySize(req *workerpb.CreateJobRequest) int64 {
	dataType := req.GetField().GetDataType()
	if dataType == schemapb.DataType_None {
		dataType = req.GetFieldType()
	}
	if size := vectorFieldByteSize(dataType, req.GetDim(), req.GetNumRows()); size > 0 {
		return size
	}

	fieldID := req.GetField().GetFieldID()
	if fieldID == 0 {
		fieldID = req.GetFieldID()
	}
	return sumFieldBinlogMemoryForField(req.GetInsertLogs(), fieldID)
}

// getIndexTypeFromParams mirrors GetIndexType in
// internal/datanode/index/util.go; the two must stay in step. That package
// is cgo-bound and cannot be imported here.
func getIndexTypeFromParams(indexParams []*commonpb.KeyValuePair) string {
	for _, param := range indexParams {
		if param.GetKey() == common.IndexTypeKey {
			return param.GetValue()
		}
	}
	return ""
}

// RequirementForIndex derives an index-build task's requirement from the
// field it targets and the index type requested, independent of anything the
// coordinator might have attached to the request.
func RequirementForIndex(req *workerpb.CreateJobRequest) Requirement {
	fieldSize := estimateFieldMemorySize(req)
	indexType := getIndexTypeFromParams(req.GetIndexParams())

	return EstimateIndexBuild(IndexInput{
		IndexType:       indexType,
		FieldMemorySize: fieldSize,
		StorageVersion:  req.GetStorageVersion(),
	})
}

// touchedFieldIDs returns the IDs of the schema fields for which matches
// reports true.
func touchedFieldIDs(schema *schemapb.CollectionSchema, matches func(*schemapb.FieldSchema) bool) map[int64]bool {
	ids := make(map[int64]bool)
	for _, f := range schema.GetFields() {
		if matches(f) {
			ids[f.GetFieldID()] = true
		}
	}
	return ids
}

// sumFieldBinlogMemoryForFieldSet sums Binlog.MemorySize across every field
// in logs whose FieldID is in ids.
func sumFieldBinlogMemoryForFieldSet(logs []*datapb.FieldBinlog, ids map[int64]bool) int64 {
	var total int64
	for _, fb := range logs {
		if !ids[fb.GetFieldID()] {
			continue
		}
		for _, b := range fb.GetBinlogs() {
			total += b.GetMemorySize()
		}
	}
	return total
}

// RequirementForStats derives a stats sub-job's requirement from the field(s)
// it actually touches, not the whole segment: today the coord charges
// whole-segment size for every sub-job, which over-charges text and
// json-key builds (see StatsInput's doc comment in estimate_index.go).
//
// Field selection mirrors internal/datanode/index/task_stats.go's Execute
// exactly: text-index jobs touch fields with typeutil.FieldSchemaHelper's
// EnableMatch, json-key jobs touch EnableJSONKeyStatsIndex fields, and BM25
// jobs touch the sparse-vector output field of a BM25 function
// (typeutil.IsBM25FunctionOutputField). Sort is deliberately excluded from
// this dispatch: EstimateStats's contract says explicitly that Sort is out
// of its scope, because sort is priced as a compaction
// (CompactionType_SortCompaction) via EstimateCompaction instead. A Sort
// sub-job reaching this function at all means a legacy/deprecated request
// shape is still in play (several CreateStatsRequest fields used only by
// Sort are marked deprecated), so it is routed to EstimateCompaction and
// logged rather than silently priced as a text index.
func RequirementForStats(req *workerpb.CreateStatsRequest) Requirement {
	subJob := req.GetSubJobType()

	if subJob == indexpb.StatsSubJob_Sort {
		mlog.Warn(context.Background(),
			"stats request carries the Sort sub-job; pricing it as a sort compaction instead of a stats job",
			mlog.String("subJobType", subJob.String()),
			mlog.Int64("taskID", req.GetTaskID()),
			mlog.Int64("segmentID", req.GetSegmentID()),
		)
		return EstimateCompaction(CompactionInput{
			Type:                  datapb.CompactionType_SortCompaction,
			StorageVersion:        req.GetStorageVersion(),
			TotalMemorySize:       sumFieldBinlogMemory(req.GetInsertLogs()),
			TotalRows:             req.GetNumRows(),
			MaxSegmentDeleteBytes: sumFieldBinlogMemory(req.GetDeltaLogs()),
		})
	}

	var touched int64
	switch subJob {
	case indexpb.StatsSubJob_TextIndexJob:
		ids := touchedFieldIDs(req.GetSchema(), func(f *schemapb.FieldSchema) bool {
			return typeutil.CreateFieldSchemaHelper(f).EnableMatch()
		})
		touched = sumFieldBinlogMemoryForFieldSet(req.GetInsertLogs(), ids)
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		ids := touchedFieldIDs(req.GetSchema(), func(f *schemapb.FieldSchema) bool {
			return typeutil.CreateFieldSchemaHelper(f).EnableJSONKeyStatsIndex()
		})
		touched = sumFieldBinlogMemoryForFieldSet(req.GetInsertLogs(), ids)
	case indexpb.StatsSubJob_BM25Job:
		schema := req.GetSchema()
		ids := touchedFieldIDs(schema, func(f *schemapb.FieldSchema) bool {
			return typeutil.IsBM25FunctionOutputField(f, schema)
		})
		touched = sumFieldBinlogMemoryForFieldSet(req.GetInsertLogs(), ids)
	default:
		// An unrecognized or future sub-job (including the None value) has
		// no known field subset to isolate, so it charges the whole segment
		// rather than guessing: under-provisioning is the direction that
		// causes OOM.
		touched = sumFieldBinlogMemory(req.GetInsertLogs())
	}

	return EstimateStats(StatsInput{SubJobType: subJob, FieldMemorySize: touched})
}

// RequirementForAnalyze derives an analyze (kmeans training) task's
// requirement from the input segments it will read.
//
// Unlike CreateJobRequest/CreateStatsRequest, AnalyzeRequest's per-segment
// SegmentStats (pkg/proto/index_coord.proto) carries only row counts and log
// IDs, not binlog MemorySize -- there is no byte figure on the wire to sum.
// The dataset size is instead recomputed the same way index-build sizing is:
// Dim x total NumRows x the target field's per-row width (see
// vectorFieldByteSize), summed across every input segment.
func RequirementForAnalyze(req *workerpb.AnalyzeRequest) Requirement {
	var totalRows int64
	for _, stats := range req.GetSegmentStats() {
		totalRows += stats.GetNumRows()
	}

	dataType := req.GetField().GetDataType()
	if dataType == schemapb.DataType_None {
		dataType = req.GetFieldType()
	}

	return EstimateAnalyze(vectorFieldByteSize(dataType, req.GetDim(), totalRows))
}

// LegacySlotToRequirement converts a scalar slot into a requirement. It is the
// last resort of the compatibility table, used only when a request carries too
// little information to recompute, and it deliberately floors at one slot so an
// absent or non-positive slot is never read as "this task is free".
func LegacySlotToRequirement(slot int64) Requirement {
	if slot <= 0 {
		slot = 1
	}
	cfg := &paramtable.Get().DataNodeCfg
	perSlot := cfg.ResourceLegacyMemoryPerSlot.GetAsInt64()
	unit := cfg.WorkerSlotUnit.GetAsFloat()
	if unit <= 0 {
		unit = 1
	}
	return Requirement{
		CPU:    float64(slot) / unit,
		Memory: slot * perSlot,
	}
}
