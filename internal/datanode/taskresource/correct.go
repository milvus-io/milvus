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

// Package taskresource refines, on the DataNode that accepts a task, the
// cpu/memory estimate DataCoord shipped with it.
//
// DataCoord prices a task from its meta and places it on the workers' ledgers.
// The worker that receives the task knows more: the exact bytes of the fields
// the task reads (a dense vector field is rows x dim x element size, every
// other field is the sum of its binlog memory sizes carried in the request),
// how much the requested index type expands its input while it builds, and its
// own machine, which bounds the families whose buffers are a share of it. The
// functions here turn a request and DataCoord's estimate into the memory this
// node books for the task. QuerySlot reports total minus the sum of those
// booked values, so DataCoord's next scheduling round places on what the tasks
// on this node actually occupy.
//
// Every correction follows one contract:
//   - A zero estimate is returned unchanged. It comes from a coordinator that
//     predates estimates, and booking zero for it is the compatibility rule.
//   - CPU is DataCoord's. It only ranks candidate workers.
//   - When the request does not carry an input better than the one DataCoord
//     priced on (for example a V3 segment whose per-field binlogs DataCoord no
//     longer holds after a restart), the estimate stands. A correction never
//     replaces a number with a guess.
//   - A corrected memory is floored at dataCoord.taskResource.minTaskMemory,
//     like the estimate itself.
//
// The correction happens once, when the task is accepted: every input it needs
// is already in the request, and a value fixed at acceptance is the value the
// ledger books and releases, with no mid-flight adjustment to keep consistent.
package taskresource

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	// rowIndexBytes is storage.Sort's per-row index entry: two int32.
	rowIndexBytes = 8
	// deleteMapExpansion covers the Go map compaction builds from the delete
	// records it loads (compaction.ComposeDeleteFromDeltalogs): map buckets and
	// boxed keys on top of the records' own bytes.
	deleteMapExpansion = 2
)

// CorrectIndex refines an index build. The input is the indexed field's exact
// bytes plus those of the optional scalar fields the build loads beside it;
// the expansion is the build model of the requested index type (index_model.go).
func CorrectIndex(req *workerpb.CreateJobRequest, estimate taskcommon.Resource) taskcommon.Resource {
	if estimate.IsZero() {
		return estimate
	}
	dataType := req.GetField().GetDataType()
	if req.GetField() == nil {
		dataType = req.GetFieldType()
	}
	fieldID := req.GetFieldID()
	if req.GetField() != nil {
		fieldID = req.GetField().GetFieldID()
	}
	raw := denseVectorBytes(dataType, req.GetDim(), req.GetNumRows())
	if raw <= 0 {
		raw = fieldBinlogMemory(req.GetInsertLogs(), fieldID)
	}
	if raw <= 0 {
		return estimate
	}
	params := indexBuildParams(req)
	memory := indexBuildMemory(params[indexTypeKey], indexInput{
		raw:      raw,
		rows:     req.GetNumRows(),
		dim:      req.GetDim(),
		dataType: dataType,
		params:   params,
	})
	for _, optional := range req.GetOptionalScalarFields() {
		memory += fieldBinlogMemory(req.GetInsertLogs(), optional.GetFieldID())
	}
	return corrected(estimate, memory)
}

// CorrectStats refines a stats task by the fields its sub job reads, each
// priced by what its writer holds (index/task_stats.go builds every target
// field concurrently, so the fields add up).
func CorrectStats(req *workerpb.CreateStatsRequest, estimate taskcommon.Resource) taskcommon.Resource {
	if estimate.IsZero() {
		return estimate
	}
	schema := req.GetSchema()
	logs := req.GetInsertLogs()
	switch req.GetSubJobType() {
	case indexpb.StatsSubJob_TextIndexJob:
		return correctPerField(estimate, logs, textMatchFields(schema), tantivyBuild)
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		tantivyMemory := max(req.GetJsonKeyStatsTantivyMemory(), 0)
		return correctPerField(estimate, logs, jsonKeyStatsFields(schema), func(raw int64) int64 {
			// The parsed documents and the shredded columns written from them,
			// plus the key index writer's budget.
			return 2*raw + tantivyMemory
		})
	case indexpb.StatsSubJob_BM25Job:
		return correctPerField(estimate, logs, bm25OutputFields(schema), func(raw int64) int64 {
			return 2 * raw
		})
	case indexpb.StatsSubJob_Sort:
		insert := binlogMemory(logs)
		if insert <= 0 {
			return estimate
		}
		return corrected(estimate, insert+
			req.GetNumRows()*rowIndexBytes+
			int64(req.GetBinlogMaxSize())+
			deleteMapExpansion*binlogMemory(req.GetDeltaLogs()))
	default:
		return estimate
	}
}

// CorrectAnalyze refines an analyze by the vectors it actually trains on: the
// raw vectors, down-sampled to maxTrainSizeRatio of this machine when larger
// (index/task_analyze.go sizes TrainSize exactly so).
func CorrectAnalyze(req *workerpb.AnalyzeRequest, estimate taskcommon.Resource) taskcommon.Resource {
	if estimate.IsZero() {
		return estimate
	}
	var rows int64
	for _, stats := range req.GetSegmentStats() {
		rows += stats.GetNumRows()
	}
	raw := denseVectorBytes(req.GetFieldType(), req.GetDim(), rows)
	if raw <= 0 {
		return estimate
	}
	train := raw
	if ratio := req.GetMaxTrainSizeRatio(); ratio > 0 {
		train = min(raw, int64(float64(hardware.GetMemoryCount())*ratio))
	}
	return corrected(estimate, scaled(train, paramtable.Get().DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat()))
}

// CorrectCompaction refines a compaction plan by the binlog, deltalog and
// statslog memory sizes it carries.
func CorrectCompaction(plan *datapb.CompactionPlan, estimate taskcommon.Resource) taskcommon.Resource {
	if estimate.IsZero() {
		return estimate
	}
	var insert, delta, l0Delta, targetStats int64
	for _, segment := range plan.GetSegmentBinlogs() {
		insert += binlogMemory(segment.GetFieldBinlogs())
		delta += binlogMemory(segment.GetDeltalogs())
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			l0Delta += binlogMemory(segment.GetDeltalogs())
		} else {
			targetStats += binlogMemory(segment.GetField2StatslogPaths())
		}
	}

	switch plan.GetType() {
	case datapb.CompactionType_Level0DeleteCompaction:
		// compactor/l0_compactor.go loads every L0 delete record, and the bloom
		// filters of the target segments it applies them to.
		if l0Delta <= 0 {
			return estimate
		}
		factor := paramtable.Get().DataCoordCfg.TaskResourceL0CompactionMemoryFactor.GetAsFloat()
		return corrected(estimate, scaled(l0Delta, factor)+targetStats)

	case datapb.CompactionType_SortCompaction:
		// storage.Sort retains every input record, indexes each row, and
		// flushes the sorted output in binlogMaxSize batches.
		if insert <= 0 {
			return estimate
		}
		binlogMaxSize := paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsInt64()
		return corrected(estimate, insert+plan.GetTotalRows()*rowIndexBytes+binlogMaxSize+deleteMapExpansion*delta)

	case datapb.CompactionType_MixCompaction, datapb.CompactionType_BumpSchemaVersionCompaction:
		// MultiSegmentWriter streams: never more than the input, never more than
		// one output segment of the plan's max size; plus the delete map.
		if insert <= 0 {
			return estimate
		}
		streamed := insert
		if bound := plan.GetMaxSize(); bound > 0 {
			streamed = min(insert, bound)
		}
		return corrected(estimate, streamed+deleteMapExpansion*delta)

	case datapb.CompactionType_ClusteringCompaction:
		// compactor/clustering_compactor.go flushes its buckets once they reach
		// memoryBufferRatio of this machine.
		if insert <= 0 {
			return estimate
		}
		ratio := paramtable.Get().DataNodeCfg.ClusteringCompactionMemoryBufferRatio.GetAsFloat()
		buffered := min(insert, int64(float64(hardware.GetMemoryCount())*ratio))
		return corrected(estimate, buffered+deleteMapExpansion*delta)

	default:
		return estimate
	}
}

// CorrectImport refines an import by the buffers its files allocate on this
// machine: every file is submitted at once with one read buffer
// (importv2.CalculateImportBufferSize, or the base buffer for an L0 import),
// and the memory allocator never lets the import buffers exceed this machine's
// import limit. The import memory factor covers the batch in sync while the
// next one is read.
func CorrectImport(req *datapb.ImportRequest, estimate taskcommon.Resource) taskcommon.Resource {
	if estimate.IsZero() {
		return estimate
	}
	files := max(int64(len(req.GetFiles())), 1)
	perFile := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	if !importutilv2.IsL0Import(req.GetOptions()) {
		// The import task's own FileStats are never filled on the worker, so the
		// largest-file cap does not apply (see importv2.ImportTask.GetBufferSize).
		perFile = importv2.CalculateImportBufferSize(len(req.GetVchannels()), len(req.GetPartitionIDs()), 0)
	}
	buffers := min(files*perFile, importv2.ImportMemoryLimit())
	return corrected(estimate, scaled(buffers, paramtable.Get().DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat()))
}

// CorrectPreImport refines a pre-import: every file is read in parallel with
// one base buffer, and nothing is kept once it is read.
func CorrectPreImport(req *datapb.PreImportRequest, estimate taskcommon.Resource) taskcommon.Resource {
	if estimate.IsZero() {
		return estimate
	}
	files := max(int64(len(req.GetImportFiles())), 1)
	return corrected(estimate, files*paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64())
}

func corrected(estimate taskcommon.Resource, memory int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    estimate.CPU,
		Memory: max(memory, paramtable.Get().DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize()),
	}
}

// correctPerField prices each target field with model and sums them. Without a
// target field, or when any target field has no binlog bytes in the request,
// the estimate stands: a partial sum would under-price the task.
func correctPerField(estimate taskcommon.Resource, logs []*datapb.FieldBinlog, fields []int64, model func(raw int64) int64) taskcommon.Resource {
	if len(fields) == 0 {
		return estimate
	}
	var memory int64
	for _, fieldID := range fields {
		raw := fieldBinlogMemory(logs, fieldID)
		if raw <= 0 {
			return estimate
		}
		memory += model(raw)
	}
	return corrected(estimate, memory)
}

func textMatchFields(schema *schemapb.CollectionSchema) []int64 {
	var fields []int64
	for _, field := range schema.GetFields() {
		if typeutil.CreateFieldSchemaHelper(field).EnableMatch() {
			fields = append(fields, field.GetFieldID())
		}
	}
	return fields
}

func jsonKeyStatsFields(schema *schemapb.CollectionSchema) []int64 {
	var fields []int64
	for _, field := range schema.GetFields() {
		if typeutil.CreateFieldSchemaHelper(field).EnableJSONKeyStatsIndex() {
			fields = append(fields, field.GetFieldID())
		}
	}
	return fields
}

func bm25OutputFields(schema *schemapb.CollectionSchema) []int64 {
	var fields []int64
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() == schemapb.FunctionType_BM25 {
			fields = append(fields, fn.GetOutputFieldIds()...)
		}
	}
	return fields
}

// denseVectorBytes is the exact size of rows fixed-dimension vectors, or 0
// when the type is not one or the dim or rows are unknown.
func denseVectorBytes(dataType schemapb.DataType, dim, rows int64) int64 {
	if !typeutil.IsFixDimVectorType(dataType) || dim <= 0 || rows <= 0 {
		return 0
	}
	return int64(float64(rows) * float64(dim) * typeutil.VectorTypeSize(dataType))
}

// fieldBinlogMemory sums the memory size of one field's binlogs, including a
// struct-array parent's binlogs that carry the field as a child.
func fieldBinlogMemory(logs []*datapb.FieldBinlog, fieldID int64) int64 {
	var size int64
	for _, fieldBinlog := range logs {
		match := fieldBinlog.GetFieldID() == fieldID
		for _, child := range fieldBinlog.GetChildFields() {
			match = match || child == fieldID
		}
		if match {
			size += binlogMemory([]*datapb.FieldBinlog{fieldBinlog})
		}
	}
	return size
}

func binlogMemory(logs []*datapb.FieldBinlog) int64 {
	var size int64
	for _, fieldBinlog := range logs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			size += binlog.GetMemorySize()
		}
	}
	return size
}

func scaled(size int64, factor float64) int64 {
	return int64(float64(size) * factor)
}
