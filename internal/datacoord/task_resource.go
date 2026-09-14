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

package datacoord

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is the only place DataCoord prices a task in CPU and memory. Every
// task family calls one of the formulas below from GetTaskResource(); the
// worker never recomputes, it books whatever the request carries.
//
// CPU is a request, not a reservation: it only ranks candidate workers.
// Memory is what a worker can refuse a task for. Both are floored so that a
// task whose inputs could not be resolved is still placed as costing
// something, never as free.
//
// Every memory formula mirrors what the worker actually holds for that family
// (the comment next to each formula names the worker-side code it mirrors),
// and errs on the high side where the worker's behavior depends on data or on
// a machine DataCoord does not see. This estimate is what DataCoord places on;
// the DataNode that accepts the task refines it with what only the worker
// knows (exact field bytes, per-index-type expansion, its own memory for the
// families whose buffers are a share of the machine) and books the refined
// value, so the next round places on the corrected availability.

func defaultCPU() int64 {
	return max(Params.DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64(), 1)
}

// clampTaskMemory applies the configured floor.
func clampTaskMemory(memory int64) int64 {
	return max(memory, Params.DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize())
}

func scaled(size int64, factor float64) int64 {
	return int64(float64(size) * factor)
}

// defaultTaskResource is the answer when a task cannot resolve its inputs
// (segment dropped between enqueue and dispatch, schema not cached yet).
func defaultTaskResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: defaultCPU(), Memory: clampTaskMemory(0)}
}

// indexTaskResource: the worker (index/task_index.go) loads the whole indexed
// field through cgo and builds the index beside it, so the field's bytes times
// the factor. DiskANN builds against a DRAM budget equal to the worker's free
// memory, so it is not priced lower than an in-memory build.
func indexTaskResource(fieldSize int64, isVectorIndex bool) taskcommon.Resource {
	cpu := defaultCPU()
	if isVectorIndex {
		cpu = max(Params.DataCoordCfg.TaskResourceVectorIndexCPU.GetAsInt64(), 1)
	}
	return taskcommon.Resource{
		CPU:    cpu,
		Memory: clampTaskMemory(scaled(fieldSize, Params.DataCoordCfg.TaskResourceIndexMemoryFactor.GetAsFloat())),
	}
}

// statsTaskResource prices a stats task by the bytes it reads: the fields it
// indexes (statsInputSize) for text-match / json-key / bm25, and the whole
// segment for a sort compaction, which holds every input record in memory
// until the sort is done (storage.Sort).
func statsTaskResource(inputSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(inputSize, Params.DataCoordCfg.TaskResourceStatsMemoryFactor.GetAsFloat())),
	}
}

// mixCompactionTaskResource: a mix (or schema bump) compaction streams its
// input through MultiSegmentWriter, which flushes at binlogMaxSize, so it never
// holds more than its input and never needs more than one output segment of
// segment.maxSize. min of the two; an unknown input size gets the upper bound.
func mixCompactionTaskResource(inputSize int64) taskcommon.Resource {
	bound := Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	memory := bound
	if inputSize > 0 {
		memory = min(inputSize, bound)
	}
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(memory),
	}
}

// l0CompactionTaskResource: the worker (compactor/l0_compactor.go) loads every
// input delta log into memory before applying it batch by batch.
func l0CompactionTaskResource(deltaSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(deltaSize, Params.DataCoordCfg.TaskResourceL0CompactionMemoryFactor.GetAsFloat())),
	}
}

// clusteringCompactionTaskResource: the worker (compactor/clustering_compactor.go)
// buckets its input in memory and flushes buckets once the buffer reaches
// dataNode.clusteringCompaction.memoryBufferRatio of the machine, so it never
// holds more than its input. That input is the estimate; the share of the
// machine is applied by the DataNode that accepts the task, which is the only
// side that knows the machine.
func clusteringCompactionTaskResource(inputSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceClusteringCompactionCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(inputSize),
	}
}

// analyzeTaskResource: the worker (index/task_analyze.go) trains on the raw
// vectors, so raw bytes times the factor. The worker down-samples to
// maxTrainSizeRatio of its machine when they exceed it; that cap is applied by
// the accepting DataNode.
func analyzeTaskResource(rawDataSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceAnalyzeCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(scaled(rawDataSize, Params.DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat())),
	}
}

// importTaskResource: the worker (importv2/task_import.go) submits every file
// of the task to its exec pool at once and each file allocates one read buffer
// of perFileBuffer bytes, so the task holds numFiles buffers; the factor covers
// the batch being serialized and uploaded while the next one is read. The
// worker's allocator, a percentage of its machine, is applied by the accepting
// DataNode.
func importTaskResource(numFiles, perFileBuffer int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(max(numFiles, 1)*perFileBuffer, Params.DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat())),
	}
}

// preImportTaskResource: the worker (importv2/task_preimport.go) reads every
// file in parallel with one base buffer each and keeps nothing: one buffer per
// file, no in-flight sync, no allocator cap.
func preImportTaskResource(numFiles, perFileBuffer int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(max(numFiles, 1) * perFileBuffer),
	}
}

// importFileBufferSize mirrors importv2.ImportTask.GetBufferSize on the worker:
// the base buffer per (vchannel, partition) pair; an L0 import uses the base
// buffer as is (importv2.L0ImportTask.GetBufferSize).
//
// It is deliberately NOT capped at the largest file. GetBufferSize reads a
// largest-file cap from the task's own ImportTaskV2.FileStats, but the worker
// never fills that field for an import task (NewImportTask leaves it nil), so
// the cap never fires there and capping here would under-price every import.
// The worker's remaining clamp, a percentage of its machine, is applied by the
// accepting DataNode.
func importFileBufferSize(job ImportJob) int64 {
	base := Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	if importutilv2.IsL0Import(job.GetOptions()) {
		return base
	}
	return base * int64(len(job.GetVchannels())) * int64(len(job.GetPartitionIDs()))
}

// lightweightTaskResource prices copy-segment and external-refresh tasks,
// which stream data and hold little of it.
func lightweightTaskResource() taskcommon.Resource {
	return defaultTaskResource()
}

// statsInputSize is what a stats task reads: the fields its sub job indexes
// when the schema is known, else the whole segment. Text match reads the fields
// with enable_match, json key stats the JSON fields with the stats index
// enabled, bm25 the sparse output field of each BM25 function (the worker's
// index/task_stats.go loops over exactly these). A sub job that targets no
// specific field, or a schema that names none, is priced on the whole segment.
func statsInputSize(segment *SegmentInfo, schema *schemapb.CollectionSchema, subJob indexpb.StatsSubJob) int64 {
	fields := statsTargetFields(schema, subJob)
	if len(fields) == 0 {
		return estimateSegmentSize(segment, schema)
	}
	var size int64
	for _, fieldID := range fields {
		size += estimateFieldSize(segment, schema, fieldID)
	}
	return size
}

// statsTargetFields lists the field IDs a stats sub job reads, or nil when
// the sub job reads the whole segment.
func statsTargetFields(schema *schemapb.CollectionSchema, subJob indexpb.StatsSubJob) []int64 {
	if schema == nil {
		return nil
	}
	var fields []int64
	switch subJob {
	case indexpb.StatsSubJob_TextIndexJob:
		for _, field := range schema.GetFields() {
			if typeutil.CreateFieldSchemaHelper(field).EnableMatch() {
				fields = append(fields, field.GetFieldID())
			}
		}
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		for _, field := range schema.GetFields() {
			if typeutil.CreateFieldSchemaHelper(field).EnableJSONKeyStatsIndex() {
				fields = append(fields, field.GetFieldID())
			}
		}
	case indexpb.StatsSubJob_BM25Job:
		for _, fn := range schema.GetFunctions() {
			if fn.GetType() == schemapb.FunctionType_BM25 {
				fields = append(fields, fn.GetOutputFieldIds()...)
			}
		}
	}
	return fields
}

// estimateSegmentSize is getSegmentSize with a fallback for segments whose
// Stats were never persisted (external-collection segments): rows times the
// schema's per-record estimate.
func estimateSegmentSize(segment *SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	if size := segment.getSegmentSize(); size > 0 {
		return size
	}
	if schema == nil || segment.GetNumOfRows() <= 0 {
		return 0
	}
	perRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		mlog.Warn(context.TODO(), "estimate segment size from schema failed",
			mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
		return 0
	}
	size := segment.GetNumOfRows() * int64(perRecord)
	mlog.Warn(context.TODO(), "segment has no size statistics, estimated from schema",
		mlog.FieldSegmentID(segment.GetID()), mlog.Int64("rows", segment.GetNumOfRows()), mlog.Int64("estimatedSize", size))
	return size
}

// estimateFieldSize returns the bytes of one field in a segment: the smaller
// of the schema's bound and the bytes of the binlogs that hold the field
// (taskcommon.EstimateFieldSize). The scalar index task slot and the index and
// stats memory estimates are all derived from this one size.
//
// In storage v2/v3 a binlog holds a whole column group, so the container bound
// is the group. V3 segments do not persist those binlogs (kv_catalog: paths
// live in the LOON manifest), so after a DataCoord restart the group is
// unknown; a variable-width field is then bounded by what the segment holds
// beyond its fixed-width fields instead. A fixed-width field needs no
// container: its schema size is exact.
func estimateFieldSize(segment *SegmentInfo, schema *schemapb.CollectionSchema, fieldID int64) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	group := taskcommon.ColumnGroupSize(segment.GetBinlogs(), fieldID)
	field := typeutil.GetFieldByID(schema, fieldID)
	if field == nil {
		// Unknown field: be conservative, its group or the whole segment.
		if group > 0 {
			return group
		}
		return estimateSegmentSize(segment, schema)
	}
	rows := segment.GetNumOfRows()
	container := group
	if _, exact, _ := taskcommon.SchemaFieldSize(field, rows); container <= 0 && !exact {
		container = variableWidthResidual(segment, schema)
	}
	if size := taskcommon.EstimateFieldSize(field, rows, container); size > 0 {
		return size
	}
	// Neither bound is known (an unbounded type in a segment without size
	// statistics, i.e. an external collection): the schema's per-row estimate.
	if size := rows * fieldBytesPerRow(field); size > 0 {
		mlog.Warn(context.TODO(), "field has neither a schema bound nor a known container, estimated per row",
			mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID), mlog.Int64("estimatedSize", size))
		return size
	}
	return estimateSegmentSize(segment, schema)
}

// variableWidthResidual is an upper bound on the bytes of the segment's
// variable-width fields together: its insert size minus every fixed-width
// field and the system fields. 0 when the insert size is unknown or the
// statistics are inconsistent.
func variableWidthResidual(segment *SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	residual := segment.EnsureStats().GetInsertBinlogSize()
	if residual <= 0 {
		return 0
	}
	rows := segment.GetNumOfRows()
	hasSystemFields := false
	for _, f := range typeutil.GetAllFieldSchemas(schema) {
		if common.IsSystemField(f.GetFieldID()) {
			hasSystemFields = true
		}
		if size, exact, _ := taskcommon.SchemaFieldSize(f, rows); exact {
			residual -= size
		}
	}
	if !hasSystemFields {
		residual -= rows * taskcommon.SystemFieldsBytesPerRow
	}
	return max(residual, 0)
}

// vectorFieldBytes is the exact size of rows dense vectors, or 0 when the
// field is not a dense vector or its dim is unknown.
func vectorFieldBytes(field *schemapb.FieldSchema, rows int64) int64 {
	if !typeutil.IsVectorType(field.GetDataType()) || typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		return 0
	}
	return rows * taskcommon.FixedFieldWidth(field)
}

// fieldBytesPerRow reuses EstimateSizePerRecord on a one-field schema, the
// estimator the rest of DataCoord uses, for a field no bound applies to.
func fieldBytesPerRow(field *schemapb.FieldSchema) int64 {
	n, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	if err != nil {
		return 0
	}
	return int64(n)
}

// resourceCache memoizes a task's requirement so what the scheduler placed and
// what the request ships are the same number, and so the meta walk runs once
// per task rather than once per scheduling round. A computation that could
// not resolve its inputs returns ok=false and is NOT cached, so the next round
// retries instead of freezing a placeholder.
type resourceCache struct {
	value atomic.Pointer[taskcommon.Resource]
}

func (c *resourceCache) get(compute func() (taskcommon.Resource, bool)) taskcommon.Resource {
	if v := c.value.Load(); v != nil {
		return *v
	}
	res, ok := compute()
	if ok {
		c.value.Store(&res)
	}
	return res
}
