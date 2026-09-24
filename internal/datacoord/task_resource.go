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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
// Every answer is placeable, including the fallbacks: the scheduler places a
// task on whatever it gets here and never waits for a better number. So a
// fallback errs towards refusing a worker, never towards a worker accepting
// more than it can hold. A task whose inputs are still resolving is priced at
// an upper bound on itself; only one whose inputs are gone for good is priced
// at the floor, and that task is on its way to being retired.
//
// Every memory formula mirrors what the worker actually holds for that family
// (the comment next to each formula names the worker-side code it mirrors),
// and errs on the high side where the worker's behavior depends on data or on
// a machine DataCoord does not see. The worker books exactly this estimate.
//
// Three families size a buffer as a share of the worker's machine
// (clustering compaction, analyze, import). DataCoord cannot see the machine,
// so their estimates are bounded by their input only and can exceed what the
// worker will actually hold.

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

// defaultTaskResource is the answer when a task's inputs are gone for good: a
// segment dropped between enqueue and dispatch, a job no longer in meta. Such
// a task is retired by CreateTaskOnWorker, and the scheduler must reach that
// code to retire it, so the floor is the right answer -- it fits every worker.
//
// It is NOT the answer for inputs that are merely still resolving (a schema
// not cached yet after a restart). Those are priced at an upper bound on the
// task instead, because the task is real, will run, and books on the worker
// exactly what it was placed on. See indexBuildTask.GetTaskResource.
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

// fmIndexTaskResource: an FM-index build's peak is not a multiple of its field.
// It holds the text, its suffix array and the sampled-SA structures at the same
// time, which estimateFMIndexBuildPeakBytes models allocation by allocation, and
// that peak is several times the field. The scalar slot of the same build is
// already derived from this peak (fmIndexBuildTaskSlots), so the memory estimate
// uses it too rather than the generic index factor, which would under-price it.
func fmIndexTaskResource(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(estimateFMIndexBuildPeakBytes(fieldSize, numRows, indexParams)),
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
// holds more than its input. The input is the estimate; the machine share is
// not applied, so for an input larger than that share this is an upper bound.
func clusteringCompactionTaskResource(inputSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceClusteringCompactionCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(inputSize),
	}
}

// analyzeTaskResource: the worker (index/task_analyze.go) trains on the raw
// vectors, so raw bytes times the factor. The worker down-samples to
// maxTrainSizeRatio of its machine when they exceed it; that machine cap is not
// applied here.
func analyzeTaskResource(rawDataSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceAnalyzeCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(scaled(rawDataSize, Params.DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat())),
	}
}

// importTaskResource: the worker (importv2/task_import.go) submits every file
// of the task to its exec pool at once and each file allocates one read buffer,
// so the task holds the buffers of all its files (importBufferedBytes); the
// factor covers the batch being serialized and uploaded while the next one is
// read.
//
// The worker has one more bound this estimate cannot express: its memory
// allocator keeps all import buffers on the machine under
// dataNode.import.memoryLimitPercentage of it, so an import never holds more
// than that share however many files it has. DataCoord does not see the
// machine, so a task with very many files is still priced above what the
// worker will use.
func importTaskResource(bufferedBytes int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(bufferedBytes, Params.DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat())),
	}
}

// preImportTaskResource: the worker (importv2/task_preimport.go) reads every
// file in parallel with one base buffer each and keeps nothing: no in-flight
// sync, no allocator cap.
func preImportTaskResource(bufferedBytes int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(bufferedBytes),
	}
}

// importBufferedBytes is what the read buffers of one import task hold: one
// buffer per file, each bounded by the file it reads, because a buffer never
// fills beyond its file. A task with no files listed yet, or a file whose size
// is not known, is charged a whole buffer.
func importBufferedBytes(fileStats []*datapb.ImportFileStats, perFileBuffer int64) int64 {
	if len(fileStats) == 0 {
		return perFileBuffer
	}
	var buffered int64
	for _, stat := range fileStats {
		size := stat.GetTotalMemorySize()
		if size <= 0 || size > perFileBuffer {
			size = perFileBuffer
		}
		buffered += size
	}
	return buffered
}

// importFileBufferSize mirrors importv2.ImportTask.GetBufferSize on the worker:
// the base buffer per (vchannel, partition) pair; an L0 import uses the base
// buffer as is (importv2.L0ImportTask.GetBufferSize).
//
// It is deliberately NOT capped at the largest file. GetBufferSize reads a
// largest-file cap from the task's own ImportTaskV2.FileStats, but the worker
// never fills that field for an import task (NewImportTask leaves it nil), so
// the cap never fires there and capping here would under-price every import.
// The worker's remaining clamp, a percentage of its machine, is not applied.
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
	// Neither bound is known: a text or geometry field (neither has a limit the
	// write path enforces) in a segment without size statistics, i.e. an
	// external collection. The whole segment is the only bound left.
	//
	// The per-row estimate that used to sit here is deliberately gone. It came
	// from EstimateSizePerRecord, whose value for these types is the dynamic
	// field AVERAGE, not a bound. Pricing a task on an average under-prices
	// every field above it, and the worker books that number while reading the
	// real column, so the memory filter admits a task the node cannot hold.
	mlog.Warn(context.TODO(), "field has neither a schema bound nor a known container, pricing on the whole segment",
		mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID))
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

// resourceCache memoizes a task's requirement so what the scheduler placed and
// what the request ships are the same number, and so the meta walk runs once
// per task rather than once per scheduling round. A computation that could
// not resolve its inputs returns ok=false and is NOT cached, so the next round
// retries instead of freezing a placeholder. ok is passed on to the caller:
// the scheduler must not place a task on a price that did not resolve, or a
// worker ends up booking far more than the floor it was placed on.
type resourceCache struct {
	value atomic.Pointer[taskcommon.Resource]
}

func (c *resourceCache) get(compute func() (taskcommon.Resource, bool)) (taskcommon.Resource, bool) {
	if v := c.value.Load(); v != nil {
		return *v, true
	}
	res, ok := compute()
	if ok {
		c.value.Store(&res)
	}
	return res, ok
}
