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
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func DefaultCPU() int64 {
	return max(paramtable.Get().DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64(), 1)
}

// ClampMemory applies the configured floor.
func ClampMemory(memory int64) int64 {
	return max(memory, paramtable.Get().DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize())
}

func scaled(size int64, factor float64) int64 {
	return int64(float64(size) * factor)
}

// Default is the answer when a task's inputs are gone for good: a
// segment dropped between enqueue and dispatch, a job no longer in meta. Such
// a task is retired by CreateTaskOnWorker, and the scheduler must reach that
// code to retire it, so the floor is the right answer -- it fits every worker.
//
// It is NOT the answer for inputs that are merely still resolving (a schema
// not cached yet after a restart). Those are priced at an upper bound on the
// task instead, because the task is real, will run, and books on the worker
// exactly what it was placed on. See index.Task.GetTaskResource.
func Default() taskcommon.Resource {
	return taskcommon.Resource{CPU: DefaultCPU(), Memory: ClampMemory(0)}
}

// Index: the worker (index/task_index.go) loads the whole indexed
// field through cgo and builds the index beside it, so the field's bytes times
// the factor. DiskANN builds against a DRAM budget equal to the worker's free
// memory, so it is not priced lower than an in-memory build.
func Index(fieldSize int64, isVectorIndex bool) taskcommon.Resource {
	cpu := DefaultCPU()
	if isVectorIndex {
		cpu = max(paramtable.Get().DataCoordCfg.TaskResourceVectorIndexCPU.GetAsInt64(), 1)
	}
	return taskcommon.Resource{
		CPU:    cpu,
		Memory: ClampMemory(scaled(fieldSize, paramtable.Get().DataCoordCfg.TaskResourceIndexMemoryFactor.GetAsFloat())),
	}
}

// FMIndex prices an FM-index build on its modeled peak. The build's peak is
// not a multiple of its field: it holds the text, its suffix array and the
// sampled-SA structures at the same time, which index.EstimateFMIndexBuildPeakBytes
// models allocation by allocation, and that peak is several times the field.
// The scalar slot of the same build is already derived from this peak, so the
// memory estimate uses it too rather than the generic index factor, which would
// under-price it. The peak is computed by the caller because the model lives
// in package index, which depends on this one.
func FMIndex(peakBytes int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(peakBytes),
	}
}

// Stats prices a stats task by the bytes it reads: the fields it
// indexes (StatsInputSize) for text-match / json-key / bm25, and the whole
// segment for a sort compaction, which holds every input record in memory
// until the sort is done (storage.Sort).
func Stats(inputSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(scaled(inputSize, paramtable.Get().DataCoordCfg.TaskResourceStatsMemoryFactor.GetAsFloat())),
	}
}

// SortCompaction prices a sort compaction by the segment it sorts:
// the worker runs it as a stats sub job (StatsSubJob_Sort) that holds every
// input record in memory until the sort is done (storage.Sort), so it is the
// stats formula applied to the whole segment. Sharing Stats means
// dataCoord.taskResource.statsMemoryFactor governs sort compaction too; this
// helper exists so that is visible at the call site instead of implied.
func SortCompaction(segmentSize int64) taskcommon.Resource {
	return Stats(segmentSize)
}

// MixCompaction: a mix (or schema bump) compaction streams its
// input through MultiSegmentWriter, which flushes at binlogMaxSize, so it never
// holds more than its input and never needs more than one output segment of
// segment.maxSize. min of the two; an unknown input size gets the upper bound.
//
// The delete maps are not streamed and come on top: each input's deltalogs
// are loaded into a pk -> ts map (compaction.ComposeDeleteFromDeltalogs) that
// lives while that input is read, and on the merge-sort path the maps of all
// inputs live until the merge ends. deleteMapBytes is what the caller worked
// out the worker holds at once (DeleteMapBytes per input, summed or maxed).
func MixCompaction(inputSize, deleteMapBytes int64) taskcommon.Resource {
	bound := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	memory := bound
	if inputSize > 0 {
		memory = min(inputSize, bound)
	}
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(memory + max(deleteMapBytes, 0)),
	}
}

// deleteMapEntryBytes is the in-memory cost of one entry of a compaction's
// pk -> ts delete map (map[any]Timestamp): the interface key, the boxed int64
// it points to, the timestamp value and the map's bucket overhead. A VarChar
// key additionally holds its string bytes, which DeleteMapBytes adds from the
// deltalog size.
const deleteMapEntryBytes = 64

// DeleteMapBytes estimates the delete map a compaction builds from one
// segment's deltalogs: its delete row count times deleteMapEntryBytes, plus
// the deltalog bytes for a VarChar primary key, whose strings the map keeps.
func DeleteMapBytes(segment *meta.SegmentInfo, pkType schemapb.DataType) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	stats := segment.EnsureStats()
	size := stats.GetDeleteNumRows() * deleteMapEntryBytes
	if pkType == schemapb.DataType_VarChar {
		size += stats.GetDeltaBinlogSize()
	}
	return max(size, 0)
}

// L0Compaction: the worker (compactor/l0_compactor.go) loads every
// input delta log into memory before applying it batch by batch (deltaSize,
// times the factor), and loads the PK stats logs of every target segment in
// full to build their bloom filters (loadBF; statsSize, held as is).
func L0Compaction(deltaSize, statsSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(scaled(deltaSize, paramtable.Get().DataCoordCfg.TaskResourceL0CompactionMemoryFactor.GetAsFloat()) + statsSize),
	}
}

// ClusteringCompaction: the worker (compactor/clustering_compactor.go)
// buckets its input in memory and flushes buckets once the buffer reaches
// dataNode.clusteringCompaction.memoryBufferRatio of the machine, so it never
// holds more than its input. The input is the estimate; the machine share is
// not applied, so for an input larger than that share this is an upper bound.
func ClusteringCompaction(inputSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(paramtable.Get().DataCoordCfg.TaskResourceClusteringCompactionCPU.GetAsInt64(), 1),
		Memory: ClampMemory(inputSize),
	}
}

// Analyze: the worker (index/task_analyze.go) trains on the raw
// vectors of the clustering-key field, so that field's size times the factor. The worker down-samples to
// maxTrainSizeRatio of its machine when they exceed it; that machine cap is not
// applied here.
func Analyze(rawDataSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(paramtable.Get().DataCoordCfg.TaskResourceAnalyzeCPU.GetAsInt64(), 1),
		Memory: ClampMemory(scaled(rawDataSize, paramtable.Get().DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat())),
	}
}

// Import: the worker (importv2/task_import.go) submits every file
// of the task to its exec pool at once and each file allocates one read buffer,
// so the task holds the buffers of all its files (ImportBufferedBytes); the
// factor covers the batch being serialized and uploaded while the next one is
// read, so it applies to rowBytes only. fixedBytes are reservations that do
// not grow while a batch is in flight, such as the delete budgets of a
// snapshot-source task, and are added as they are.
//
// The worker bounds this task further: every import reader reserves from one
// allocator that keeps all import buffers on the machine under
// dataNode.import.memoryLimitPercentage of it. That bound depends on the node,
// so it is not applied here but at placement (ImportTaskNodeMemoryShare).
func Import(rowBytes, fixedBytes int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(scaled(rowBytes, paramtable.Get().DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat()) + max(fixedBytes, 0)),
	}
}

// PreImport: the worker (importv2/task_preimport.go) reads every
// file in parallel with one base buffer each and keeps nothing, so there is no
// in-flight sync to cover. A snapshot-source pre-import reserves each reader
// from the import allocator (reserveSnapshotRead), so ImportNodeMemoryShare
// bounds it too; a plain pre-import does not go through the allocator.
func PreImport(bufferedBytes int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(bufferedBytes),
	}
}

// ImportNodeMemoryShare is the fraction of a worker's memory that the import
// allocator lets all import readers on it hold together
// (dataNode.import.memoryLimitPercentage, which its formatter keeps in
// (0, 100]). A task whose readers all reserve from that allocator can never
// hold more than this share of the node it runs on, whatever its estimate says.
func ImportNodeMemoryShare() float64 {
	return paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.GetAsFloat() / 100
}

// ImportTaskNodeMemoryShare bounds an import task on a node. The allocator
// bounds only the read buffers (ImportNodeMemoryShare); every batch read is
// also hashed into a copy that waits for an asynchronous sync while the next
// batch is read, and those copies are not reserved from the allocator. Their
// only bound is the sync dispatcher's node-wide pending-task semaphore, which
// cannot be attributed to one task, so the same importMemoryFactor that the
// estimate applies to the buffers covers them here too. This is an estimate,
// not a hard bound: a worker whose sync backlog holds more shows it in its
// resident memory, which bounds the available memory it reports.
func ImportTaskNodeMemoryShare() float64 {
	return ImportNodeMemoryShare() * paramtable.Get().DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat()
}

// ImportBufferedBytes is what the read buffers of one import task hold: one
// buffer per file, each bounded by the file it reads, because a buffer never
// fills beyond its file. A task with no files listed yet, or a file whose size
// is not known, is charged a whole buffer.
func ImportBufferedBytes(fileStats []*datapb.ImportFileStats, perFileBuffer int64) int64 {
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

// ImportV3 prices a reshard or an import v3 merge on the working
// set the reshardmem model computes for it (reshardWorkingSet /
// importV3WorkingSet). The model is shared with the worker's own budgeting and
// already carries its expansion factor, so no further factor is applied: the
// worker books exactly what the model says it holds.
func ImportV3(workingSet int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(workingSet),
	}
}

// Lightweight prices copy-segment tasks, which stream data and hold
// little of it.
func Lightweight() taskcommon.Resource {
	return Default()
}

// ExternalRefresh prices a refresh-external-collection task at a fixed
// dataCoord.taskResource.externalRefreshMemory. The worker
// (datanode/external) explores the external source and reads its file
// metadata and the collection's current segments; how much of that it holds
// depends on the source, which DataCoord does not see, so the price is a
// configured budget rather than a formula.
func ExternalRefresh() taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(paramtable.Get().DataCoordCfg.TaskResourceExternalRefreshMemory.GetAsSize()),
	}
}

// StatsInputSize is what a stats task reads: the fields its sub job indexes
// when the schema is known, else the whole segment. Text match reads the fields
// with enable_match, json key stats the JSON fields with the stats index
// enabled, bm25 the sparse output field of each BM25 function (the worker's
// index/task_stats.go loops over exactly these). A sub job that targets no
// specific field, or a schema that names none, is priced on the whole segment.
func StatsInputSize(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema, subJob indexpb.StatsSubJob) int64 {
	fields := StatsTargetFields(schema, subJob)
	if len(fields) == 0 {
		return EstimateSegmentSize(segment, schema)
	}
	var size int64
	for _, fieldID := range fields {
		size += EstimateFieldSize(segment, schema, fieldID)
	}
	return size
}

// StatsTargetFields lists the field IDs a stats sub job reads, or nil when
// the sub job reads the whole segment.
func StatsTargetFields(schema *schemapb.CollectionSchema, subJob indexpb.StatsSubJob) []int64 {
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

// EstimateSegmentSize is getSegmentSize with a fallback for segments whose
// Stats were never persisted (external-collection segments): rows times the
// schema's per-record estimate.
func EstimateSegmentSize(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	if size := segment.GetSegmentSize(); size > 0 {
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

// readWindowBytes is how much of a column group the storage v2 file reader
// decodes at once: DEFAULT_FIELD_MAX_MEMORY_LIMIT, the window_bytes default of
// VisitFieldDataFromStorageV2Paged in internal/core/src/storage/Util.cpp. A
// row group larger than the window is still read whole; this does not model
// that case.
const readWindowBytes = int64(128) << 20

// decodesWholeColumnGroup reports whether a worker reading one field of the
// segment decodes the field's whole column group. A storage v2 segment
// without a manifest is read through its insert files, and that reader does
// not project columns. A manifest reader (storage v2 with a manifest, and
// storage v3) projects the one column, and storage v1 records each field in
// its own binlog.
func decodesWholeColumnGroup(segment *meta.SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV2 && segment.GetManifestPath() == ""
}

// EstimateFieldSize returns the bytes of one field in a segment. It is the one
// field-size rule every caller uses: the index and stats memory estimates, and
// the scalar index task slot derived from the same size.
//
//  1. The size meta recorded for the field (recordedFieldSize: its own
//     FieldBinlog in storage v1, its column group in v2, and the column group
//     in Stats.LoadResource for a v3 manifest segment). What the writer
//     measured beats any bound, so this answers whenever it is positive. A
//     fixed-width field is capped at its exact width, plus one read window
//     where the worker decodes the whole column group
//     (decodesWholeColumnGroup).
//  2. Otherwise the schema's upper bound (taskcommon.EstimateFieldSize). This is
//     the storage v3 case after a DataCoord restart, whose per-field binlogs are
//     not persisted in meta.
//  3. Otherwise -- no schema, a field the schema does not know, or a type the
//     write path does not bound (text, geometry) -- the whole segment.
//
// Steps 2 and 3 are both capped at the segment: one field can never hold more
// than the segment it is part of. The cap is not decoration. A schema bound is
// rows x the per-row limit, and for a varchar(65535) or a JSON field (64KB per
// value) that is orders of magnitude above the real data; a task priced there
// exceeds every worker, is dispatched as oversized, and the worker then books
// the whole of it and reports no free memory until the task ends. A few such
// tasks take every worker out of memory-aware placement at once.
//
// The rule does not look at whether the collection is external. External
// segments differ only in what meta holds for them, and that is exactly what
// the three steps key on.
func EstimateFieldSize(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema, fieldID int64) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	field := typeutil.GetFieldByID(schema, fieldID)
	if recorded := segment.RecordedFieldSize(fieldID); recorded > 0 {
		// In storage v2/v3 the recorded size is the whole column group. For a
		// fixed-width type the schema bound is that column's exact size, so it
		// caps the group: an int64 sharing a group with a wide varchar is
		// priced on its 8 bytes a row, not on the varchar. A variable-width
		// bound is only a limit, never tighter than a measurement, so it does
		// not apply.
		//
		// The cap assumes the worker decodes only that column. A reader that
		// opens the segment through its manifest does (it projects the one
		// column), but a storage v2 segment without a manifest is read file by
		// file, and that reader decodes every column of the group in windows
		// of up to readWindowBytes before it takes the one column out
		// (GetFieldDatasFromStorageV2). For such a segment the cap includes
		// one window. recorded is the group's decoded size, so a small group
		// is still priced on what it holds.
		if bound, exact, ok := taskcommon.SchemaFieldSize(field, segment.GetNumOfRows()); ok && exact && bound > 0 {
			if decodesWholeColumnGroup(segment) {
				bound += readWindowBytes
			}
			return min(recorded, bound)
		}
		return recorded
	}
	segmentSize := EstimateSegmentSize(segment, schema)
	if field != nil {
		if bound := taskcommon.EstimateFieldSize(field, segment.GetNumOfRows(), 0); bound > 0 {
			if segmentSize > 0 {
				return min(bound, segmentSize)
			}
			return bound
		}
	}
	mlog.Warn(context.TODO(), "field has no binlog size in meta and no schema bound, pricing on the whole segment",
		mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID))
	return segmentSize
}

// Cache memoizes a task's requirement so what the scheduler placed and
// what the request ships are the same number, and so the meta walk runs once
// per task rather than once per scheduling round.
//
// ok controls caching and nothing else. A computation that could not resolve
// its inputs returns ok=false and is NOT cached, so the next round retries
// instead of freezing a placeholder. The scheduler places the task on that
// answer regardless: a still-resolving task answers an upper bound on itself,
// which errs towards refusing a worker, and one whose inputs are gone for good
// answers the floor so it can reach CreateTaskOnWorker, the only code that
// retires it. Waiting for an exact price instead would strand the second kind
// forever.
type Cache struct {
	value atomic.Pointer[taskcommon.Resource]
}

func (c *Cache) Get(compute func() (taskcommon.Resource, bool)) (taskcommon.Resource, bool) {
	if v := c.value.Load(); v != nil {
		return *v, true
	}
	res, ok := compute()
	if ok {
		c.value.Store(&res)
	}
	return res, ok
}
