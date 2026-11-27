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
	"math"
	"slices"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/fieldsize"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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
// Every answer is placeable, including fallbacks for unresolved metadata.
// These are scheduling estimates, not physical memory limits. The formulas
// name the worker allocations they model; expansion factors and the minimum
// charge provide headroom. In particular, missing segment statistics use
// average row widths, and streaming readers can exceed their nominal buffers
// when decoding a large row group or TEXT LOB values. The worker books the
// estimate against its task capacity.
//
// Clustering compaction and analyze use input-based estimates even though
// their worker buffers depend on machine memory. Allocator-backed import
// tasks additionally expose NodeMemoryBounded: the picker caps their estimate
// for the selected worker before sending it unchanged to the worker.

func DefaultCPU() int64 {
	return max(paramtable.Get().DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64(), 1)
}

// ClampMemory charges at least one byte, including when the configured floor is zero.
func ClampMemory(memory int64) int64 {
	return max(memory, paramtable.Get().DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize(), 1)
}

func scaled(size int64, factor float64) int64 {
	value := float64(size) * factor
	if value >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(value)
}

// Default is the answer when a task's inputs are gone for good: a
// segment dropped between enqueue and dispatch, a job no longer in meta. Such
// a task has no real worker demand. Index tasks retire on the coordinator
// before placement; other families can retire through CreateTaskOnWorker. The
// floor remains subject to memory admission and need not fit a full worker.
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

// MixCompaction budgets the streaming writer at min(input, segment.maxSize).
// mergeSplit reads one segment at a time. MergeSort instead keeps a reader
// window for every input alive beside the writer; readerBytes sums those
// windows and must not be capped at one output segment.
//
// The delete maps are not streamed and come on top: each input's deltalogs
// are loaded into a pk -> ts map (compaction.ComposeDeleteFromDeltalogs) that
// lives while that input is read, and on the merge-sort path the maps of all
// inputs live until the merge ends. deleteMapBytes is what the caller worked
// out the worker holds at once (DeleteMapBytes per input, summed or maxed).
func MixCompaction(inputSize, readerBytes, deleteMapBytes int64) taskcommon.Resource {
	bound := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	memory := bound
	if inputSize > 0 {
		memory = min(inputSize, bound)
	}
	return taskcommon.Resource{
		CPU:    DefaultCPU(),
		Memory: ClampMemory(memory + readerBytes + deleteMapBytes),
	}
}

// CompactionReaderMemory estimates one live reader. Packed readers stream;
// price their working buffers, not two copies of the entire input segment.
// Use DefaultReadBufferSize as the nominal streaming budget. V2 passes it
// to PackedRecordBatchReader. V3 ignores both the Go bufferSize and the C FFI
// stream's batch argument; its backend reader.record_batch_max_size default
// currently matches this budget. The factor of two allows decoding and
// materialization beside the reader. Neither ignored FFI argument is a cap.
// This is a scheduling estimate, NOT a physical memory bound: V2 can read a
// row group larger than its buffer, V3 slices may retain underlying chunks,
// variable-width rows may be skewed, and TEXT LOB decoding adds allocations.
func CompactionReaderMemory(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	size := EstimateSegmentSize(segment, schema)
	if segment.GetStorageVersion() != storage.StorageV1 || segment.GetManifestPath() != "" {
		window := int64(packed.DefaultReadBufferSize)
		if size > 0 {
			window = min(window, size)
		}
		return 2 * window
	}
	// V1 retains the encoded binlog while decoding it, one chunk per field.
	fallback := 2 * size
	if len(segment.GetBinlogs()) == 0 {
		return fallback
	}
	var memory int64
	for _, field := range segment.GetBinlogs() {
		if len(field.GetBinlogs()) == 0 {
			return fallback
		}
		var window int64
		for _, binlog := range field.GetBinlogs() {
			if binlog.GetMemorySize() <= 0 || binlog.GetLogSize() <= 0 {
				return fallback
			}
			window = max(window, binlog.GetMemorySize()+binlog.GetLogSize())
		}
		memory += window
	}
	return memory
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
	stats := segment.EnsureStats()
	size := stats.GetDeleteNumRows() * deleteMapEntryBytes
	if pkType == schemapb.DataType_VarChar {
		size += stats.GetDeltaBinlogSize()
	}
	return max(size, 0)
}

// L0Compaction: the worker (compactor/l0_compactor.go) loads every
// input delta log into memory before applying it batch by batch (deltaSize,
// times the factor), and loads the target segments' PK stats logs in batches
// to build their bloom filters (loadBF). statsSize sums all targets, so it is
// conservative across batches.
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
		Memory: ClampMemory(scaled(rowBytes, paramtable.Get().DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat()) + fixedBytes),
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
// estimate applies to the buffers covers them here too. A larger sync backlog
// can use more memory than the estimate.
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
	remaining := make(map[int64]struct{}, len(fields))
	for _, fieldID := range fields {
		remaining[fieldID] = struct{}{}
	}
	known := meta.SegmentFieldLogicalSizes(segment.SegmentInfo)
	var measured []*datapb.FieldLogicalSize
	var size int64
	for fieldID := range remaining {
		if field, ok := known.GetFields()[fieldID]; ok {
			measured = append(measured, field)
			delete(remaining, fieldID)
		} else if typeutil.GetFieldByID(schema, fieldID).GetDataType() == schemapb.DataType_Text {
			size = meta.SaturatingAdd(size, EstimateFieldSize(segment, schema, fieldID))
			delete(remaining, fieldID)
		}
	}
	size = meta.SaturatingAdd(size, logicalSizeBytes(measured...))
	// Logical totals and unknown TEXT estimates can include external LOB bodies,
	// so encoded groups must not cap them. Other unknown fields retain the group
	// estimate, which may include encoded bytes of fields already priced. Parallel
	// readers and index allocations remain covered by the stats factor; these
	// input estimates are not a hard bound on peak RSS.
	chargeGroup := func(groupID int64, members []int64, groupSize int64) {
		if groupSize <= 0 {
			return
		}
		var requestedSize int64
		for fieldID := range remaining {
			if fieldID == groupID || slices.Contains(members, fieldID) {
				requestedSize += EstimateFieldSize(segment, schema, fieldID)
				delete(remaining, fieldID)
			}
		}
		size = meta.SaturatingAdd(size, min(requestedSize, groupSize))
	}
	for _, group := range segment.GetBinlogs() {
		var groupSize int64
		for _, binlog := range group.GetBinlogs() {
			groupSize += binlog.GetMemorySize()
		}
		chargeGroup(group.GetFieldID(), group.GetChildFields(), groupSize)
	}
	// Manifest segments retain group statistics across coordinator restarts.
	for _, group := range segment.EnsureStats().GetLoadResource().GetColumnGroups() {
		chargeGroup(group.GetGroupId(), group.GetFieldIds(), group.GetMemorySize())
	}
	for fieldID := range remaining {
		size = meta.SaturatingAdd(size, EstimateFieldSize(segment, schema, fieldID))
	}
	return size
}

// StatsTargetFields lists the field IDs a stats sub job reads, or nil when
// the sub job reads the whole segment.
func StatsTargetFields(schema *schemapb.CollectionSchema, subJob indexpb.StatsSubJob) []int64 {
	var fields []int64
	switch subJob {
	case indexpb.StatsSubJob_TextIndexJob:
		for _, field := range schema.GetFields() {
			if typeutil.IsTextMatchEnabled(field) {
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
// schema's average per-record estimate. This is an intentional trade-off within
// the conservative task formulas (expansion factors and memory floor): schema
// maxima can serialize work through the oversized path. It is not a strict
// bound on every variable-width value. EstimateFieldSize uses it as a cap too.
// EstimateMaxSizePerRecord still uses heuristics for text, JSON, arrays,
// geometry and sparse vectors; actual field sizes remain a follow-up.
func EstimateSegmentSize(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	if size := segment.GetSegmentSize(); size > 0 {
		return size
	}
	if schema == nil || segment.GetNumOfRows() <= 0 {
		return 0
	}
	perRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		mlog.RatedWarn(context.TODO(), 1, "estimate segment size from schema failed",
			mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
		return 0
	}
	size := segment.GetNumOfRows() * int64(perRecord)
	mlog.RatedWarn(context.TODO(), 1, "segment has no size statistics, estimated from schema",
		mlog.FieldSegmentID(segment.GetID()), mlog.Int64("rows", segment.GetNumOfRows()), mlog.Int64("estimatedSize", size))
	return size
}

// decodesWholeColumnGroup reports whether a worker reading one field of the
// segment decodes the field's whole column group. A storage v2 segment
// without a manifest is read through its insert files, and that reader does
// not project columns. A manifest reader (storage v2 with a manifest, and
// storage v3) projects the one column, and storage v1 records each field in
// its own binlog.
func decodesWholeColumnGroup(segment *meta.SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV2 && segment.GetManifestPath() == ""
}

// logicalSizeBytes combines validated field totals before rounding validity
// bits. Counters are uint64 on the wire; task budgets saturate at MaxInt64.
func logicalSizeBytes(fields ...*datapb.FieldLogicalSize) int64 {
	var size, remainingBits int64
	for _, field := range fields {
		size = meta.SaturatingAdd(size,
			int64(min(field.PayloadBytes, uint64(math.MaxInt64))), int64(field.ValidityBits/8))
		remainingBits += int64(field.ValidityBits % 8)
	}
	return meta.SaturatingAdd(size, meta.CeilDiv(remainingBits, 8))
}

// EstimateFieldSize estimates field input bytes for index and stats task
// memory. Legacy scalar slots use the inspectors' estimates based on
// meta.EstimateFieldsReadSize and meta.SupportsFieldProjection instead. Those
// paths share fixed-width helpers, but not the full estimation policy.
//
//  1. A validated V3 logical-size entry, including a known zero. It covers the
//     current manifest and row count, and includes TEXT bodies stored in LOBs.
//     Logical bytes are not capped by encoded column-group or segment sizes.
//  2. Unknown TEXT uses the larger of its recorded size and rows times
//     textFallbackBytesPerRow (64 KiB by default). This is an assumed average
//     width, not a schema limit; TEXT writes do not enforce max_length.
//     Encoded group and segment sizes do not cap this estimate either.
//  3. Otherwise the size meta recorded for the field (recordedFieldSize: its own
//     FieldBinlog in storage v1, its column group in v2, and the column group
//     in Stats.LoadResource for a v3 manifest segment). VarChar is capped at
//     its schema upper bound so a shared group's other fields do not inflate
//     its price. Non-nullable fixed-width fields are capped at their exact width
//     when the reader projects them; StorageV2 file reads retain the whole-group
//     size for those fields. The VarChar cap estimates field input, not peak
//     memory while a V2 file reader decodes the whole group.
//  4. Otherwise the schema's upper bound (fieldsize.EstimateFieldSize). This is
//     the storage v3 case after a DataCoord restart, whose per-field binlogs are
//     not persisted in meta.
//  5. Otherwise -- no schema, a field the schema does not know, or another type
//     the write path does not bound (geometry) -- the whole segment.
//
// Steps 4 and 5 retain the segment cap as a fallback heuristic. A schema bound is
// rows x the per-row limit, and for a varchar(65535) or a JSON field (64KB per
// value) that is orders of magnitude above the real data; a task priced there
// exceeds every worker, is dispatched as oversized, and the worker then books
// the whole of it and reports no free memory until the task ends. A few such
// tasks take every worker out of memory-aware placement at once.
// If the segment has no recorded size, the cap is itself a heuristic; see
// EstimateSegmentSize for that fallback's accepted estimation trade-off.
// The TEXT fallback can still under-price a field whose actual average width
// exceeds the configured assumption. Resolving sizes performs no storage I/O.
//
// The rule does not look at whether the collection is external. External
// segments differ only in what meta holds for them, and that is exactly what
// these steps key on.
func EstimateFieldSize(segment *meta.SegmentInfo, schema *schemapb.CollectionSchema, fieldID int64) int64 {
	if size, ok := meta.SegmentFieldLogicalSizes(segment.SegmentInfo).GetFields()[fieldID]; ok {
		return logicalSizeBytes(size)
	}
	field := typeutil.GetFieldByID(schema, fieldID)
	recorded := segment.RecordedFieldSize(fieldID)
	if field.GetDataType() == schemapb.DataType_Text {
		return max(recorded, meta.SaturatingMul(segment.GetNumOfRows(),
			paramtable.Get().DataCoordCfg.TaskResourceTextFallbackBytesPerRow.GetAsSize()))
	}
	if recorded > 0 {
		if bound, exact, ok := fieldsize.SchemaFieldSize(field, segment.GetNumOfRows()); ok && bound > 0 {
			if field.GetDataType() == schemapb.DataType_VarChar || (exact && !decodesWholeColumnGroup(segment)) {
				return min(recorded, bound)
			}
		}
		return recorded
	}
	segmentSize := EstimateSegmentSize(segment, schema)
	if bound := fieldsize.EstimateFieldSize(field, segment.GetNumOfRows(), 0); bound > 0 {
		if segmentSize > 0 {
			return min(bound, segmentSize)
		}
		return bound
	}
	mlog.RatedWarn(context.TODO(), 1, "field has no binlog size in meta and no schema bound, pricing on the whole segment",
		mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID))
	return segmentSize
}

// Cache memoizes a resolved estimate to avoid repeating the metadata walk.
// Unresolved estimates are retried on the next pricing call. Placement passes
// its selected estimate to request construction unchanged.
//
// ok controls caching and nothing else. A computation that could not resolve
// its inputs returns ok=false and is NOT cached, so the next round retries
// instead of freezing a placeholder. The scheduler places the task on that
// answer regardless: a still-resolving task answers an upper bound on itself,
// which errs towards refusing a worker, and one whose inputs are gone for good
// answers the floor. Index tasks check target liveness before this cache and
// retire through coordinator-only preparation; caching a size must not cache
// the assumption that its target still exists.
type Cache struct {
	value atomic.Pointer[taskcommon.Resource]
}

func (c *Cache) Get(compute func() (taskcommon.Resource, bool)) taskcommon.Resource {
	if v := c.value.Load(); v != nil {
		return *v
	}
	res, ok := compute()
	if ok {
		c.value.Store(&res)
	}
	return res
}
