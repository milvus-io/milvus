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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

// statsTaskResource prices text-match / bm25 / json-key stats and sort
// compaction alike: all of them read the whole segment.
func statsTaskResource(segmentSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(segmentSize, Params.DataCoordCfg.TaskResourceStatsMemoryFactor.GetAsFloat())),
	}
}

// mixCompactionTaskResource is bounded by the output: a mix (or schema bump)
// compaction writes at most one segment of segment.maxSize.
func mixCompactionTaskResource() taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024),
	}
}

func l0CompactionTaskResource(deltaSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(deltaSize, Params.DataCoordCfg.TaskResourceL0CompactionMemoryFactor.GetAsFloat())),
	}
}

func clusteringCompactionTaskResource() taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceClusteringCompactionCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(Params.DataCoordCfg.TaskResourceClusteringCompactionMemory.GetAsSize()),
	}
}

func analyzeTaskResource(rawDataSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceAnalyzeCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(scaled(rawDataSize, Params.DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat())),
	}
}

// importTaskResource prices an import by its write buffer, which is what the
// worker actually holds in memory (see CalculateTaskBufferSize).
func importTaskResource(bufferSize int64) taskcommon.Resource {
	return taskcommon.Resource{CPU: defaultCPU(), Memory: clampTaskMemory(bufferSize)}
}

// lightweightTaskResource prices copy-segment and external-refresh tasks,
// which stream data and hold little of it.
func lightweightTaskResource() taskcommon.Resource {
	return defaultTaskResource()
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

// estimateFieldSize returns the bytes of one field in a segment.
//
// The per-field binlog arrays are authoritative when present, but V3 segments
// do not persist them (kv_catalog: paths live in the LOON manifest), so after
// a DataCoord restart they are empty. Then: a vector field is rows x dim x
// element size, exact on every storage version; a scalar field is its share
// of the segment size, apportioned by the schema's per-record estimate.
func estimateFieldSize(segment *SegmentInfo, schema *schemapb.CollectionSchema, fieldID int64) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	if size := rawFieldBinlogSize(segment, fieldID); size > 0 {
		return size
	}
	field := typeutil.GetFieldByID(schema, fieldID)
	if field == nil {
		// Unknown field: be conservative and charge the whole segment.
		return estimateSegmentSize(segment, schema)
	}
	rows := segment.GetNumOfRows()
	if typeutil.IsVectorType(field.GetDataType()) {
		if size := vectorFieldBytes(field, rows); size > 0 {
			mlog.Warn(context.TODO(), "vector field has no binlog size, estimated from dim and rows",
				mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID), mlog.Int64("estimatedSize", size))
			return size
		}
	}
	fieldBytes := fieldBytesPerRow(field)
	perRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil || perRecord <= 0 || fieldBytes <= 0 {
		return estimateSegmentSize(segment, schema)
	}
	var size int64
	if segmentSize := segment.getSegmentSize(); segmentSize > 0 {
		size = segmentSize * fieldBytes / int64(perRecord)
	} else {
		size = rows * fieldBytes
	}
	mlog.Warn(context.TODO(), "field has no binlog size, estimated from schema",
		mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID), mlog.Int64("estimatedSize", size))
	return size
}

// rawFieldBinlogSize is getFieldBinlogSize WITHOUT its whole-segment fallback,
// so the caller can tell "no binlog bytes" from "small field".
func rawFieldBinlogSize(segment *SegmentInfo, fieldID int64) int64 {
	var size int64
	for _, binlogs := range segment.GetBinlogs() {
		match := binlogs.GetFieldID() == fieldID
		if !match {
			for _, child := range binlogs.GetChildFields() {
				if child == fieldID {
					match = true
					break
				}
			}
		}
		if !match {
			continue
		}
		for _, l := range binlogs.GetBinlogs() {
			size += l.GetMemorySize()
		}
	}
	return size
}

func vectorFieldBytes(field *schemapb.FieldSchema, rows int64) int64 {
	dim, err := typeutil.GetDim(field)
	if err != nil || dim <= 0 {
		return 0
	}
	return int64(float64(rows) * float64(dim) * typeutil.VectorTypeSize(field.GetDataType()))
}

// fieldBytesPerRow reuses EstimateSizePerRecord on a one-field schema so the
// scalar apportioning uses exactly the estimator the rest of DataCoord uses.
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
