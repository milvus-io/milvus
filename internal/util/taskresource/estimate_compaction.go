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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// CompactionInput carries everything the compaction estimators need. All sizes
// are uncompressed bytes: SegmentInfo.getSegmentSize sums Binlog.MemorySize,
// not LogSize, so no decompression factor is applied anywhere below.
type CompactionInput struct {
	Type           datapb.CompactionType
	StorageVersion int64
	// TotalMemorySize is the summed uncompressed size of all input segments.
	TotalMemorySize int64
	TotalRows       int64
	// MaxSegmentDeleteBytes is the largest per-segment delete payload for the
	// streaming compactors, and the summed L0 delete payload for L0 compaction.
	// ComposeDeleteFromDeltalogs loads one segment's deltalogs at a time, while
	// L0's ComposeDeleteDataFromSegments loads all of them at once.
	MaxSegmentDeleteBytes int64
}

// EstimateCompaction returns the peak resource footprint of a compaction task.
//
// The estimators are derived from the DataNode execution paths rather than from
// tuned constants; see the design doc section 5 for the per-path derivation.
func EstimateCompaction(in CompactionInput) Requirement {
	switch in.Type {
	case datapb.CompactionType_SortCompaction:
		return estimateSortCompaction(in)
	case datapb.CompactionType_Level0DeleteCompaction:
		return estimateL0Compaction(in)
	case datapb.CompactionType_ClusteringCompaction:
		return estimateClusteringCompaction(in)
	default:
		// MixCompaction and BumpSchemaVersionCompaction share the streaming shape.
		return estimateStreamingCompaction(in)
	}
}

// binlogChunkBytes is the reader/writer chunk granularity. A chunk is cut when
// GetWrittenUncompressed reaches BinLogMaxSize, across all fields, so this is
// already an uncompressed figure.
func binlogChunkBytes() int64 {
	return paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsInt64()
}

func arrowExpansion() float64 {
	return paramtable.Get().DataCoordCfg.ResourceArrowExpansionFactor.GetAsFloat()
}

// estimateStreamingCompaction covers mix compaction on the chunked paths and
// bump-schema-version compaction.
//
// Storage v3 is the exception: two independent incidents in issue #52180 show
// heap growing to roughly the full input (32GiB heap for 36GiB in, 60GiB RSS
// for 70GiB in). The Go read path is per-batch bounded and does not explain
// that, so the factor below is an empirical bound on an unexplained retention,
// not a model of it. See the design doc section 5.1.
func estimateStreamingCompaction(in CompactionInput) Requirement {
	if in.StorageVersion >= 3 {
		factor := paramtable.Get().DataCoordCfg.ResourceMixCompactionV3Factor.GetAsFloat()
		mem := int64(float64(in.TotalMemorySize)*factor) + in.MaxSegmentDeleteBytes
		return Requirement{CPU: 1.0, Memory: atLeast(mem, binlogChunkBytes())}
	}

	chunk := binlogChunkBytes()
	// reader chunk + writer buffer + RecordBuilder worst-case copy
	mem := int64(float64(chunk)*arrowExpansion())*2 + chunk + in.MaxSegmentDeleteBytes
	return Requirement{CPU: 1.0, Memory: mem}
}

// estimateSortCompaction models storage.Sort, which Retains every record from
// every reader before sorting, then holds one rowIndex per surviving row.
func estimateSortCompaction(in CompactionInput) Requirement {
	const rowIndexBytes = 8

	mem := int64(float64(in.TotalMemorySize)*arrowExpansion()) +
		in.TotalRows*rowIndexBytes +
		binlogChunkBytes() +
		in.MaxSegmentDeleteBytes
	return Requirement{CPU: 1.0, Memory: atLeast(mem, binlogChunkBytes())}
}

// estimateL0Compaction models ComposeDeleteDataFromSegments, which loads every
// L0 segment's deletes at once, plus the bloom filters of one target batch.
//
// The previous formula divided TotalRows by BloomFilterApplyBatchSize; that
// divisor bounds how many primary keys are tested per call and has nothing to
// do with resident bytes. batchBFAllowance below is a per-segment bloom-filter
// allowance (L0CompactionMaxBatchSize is a segment count, not a size) times the
// batch count — a rough allowance, not a measured byte figure.
func estimateL0Compaction(in CompactionInput) Requirement {
	batchBFAllowance := paramtable.Get().DataNodeCfg.L0CompactionMaxBatchSize.GetAsInt64() * mib
	mem := in.MaxSegmentDeleteBytes + batchBFAllowance
	return Requirement{CPU: 1.0, Memory: atLeast(mem, binlogChunkBytes())}
}

// estimateClusteringCompaction returns the grant that the task's internal
// buffer will be capped to. Today the task sizes that buffer off the node
// (GetMemoryCount x 0.3) regardless of how much data it actually has; phase 2
// converts it to read this grant instead.
func estimateClusteringCompaction(in CompactionInput) Requirement {
	cfg := &paramtable.Get().DataCoordCfg
	want := int64(float64(in.TotalMemorySize) * cfg.ResourceClusteringFactor.GetAsFloat())
	minMem := cfg.ResourceClusteringMinMemory.GetAsInt64()
	maxMem := cfg.ResourceClusteringMaxMemory.GetAsInt64()

	if want < minMem {
		want = minMem
	}
	if want > maxMem {
		want = maxMem
	}
	cpu := float64(paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize.GetAsInt())
	if cpu < 1 {
		cpu = 1
	}
	return Requirement{CPU: cpu, Memory: want}
}

func atLeast(v, floor int64) int64 {
	if v < floor {
		return floor
	}
	return v
}
