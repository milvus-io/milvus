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
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
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

// defaultL0BatchSize stands in for L0CompactionMaxBatchSize's "no limit"
// sentinel (<= 0, default -1: see its Doc). The config counts segments per
// batch, not bytes, so this is a rough per-segment bloom-filter allowance
// times a batch count, not a unit conversion; 32 is a conservative stand-in
// for "unbounded" chosen to keep the term positive and non-trivial at default
// config, not a measured figure.
const defaultL0BatchSize = 32

// estimateL0Compaction models ComposeDeleteDataFromSegments, which loads every
// L0 segment's deletes at once, plus the bloom filters of one target batch.
//
// The previous formula divided TotalRows by BloomFilterApplyBatchSize; that
// divisor bounds how many primary keys are tested per call and has nothing to
// do with resident bytes. batchBFAllowance below is a per-segment bloom-filter
// allowance (L0CompactionMaxBatchSize is a segment count, not a size) times the
// batch count — a rough allowance, not a measured byte figure.
func estimateL0Compaction(in CompactionInput) Requirement {
	batchSize := paramtable.Get().DataNodeCfg.L0CompactionMaxBatchSize.GetAsInt64()
	if batchSize <= 0 {
		// <= 0 is the "no limit" sentinel (default -1); it is a segment count,
		// not a byte multiplier, so it cannot be used as-is.
		batchSize = defaultL0BatchSize
	}
	batchBFAllowance := batchSize * mib
	mem := in.MaxSegmentDeleteBytes + batchBFAllowance
	return Requirement{CPU: 1.0, Memory: atLeast(mem, binlogChunkBytes())}
}

// estimateClusteringCompaction charges what the clustering compactor allocates
// TODAY, which is not the grant the factor/bounds below describe.
// clusteringCompactor.getMemoryBufferHighWatermark
// (internal/datanode/compactor/clustering_compactor.go) sizes the write buffer
// as hardware.GetMemoryCount() x dataNode.clusteringCompaction.memoryBufferRatio
// -- 0.3 of the whole node, 19.2GiB on a 64GiB node -- regardless of how much
// data the task actually has. Phase 2 converts the task to read the grant
// instead, and this function then reduces to the grant alone.
//
// Charging the grant before that conversion would be a live regression, not
// just an under-estimate: until this branch clustering was serialized by
// dataCoord.slot.clusteringCompactionSlotUsage=65535, and phase 0 reroutes
// AvailableSlots onto the ledger so that constant no longer serializes
// anything.
//
// What replaces the sentinel here is a bound, NOT serialization, and that is
// deliberate. Unlike EstimateAnalyze -- whose 0.8 of the node exceeds the 0.75
// memoryRatio budget, so an analyze task is oversized under defaults and does
// run alone -- 0.3 of the node fits the budget comfortably. On the 16c/64GiB
// node from issue #52180 the budget is 48GiB and each clustering task is
// charged 19.2GiB and 8 CPU, so exactly two run concurrently (38.4GiB of 48,
// 16 of 16 cores) and a third is deferred. That is a real capacity gain over
// the sentinel, and it is *accounted for*, which is the whole point of the
// ledger. Inflating the charge to force exclusivity would trade that gain for a
// number that merely looked like the old behavior.
//
// Note this estimator is DataNode-only: DataCoord's clustering task still
// reports the 65535 constant and does not call it.
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

	// Today's allocation. Unlike analyze's training set this is a write buffer,
	// not a bounded read of the input, so the input size does not cap it.
	buffer := int64(float64(hardware.GetMemoryCount()) *
		paramtable.Get().DataNodeCfg.ClusteringCompactionMemoryBufferRatio.GetAsFloat())
	if buffer > want {
		want = buffer
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
