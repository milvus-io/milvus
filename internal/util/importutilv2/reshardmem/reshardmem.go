// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package reshardmem is the single source of truth for the Import V3 reshard
// memory model. Both sides of the contract compute from the same formulas:
// DataCoord charges a task's slot budget from WorkingSet, and the DataNode
// derives its per-bucket spill trigger (BucketTailCap), its fragment
// accounting overhead (FragmentOverhead), its sort-input cap (SortInput) and
// its dynamic free-memory checkpoint (CheckpointFloor) from the same
// components -- so what the scheduler charges and what the worker enforces
// cannot drift apart.
//
// Component glossary:
//
//	R  Model.ReadBuffer      one batch: dataNode.import.readBufferSizeInMB
//	F  Model.FragmentTarget  per-bucket flush trigger: dataCoord.import.fragmentSizeInMB
//	P  parquet read stream   fixed buffered stream of one source reader (TotalReadBufferSize)
//	W  packed writer buffer  packed.DefaultWriteBufferSize
//
// A reshard run's peak is the sum of: the resident routed batches (bounded
// per bucket by BucketTailCap so the total resident accounting never exceeds
// min(buckets, bucketCap) x F), the structural overhead those fragments carry
// on top of their decoded bytes (StructOverhead), one flushBucket spike (the
// Sort materialization of one fragment group plus its output batch, the
// prepare pipeline and the fixed reader/writer overhead), and the GC headroom
// the Go runtime keeps on top of the live set (GCFactor).
package reshardmem

import (
	"math"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	importparquet "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
)

// ReserveRatio is the fraction of system memory (the cgroup limit in
// containers) the dynamic spill checkpoint keeps free on top of one flush
// spike. The reserve absorbs what RSS-based free memory overestimates -- the
// page cache that the cgroup OOM counter includes but GetUsedMemoryCount
// excludes -- and the in-flight growth between checkpoints. In the same
// spirit as the existing watermark knobs (dataNode.memory.forceSyncWatermark,
// streaming.walRateLimit.nodeMemory.*, tieredStorage watermarks), it is a
// fixed constant rather than a user knob.
const ReserveRatio = 0.1

// FragmentFieldOverhead is the per-field live-heap overhead a routed fragment
// carries on top of its decoded bytes: the FieldData wrapper struct, the
// InsertData.Data map entry, the slice headers, the append-grown capacity
// beyond len and the allocator size-class rounding. Calibrated by measuring
// the HeapAlloc delta of 4-row fragments built the way hash routing builds
// them (NewInsertDataWithCap + AppendRow): ~173B per field at a 15-field
// temporary schema, ~615B at 5 fields; 200B keeps a margin for the
// string/JSON-heavy field mixes. Fragments only dominate the live set when
// buckets split every source batch into many small pieces, which is exactly
// the high-partition case this term exists for.
const FragmentFieldOverhead = 200

// PipelineCopies counts the in-flight batch copies of the prepare and routing
// pipeline: the source batch, its normalized copy, the routed fragments and
// the prefetch channel slot held by the prepare stage (4R), plus the Sort
// output batch (1R) and the spill-side arrow rebuild of one drained tail
// (1R).
const PipelineCopies = 6

// GCFactor is the Go-runtime headroom charged on top of the live resident
// set: with the default GOGC=100 the heap grows toward roughly twice the live
// size between collections, and reshard only returns memory at source and
// flush boundaries. 1.5 is the measured-median compromise between charging
// the full 2x (which would halve scheduling density) and ignoring GC
// entirely (the pre-calibration model, which undercharged real usage 4-10x).
const GCFactor = 1

// Model binds the two deployment-dependent buffer sizes every formula shares.
// FragmentTarget may be the live config (DataCoord planning) or the value
// frozen in the task plan (DataNode execution); both sides must pass the
// same pair they charge/enforce with.
type Model struct {
	ReadBuffer     int64
	FragmentTarget int64
}

// FragmentOverhead is the structural live-heap overhead of one routed
// fragment with nFields fields (the temporary schema's field count). The
// DataNode adds it to GetMemorySize when accounting a routed batch, so the
// accounted resident bytes track the real heap instead of the decoded
// payload; WorkingSet charges the same term through StructOverhead.
func FragmentOverhead(nFields int64) int64 {
	return nFields * FragmentFieldOverhead
}

// FixedOverhead is the per-task fixed IO footprint: the parquet read
// buffered stream (charged once, sources are read sequentially) plus the
// packed fragment writer buffer.
func (m Model) FixedOverhead() int64 {
	return importparquet.TotalReadBufferSize + int64(packed.DefaultWriteBufferSize)
}

// Pipeline covers the in-flight batch copies: the source/normalized/routed
// prepare pipeline plus the Sort output batch and the spill-side arrow
// rebuild (see PipelineCopies).
func (m Model) Pipeline() int64 {
	return PipelineCopies * m.ReadBuffer
}

// BucketTailCap is the accounted-bytes ceiling one (vchannel, partition)
// bucket may keep memory-resident before the DataNode appends its tail to
// the run's spill log. While buckets fit inside bucketCap the whole
// in-flight set stays resident exactly as the slot charge assumes, so the
// tail cap is unbounded and spill never triggers statically -- jobs with at
// most bucketCap buckets reshard without local spill on an idle node. Beyond
// the cap the same total resident budget (bucketCap x F) is spread across
// all buckets, so each bucket's tail is bounded by bucketCap x F / buckets
// and the excess streams to disk as it arrives instead of accumulating. The
// cap bounds the per-task slot demand through WorkingSet independently, so
// an uncapped bucket count stays schedulable on small nodes.
func (m Model) BucketTailCap(buckets, bucketCap int64) int64 {
	if buckets <= bucketCap {
		return math.MaxInt64
	}
	return max(bucketCap*m.FragmentTarget/max(buckets, 1), 1)
}

// StructOverhead estimates the structural live-heap overhead of the resident
// fragments (see FragmentFieldOverhead). One source batch (R decoded bytes)
// splits across every bucket, so a resident set of residentBytes holds about
// residentBytes x buckets / R fragments; multiplying by the per-fragment
// overhead is what keeps the slot charge honest when a high bucket count
// shreds the live set into small pieces.
func (m Model) StructOverhead(residentBytes, buckets, nFields int64) int64 {
	if m.ReadBuffer <= 0 || buckets <= 0 || residentBytes <= 0 {
		return 0
	}
	return residentBytes / m.ReadBuffer * buckets * FragmentOverhead(nFields)
}

// WorkingSet is the memory DataCoord charges for a reshard task: fixed
// overhead + pipeline + GCFactor x (resident buckets + their structural
// overhead) + one sort copy. One-pass hash routing keeps up to buckets x F
// of unflushed logical data across all (vchannel, partition) buckets, so
// charging min(buckets, bucketCap) whole buckets lets the DataNode hold the
// full in-flight set resident when it fits inside the cap; beyond the cap
// BucketTailCap spreads the same total across every bucket and the excess
// spills. The sort copy is not GC-scaled: it is a short-lived flush spike,
// not part of the persistent live set.
func (m Model) WorkingSet(buckets, bucketCap, nFields int64) int64 {
	resident := max(min(buckets, bucketCap), 1) * m.FragmentTarget
	structural := m.StructOverhead(resident, max(buckets, 1), nFields)
	return m.FixedOverhead() + m.Pipeline() + int64(GCFactor*float64(resident+structural)) + m.FragmentTarget
}

// SortInput is the largest logical input one storage.Sort may materialize
// inside the given slot budget. Sort holds the group's records and its
// materialized copy at the same time, hence the halving after reserving the
// pipeline and fixed overhead. Degenerate budgets clamp to 1: every spill
// item then becomes its own sort group -- slow, but bounded.
func (m Model) SortInput(memoryBudget int64) int64 {
	return min(m.FragmentTarget, max((memoryBudget-m.Pipeline()-m.FixedOverhead())/2, 1))
}

// FlushSpike is the transient one flushBucket adds on top of resident data:
// the Sort materialization of one group plus the pipeline and fixed overhead.
func (m Model) FlushSpike(memoryBudget int64) int64 {
	return m.SortInput(memoryBudget) + m.Pipeline() + m.FixedOverhead()
}

// CheckpointFloor is the free-memory floor of the dynamic spill checkpoint:
// one flush spike plus the system-memory reserve. While real free memory is
// below the floor, the routing loop spills the largest bucket once per
// source batch, converging back above it. This is cooperative backpressure
// across every memory consumer in the process (concurrent V3 tasks, V2
// import, compaction, GC churn, cgo buffers); it can only push a task below
// its static ceiling, never above it.
func (m Model) CheckpointFloor(memoryBudget, totalMemory int64) int64 {
	return m.FlushSpike(memoryBudget) + int64(ReserveRatio*float64(totalMemory))
}
