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
//	N  Model.FlushConcurrency detached fragment writes in flight: dataCoord.import.reshardFlushConcurrency
//	P  parquet read stream   fixed buffered stream of one source reader (TotalReadBufferSize)
//	W  packed writer buffer  packed.DefaultWriteBufferSize
//
// A reshard run's peak is the sum of: the resident routed batches (bounded
// per bucket by BucketTailCap so the total resident accounting never exceeds
// min(buckets, bucketCap) x F), the N detached fragment inputs a run keeps
// live while their writes overlap the routing side (N x F), the structural
// overhead those fragments carry on top of their decoded bytes
// (StructOverhead), the flush spike (one Sort materialization per in-flight
// write plus the prepare pipeline and the fixed reader/writer overhead), and
// the expansion factor applied to the live resident set (Model.ExpansionFactor,
// dataCoord.import.reshardMemoryExpansionFactor).
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

// DefaultExpansionFactor is the startup default of
// dataCoord.import.reshardMemoryExpansionFactor. It scales the live resident
// set (resident buckets + detached fragment inputs + their structural
// overhead) to cover the Go runtime's headroom and the other growth between
// the source/flush boundaries the run returns memory at. The configured value
// travels on Model.ExpansionFactor; this constant is the fallback for a
// zero-valued Model.
const DefaultExpansionFactor = 1.5

// NodeWorkerMemoryBytes is the memory of the 2c8g worker that
// dataNode.workerSlotUnit is expressed in. One node slot therefore represents
// NodeWorkerMemoryBytes / workerSlotUnit of memory -- the same convention
// CalculateNodeSlots uses -- and reshard converts WorkingSet to slots on that
// unit, independent of the import-specific dataCoord.import.memoryLimitPerSlot.
const NodeWorkerMemoryBytes = int64(8) << 30

// MemoryPerSlot is the memory one node slot represents: the 2c8g worker memory
// divided by dataNode.workerSlotUnit. DataCoord's slot estimate and the
// DataNode's memory budget both use it, so charge and enforcement cannot drift.
func MemoryPerSlot(workerSlotUnit int64) int64 {
	return NodeWorkerMemoryBytes / max(workerSlotUnit, 1)
}

// Model binds the deployment-dependent sizes every formula shares.
// FragmentTarget may be the live config (DataCoord planning) or the value
// frozen in the task plan (DataNode execution), and FlushConcurrency is
// dataCoord.import.reshardFlushConcurrency on both sides; charge and
// enforcement must pass the same values.
type Model struct {
	ReadBuffer       int64
	FragmentTarget   int64
	FlushConcurrency int64
	// ExpansionFactor scales the live resident set. Zero falls back to
	// DefaultExpansionFactor; callers pass
	// dataCoord.import.reshardMemoryExpansionFactor.
	ExpansionFactor float64
}

// Expansion returns the live-set expansion factor, falling back to the default
// for a zero-valued Model.
func (m Model) Expansion() float64 {
	if m.ExpansionFactor <= 0 {
		return DefaultExpansionFactor
	}
	return m.ExpansionFactor
}

// Flushes is the in-flight fragment write count the formulas charge and the
// DataNode's semaphore enforces, clamped so a zero-valued Model still
// describes one detached write.
func (m Model) Flushes() int64 {
	return max(m.FlushConcurrency, 1)
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
// buffered stream (charged once, sources are read sequentially) plus one
// packed fragment writer buffer per in-flight write.
func (m Model) FixedOverhead() int64 {
	return importparquet.TotalReadBufferSize + m.Flushes()*int64(packed.DefaultWriteBufferSize)
}

// Pipeline covers the in-flight batch copies: the source/normalized/routed
// prepare pipeline plus the Sort output batch and the spill-side arrow
// rebuild (see PipelineCopies).
func (m Model) Pipeline() int64 {
	return PipelineCopies * m.ReadBuffer
}

// BucketTailCap is the accounted-bytes ceiling one (vchannel, partition)
// bucket may keep memory-resident before the DataNode appends its tail to
// the run's spill manager. While buckets fit inside bucketCap the whole
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

// WorkingSet is the memory DataCoord charges for a reshard task: the whole
// per-task heap footprint scaled by the expansion factor. The footprint is the
// fixed IO overhead (the parquet stream plus one packed writer buffer per
// detached write), the in-flight pipeline copies, the resident buckets, the
// detached fragment inputs, their structural overhead, and one sort copy per
// detached write.
//
// The expansion factor scales ALL of it, not just the routed buckets: the Go GC
// headroom is a fraction of the whole live heap, and a small task's footprint
// is dominated by the fixed + pipeline + sort terms, so scaling only the
// resident set undercharges exactly the many-small-task case (many tasks, each
// with a footprint larger than its bucket set alone implies).
//
// One-pass hash routing keeps up to buckets x F of unflushed logical data
// across all (vchannel, partition) buckets, so charging min(buckets, bucketCap)
// whole buckets lets the DataNode hold the full in-flight set resident when it
// fits inside the cap; beyond the cap BucketTailCap spreads the same total
// across every bucket and the excess spills. A detached write keeps its
// fragment input live while the bucket it was cut from refills, so N in-flight
// writes add N x F on top of the resident buckets.
func (m Model) WorkingSet(buckets, bucketCap, nFields int64) int64 {
	resident := max(min(buckets, bucketCap), 1) * m.FragmentTarget
	inFlight := m.Flushes() * m.FragmentTarget
	structural := m.StructOverhead(resident, max(buckets, 1), nFields)
	footprint := m.FixedOverhead() + m.Pipeline() + resident + inFlight + structural + inFlight
	return int64(m.Expansion() * float64(footprint))
}

// SortInput is the largest logical input one storage.Sort may materialize
// inside the given slot budget. Sort holds the group's records and its
// materialized copy at the same time, and every detached write sorts
// concurrently, hence the division by 2N after reserving the pipeline and
// fixed overhead. Degenerate budgets clamp to 1: every spill item then
// becomes its own sort group -- slow, but bounded.
func (m Model) SortInput(memoryBudget int64) int64 {
	return min(m.FragmentTarget, max((memoryBudget-m.Pipeline()-m.FixedOverhead())/(2*m.Flushes()), 1))
}

// FlushSpike is the transient the in-flight writes add on top of resident
// data: one Sort materialization per detached write plus the pipeline and
// fixed overhead.
func (m Model) FlushSpike(memoryBudget int64) int64 {
	return m.Flushes()*m.SortInput(memoryBudget) + m.Pipeline() + m.FixedOverhead()
}

// CheckpointFloor is the free-memory floor of the dynamic spill checkpoint:
// the flush spike of every in-flight write plus the system-memory reserve.
// While real free memory is
// below the floor, the routing loop spills the largest bucket once per
// source batch, converging back above it. This is cooperative backpressure
// across every memory consumer in the process (concurrent V3 tasks, V2
// import, compaction, GC churn, cgo buffers); it can only push a task below
// its static ceiling, never above it.
func (m Model) CheckpointFloor(memoryBudget, totalMemory int64) int64 {
	return m.FlushSpike(memoryBudget) + int64(ReserveRatio*float64(totalMemory))
}
