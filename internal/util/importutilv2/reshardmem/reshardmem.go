// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package reshardmem is the single source of truth for the Import V3 reshard
// memory model. Both sides of the contract compute from the same formulas:
// DataCoord charges a task's slot budget from WorkingSet, and the DataNode
// derives its sort-input cap (SortInput), its static spill ceiling (the slot
// budget itself) and its dynamic free-memory checkpoint (CheckpointFloor)
// from the same components -- so what the scheduler charges and what the
// worker enforces cannot drift apart.
//
// Component glossary:
//
//	R  Model.ReadBuffer      one batch: dataNode.import.readBufferSizeInMB
//	F  Model.FragmentTarget  per-bucket flush trigger: dataCoord.import.fragmentSizeInMB
//	P  parquet read stream   fixed buffered stream of one source reader (TotalReadBufferSize)
//	W  packed writer buffer  packed.DefaultWriteBufferSize
//
// A reshard run's peak is the sum of: the resident routed batches (bounded
// by the static ceiling / the dynamic checkpoint), one flushBucket spike
// (FlushSpike: the Sort materialization of one fragment group plus its
// output batch, the prepare pipeline and the fixed reader/writer overhead),
// which is exactly what WorkingSet charges for min(buckets, cap) resident
// buckets plus one sort copy.
package reshardmem

import (
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

// Model binds the two deployment-dependent buffer sizes every formula shares.
// FragmentTarget may be the live config (DataCoord planning) or the value
// frozen in the task plan (DataNode execution); both sides must pass the
// same pair they charge/enforce with.
type Model struct {
	ReadBuffer     int64
	FragmentTarget int64
}

// FixedOverhead is the per-task fixed IO footprint: the parquet read
// buffered stream (charged once, sources are read sequentially) plus the
// packed fragment writer buffer.
func (m Model) FixedOverhead() int64 {
	return importparquet.TotalReadBufferSize + int64(packed.DefaultWriteBufferSize)
}

// Pipeline covers the in-flight batch copies: the source/normalized/routed
// prepare pipeline (3R -- the prefetcher holds one buffered batch plus one
// in-flight Read while the routing side flushes) plus the Sort output
// batch (1R).
func (m Model) Pipeline() int64 {
	return 4 * m.ReadBuffer
}

// WorkingSet is the memory DataCoord charges for a reshard task: fixed
// overhead + pipeline + (resident buckets + 1 sort copy) x F. One-pass hash
// routing keeps up to buckets x F of unflushed logical data across all
// (vchannel, partition) buckets, so charging min(buckets, bucketCap) whole
// buckets lets the DataNode hold the full in-flight set resident -- a job
// with at most bucketCap buckets reshards without local spill on an idle
// node. Beyond the cap the excess spills; the cap also bounds per-task slot
// demand for schedulability (an uncapped linear estimate could exceed a
// node's total slots and never schedule).
func (m Model) WorkingSet(buckets, bucketCap int64) int64 {
	resident := max(min(buckets, bucketCap), 1)
	return m.FixedOverhead() + m.Pipeline() + (resident+1)*m.FragmentTarget
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
