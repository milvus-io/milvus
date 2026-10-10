// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// This file owns the observability of one reshard run: the counters the routing
// side accumulates, their fold into the run metrics, the run summary log, and
// the collection of the detached writes' seq-ordered outcomes. It is log-only:
// no counter feeds back into planning, packing, or quota.

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// reshardRunCounters aggregates per-run observations for the reshard summary
// log. It is log-only: no counter feeds back into planning, packing, or quota,
// so miscounting here can mislead an operator but never corrupts data.
type reshardRunCounters struct {
	fragments    int
	rows         int64
	logicalBytes int64 // sum of published descriptor.LogicalBytes (normalized decoded bytes)
	writtenBytes int64 // sum of the packed writer's uncompressed output, reported
	// alongside logicalBytes so the run log keeps both sides of the packing metric
	spillRanges   int
	spillBytes    int64
	sortReadCost  time.Duration
	sortCost      time.Duration
	sortWriteCost time.Duration
	// readWait is the time the routing side blocked waiting for the next
	// prepared source batch (read + normalize + function execution); flushBlock is
	// the time the routing side spent waiting for a free write slot: zero means
	// every write hid behind reading and hashing, anything above zero means the
	// run is write-bound and dataCoord.import.reshardFlushConcurrency is the
	// limiter. (The summed wall time of the detached writes themselves is owned
	// by the writer, not by these counters.)
	readWait   time.Duration
	flushBlock time.Duration
	// routeCost is the time the routing side spent hashing a batch into buckets
	// and applying the flush/spill triggers; runWall is the whole run's wall time,
	// the denominator for the phase breakdown. readWait + routeCost + flushBlock
	// make up the routing side; the prepare stage's split
	// (read/normalize/functions/send) is in stageTimings.
	routeCost    time.Duration
	runWall      time.Duration
	peakResident int64
}

// collectOutcomes folds the seq-ordered fragment outcomes into the manifest and
// the run counters. Collected in sequence order, which is the routing order the
// synchronous pipeline appended to the manifest in.
func (r *reshardPlanExecutor) collectOutcomes() {
	for _, outcome := range r.writer.outcomes() {
		r.manifest.Fragments = append(r.manifest.Fragments, outcome.descriptor)
		r.counters.fragments++
		r.counters.rows += outcome.descriptor.GetRows()
		r.counters.logicalBytes += outcome.descriptor.GetLogicalBytes()
		r.counters.writtenBytes += int64(outcome.writtenUncompressed)
		if outcome.timings != nil {
			r.counters.sortReadCost += outcome.timings.ReadCost
			r.counters.sortCost += outcome.timings.SortCost
			r.counters.sortWriteCost += outcome.timings.WriteCost
		}
	}
}

// observeRunMetrics reports one finished run's volume and phase latencies.
func (r *reshardPlanExecutor) observeRunMetrics() {
	r.metrics.ObserveReshardRun(ReshardRunObservation{
		Rows:         r.counters.rows,
		LogicalBytes: r.counters.logicalBytes,
		WrittenBytes: r.counters.writtenBytes,
		SpillBytes:   r.counters.spillBytes,
		ReadWait:     r.counters.readWait,
		RouteCost:    r.counters.routeCost,
		FlushBlock:   r.counters.flushBlock,
		FlushWall:    r.writer.flushWallDuration(),

		PrepareRead:      time.Duration(r.stageTimings.read.Load()),
		PrepareNormalize: time.Duration(r.stageTimings.normalize.Load()),
		PrepareFunctions: time.Duration(r.stageTimings.functions.Load()),
		PrepareSend:      time.Duration(r.stageTimings.send.Load()),

		SortRead:  r.counters.sortReadCost,
		Sort:      r.counters.sortCost,
		SortWrite: r.counters.sortWriteCost,
	})
}

// logSummary reports one finished run's configuration, volume and phase split.
func (r *reshardPlanExecutor) logSummary() {
	mlog.Info(r.ctx, "import v3 reshard run finished",
		mlog.FieldJobID(r.req.GetJobId()),
		mlog.Int64("taskID", r.req.GetTaskId()),
		mlog.Int64("runID", r.req.GetRunId()),
		mlog.Int64("slot", r.slot),
		mlog.Int64("memoryBudget", r.memory.budget),
		mlog.Int64("fragmentTarget", r.fragment.target),
		mlog.Int64("effectiveFragmentInput", r.fragment.sortInput),
		mlog.Int64("residentBudget", r.memory.residentBudget),
		mlog.Int64("flushConcurrency", r.memory.model.Flushes()),
		mlog.Int("buckets", r.buckets.len()),
		mlog.Int("sources", len(r.plan.GetFiles())),
		mlog.Int("fragments", r.counters.fragments),
		mlog.Int64("rows", r.counters.rows),
		mlog.Int64("logicalBytes", r.counters.logicalBytes),
		mlog.Int64("writtenUncompressedBytes", r.counters.writtenBytes),
		mlog.Int("spillRanges", r.counters.spillRanges),
		mlog.Int("spillStreams", r.spillMgr.Streams()),
		mlog.Int("spillFiles", r.spillMgr.Files()),
		mlog.Int64("spillBytes", r.counters.spillBytes),
		mlog.Int64("peakResidentBytes", r.counters.peakResident),
		mlog.Duration("runWall", r.counters.runWall),
		// Routing side: readWait is time starved by the prepare stage, routeCost
		// is hashing + flush/spill triggers, flushBlock is waiting for a write
		// slot.
		mlog.Duration("readWait", r.counters.readWait),
		mlog.Duration("routeCost", r.counters.routeCost),
		mlog.Duration("flushWall", r.writer.flushWallDuration()),
		mlog.Duration("flushBlock", r.counters.flushBlock),
		// Prepare stage split: stageRead is the source reader (IO + decode),
		// stageNormalize and stageFunctions are the prepare steps, stageSend is
		// the stage blocked handing off to the routing side.
		mlog.Duration("stageRead", time.Duration(r.stageTimings.read.Load())),
		mlog.Duration("stageNormalize", time.Duration(r.stageTimings.normalize.Load())),
		mlog.Duration("stageFunctions", time.Duration(r.stageTimings.functions.Load())),
		mlog.Duration("stageSend", time.Duration(r.stageTimings.send.Load())),
		mlog.Duration("sortReadCost", r.counters.sortReadCost),
		mlog.Duration("sortCost", r.counters.sortCost),
		mlog.Duration("sortWriteCost", r.counters.sortWriteCost),
	)
}
