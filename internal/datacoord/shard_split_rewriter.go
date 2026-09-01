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
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The rewrite phase of a hash-routed (primary-key) shard split.
//
// A hash-routed shard's segments straddle every split boundary: the rows of one
// segment hash all over the key space, so — unlike the namespace split, where a
// namespace's data is confined to one shard and a segment can be relabeled whole
// — the source data must be physically repartitioned. Each source segment is
// rewritten by a HashSplitCompaction into exactly two output segments, one per
// target vchannel, and the source segment is dropped at adoption.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §6.5.

// rewritePlanDispatcher submits a hash-split compaction for one source segment
// and reports the plan id. It is the seam between the split task and the
// compaction subsystem, so the rewrite rounds can be tested without a live
// compaction inspector.
type rewritePlanDispatcher interface {
	// DispatchHashSplit enqueues a HashSplitCompaction rewriting segmentID into
	// one output segment per target. It returns the plan id of the enqueued
	// plan. Implementations must be idempotent per (task, segment): a
	// re-dispatch of a segment whose plan is still live returns that plan.
	DispatchHashSplit(task *datapb.SplitShardTask, segmentID int64) (int64, error)
	// HashSplitPlanState reports whether a dispatched plan has committed its
	// outputs (done), is still running (running), or is gone/failed and must be
	// re-dispatched (neither).
	HashSplitPlanState(planID int64) (done bool, running bool)
}

// rewriteRoundResult summarizes one rewrite round for logging and tests.
type rewriteRoundResult struct {
	dispatched []int64 // source segment ids newly dispatched this round
	completed  []int64 // plan ids observed committed this round
	skipped    int     // source segments not eligible this round
}

// pendingRewriteSegments returns the source segments that still need a rewrite:
// every healthy segment on the source vchannel that the task has not yet
// recorded as rewritten.
//
// The task's pending list is the authoritative work list rather than a plain
// re-scan, because a rewrite's outputs live on the TARGET vchannels: once a
// source segment is rewritten its outputs are not on the source channel, but
// the source segment itself stays there until adoption drops it. A scan alone
// therefore cannot tell "not yet rewritten" from "already rewritten"; the task
// state can.
func pendingRewriteSegments(task *datapb.SplitShardTask) map[string]typeutil.Set[int64] {
	out := make(map[string]typeutil.Set[int64], len(task.GetSources()))
	for _, source := range task.GetSources() {
		out[source.GetVchannel()] = typeutil.NewSet(source.GetPendingSegments()...)
	}
	return out
}

// totalPendingRewrites counts the segments still to rewrite across all sources.
func totalPendingRewrites(pending map[string]typeutil.Set[int64]) int {
	total := 0
	for _, segments := range pending {
		total += segments.Len()
	}
	return total
}

// initialPendingSegments lists the source segments a freshly fenced task must
// rewrite. Called once when the task enters Rewriting.
//
// Segments that are still importing are included: the drain (§6.3) keeps the
// task in Rewriting until they are flushed, and each round re-checks
// eligibility, so an importing segment is simply not dispatched until it
// settles.
func (m *shardSplitManager) initialPendingSegments(vchannel string) []int64 {
	return m.sourceSegmentIDs(vchannel)
}

// eligibleForRewrite reports whether a source segment can be dispatched this
// round. Compacting and importing segments are deferred exactly as the relabel
// path defers them: another worker is mutating the segment's binlogs through
// meta updates, and rewriting it concurrently would race with those writes.
func (m *shardSplitManager) eligibleForRewrite(segment *SegmentInfo) bool {
	if segment == nil {
		return false
	}
	if segment.isCompacting {
		return false
	}
	if segment.GetIsImporting() {
		return false
	}
	return true
}

// advanceRewriting runs one rewrite round of a hash-routed split task:
// it harvests the plans that committed since the last round, then dispatches
// hash-split compactions for a bounded batch of not-yet-rewritten source
// segments. It advances the task to Adopting once the source shard is fully
// rewritten and drained.
//
// Rewriting is idempotent under crash: a plan is deterministic in its inputs
// (a source segment) and its partition function (the targets' fixed routing
// predicates), so a re-dispatch after a lost plan reproduces the same two
// outputs. A source segment leaves the pending list only when its plan is
// observed committed, and no output is adopted before the drain below, so a
// duplicated output from a dead plan is collected as ordinary failed-compaction
// garbage instead of being double-counted.
func (m *shardSplitManager) advanceRewriting(
	task *datapb.SplitShardTask,
	dispatcher rewritePlanDispatcher,
	batchSize int,
) rewriteRoundResult {
	logger := m.taskLogger(task)
	result := rewriteRoundResult{}

	// 1. Harvest: a dispatched plan that committed retires its source segment.
	stillDispatched := make([]int64, 0, len(task.GetDispatchedPlanIds()))
	for _, planID := range task.GetDispatchedPlanIds() {
		done, running := dispatcher.HashSplitPlanState(planID)
		switch {
		case done:
			result.completed = append(result.completed, planID)
		case running:
			stillDispatched = append(stillDispatched, planID)
		default:
			// Lost or failed: drop it so the segment is re-dispatched below.
			logger.Warn(m.ctx, "hash split rewrite plan lost, will re-dispatch",
				mlog.Int64("planID", planID))
		}
	}

	pending := pendingRewriteSegments(task)
	// The outputs' lineage is the record of what has already been rewritten; it
	// answers both "retire this" and "is this new work" below, so scan once.
	rewritten := m.rewrittenSourceSegments(task)

	// 1b. Pick up work that arrived after the list was seeded.
	arrived := adoptNewSourceSegments(task, pending, rewritten, m.sourceSegmentIDs)
	// A committed plan retires the source segments it rewrote. The dispatcher
	// owns the plan->segment mapping, so completion is reported back through
	// the segment list the plan carried; here the retirement is derived from
	// the target channels: a rewritten source segment has its outputs in meta.
	retired := retireRewrittenSegments(pending, rewritten)

	// 2. Drain check: every source segment rewritten, every source's fence-sealed
	// segments flushed (its checkpoint >= its own T_switch), and no active import
	// on any source. Same three conjuncts as the relabel drain, with "no segment
	// left" replaced by "nothing left to rewrite", because the source segments
	// stay in meta until adoption drops them. Every conjunct must hold for ALL
	// sources: the targets go live together, so one lagging source would let a
	// target be adopted while part of its key range was still unwritten.
	if totalPendingRewrites(pending) == 0 && len(stillDispatched) == 0 &&
		m.hashFenceFlushed(task) && !m.anyHashSourceImporting(task) {
		if err := m.updateTask(task, func(t *datapb.SplitShardTask) {
			t.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
			for _, source := range t.Sources {
				source.PendingSegments = nil
			}
			t.DispatchedPlanIds = nil
		}); err != nil {
			logger.Warn(m.ctx, "persist the rewritten split task failed", mlog.Err(err))
			return result
		}
		logger.Info(m.ctx, "every source segment rewritten, advance to adopting",
			mlog.Int("retired", retired))
		return result
	}

	// 3. Dispatch a bounded batch of the remaining segments. The batch bounds the
	// whole task, not each source: the sources are rewritten concurrently, so a
	// per-source bound would multiply the in-flight compaction load by the number
	// of sources.
	dispatchedNow := make([]int64, 0, batchSize)
	for _, source := range task.GetSources() {
		// Iterate the live pending set, not the task's persisted list: a segment
		// that arrived this round is in the former only, and waiting a round to
		// dispatch it would be a needless delay on the drain.
		for _, segmentID := range sortedSegmentIDs(pending[source.GetVchannel()]) {
			if len(dispatchedNow) >= batchSize {
				break
			}
			segment := m.meta.GetSegment(m.ctx, segmentID)
			if !m.eligibleForRewrite(segment) {
				result.skipped++
				continue
			}
			planID, err := dispatcher.DispatchHashSplit(task, segmentID)
			if err != nil {
				// One undispatchable segment must not wedge the task — abort is
				// illegal past the fence. Retry it on the next round.
				logger.Warn(m.ctx, "dispatch a hash split rewrite failed, retry next round",
					mlog.Int64("segmentID", segmentID), mlog.Err(err))
				result.skipped++
				continue
			}
			dispatchedNow = append(dispatchedNow, planID)
			result.dispatched = append(result.dispatched, segmentID)
		}
		if len(dispatchedNow) >= batchSize {
			break
		}
	}

	if len(dispatchedNow) == 0 && len(result.completed) == 0 && retired == 0 && arrived == 0 {
		return result
	}
	if err := m.updateTask(task, func(t *datapb.SplitShardTask) {
		for _, source := range t.Sources {
			if remaining, ok := pending[source.GetVchannel()]; ok {
				source.PendingSegments = remaining.Collect()
			}
		}
		t.DispatchedPlanIds = append(stillDispatched, dispatchedNow...)
	}); err != nil {
		logger.Warn(m.ctx, "persist the rewrite round failed", mlog.Err(err))
		return result
	}
	logger.Info(m.ctx, "hash split rewrite round",
		mlog.Int("arrived", arrived),
		mlog.Int("dispatched", len(result.dispatched)),
		mlog.Int("completed", len(result.completed)),
		mlog.Int("skipped", result.skipped),
		mlog.Int("pending", totalPendingRewrites(pending)))
	return result
}

// anyHashSourceImporting reports whether any source vchannel still has an
// import in flight, which would add segments after the drain declared the
// sources finished.
func (m *shardSplitManager) anyHashSourceImporting(task *datapb.SplitShardTask) bool {
	return m.hasActiveImportOnAnyVChannel(splitSourceVChannels(task))
}

// retireRewrittenSegments removes from pending every source segment whose
// rewrite outputs are already committed on the target vchannels, and returns
// how many were retired.
//
// Completion is judged from meta, not from the plan result alone, so a crash
// between "plan committed" and "task persisted" still converges: the next round
// sees the outputs in meta and retires the segment without re-running the
// rewrite.
func retireRewrittenSegments(pending map[string]typeutil.Set[int64], rewritten typeutil.Set[int64]) int {
	if totalPendingRewrites(pending) == 0 {
		return 0
	}
	retired := 0
	for _, segmentID := range rewritten.Collect() {
		// A segment id is unique across the collection, so it can only match one
		// source's pending set; scanning them all avoids having to look the
		// segment's channel back up in meta after it may already be gone.
		for _, segments := range pending {
			if segments.Contain(segmentID) {
				segments.Remove(segmentID)
				retired++
				break
			}
		}
	}
	return retired
}

// adoptNewSourceSegments adds to the work list every segment that has appeared
// on a source vchannel since the list was seeded, and returns how many.
//
// The list is seeded once, when the task enters Rewriting. The sources keep
// receiving segments after that: an import is deliberately not stopped by the
// fence -- it is a bulk load addressed to the collection, and the drain waits
// for it to finish rather than failing it -- so its segments land on a source
// that has already been surveyed.
//
// Without this they would never be rewritten, and adoption retires every
// segment on the source channels. A split that reported success would have
// deleted the imported rows.
//
// "Already rewritten" comes from the outputs' lineage, the same set the retire
// above works from, so a segment finished in an earlier round is not queued
// again -- which matters because a rewritten source segment stays on its source
// channel until adoption, and a plain re-scan cannot tell the two apart.
func adoptNewSourceSegments(
	task *datapb.SplitShardTask,
	pending map[string]typeutil.Set[int64],
	rewritten typeutil.Set[int64],
	segmentsOf func(vchannel string) []int64,
) int {
	arrived := 0
	for _, source := range task.GetSources() {
		vchannel := source.GetVchannel()
		queued, ok := pending[vchannel]
		if !ok {
			queued = typeutil.NewSet[int64]()
			pending[vchannel] = queued
		}
		for _, segmentID := range segmentsOf(vchannel) {
			if queued.Contain(segmentID) || rewritten.Contain(segmentID) {
				continue
			}
			queued.Insert(segmentID)
			arrived++
		}
	}
	return arrived
}

// sortedSegmentIDs gives a set a stable iteration order, so a rewrite round
// dispatches the same batch every time it runs over the same work list.
func sortedSegmentIDs(segments typeutil.Set[int64]) []int64 {
	ids := segments.Collect()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// sourceSegmentIDs lists the ids of every segment currently on a vchannel.
func (m *shardSplitManager) sourceSegmentIDs(vchannel string) []int64 {
	segments := m.meta.GetSegmentsByChannel(vchannel)
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.GetID())
	}
	return ids
}

// rewrittenSourceSegments returns the source segment ids that have committed
// rewrite outputs on the target vchannels. A hash-split output records the
// source segment it was rewritten from in its compaction lineage
// (SegmentInfo.CompactionFrom), so the set is derived by scanning the targets.
func (m *shardSplitManager) rewrittenSourceSegments(task *datapb.SplitShardTask) typeutil.Set[int64] {
	out := typeutil.NewSet[int64]()
	for _, target := range task.GetTargets() {
		for _, segment := range m.meta.GetSegmentsByChannel(target.GetVchannel()) {
			for _, from := range segment.GetCompactionFrom() {
				out.Insert(from)
			}
		}
	}
	return out
}
