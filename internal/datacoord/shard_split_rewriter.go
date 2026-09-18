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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The rewrite of a shard split (design doc §6.3 steps 2-4).
//
// A collection placed by primary key hashes its rows all over the key space, so
// the split boundary cuts through every segment of the source and its data must
// be physically repartitioned. Each flushed source segment is rewritten by a
// HashSplitCompaction into one output segment per target, and the commit that
// publishes the outputs drops the input (completeHashSplitCompactionMutation).
//
// The split manager calls the rewrite once per tick while the task is
// Redistributing and its source is past its T_switch, and moves the task to
// Adopting itself once the drain predicate holds. A round therefore only moves
// data: it harvests the plans that committed, picks up source segments that
// became rewrite inputs, and dispatches plans for the rest, keeping the work
// list and the in-flight plans on the task record so a restart resumes where
// it left off.

// rewritePlanDispatcher submits the rewrite of one source segment and reads
// back its plan's state. The seam between the split task and the compaction
// subsystem.
type rewritePlanDispatcher interface {
	// DispatchHashSplit enqueues a HashSplitCompaction rewriting segmentID into
	// one output segment per target, and returns its plan id. Idempotent per
	// (task, segment): a re-dispatch of a segment whose plan is still live
	// returns that plan.
	DispatchHashSplit(task *datapb.SplitShardTask, segmentID int64) (int64, error)
	// HashSplitPlanState reports whether a dispatched plan has committed (done),
	// is still running, or is neither; in the neither case inputSegments names
	// the plan's own inputs when its record is still found, which is how a plan
	// committed and since cleaned up is told apart from one truly lost.
	HashSplitPlanState(planID int64) (done bool, running bool, inputSegments []int64)
}

// hashSplitRewriter is the split manager's redistribution: the rewrite.
type hashSplitRewriter struct {
	manager    *shardSplitManager
	dispatcher *inspectorRewriteDispatcher
}

var _ splitRedistributor = (*hashSplitRewriter)(nil)

func newHashSplitRewriter(manager *shardSplitManager, dispatcher *inspectorRewriteDispatcher) *hashSplitRewriter {
	return &hashSplitRewriter{manager: manager, dispatcher: dispatcher}
}

// redistribute runs one rewrite round of a fenced task.
func (r *hashSplitRewriter) redistribute(ctx context.Context, task *datapb.SplitShardTask) {
	batchSize := paramtable.Get().DataCoordCfg.ShardSplitRewriteBatchSize.GetAsInt()
	r.manager.rewriteRound(ctx, task, r.dispatcher.forTask(task.GetTaskId()), batchSize)
}

// rewriteRoundResult summarizes one rewrite round for logging and tests.
type rewriteRoundResult struct {
	dispatched []int64 // source segment ids newly dispatched this round
	completed  []int64 // plan ids observed committed this round
	skipped    int     // source segments not dispatchable this round
}

// isRewriteInput reports whether a segment is data a rewrite may take as its
// input: a Flushed segment that is not L0.
//
// The commit drops the input, so anything else must never reach a plan. An L0
// carries deletes rather than rows, and the rewrite would drop them with it; a
// growing, sealed or flushing segment is still being written, and joins the
// work list once it is Flushed. Neither is lost by being left out: the drain
// counts every live segment on the source, so the task cannot adopt past one.
func isRewriteInput(segment *SegmentInfo) bool {
	return segment != nil &&
		segment.GetState() == commonpb.SegmentState_Flushed &&
		segment.GetLevel() != datapb.SegmentLevel_L0
}

// dispatchableForRewrite reports whether a rewrite input can be dispatched this
// round: no other worker is changing it and no snapshot holds it. A compacting
// segment is held by a running compaction (the split's own plan, once
// dispatched), and an importing one is still being committed by its import.
//
// A segment a snapshot protects, or one of a collection whose compaction is
// blocked until its snapshot RefIndex loads, is refused at commit
// (ValidateSegmentStateBeforeCompleteCompactionMutation), exactly as every
// compaction policy skips it. Dispatching it anyway would fail the plan and
// dispatch it again every round for as long as the protection lasts. It stays
// listed, and the drain waits for it.
func (m *shardSplitManager) dispatchableForRewrite(segment *SegmentInfo) bool {
	return isRewriteInput(segment) && !segment.isCompacting && !segment.GetIsImporting() &&
		!m.meta.isCollectionCompactionBlocked(segment.GetCollectionID()) &&
		!m.meta.isSegmentCompactionProtected(segment.GetID())
}

// rewriteRound runs one rewrite round of a task:
//
//  1. harvest: a dispatched plan that committed is done; one lost is dropped so
//     its segment is dispatched again;
//  2. re-scan: every rewrite input on the source joins the work list, and every
//     listed segment that is rewritten or no longer an input leaves it;
//  3. dispatch plans for the listed segments, up to batchSize in flight.
//
// It is idempotent under a crash: a plan is deterministic in its input and the
// targets' residues, so a re-dispatch after a lost plan reproduces the same
// outputs, and the commit drops the input in the same write that publishes the
// outputs, so of two plans for one segment only the first commits.
func (m *shardSplitManager) rewriteRound(
	ctx context.Context,
	task *datapb.SplitShardTask,
	dispatcher rewritePlanDispatcher,
	batchSize int,
) rewriteRoundResult {
	logger := m.taskLogger(task)
	result := rewriteRoundResult{}

	// The outputs' lineage is the record of what has already been rewritten:
	// it retires work below, keeps a rewritten segment from being queued
	// again, and is what a plan's own inputs are checked against when
	// compaction meta alone cannot tell a committed plan from a lost one.
	rewritten := m.rewrittenSourceSegments(task)

	// 1. Harvest.
	stillDispatched := make([]int64, 0, len(task.GetDispatchedPlanIds()))
	for _, planID := range task.GetDispatchedPlanIds() {
		done, running, inputSegments := dispatcher.HashSplitPlanState(planID)
		switch {
		case done:
			result.completed = append(result.completed, planID)
		case running:
			stillDispatched = append(stillDispatched, planID)
		case m.planAlreadyCommitted(ctx, inputSegments, rewritten):
			// Committed, and since cleaned up from compaction meta: not lost.
			logger.Debug(ctx, "shard split rewrite plan already committed, dropping the stale reference",
				mlog.Int64("planID", planID))
		default:
			// Lost or failed: dropped here, its segment is dispatched again.
			logger.Warn(ctx, "shard split rewrite plan lost, dispatching its segment again",
				mlog.Int64("planID", planID))
		}
	}

	// 2. Re-scan. Imports keep adding segments to a fenced source until the
	// drain lets the task adopt, and each is rewritten once it is Flushed.
	pending := pendingRewriteSegments(task)
	arrived := adoptNewSourceSegments(task, pending, rewritten, m.rewriteInputIDs)
	retired := retireRewrittenSegments(pending, rewritten) +
		forgetNonInputSegments(pending, func(segmentID int64) *SegmentInfo {
			return m.meta.GetSegment(ctx, segmentID)
		})

	// Nothing may be dispatched before the source is past its T_switch: a plan
	// built earlier could miss a delete that flushes into a source L0 later, and
	// its commit drops the input, leaving that delete nowhere to apply. The
	// manager only calls a round past the fence; this holds the rule here too.
	if reason := m.coordinator.fenceFlushBlockReason(task); reason != "" {
		logger.RatedInfo(ctx, 30, "shard split rewrite waits for the fence to flush before dispatching",
			mlog.String("reason", reason), mlog.Int("pending", totalPendingRewrites(pending)))
		if retired == 0 && arrived == 0 && len(result.completed) == 0 {
			return result
		}
		return m.persistRewriteRound(ctx, task, result, pending, stillDispatched, nil, arrived)
	}

	// Every plan that ran carried the source's L0s and folded them into the
	// outputs it wrote; once no data is left on the source to fold them, they
	// hold nothing that is not already applied, and they are all that keeps the
	// drain false.
	m.retireSourceLevelZeroSegments(ctx, task)

	// 3. Dispatch, bounded by the plans the task has in flight.
	dispatchedNow := make([]int64, 0)
	budget := batchSize - len(stillDispatched)
	for _, source := range task.GetSources() {
		for _, segmentID := range sortedSegmentIDs(pending[source.GetVchannel()]) {
			if len(dispatchedNow) >= budget {
				break
			}
			segment := m.meta.GetSegment(ctx, segmentID)
			if !m.dispatchableForRewrite(segment) {
				result.skipped++
				continue
			}
			planID, err := dispatcher.DispatchHashSplit(task, segmentID)
			if err != nil {
				// Past the fence the task cannot abort, so one segment that
				// cannot be dispatched is retried next round.
				logger.RatedWarn(ctx, 30, "dispatch a shard split rewrite failed, retrying next round",
					mlog.Int64("segmentID", segmentID), mlog.Err(err))
				result.skipped++
				continue
			}
			if slices.Contains(stillDispatched, planID) {
				continue
			}
			dispatchedNow = append(dispatchedNow, planID)
			result.dispatched = append(result.dispatched, segmentID)
		}
	}

	if len(dispatchedNow) == 0 && len(result.completed) == 0 && retired == 0 && arrived == 0 &&
		len(stillDispatched) == len(task.GetDispatchedPlanIds()) {
		return result
	}
	return m.persistRewriteRound(ctx, task, result, pending, stillDispatched, dispatchedNow, arrived)
}

// retireSourceLevelZeroSegments drops the L0 segments of every source that
// holds no data left to fold them.
//
// Safe because every rewrite plan carries its source's L0s
// (hashSplitDeleteSources) and folds them into the outputs it writes, so a row
// deleted through a source L0 is already gone from the targets. The guard is
// wider than "no rewrite input": a Sealed or Growing segment is data about to
// become an input that has folded nothing, and datacoord does not assume the
// fence makes one impossible, so the scan is the drain's own (every real
// non-Dropped segment on the channel) minus the L0s being retired. An input
// stays Flushed until its own commit, so the retire never runs while a plan on
// the source is still in flight.
//
// Not part of any commit: a source may hold L0s and no input at all (every row
// deleted, or its inputs rewritten in an earlier round), and then no commit
// would ever run to carry it. Its own write, after the last commit: a crash in
// between leaves the deletes applied twice, harmlessly, and the next round
// retires them.
func (m *shardSplitManager) retireSourceLevelZeroSegments(ctx context.Context, task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	for _, source := range task.GetSources() {
		vchannel := source.GetVchannel()
		if blocking := m.unfoldedSourceData(vchannel); blocking != nil {
			logger.RatedInfo(ctx, 30, "not retiring the source's L0 segments yet, the source still holds data to fold them into",
				mlog.String("vchannel", vchannel),
				mlog.Int64("segmentID", blocking.GetID()),
				mlog.String("state", blocking.GetState().String()),
				mlog.String("level", blocking.GetLevel().String()))
			continue
		}
		ids := m.sourceLevelZeroIDs(vchannel)
		if len(ids) == 0 {
			continue
		}
		if err := m.meta.RetireLevelZeroSegments(ctx, ids); err != nil {
			// Nothing is lost: the drain still refuses while they live, and
			// the next round tries again.
			logger.Warn(ctx, "retire the source's L0 segments failed, retrying next round",
				mlog.String("vchannel", vchannel), mlog.Int64s("segmentIDs", ids), mlog.Err(err))
			continue
		}
		logger.Info(ctx, "retired the source's L0 segments the rewrite folded",
			mlog.String("vchannel", vchannel), mlog.Int64s("segmentIDs", ids))
	}
}

// unfoldedSourceData returns the first real segment on vchannel that is
// neither Dropped nor L0, or nil when there is none.
func (m *shardSplitManager) unfoldedSourceData(vchannel string) *SegmentInfo {
	for _, segment := range m.meta.GetRealSegmentsForChannel(vchannel) {
		if segment.GetState() == commonpb.SegmentState_Dropped || segment.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		return segment
	}
	return nil
}

// sourceLevelZeroIDs lists the healthy L0 segments on a vchannel, in id order:
// the set every rewrite plan of the source folded, over every partition.
func (m *shardSplitManager) sourceLevelZeroIDs(vchannel string) []int64 {
	ids := make([]int64, 0)
	for _, segment := range m.meta.GetSegmentsByChannel(vchannel) {
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			ids = append(ids, segment.GetID())
		}
	}
	slices.Sort(ids)
	return ids
}

// persistRewriteRound writes back what one round changed: the work list and the
// plans still to watch. It writes only while the task is still Redistributing;
// once it has moved on, its rewrite is over.
func (m *shardSplitManager) persistRewriteRound(
	ctx context.Context,
	task *datapb.SplitShardTask,
	result rewriteRoundResult,
	pending map[string]typeutil.Set[int64],
	stillDispatched, dispatchedNow []int64,
	arrived int,
) rewriteRoundResult {
	logger := m.taskLogger(task)
	if _, err := m.store.modify(ctx, m.catalog, task.GetTaskId(), func(t *datapb.SplitShardTask) bool {
		if t.GetState() != datapb.SplitShardTaskState_SplitShardTaskRedistributing {
			return false
		}
		for _, source := range t.GetSources() {
			if remaining, ok := pending[source.GetVchannel()]; ok {
				source.PendingSegments = sortedSegmentIDs(remaining)
			}
		}
		t.DispatchedPlanIds = append(slices.Clone(stillDispatched), dispatchedNow...)
		return true
	}); err != nil {
		logger.Warn(ctx, "persist the shard split rewrite round failed", mlog.Err(err))
		return result
	}
	logger.Info(ctx, "shard split rewrite round",
		mlog.Int("arrived", arrived),
		mlog.Int("dispatched", len(result.dispatched)),
		mlog.Int("completed", len(result.completed)),
		mlog.Int("skipped", result.skipped),
		mlog.Int("inFlight", len(stillDispatched)+len(dispatchedNow)),
		mlog.Int("pending", totalPendingRewrites(pending)))
	return result
}

// pendingRewriteSegments returns the source segments still to rewrite, as the
// task's persisted work list records them.
func pendingRewriteSegments(task *datapb.SplitShardTask) map[string]typeutil.Set[int64] {
	out := make(map[string]typeutil.Set[int64], len(task.GetSources()))
	for _, source := range task.GetSources() {
		out[source.GetVchannel()] = typeutil.NewSet(source.GetPendingSegments()...)
	}
	return out
}

// totalPendingRewrites counts the segments still to rewrite.
func totalPendingRewrites(pending map[string]typeutil.Set[int64]) int {
	total := 0
	for _, segments := range pending {
		total += segments.Len()
	}
	return total
}

// planAlreadyCommitted reports whether a plan that is neither done nor running
// committed before compaction meta cleaned it up: each of its own inputs has
// committed outputs naming it, or is no longer a rewrite input at all -- a
// zero-output commit drops its input with no output to name it. An input
// still there to rewrite means the plan failed. Empty -- a plan record not
// found at all -- is never committed: there is nothing to confirm it against,
// so such a plan counts as lost.
func (m *shardSplitManager) planAlreadyCommitted(ctx context.Context, inputSegments []int64, rewritten typeutil.Set[int64]) bool {
	if len(inputSegments) == 0 {
		return false
	}
	for _, segmentID := range inputSegments {
		if !rewritten.Contain(segmentID) && isRewriteInput(m.meta.GetSegment(ctx, segmentID)) {
			return false
		}
	}
	return true
}

// retireRewrittenSegments removes from pending every segment whose rewrite
// outputs are committed, and returns how many it removed. Judging completion
// from meta, not from the plan report, is what makes a crash between the
// commit and the task write converge.
func retireRewrittenSegments(pending map[string]typeutil.Set[int64], rewritten typeutil.Set[int64]) int {
	retired := 0
	for _, segments := range pending {
		for _, segmentID := range segments.Collect() {
			if rewritten.Contain(segmentID) {
				segments.Remove(segmentID)
				retired++
			}
		}
	}
	return retired
}

// forgetNonInputSegments removes from pending every segment that is no longer a
// rewrite input, and returns how many it removed.
//
// A listed segment stops being an input when it is dropped: by its own rewrite
// commit -- which the lineage scan misses when that rewrite had no non-empty
// output -- or with its collection or partition. A Dropped segment is never
// dispatched, so keeping it listed would hold the task forever.
func forgetNonInputSegments(pending map[string]typeutil.Set[int64], segmentOf func(segmentID int64) *SegmentInfo) int {
	forgotten := 0
	for _, segments := range pending {
		for _, segmentID := range segments.Collect() {
			if isRewriteInput(segmentOf(segmentID)) {
				continue
			}
			segments.Remove(segmentID)
			forgotten++
		}
	}
	return forgotten
}

// adoptNewSourceSegments adds to the work list every rewrite input on a source
// that is neither listed nor already rewritten, and returns how many. The list
// starts empty and is re-scanned every round, so it takes in the segments the
// fence sealed and every import that lands on the source afterwards.
func adoptNewSourceSegments(
	task *datapb.SplitShardTask,
	pending map[string]typeutil.Set[int64],
	rewritten typeutil.Set[int64],
	inputsOf func(vchannel string) []int64,
) int {
	arrived := 0
	for _, source := range task.GetSources() {
		vchannel := source.GetVchannel()
		queued, ok := pending[vchannel]
		if !ok {
			queued = typeutil.NewSet[int64]()
			pending[vchannel] = queued
		}
		for _, segmentID := range inputsOf(vchannel) {
			if queued.Contain(segmentID) || rewritten.Contain(segmentID) {
				continue
			}
			queued.Insert(segmentID)
			arrived++
		}
	}
	return arrived
}

// sortedSegmentIDs gives a set a stable order, so a round dispatches the same
// batch every time it runs over the same work list.
func sortedSegmentIDs(segments typeutil.Set[int64]) []int64 {
	ids := segments.Collect()
	slices.Sort(ids)
	return ids
}

// rewriteInputIDs lists the rewrite inputs currently on a vchannel.
func (m *shardSplitManager) rewriteInputIDs(vchannel string) []int64 {
	segments := m.meta.GetSegmentsByChannel(vchannel)
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		if isRewriteInput(segment) {
			ids = append(ids, segment.GetID())
		}
	}
	return ids
}

// rewrittenSourceSegments returns the source segment ids that have committed
// rewrite outputs on the targets: an output records the segment it was
// rewritten from in its compaction lineage (SegmentInfo.CompactionFrom).
func (m *shardSplitManager) rewrittenSourceSegments(task *datapb.SplitShardTask) typeutil.Set[int64] {
	out := typeutil.NewSet[int64]()
	for _, target := range task.GetTargets() {
		for _, segment := range m.meta.GetSegmentsByChannel(target.GetVchannel()) {
			out.Insert(segment.GetCompactionFrom()...)
		}
	}
	return out
}
