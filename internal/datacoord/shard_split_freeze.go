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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// The compaction freeze of a shard split (design doc §8.2).
//
// From the moment a split task exists until it is Done, the split's source
// and its targets take no compaction but the split's own rewrite: a
// compaction on the source would replace the segments the rewrite reads and
// fold the source's L0 deletes into L1 behind its back, and one on a target
// would replace segments a child delegator tells apart by id. Every
// compaction path -- mix, L0, clustering, sort, manual, an import's sort step
// -- funnels through enqueueCompaction, which refuses it. A task created
// before the freeze is caught where it would run: the schedule loop drops a
// frozen task instead of dispatching it (and re-checks under the executing
// guard, so a freeze landing after the dequeue is seen either there or by the
// preemption), loadMeta preempts a frozen task it revives from the catalog
// after a restart, and a compaction already running on the source when the
// split starts is preempted (preemptTasksByChannel).
//
// One compaction on a target is let through: the sort of a rewrite output
// (exemptFromSplitFreeze). An output of an unsorted input is published
// unsorted, and the index and stats inspectors skip an unsorted segment, so
// frozen it would be served brute-force from raw vectors for the whole window.

// compactionPreempter kills the queued and executing compactions of a
// channel; implemented by the compaction inspector.
type compactionPreempter interface {
	preemptTasksByChannel(channel string)
}

// setChannelSplittingChecker installs the freeze predicate.
func (c *compactionInspector) setChannelSplittingChecker(checker func(channel string) bool) {
	c.isChannelSplitting = checker
}

// setChannelSplitTargetChecker installs the predicate that tells a split's
// targets from its source (shardSplitManager.IsVChannelSplitTarget).
func (c *compactionInspector) setChannelSplitTargetChecker(checker func(channel string) bool) {
	c.isChannelSplitTarget = checker
}

// exemptFromSplitFreeze reports whether a compaction may run on a splitting
// channel.
//
// The split's own rewrite is exempt: it IS the redistribution, dispatched by
// the split task on the source channel by construction, and freezing or
// preempting it would undo the split's work as fast as it is dispatched.
//
// An import's own sort step is exempt too. It rewrites segments the import is
// still committing (IsImporting): no reader and no redistribution sees them
// until the import finishes, and the split cannot finish before the import
// does -- its drain waits for every unfinished import on the source. Frozen,
// the import would never leave its sort step and the split would never drain.
//
// So is the sort of rewrite outputs on a target (sortsRewriteOutputsOnTarget).
func (c *compactionInspector) exemptFromSplitFreeze(task *datapb.CompactionTask) bool {
	switch task.GetType() {
	case datapb.CompactionType_HashSplitCompaction:
		return true
	case datapb.CompactionType_SortCompaction:
		return c.allInputsAre(task, (*SegmentInfo).GetIsImporting) || c.sortsRewriteOutputsOnTarget(task)
	default:
		return false
	}
}

// sortsRewriteOutputsOnTarget reports whether a sort runs on a split's target
// over nothing but rewrite outputs.
//
// A rewrite output holds rows no target WAL ever carried, so no child
// delegator holds a copy of it: it reaches the reads only through the
// source's view, which takes in its sorted replacement and drops it at one
// target version, exactly as an ordinary sort replaces a segment. A segment
// flushed from a target WAL is different: the child consuming that WAL still
// serves its rows as growing and skips them only by segment id once the
// source serves them sealed, and a sort gives them a new id -- so it stays
// frozen until Done.
//
// On a target, a compaction output is a rewrite output: nothing else compacts
// there while the split is in flight, and the target did not exist before it.
// An invisible one is a compaction's staging output, not a rewrite's, and is
// not let through. A source is never a target here, even when it is a target
// of an earlier split still in flight.
//
// And only an output whose rewrite has fully committed: every parent it names
// is Dropped. A rewrite commit that takes the chunked catalog path and is torn
// between chunks leaves an output published while its input is still Flushed,
// and the recovery view then serves the input and hides the output
// (retrieveSegment keeps the parents while they are all present). A sort of
// that output would be a child whose only parent is gone, so the view would
// serve it next to the input and read the rows of that half twice. The
// re-run of the rewrite drops the input, and the sort is let through then.
func (c *compactionInspector) sortsRewriteOutputsOnTarget(task *datapb.CompactionTask) bool {
	if c.isChannelSplitTarget == nil || !c.isChannelSplitTarget(task.GetChannel()) {
		return false
	}
	return c.allInputsAre(task, func(segment *SegmentInfo) bool {
		return isVisibleCompactionOutput(segment) && c.allParentsDropped(segment)
	})
}

// allParentsDropped reports whether every segment a compaction output names in
// its lineage is Dropped or already gone from meta.
func (c *compactionInspector) allParentsDropped(segment *SegmentInfo) bool {
	for _, parentID := range segment.GetCompactionFrom() {
		parent := c.meta.GetSegment(context.TODO(), parentID)
		if parent != nil && parent.GetState() != commonpb.SegmentState_Dropped {
			return false
		}
	}
	return true
}

// isVisibleCompactionOutput reports whether a segment was written by a
// compaction and is visible.
func isVisibleCompactionOutput(segment *SegmentInfo) bool {
	return segment.GetCreatedByCompaction() && !segment.GetIsInvisible()
}

// allInputsAre reports whether the task has inputs and every one of them is a
// healthy segment satisfying pred.
func (c *compactionInspector) allInputsAre(task *datapb.CompactionTask, pred func(*SegmentInfo) bool) bool {
	if len(task.GetInputSegments()) == 0 {
		return false
	}
	for _, segmentID := range task.GetInputSegments() {
		segment := c.meta.GetHealthySegment(context.TODO(), segmentID)
		if segment == nil || !pred(segment) {
			return false
		}
	}
	return true
}

// frozenBySplit reports whether a split in flight on the task's channel
// forbids the compaction.
func (c *compactionInspector) frozenBySplit(task *datapb.CompactionTask) bool {
	if c.isChannelSplitting == nil || !c.isChannelSplitting(task.GetChannel()) {
		return false
	}
	return !c.exemptFromSplitFreeze(task)
}

// preemptTasksByChannel stops every queued and executing compaction of the
// channel that the freeze forbids.
//
// Each victim is removed from the inspector AND aborted in the global
// scheduler (AbortAndRemoveTask), which drops it on its worker and stops
// polling it. Removing it from the inspector alone is not enough: the
// scheduler would keep querying the worker and, once the worker finished,
// commit the result -- a mix on the source past its fence, or an L0 that
// retires the source's L0s before the split has folded them. The abort takes
// the scheduler's per-task lock, so it waits for a check already polling the
// task and no check runs after it; only then is the task cleaned, its cleaned
// state persisted (it is not revived from the catalog after a restart) and
// its inputs' compacting flags released (a segment locked by a dead task never
// starves the split's redistribution). A result a check committed before the
// abort took the lock is committed; the freeze exists from the moment the task
// is created, so that is a compaction already running before the split
// started, and the fence comes after the preemption.
func (c *compactionInspector) preemptTasksByChannel(channel string) {
	ctx := context.TODO()
	victim := func(task CompactionTask) bool {
		return task.GetTaskProto().GetChannel() == channel && !c.exemptFromSplitFreeze(task.GetTaskProto())
	}
	preempted := make([]CompactionTask, 0)
	c.queueTasks.RemoveAll(func(task CompactionTask) bool {
		if !victim(task) {
			return false
		}
		preempted = append(preempted, task)
		decPendingCompaction(task)
		return true
	})

	c.executingGuard.Lock()
	for id, task := range c.executingTasks {
		if victim(task) {
			preempted = append(preempted, task)
			delete(c.executingTasks, id)
			metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", task.GetTaskProto().GetNodeID()), task.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		}
	}
	c.executingGuard.Unlock()

	for _, task := range preempted {
		c.scheduler.AbortAndRemoveTask(task.GetTaskProto().GetPlanID())
		c.cleanPreemptedTask(ctx, task, "compaction task preempted by a shard split")
	}
}

// decPendingCompaction takes a task that leaves the queue off the Pending
// gauge, under the label submitTask counted it under: NullNodeID, whatever
// node the task names.
func decPendingCompaction(task CompactionTask) {
	metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), task.GetTaskProto().GetType().String(), metrics.Pending).Dec()
}

// dropFrozenQueuedTask drops a task the schedule loop dequeued on a channel a
// split freezes. It was never handed to the global scheduler, so there is
// nothing to abort on a worker; it is cleaned like a preempted one.
func (c *compactionInspector) dropFrozenQueuedTask(task CompactionTask) {
	decPendingCompaction(task)
	c.cleanPreemptedTask(context.TODO(), task, "queued compaction task dropped, a shard split froze its channel")
}

// cleanPreemptedTask cleans a task taken off a frozen channel: its cleaned
// state is persisted (it is not revived from the catalog after a restart) and
// its inputs' compacting flags are released. A failed clean is left to the
// clean loop.
func (c *compactionInspector) cleanPreemptedTask(ctx context.Context, task CompactionTask, msg string) {
	channel := task.GetTaskProto().GetChannel()
	mlog.Info(ctx, msg,
		mlog.String("channel", channel),
		mlog.Int64("planID", task.GetTaskProto().GetPlanID()),
		mlog.String("type", task.GetTaskProto().GetType().String()),
		mlog.String("state", task.GetTaskProto().GetState().String()))
	if !task.Clean() {
		mlog.Warn(ctx, "clean the preempted compaction task failed, the clean loop retries it",
			mlog.String("channel", channel), mlog.Int64("planID", task.GetTaskProto().GetPlanID()))
		c.cleaningGuard.Lock()
		c.cleaningTasks[task.GetTaskProto().GetPlanID()] = task
		c.cleaningGuard.Unlock()
	}
}

// IsVChannelSplitting reports whether a split task that is not Done or
// Aborted names the vchannel as its source or one of its targets. The
// compaction freeze, the garbage collector's dropped-segment hold and the
// trigger's exclusion all read it, so all three last exactly until Done.
func (m *shardSplitManager) IsVChannelSplitting(vchannel string) bool {
	return m.hasActiveTaskOnVChannel(vchannel)
}

// IsVChannelSplitTarget reports whether a split task that is not Done or
// Aborted names the vchannel as one of its targets and none names it as its
// source. The compaction freeze lets the sort of a rewrite output through on
// such a channel only.
func (m *shardSplitManager) IsVChannelSplitTarget(vchannel string) bool {
	source, target := m.store.activeSplitRoles(vchannel)
	return target && !source
}

// setCompactionPreempter wires the compaction inspector in.
func (m *shardSplitManager) setCompactionPreempter(preempter compactionPreempter) {
	m.preempter = preempter
}

// preemptSourceCompactions kills what is compacting the task's source. It runs
// before the write switch and on every redistribution tick -- the latter so a
// secondary, whose task starts at Redistributing, preempts too. Idempotent:
// once the freeze holds nothing new arrives to preempt.
func (m *shardSplitManager) preemptSourceCompactions(task *datapb.SplitShardTask) {
	if m.preempter == nil {
		return
	}
	if source := splitTaskSource(task); source != "" {
		m.preempter.preemptTasksByChannel(source)
	}
}
