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
// would replace rewrite outputs the source's view presents, before the
// cross-channel lineage and the index fallback on targets are tested. Every
// compaction path -- mix, L0, clustering, sort, manual, an import's sort step
// -- funnels through enqueueCompaction, which refuses it. A task created
// before the freeze is caught where it would run: the schedule loop drops a
// frozen task instead of dispatching it (and re-checks under the executing
// guard, so a freeze landing after the dequeue is seen either there or by the
// preemption), loadMeta preempts a frozen task it revives from the catalog
// after a restart, and a compaction already running on the source when the
// split starts is preempted (preemptTasksByChannel).

// compactionPreempter kills the queued and executing compactions of a
// channel; implemented by the compaction inspector.
type compactionPreempter interface {
	preemptTasksByChannel(channel string)
}

// setChannelSplittingChecker installs the freeze predicate.
func (c *compactionInspector) setChannelSplittingChecker(checker func(channel string) bool) {
	c.isChannelSplitting = checker
}

// exemptFromSplitFreeze reports whether a compaction may run on a splitting
// channel.
//
// An import's own sort step is exempt. It rewrites segments the import is
// still committing (IsImporting): no reader and no redistribution sees them
// until the import finishes, and the split cannot finish before the import
// does -- its drain waits for every unfinished import on the source. Frozen,
// the import would never leave its sort step and the split would never drain.
func (c *compactionInspector) exemptFromSplitFreeze(task *datapb.CompactionTask) bool {
	if task.GetType() != datapb.CompactionType_SortCompaction || len(task.GetInputSegments()) == 0 {
		return false
	}
	for _, segmentID := range task.GetInputSegments() {
		segment := c.meta.GetHealthySegment(context.TODO(), segmentID)
		if segment == nil || !segment.GetIsImporting() {
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
