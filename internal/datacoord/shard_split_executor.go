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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// advanceTasks advances every active split task by one step.
// The task FSM is forward-only after the write fence; every step is
// idempotent so a crash at any point is resumed by the next tick.
func (m *shardSplitManager) advanceTasks() {
	retention := paramtable.Get().DataCoordCfg.ShardSplitTaskRetention.GetAsDuration(time.Second)
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if isSplitShardTaskActive(task) {
			m.advanceTask(task)
			return true
		}
		m.reapTerminalTask(task, retention)
		return true
	})
}

// reapTerminalTask drops a Done/Aborted task from meta and the in-memory cache
// once it has been terminal for the retention window, so completed splits do
// not accumulate as permanent etcd keys (reloaded and iterated on every restart
// and every tick). Reaping is retried on the next tick if the meta delete fails.
func (m *shardSplitManager) reapTerminalTask(task *datapb.SplitShardTask, retention time.Duration) {
	endTime := task.GetEndTime()
	if endTime == 0 || time.Since(time.Unix(int64(endTime), 0)) < retention {
		return
	}
	if err := m.catalog.DropSplitShardTask(m.ctx, task); err != nil {
		m.taskLogger(task).Warn(m.ctx, "reap the terminal split task failed, will retry", mlog.Err(err))
		return
	}
	m.tasks.Remove(task.GetTaskId())
	m.taskLogger(task).Info(m.ctx, "reaped the terminal split task")
}

// advanceTask runs one tick of a split task's state machine.
//
// Both kinds of split share it. The redistribution phase is the only one that
// differs, and `redistribution` selects which of the two ways to move the data
// this task uses.
func (m *shardSplitManager) advanceTask(task *datapb.SplitShardTask) {
	switch task.GetState() {
	case datapb.SplitShardTaskState_SplitShardTaskPreparing:
		m.advancePreparing(task)
	case datapb.SplitShardTaskState_SplitShardTaskFencing:
		m.advanceFencing(task)
	case datapb.SplitShardTaskState_SplitShardTaskRedistributing:
		m.advanceRedistributing(task)
	case datapb.SplitShardTaskState_SplitShardTaskAdopting:
		m.advanceAdopting(task)
	}
}

// rewrites reports whether this task moves its data by rewriting it, rather
// than by relabeling the segments' owning vchannel.
//
// Only an explicit SplitShardRewrite rewrites. Anything else -- including an
// unset field -- relabels, which is what this task type did before it carried a
// strategy at all, so a task written by an older binary keeps its behavior.
func rewrites(task *datapb.SplitShardTask) bool {
	return task.GetRedistribution() == datapb.SplitShardRedistribution_SplitShardRewrite
}

// advancePreparing allocates the target vchannels and plans the split point.
// The task is still abortable in this state: no external side effect exists
// until the write fence.
func (m *shardSplitManager) advancePreparing(task *datapb.SplitShardTask) {
	if rewrites(task) {
		// A rewriting split had its targets and their hash buckets decided by the
		// trigger, so preparing only seeds the work list; there is no split point
		// to search for.
		m.advanceRewritePreparing(task)
		return
	}
	logger := m.taskLogger(task)
	collection := m.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		m.abortTask(task, "collection dropped before the write fence")
		return
	}

	// Preempt the in-flight compaction of the source shard, so the split
	// never waits behind a long compaction (e.g. clustering): the enqueue
	// freeze rejects new tasks from the moment this task exists, and the
	// preemption kills the queued/executing ones. Idempotent per tick.
	if m.preempter != nil {
		m.preempter.preemptTasksByChannel(firstSourceVChannel(task))
	}

	vchannels, err := m.vchannelAllocator.AllocVirtualChannels(m.ctx, balancer.AllocVChannelParam{
		CollectionID:      task.GetCollectionId(),
		Num:               2,
		ExistingVChannels: collection.VChannelNames,
	})
	if err != nil {
		// e.g. not enough pchannels: skip this split (abort) with an alert,
		// the trigger will fire again when the headroom recovers.
		logger.Warn(m.ctx, "allocate target vchannels failed, abort the split task", mlog.Err(err))
		m.abortTask(task, err.Error())
		return
	}

	targets, routingModulus, err := m.planner.PlanTargets(m.ctx, collection, firstSourceVChannel(task), vchannels)
	if err != nil {
		// the planner may not be ready (e.g. statistics still loading);
		// stay in Preparing and retry on the next tick.
		logger.RatedWarn(m.ctx, 60, "plan split targets failed, stay in preparing", mlog.Err(err))
		return
	}

	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.Targets = targets
		task.RoutingModulus = routingModulus
		task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	}); err != nil {
		logger.Warn(m.ctx, "persist the planned split task failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "split targets planned, advance to fencing", mlog.Int("targets", len(targets)))
}

// advanceRedistributing relabels one batch of the source shard's segments to
// their target shards. It runs in rounds: every tick picks up the segments
// visible at that time (including the ones flushed by the fence), until the
// source shard has none left.
func (m *shardSplitManager) advanceRedistributing(task *datapb.SplitShardTask) {
	// A collection dropped mid-redistribution leaves nothing to redistribute,
	// and this phase never reaches a routing commit, so the check that retires
	// the other phases cannot help here. Without it the task neither drains nor
	// finishes: it re-dispatches a rewrite for segments that no longer exist,
	// forever, holding a slot in the cluster-wide concurrency budget and
	// stopping every future split. Observed as 426 consecutive
	// "rewrite plan lost, will re-dispatch" for a collection already gone.
	if m.meta.GetCollection(task.GetCollectionId()) == nil {
		m.finishTask(task, "collection dropped during redistribution")
		return
	}
	if rewrites(task) {
		m.advanceRewritingPhase(task)
		return
	}
	logger := m.taskLogger(task)
	segments := m.meta.GetSegmentsByChannel(firstSourceVChannel(task))
	// The source shard is drained only when three datacoord-local conditions
	// hold: no segment remains on the source vchannel, the flusher has flushed
	// the fence-sealed segments up to T_switch, AND no active import job targets
	// it.
	//   - fenceFlushed closes the async-flush window: the SplitShard fence only
	//     appends a message; the streamingnode flusher seals and reports the
	//     sealed segments to datacoord asynchronously afterwards. Without this
	//     guard the empty scan can pass before those segments are reported, and
	//     they would land on the just-dropped source as orphans.
	//   - the import conjunct closes a second blind window: a job still in
	//     Pending/PreImporting has registered no segment in meta yet, so the
	//     segment scan cannot see it, and it could otherwise allocate its
	//     segments onto the just-dropped shard after this empty check passed.
	if len(segments) == 0 && m.fenceFlushed(task) && !m.hasActiveImportOnVChannel(firstSourceVChannel(task)) {
		if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
			task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
		}); err != nil {
			logger.Warn(m.ctx, "persist the redistributed split task failed", mlog.Err(err))
			return
		}
		logger.Info(m.ctx, "every segment of the source shard redistributed, advance to adopting")
		return
	}

	batchSize := paramtable.Get().DataCoordCfg.ShardSplitRelabelBatchSize.GetAsInt()
	operators := make([]UpdateOperator, 0, batchSize)
	relabeled := make([]int64, 0, batchSize)
	skipped := 0
	for _, segment := range segments {
		if len(operators) >= batchSize {
			break
		}
		if segment.isCompacting {
			// Defensive: the preemption of advancePreparing plus the enqueue
			// freeze should leave no compacting segment on the source shard;
			// a leftover one is skipped and retried on the next round.
			skipped++
			continue
		}
		if segment.GetIsImporting() {
			// An import worker is still committing this segment's binlogs
			// through meta updates; relabeling it mid-import would race with
			// those writes. It is picked up once it is flushed (the drain
			// check keeps the task in Redistributing until then).
			skipped++
			continue
		}
		idx, err := m.planner.AssignSegment(m.ctx, segment, task.GetTargets())
		if err != nil {
			// One unroutable segment must not wedge the whole task — abort is
			// illegal past the fence. Skip it (operator-visible via this warn
			// and the unblocked-task metric), keep relabeling the rest, and
			// retry it next round: the planner refreshes its partition-key
			// cache on a miss, so a namespace added mid-redistribution becomes
			// routable instead of pinning the task in Redistributing forever.
			logger.Warn(m.ctx, "assign a segment to the split targets failed, skipping it this round",
				mlog.Int64("segmentID", segment.GetID()), mlog.Err(err))
			skipped++
			continue
		}
		operators = append(operators, UpdateInsertChannelOperator(segment.GetID(), task.GetTargets()[idx].GetVchannel()))
		relabeled = append(relabeled, segment.GetID())
	}
	if skipped > 0 {
		logger.Warn(m.ctx, "skipped compacting/importing/unroutable segments during relabel, retry on the next round",
			mlog.Int("skipped", skipped))
	}
	if len(operators) == 0 {
		return
	}
	if err := m.meta.UpdateSegmentsInfo(m.ctx, operators...); err != nil {
		logger.Warn(m.ctx, "relabel a batch of segments failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "relabeled a batch of segments", mlog.Int64s("segmentIDs", relabeled))
}

// updateTask clones the task, applies the mutation, persists it and then
// replaces the cached entry. The persisted state is the source of truth.
func (m *shardSplitManager) updateTask(task *datapb.SplitShardTask, mutate func(*datapb.SplitShardTask)) error {
	cloned := proto.Clone(task).(*datapb.SplitShardTask)
	mutate(cloned)
	if err := m.catalog.SaveSplitShardTask(m.ctx, cloned); err != nil {
		return err
	}
	m.tasks.Insert(cloned.GetTaskId(), cloned)
	return nil
}

// abortTask aborts a split task. Abort is only legal before the write fence.
func (m *shardSplitManager) abortTask(task *datapb.SplitShardTask, reason string) {
	if task.GetFenced() {
		m.taskLogger(task).Error(m.ctx, "refuse to abort a split task past the write fence", mlog.String("reason", reason))
		return
	}
	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.State = datapb.SplitShardTaskState_SplitShardTaskAborted
		task.FailReason = reason
		task.EndTime = uint64(time.Now().Unix())
	}); err != nil {
		m.taskLogger(task).Warn(m.ctx, "persist the aborted split task failed", mlog.Err(err))
		return
	}
	m.recordTerminalMetrics(m.mustGetTask(task.GetTaskId()))
	m.taskLogger(task).Info(m.ctx, "split task aborted", mlog.String("reason", reason))
}

func (m *shardSplitManager) mustGetTask(taskID int64) *datapb.SplitShardTask {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		panic("the split task disappeared from the cache, there is a bug in the shard split manager")
	}
	return task
}

func (m *shardSplitManager) taskLogger(task *datapb.SplitShardTask) *mlog.Logger {
	return mlog.With(
		mlog.FieldComponent("shard-split-manager"),
		mlog.String("splitKind", splitTaskKind(task)),
		mlog.Int64("taskID", task.GetTaskId()),
		mlog.Int64("collectionID", task.GetCollectionId()),
		mlog.Strings("sourceVChannels", splitSourceVChannels(task)),
		mlog.String("state", task.GetState().String()))
}

// commitRouting commits the split's routing change into the collection meta via
// rootcoord: the source shard moves to sourceState and every target shard to
// targetState (carrying its key range), and the collection switches to range
// routing. The full post-split topology is sent; rootcoord applies it
// idempotently by shard state, so a retry is safe.
//
// The full topology is built from the collection's current shard infos (read
// from DescribeCollection): the source and the two targets get their new state
// and ranges, and every other pre-existing shard is carried through with its
// current range and state — so splitting one shard of a multi-shard collection
// leaves the rest untouched.
// residueShardInfoPB builds a CollectionShardInfo carrying the shard's
// residues; the routing oneof is left unset when it owns none, which is what a
// fenced or dropped split source looks like.
func residueShardInfoPB(state schemapb.ShardState, lastTruncateTimeTick uint64, residues []uint64) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state, LastTruncateTimeTick: lastTruncateTimeTick}
	if len(residues) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: append([]uint64(nil), residues...)},
		}
	}
	return si
}

// toMessageSplitTargets converts the persisted targets to the message form.
func toMessageSplitTargets(targets []*datapb.SplitShardTaskTarget) []*message.SplitShardTarget {
	converted := make([]*message.SplitShardTarget, 0, len(targets))
	for _, target := range targets {
		converted = append(converted, &message.SplitShardTarget{
			Vchannel: target.GetVchannel(),
			Routing:  &schemapb.HashRouting{Buckets: append([]uint64(nil), target.GetBuckets()...)},
		})
	}
	return converted
}

func splitTargetVChannels(targets []*datapb.SplitShardTaskTarget) []string {
	vchannels := make([]string, 0, len(targets))
	for _, target := range targets {
		vchannels = append(vchannels, target.GetVchannel())
	}
	return vchannels
}
