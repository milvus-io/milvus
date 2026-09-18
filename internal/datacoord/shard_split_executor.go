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
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// advanceTasks advances every task that is not Done or Aborted by one step.
// Every step is idempotent, so a crash anywhere is resumed by the next tick.
func (m *shardSplitManager) advanceTasks() {
	for _, task := range m.store.list() {
		if isSplitShardTaskActive(task) {
			m.advanceTask(task)
		}
	}
}

// advanceTask runs one step of a task's state machine. Before the fence the
// task may still be aborted; from Fencing on it only moves forward.
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

// splitTargetsAllocated reports whether a task's target vchannels have been
// allocated and persisted. From then on a write switch of the task may be in
// the WAL -- it is issued right after -- so the task is never re-planned: a
// re-issue must present the same id and topology. It is aborted only on a
// refusal made under the collection's keys before any broadcast
// (abortUnfencedTask).
func splitTargetsAllocated(task *datapb.SplitShardTask) bool {
	if len(task.GetTargets()) != 2 {
		return false
	}
	for _, target := range task.GetTargets() {
		if target.GetVchannel() == "" {
			return false
		}
	}
	return true
}

// advancePreparing allocates the targets' vchannels, persists them, and issues
// the write switch. Until the vchannels are persisted nothing outside this
// datacoord has happened, and the task may be aborted.
func (m *shardSplitManager) advancePreparing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	if !paramtable.Get().DataCoordCfg.EnableCompaction.GetAsBool() {
		// The rewrite is dispatched as compaction plans; fenced with compaction
		// off, the split could never move and never abort.
		logger.RatedWarn(m.ctx, 60, "a shard split needs dataCoord.enableCompaction, not fencing it")
		return
	}
	if !splitTargetsAllocated(task) {
		allocated, ok := m.allocateTargets(task)
		if !ok {
			return
		}
		task = allocated
	}
	// The freeze has refused new compactions on the source since the task was
	// created; what is already running on it is killed before the fence, so
	// the split never waits behind a long compaction.
	m.preemptSourceCompactions(task)
	if err := m.coordinator.issueShardSplit(m.ctx, task, m.controlChannel()); err != nil {
		if m.finishOnDroppedCollection(task, err, "collection dropped before the write switch") {
			return
		}
		if errors.Is(err, errSplitRefusedBeforeBroadcast) && !merr.IsRetryableErr(err) {
			// Refused under the keys before any broadcast, and nothing a retry
			// can change: the source is fenced nowhere, so the task aborts and
			// frees its slot instead of retrying forever.
			m.abortUnfencedTask(task, "the write switch was refused: "+err.Error())
			return
		}
		logger.RatedWarn(m.ctx, 30, "issue the shard split write switch failed, retrying", mlog.Err(err))
		return
	}
	if _, err := m.store.modify(m.ctx, m.catalog, task.GetTaskId(), func(t *datapb.SplitShardTask) bool {
		// The ack callback may already have moved the record past Preparing,
		// which is further along than Fencing and must not be dragged back.
		if t.GetState() != datapb.SplitShardTaskState_SplitShardTaskPreparing {
			return false
		}
		t.State = datapb.SplitShardTaskState_SplitShardTaskFencing
		return true
	}); err != nil {
		// The broadcast is out; losing this write only re-issues the same
		// broadcast, which its idempotency key resolves to the first one.
		logger.Warn(m.ctx, "persist the fencing shard split task failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "shard split write switch issued, waiting for its ack callback")
}

// allocateTargets allocates the two target vchannels next to the collection's
// current ones and persists them. It reports false when the task is not ready
// to be issued: it waits, or it was aborted.
func (m *shardSplitManager) allocateTargets(task *datapb.SplitShardTask) (*datapb.SplitShardTask, bool) {
	logger := m.taskLogger(task)
	coll, err := m.coordinator.describeSplitCollection(m.ctx, task.GetCollectionId())
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			m.abortTask(task, "collection dropped before the write switch")
			return nil, false
		}
		logger.RatedWarn(m.ctx, 30, "describe the collection to allocate split targets failed, retrying", mlog.Err(err))
		return nil, false
	}
	if reason := splitRefusalReason(coll.schema); reason != "" {
		// Changed since planning; nothing of the task is in any WAL yet.
		m.abortTask(task, reason)
		return nil, false
	}
	vchannels, err := m.vchannelAllocator.AllocVirtualChannels(m.ctx, balancer.AllocVChannelParam{
		CollectionID:      task.GetCollectionId(),
		Num:               len(task.GetTargets()),
		ExistingVChannels: m.knownVChannels(coll),
	})
	if err != nil {
		// Not enough free pchannels, say: nothing is fenced yet, so the task is
		// aborted and the trigger fires again once there is headroom.
		m.abortTask(task, "allocate the target vchannels failed: "+err.Error())
		return nil, false
	}
	if len(vchannels) != len(task.GetTargets()) {
		m.abortTask(task, "the allocator returned the wrong number of target vchannels")
		return nil, false
	}
	allocated, err := m.store.modify(m.ctx, m.catalog, task.GetTaskId(), func(t *datapb.SplitShardTask) bool {
		if t.GetState() != datapb.SplitShardTaskState_SplitShardTaskPreparing || splitTargetsAllocated(t) {
			return false
		}
		for i := range t.GetTargets() {
			t.Targets[i].Vchannel = vchannels[i]
		}
		return true
	})
	if err != nil {
		logger.Warn(m.ctx, "persist the split targets failed", mlog.Err(err))
		return nil, false
	}
	if allocated.GetState() != datapb.SplitShardTaskState_SplitShardTaskPreparing || !splitTargetsAllocated(allocated) {
		return nil, false
	}
	logger.Info(m.ctx, "shard split targets allocated", mlog.Strings("allocatedTargets", splitTaskTargetVChannels(allocated)))
	return allocated, true
}

// knownVChannels is the collection's vchannels as the allocator must see them:
// the ones it lists, plus every source and target a split of it that is not
// Done or Aborted names. The allocator excludes their pchannels and continues
// the shard index after the largest of them, so a target name is never reused
// -- QueryCoord's window rules key on vchannel names and would read a reused
// one as the shard it once was.
//
// The list alone is not enough: another split's targets are allocated and
// persisted before its write switch lists them, and their pchannels are taken
// from then on. Finished splits add nothing: a Done split's targets are listed
// (or were retired in favor of later, higher-numbered targets), and the source
// it retired carries a lower index than those targets, so the largest index
// ever used is always among the names above. Leaving the retired sources out is
// also what lets their pchannels be used again.
func (m *shardSplitManager) knownVChannels(coll *splitCollection) []string {
	known := typeutil.NewSet(coll.VirtualChannelNames...)
	for _, task := range m.store.list() {
		if task.GetCollectionId() != coll.CollectionID || !isSplitShardTaskActive(task) {
			continue
		}
		for _, vchannel := range append(splitSourceVChannels(task), splitTaskTargetVChannels(task)...) {
			if vchannel != "" {
				known.Insert(vchannel)
			}
		}
	}
	names := known.Collect()
	slices.Sort(names)
	return names
}

// advanceFencing waits for the write switch's ack callback, which records the
// fence and moves the task to Redistributing on every cluster the broadcast
// reaches. The manager writes nothing here. The one exception is a collection
// rootcoord reports dropped: the callback ignores a split of a collection that
// is gone, so nothing else would ever end the task.
func (m *shardSplitManager) advanceFencing(task *datapb.SplitShardTask) {
	if _, ok := m.liveCollection(task, "collection dropped during the write switch"); !ok {
		return
	}
	m.taskLogger(task).RatedInfo(m.ctx, 60, "waiting for the shard split ack callback to record the fence")
}

// splitFenceRecorded reports whether the ack callback has recorded the fence:
// the task is fenced and its source carries the tick the fence landed on.
func splitFenceRecorded(task *datapb.SplitShardTask) bool {
	if !task.GetFenced() || len(task.GetSources()) != 1 {
		return false
	}
	return task.GetSources()[0].GetSwitchTimeTick() != 0
}

// advanceRedistributing runs one redistribution round of a fenced task.
//
// Nothing moves before the source is past its T_switch (design doc §6.3 step
// 2.1): from then on its WAL is closed and every segment the fence sealed is
// in meta, so a round sees the source's final L0 set.
func (m *shardSplitManager) advanceRedistributing(task *datapb.SplitShardTask) {
	if _, ok := m.liveCollection(task, "collection dropped during redistribution"); !ok {
		return
	}
	logger := m.taskLogger(task)
	if !splitFenceRecorded(task) {
		// The ack callback writes Redistributing, the fence and the tick
		// together, so no callback produced this record. Nothing moves before
		// the fence is known, and the task cannot abort past it.
		logger.RatedError(m.ctx, 60, "a redistributing shard split task carries no recorded fence, waiting")
		return
	}
	m.preemptSourceCompactions(task)
	if reason := m.coordinator.fenceFlushBlockReason(task); reason != "" {
		logger.RatedInfo(m.ctx, 60, "shard split redistribution waits for its source to pass the fence", mlog.String("reason", reason))
		return
	}
	if m.redistributor != nil {
		m.redistributor.redistribute(m.ctx, task)
	} else {
		logger.RatedWarn(m.ctx, 60, "no shard split redistribution is wired, only an empty source drains")
	}
	// The drain is the adoption gate's own predicate (CheckShardSplitDrained):
	// asking anything weaker would only issue an adoption whose callback then
	// waits, holding the collection's keys.
	if reason := m.coordinator.splitDrainBlockReason(m.ctx, task); reason != "" {
		logger.RatedInfo(m.ctx, 60, "shard split redistribution in progress", mlog.String("waitingOn", reason))
		return
	}
	if _, err := m.store.modify(m.ctx, m.catalog, task.GetTaskId(), func(t *datapb.SplitShardTask) bool {
		if t.GetState() != datapb.SplitShardTaskState_SplitShardTaskRedistributing {
			return false
		}
		t.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
		return true
	}); err != nil {
		logger.Warn(m.ctx, "persist the adopting shard split task failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "shard split source drained, adopting its targets")
}

// liveCollection reads the task's collection from rootcoord. A dropped
// collection finishes the task and reports false; so does a failed read, which
// the next tick repeats.
func (m *shardSplitManager) liveCollection(task *datapb.SplitShardTask, droppedReason string) (*splitCollection, bool) {
	coll, err := m.coordinator.describeSplitCollection(m.ctx, task.GetCollectionId())
	if err != nil {
		if !m.finishOnDroppedCollection(task, err, droppedReason) {
			m.taskLogger(task).RatedWarn(m.ctx, 30, "describe the shard split's collection failed, waiting", mlog.Err(err))
		}
		return nil, false
	}
	return coll, true
}

// finishOnDroppedCollection finishes a task whose collection rootcoord reports
// gone, and reports whether it did. Retrying cannot help -- the collection is
// not coming back -- and a task that never ends holds its slot in the
// cluster-wide concurrency cap forever. Done rather than Aborted: past the
// fence there is no abort, and what the task was working on no longer exists.
// The reason is recorded either way.
func (m *shardSplitManager) finishOnDroppedCollection(task *datapb.SplitShardTask, err error, reason string) bool {
	if !errors.Is(err, merr.ErrCollectionNotFound) {
		return false
	}
	m.finishTask(task, reason)
	return true
}

// finishTask moves a task to Done and stamps its end time. The record is kept:
// records are never removed (design doc §5).
func (m *shardSplitManager) finishTask(task *datapb.SplitShardTask, reason string) {
	finished, err := m.store.modify(m.ctx, m.catalog, task.GetTaskId(), func(t *datapb.SplitShardTask) bool {
		if !isSplitShardTaskActive(t) {
			return false
		}
		t.State = datapb.SplitShardTaskState_SplitShardTaskDone
		t.EndTime = uint64(time.Now().Unix())
		t.FailReason = reason
		return true
	})
	if err != nil {
		m.taskLogger(task).Warn(m.ctx, "persist the done shard split task failed", mlog.Err(err))
		return
	}
	m.recordTerminalMetrics(finished)
	m.taskLogger(finished).Info(m.ctx, "shard split task done", mlog.String("reason", reason))
}

// abortTask aborts a task that is still Preparing with no target vchannel
// persisted: nothing of it can be in any WAL. Anything later is refused.
func (m *shardSplitManager) abortTask(task *datapb.SplitShardTask, reason string) {
	m.abortIf(task, reason, func(t *datapb.SplitShardTask) bool { return !splitTargetsAllocated(t) })
}

// abortUnfencedTask aborts a task still Preparing whose targets are allocated
// but whose write switch was refused before any broadcast. The record is
// re-checked under the task's lock: a source fenced (a T_switch recorded, or
// the task Fenced or past Preparing) is never aborted.
//
// The abandoned target names are harmless: the vchannel allocator persists
// nothing (it derives names from the collection's known vchannels), the names
// were never listed in the collection meta, so no querycoord has seen them,
// and an Aborted task no longer reserves them (knownVChannels), so a later
// split may reuse them. Their checkpoints and channel marks are written only
// by the ack callback, which never ran.
func (m *shardSplitManager) abortUnfencedTask(task *datapb.SplitShardTask, reason string) {
	m.abortIf(task, reason, func(t *datapb.SplitShardTask) bool {
		for _, source := range t.GetSources() {
			if source.GetSwitchTimeTick() != 0 {
				return false
			}
		}
		return true
	})
}

// abortIf aborts a task still Preparing and not fenced, when allowed also
// holds for its latest record.
func (m *shardSplitManager) abortIf(task *datapb.SplitShardTask, reason string, allowed func(t *datapb.SplitShardTask) bool) {
	aborted, err := m.store.modify(m.ctx, m.catalog, task.GetTaskId(), func(t *datapb.SplitShardTask) bool {
		if t.GetState() != datapb.SplitShardTaskState_SplitShardTaskPreparing || t.GetFenced() || !allowed(t) {
			return false
		}
		t.State = datapb.SplitShardTaskState_SplitShardTaskAborted
		t.EndTime = uint64(time.Now().Unix())
		t.FailReason = reason
		return true
	})
	if err != nil {
		m.taskLogger(task).Warn(m.ctx, "persist the aborted shard split task failed", mlog.Err(err))
		return
	}
	if aborted.GetState() != datapb.SplitShardTaskState_SplitShardTaskAborted {
		m.taskLogger(aborted).Error(m.ctx, "refuse to abort a shard split task whose write switch may already be issued",
			mlog.String("reason", reason))
		return
	}
	m.recordTerminalMetrics(aborted)
	m.taskLogger(aborted).Info(m.ctx, "shard split task aborted", mlog.String("reason", reason))
}

func (m *shardSplitManager) taskLogger(task *datapb.SplitShardTask) *mlog.Logger {
	return mlog.With(
		mlog.FieldComponent("shard-split-manager"),
		mlog.Int64("taskID", task.GetTaskId()),
		mlog.FieldCollectionID(task.GetCollectionId()),
		mlog.String("source", splitTaskSource(task)),
		mlog.Strings("targets", splitTaskTargetVChannels(task)),
		mlog.String("state", task.GetState().String()))
}
