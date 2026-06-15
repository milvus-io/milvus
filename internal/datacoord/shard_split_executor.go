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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// advanceTasks advances every active split task by one step.
// The task FSM is forward-only after the write fence; every step is
// idempotent so a crash at any point is resumed by the next tick.
func (m *shardSplitManager) advanceTasks() {
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if isSplitShardTaskActive(task) {
			m.advanceTask(task)
		}
		return true
	})
}

func (m *shardSplitManager) advanceTask(task *datapb.SplitShardTask) {
	switch task.GetState() {
	case datapb.SplitShardTaskState_SplitShardTaskPreparing:
		m.advancePreparing(task)
	case datapb.SplitShardTaskState_SplitShardTaskFencing:
		m.advanceFencing(task)
	case datapb.SplitShardTaskState_SplitShardTaskRedistributing:
		m.advanceRedistributing(task)
	case datapb.SplitShardTaskState_SplitShardTaskAdopting:
		// The adoption flip — the target shards become Normal, the source
		// shard becomes Dropped and the routing version is bumped in the
		// collection meta, all in one transaction — belongs to the
		// authoritative routing table milestone. TODO: issue #50463.
		// On the flip to Done, call m.recordTerminalMetrics(task) to record
		// the outcome counter and the split duration.
	}
}

// advancePreparing allocates the target vchannels and plans the split point.
// The task is still abortable in this state: no external side effect exists
// until the write fence.
func (m *shardSplitManager) advancePreparing(task *datapb.SplitShardTask) {
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
		m.preempter.preemptTasksByChannel(task.GetSourceVchannel())
	}

	vchannels, err := m.vchannelAllocator.AllocVirtualChannels(m.ctx, balancer.AllocVChannelParam{
		CollectionID:      task.GetCollectionId(),
		Num:               2,
		ExistingVChannels: collection.VChannelNames,
	})
	if err != nil {
		// e.g. not enough pchannels: skip this split (abort) with an alert,
		// the trigger will fire again when the headroom recovers.
		logger.Warn("allocate target vchannels failed, abort the split task", zap.Error(err))
		m.abortTask(task, err.Error())
		return
	}

	targets, err := m.planner.PlanTargets(m.ctx, collection, task.GetSourceVchannel(), vchannels)
	if err != nil {
		// the planner may not be ready (e.g. statistics still loading);
		// stay in Preparing and retry on the next tick.
		logger.RatedWarn(60, "plan split targets failed, stay in preparing", zap.Error(err))
		return
	}

	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.Targets = targets
		task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	}); err != nil {
		logger.Warn("persist the planned split task failed", zap.Error(err))
		return
	}
	logger.Info("split targets planned, advance to fencing", zap.Int("targets", len(targets)))
}

// advanceFencing executes the write switch: it fences the source vchannel
// (ManualFlush + SplitShard, T_switch persisted right after), then
// initializes the target vchannels with BarrierTimeTick = T_switch.
func (m *shardSplitManager) advanceFencing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	collection := m.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		m.abortTask(task, "collection dropped during the write fence")
		return
	}

	if task.GetSwitchTimeTick() == 0 {
		flushTs, err := m.allocator.AllocTimestamp(m.ctx)
		if err != nil {
			logger.Warn("allocate the flush timestamp failed", zap.Error(err))
			return
		}
		result, err := streaming.SplitShard(m.ctx, m.wal, streaming.SplitShardParam{
			CollectionID:   task.GetCollectionId(),
			SourceVChannel: task.GetSourceVchannel(),
			SplitTaskID:    task.GetTaskId(),
			FlushTs:        flushTs,
			Targets:        toMessageSplitTargets(task.GetTargets()),
		})
		if errors.Is(err, streaming.ErrSourceVChannelFenced) {
			// fenced by a previous attempt but T_switch was lost before it
			// was persisted; recovering it from the vchannel meta of the
			// streamingnode is not wired yet.
			logger.Warn("source vchannel already fenced but T_switch is unknown, manual intervention required", zap.Error(err))
			return
		}
		if err != nil {
			logger.Warn("fence the source vchannel failed", zap.Error(err))
			return
		}
		// persist T_switch before initializing the targets, so a crash here
		// resumes with the recorded T_switch instead of hitting the fence.
		if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
			task.SwitchTimeTick = result.SwitchTimeTick
		}); err != nil {
			logger.Warn("persist T_switch failed", zap.Error(err))
			return
		}
		task = m.mustGetTask(task.GetTaskId())
		logger.Info("source vchannel fenced",
			zap.Uint64("switchTimeTick", result.SwitchTimeTick),
			zap.Int64s("flushedSegments", result.FlushedSegmentIDs))
	}

	if err := streaming.InitSplitTargetVChannels(m.ctx, m.wal, streaming.InitSplitTargetVChannelsParam{
		CollectionID:    task.GetCollectionId(),
		DBID:            collection.DatabaseID,
		DBName:          collection.DatabaseName,
		CollectionName:  collection.Schema.GetName(),
		Schema:          collection.Schema,
		PartitionIDs:    collection.Partitions,
		SplitTaskID:     task.GetTaskId(),
		SourceVChannel:  task.GetSourceVchannel(),
		SwitchTimeTick:  task.GetSwitchTimeTick(),
		TargetVChannels: splitTargetVChannels(task.GetTargets()),
	}); err != nil {
		logger.Warn("initialize the target vchannels failed", zap.Error(err))
		return
	}

	// TODO: register the target vchannels into the collection meta vchannel
	// list and reconcile the partitions created during the window
	// (authoritative routing table milestone, issue #50463).

	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	}); err != nil {
		logger.Warn("persist the fenced split task failed", zap.Error(err))
		return
	}
	logger.Info("target vchannels initialized, advance to redistributing")
}

// advanceRedistributing relabels one batch of the source shard's segments to
// their target shards. It runs in rounds: every tick picks up the segments
// visible at that time (including the ones flushed by the fence), until the
// source shard has none left.
func (m *shardSplitManager) advanceRedistributing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	segments := m.meta.GetSegmentsByChannel(task.GetSourceVchannel())
	if len(segments) == 0 {
		if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
			task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
		}); err != nil {
			logger.Warn("persist the redistributed split task failed", zap.Error(err))
			return
		}
		logger.Info("every segment of the source shard redistributed, advance to adopting")
		return
	}

	batchSize := paramtable.Get().DataCoordCfg.ShardSplitRelabelBatchSize.GetAsInt()
	operators := make([]UpdateOperator, 0, batchSize)
	relabeled := make([]int64, 0, batchSize)
	skippedCompacting := 0
	for _, segment := range segments {
		if len(operators) >= batchSize {
			break
		}
		if segment.isCompacting {
			// Defensive: the preemption of advancePreparing plus the enqueue
			// freeze should leave no compacting segment on the source shard;
			// a leftover one is skipped and retried on the next round.
			skippedCompacting++
			continue
		}
		idx, err := m.planner.AssignSegment(m.ctx, segment, task.GetTargets())
		if err != nil {
			logger.Warn("assign a segment to the split targets failed",
				zap.Int64("segmentID", segment.GetID()), zap.Error(err))
			return
		}
		operators = append(operators, UpdateInsertChannelOperator(segment.GetID(), task.GetTargets()[idx].GetVchannel()))
		relabeled = append(relabeled, segment.GetID())
	}
	if skippedCompacting > 0 {
		logger.Warn("skipped compacting segments during relabel, retry on the next round",
			zap.Int("skipped", skippedCompacting))
	}
	if len(operators) == 0 {
		return
	}
	if err := m.meta.UpdateSegmentsInfo(m.ctx, operators...); err != nil {
		logger.Warn("relabel a batch of segments failed", zap.Error(err))
		return
	}
	logger.Info("relabeled a batch of segments", zap.Int64s("segmentIDs", relabeled))
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
	if task.GetSwitchTimeTick() > 0 {
		m.taskLogger(task).Error("refuse to abort a split task past the write fence", zap.String("reason", reason))
		return
	}
	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.State = datapb.SplitShardTaskState_SplitShardTaskAborted
		task.FailReason = reason
		task.EndTime = uint64(time.Now().Unix())
	}); err != nil {
		m.taskLogger(task).Warn("persist the aborted split task failed", zap.Error(err))
		return
	}
	m.recordTerminalMetrics(m.mustGetTask(task.GetTaskId()))
	m.taskLogger(task).Info("split task aborted", zap.String("reason", reason))
}

func (m *shardSplitManager) mustGetTask(taskID int64) *datapb.SplitShardTask {
	task, ok := m.tasks.Get(taskID)
	if !ok {
		panic("the split task disappeared from the cache, there is a bug in the shard split manager")
	}
	return task
}

func (m *shardSplitManager) taskLogger(task *datapb.SplitShardTask) *log.MLogger {
	return log.Ctx(m.ctx).With(
		log.FieldComponent("shard-split-manager"),
		zap.Int64("taskID", task.GetTaskId()),
		zap.Int64("collectionID", task.GetCollectionId()),
		zap.String("sourceVChannel", task.GetSourceVchannel()),
		zap.String("state", task.GetState().String()))
}

// toMessageSplitTargets converts the persisted targets to the message form.
func toMessageSplitTargets(targets []*datapb.SplitShardTaskTarget) []*message.SplitShardTarget {
	converted := make([]*message.SplitShardTarget, 0, len(targets))
	for _, target := range targets {
		converted = append(converted, &message.SplitShardTarget{
			Vchannel: target.GetVchannel(),
			KeyRange: &message.KeyRange{
				Lower: target.GetRoutingKeyLower(),
				Upper: target.GetRoutingKeyUpper(),
			},
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
