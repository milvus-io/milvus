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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
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
		m.taskLogger(task).Warn("reap the terminal split task failed, will retry", zap.Error(err))
		return
	}
	m.tasks.Remove(task.GetTaskId())
	m.taskLogger(task).Info("reaped the terminal split task")
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
		m.advanceAdopting(task)
	}
}

// advanceAdopting performs the adoption flip and completes the task: the target
// shards become Normal (visible to querycoord, which then watches them and
// releases the source) and the source shard becomes Dropped, in one routing
// commit. The commit is idempotent by shard state, so a crash before the task
// reaches Done is resumed by re-committing the same states.
func (m *shardSplitManager) advanceAdopting(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	collection := m.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		// The collection was dropped after the fence; there is nothing left to
		// route. Complete the task so the source-shard freeze is lifted.
		if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
			task.State = datapb.SplitShardTaskState_SplitShardTaskDone
			task.EndTime = uint64(time.Now().Unix())
		}); err != nil {
			logger.Warn("persist the done split task failed", zap.Error(err))
			return
		}
		m.recordTerminalMetrics(m.mustGetTask(task.GetTaskId()))
		return
	}

	if err := m.commitRouting(task, collection, schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal); err != nil {
		logger.Warn("commit the adoption routing failed", zap.Error(err))
		return
	}

	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.State = datapb.SplitShardTaskState_SplitShardTaskDone
		task.EndTime = uint64(time.Now().Unix())
	}); err != nil {
		logger.Warn("persist the adopted split task failed", zap.Error(err))
		return
	}
	m.recordTerminalMetrics(m.mustGetTask(task.GetTaskId()))
	logger.Info("split routing adopted, task done")
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

// advanceFencing executes the write switch: it fences the source vchannel with
// a single SplitShard message, then creates the target vchannels with a freshly
// allocated barrier time tick (always greater than T_switch). It records
// T_switch on the task so the redistribution drain can gate on the source
// channel checkpoint reaching it.
func (m *shardSplitManager) advanceFencing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	collection := m.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		m.abortTask(task, "collection dropped during the write fence")
		return
	}

	// TODO(#50463): hold the Broadcaster's ExclusiveCollectionName resource key
	// across the fence -> create -> routing-commit section (design §6.1, approach
	// "fence-as-lock-holding-broadcast" + creates as plain appends under the held
	// lock) so no collection DDL (AlterCollection/CreatePartition) can change the
	// schema or partition snapshot that the CreateVChannel messages embed. This
	// is deferred to the authoritative routing table because (1) the lock's
	// release point is the routing-commit that #50463 delivers, (2) crash-safe
	// recovery needs the lock-holding broadcast id persisted on the task — a
	// re-broadcast would otherwise self-deadlock on the orphaned resource lock,
	// and (3) the protected write window is not live until routing exists. Until
	// then the fence and create below use plain appends.
	if !task.GetFenced() {
		// A single SplitShard message fences the source vchannel: the source
		// streamingnode auto-flushes the growing segments and embeds their ids.
		// The append is idempotent — a retry on an already-fenced vchannel
		// returns ErrSourceVChannelFenced, which is success: the fence holds and
		// the barrier below is freshly allocated, so no exact T_switch is needed.
		result, err := streaming.SplitShard(m.ctx, m.wal, streaming.SplitShardParam{
			CollectionID:   task.GetCollectionId(),
			SourceVChannel: task.GetSourceVchannel(),
			SplitTaskID:    task.GetTaskId(),
			Targets:        toMessageSplitTargets(task.GetTargets()),
		})
		if err != nil && !errors.Is(err, streaming.ErrSourceVChannelFenced) {
			logger.Warn("fence the source vchannel failed", zap.Error(err))
			return
		}
		// Record T_switch (the SplitShard message's time tick) so the
		// redistribution drain can wait for the flusher to catch up to it. A
		// fresh fence carries it on the append result; an already-fenced retry
		// carries the recorded T_switch back on the ShardFenced error, so a
		// crash that lost the persisted value still recovers it on re-fence.
		var switchTimeTick uint64
		if result != nil {
			switchTimeTick = result.SwitchTimeTick
		}
		// persist the fenced flag before creating the targets, so a crash here
		// resumes forward-only (abort is refused once the source is fenced).
		if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
			task.Fenced = true
			if switchTimeTick != 0 {
				task.SwitchTimeTick = switchTimeTick
			}
		}); err != nil {
			logger.Warn("persist the fenced flag failed", zap.Error(err))
			return
		}
		task = m.mustGetTask(task.GetTaskId())
		logger.Info("source vchannel fenced")
	}

	// The new vchannels are created strictly after the fence: a freshly
	// allocated barrier is always greater than T_switch (the TSO is monotonic),
	// so every message of the new WALs lands after the fence.
	barrier, err := m.allocator.AllocTimestamp(m.ctx)
	if err != nil {
		logger.Warn("allocate the barrier timestamp failed", zap.Error(err))
		return
	}
	// The target vchannels are ordinary vchannels: their genesis checkpoint is
	// reported to the channel-checkpoint store by the streamingnode when it opens
	// the new WAL, so no consume start position needs to be persisted on the task
	// — the child delegators read it back through the normal recovery seek path.
	if err := streaming.InitSplitTargetVChannels(m.ctx, m.wal, streaming.InitSplitTargetVChannelsParam{
		CollectionID:    task.GetCollectionId(),
		DBID:            collection.DatabaseID,
		DBName:          collection.DatabaseName,
		CollectionName:  collection.Schema.GetName(),
		Schema:          collection.Schema,
		PartitionIDs:    collection.Partitions,
		SplitTaskID:     task.GetTaskId(),
		SourceVChannel:  task.GetSourceVchannel(),
		BarrierTimeTick: barrier,
		Targets:         toMessageSplitTargets(task.GetTargets()),
	}); err != nil {
		logger.Warn("create the target vchannels failed", zap.Error(err))
		return
	}

	// Routing commit: register the target vchannels into the collection meta and
	// switch routing. The source shard becomes Splitting (fenced, unroutable for
	// new writes) and the targets become Creating (write-routable, read-invisible
	// to querycoord until adoption). Idempotent by shard state, so a retry — and a
	// crash before the state advances below — is safe.
	if err := m.commitRouting(task, collection, schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating); err != nil {
		logger.Warn("commit the split routing failed", zap.Error(err))
		return
	}

	if err := m.updateTask(task, func(task *datapb.SplitShardTask) {
		task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	}); err != nil {
		logger.Warn("persist the fenced split task failed", zap.Error(err))
		return
	}
	logger.Info("target vchannels created and routing committed, advance to redistributing")
}

// advanceRedistributing relabels one batch of the source shard's segments to
// their target shards. It runs in rounds: every tick picks up the segments
// visible at that time (including the ones flushed by the fence), until the
// source shard has none left.
func (m *shardSplitManager) advanceRedistributing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	segments := m.meta.GetSegmentsByChannel(task.GetSourceVchannel())
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
	if len(segments) == 0 && m.fenceFlushed(task) && !m.hasActiveImportOnVChannel(task.GetSourceVchannel()) {
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
			logger.Warn("assign a segment to the split targets failed, skipping it this round",
				zap.Int64("segmentID", segment.GetID()), zap.Error(err))
			skipped++
			continue
		}
		operators = append(operators, UpdateInsertChannelOperator(segment.GetID(), task.GetTargets()[idx].GetVchannel()))
		relabeled = append(relabeled, segment.GetID())
	}
	if skipped > 0 {
		logger.Warn("skipped compacting/importing/unroutable segments during relabel, retry on the next round",
			zap.Int("skipped", skipped))
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
	if task.GetFenced() {
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
// rangeShardInfoPB builds a CollectionShardInfo carrying the range-routing
// predicate; the routing oneof is left unset when ranges is empty.
func rangeShardInfoPB(state schemapb.ShardState, lastTruncateTimeTick uint64, ranges []*schemapb.RoutingKeyRange) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state, LastTruncateTimeTick: lastTruncateTimeTick}
	if len(ranges) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_RangeRouting{RangeRouting: &schemapb.RangeRouting{Ranges: ranges}}
	}
	return si
}

func (m *shardSplitManager) commitRouting(task *datapb.SplitShardTask, collection *collectionInfo, sourceState, targetState schemapb.ShardState) error {
	targets := task.GetTargets()
	targetByVChannel := make(map[string]*datapb.SplitShardTaskTarget, len(targets))
	for _, target := range targets {
		targetByVChannel[target.GetVchannel()] = target
	}

	// the full new vchannel list = the collection's current vchannels plus any
	// target not already present (a target is already present on a retry after a
	// prior commit, e.g. the adoption commit after the write-switch commit).
	vchannels := make([]string, len(collection.VChannelNames))
	copy(vchannels, collection.VChannelNames)
	for _, target := range targets {
		if !slices.Contains(vchannels, target.GetVchannel()) {
			vchannels = append(vchannels, target.GetVchannel())
		}
	}

	pchannels := make([]string, len(vchannels))
	shardInfos := make([]*schemapb.CollectionShardInfo, len(vchannels))
	for i, vchannel := range vchannels {
		pchannels[i] = funcutil.ToPhysicalChannel(vchannel)
		switch {
		case vchannel == task.GetSourceVchannel():
			// the source is fenced (Splitting) then released (Dropped); it owns no
			// routing range any more, so it carries only the state.
			shardInfos[i] = &schemapb.CollectionShardInfo{State: sourceState}
		case targetByVChannel[vchannel] != nil:
			target := targetByVChannel[vchannel]
			shardInfos[i] = rangeShardInfoPB(targetState, 0, []*schemapb.RoutingKeyRange{
				{Lower: target.GetRoutingKeyLower(), Upper: target.GetRoutingKeyUpper()},
			})
		default:
			// a pre-existing shard that is neither the source nor a target:
			// carry its current ranges and state through unchanged so a split of
			// a multi-shard collection keeps the other shards' routing intact.
			if info, ok := collection.ShardInfos[vchannel]; ok {
				shardInfos[i] = rangeShardInfoPB(info.GetState(), info.GetLastTruncateTimeTick(), info.GetRangeRouting().GetRanges())
			} else {
				shardInfos[i] = &schemapb.CollectionShardInfo{State: schemapb.ShardState_ShardNormal}
			}
		}
	}

	return m.router.CommitShardSplitRouting(m.ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               collection.DatabaseName,
		CollectionName:       collection.Schema.GetName(),
		CollectionId:         task.GetCollectionId(),
		VirtualChannelNames:  vchannels,
		PhysicalChannelNames: pchannels,
		ShardInfos:           shardInfos,
		RoutingMode:          schemapb.RoutingMode_RoutingModeRange,
	})
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
