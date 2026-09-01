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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The state machine of a hash-routed (primary-key) shard split.
//
// It is the namespace split's lifecycle with one phase replaced: where the
// namespace task relabels segment metadata (zero rewrite, only possible because
// a namespace's data is confined to one shard), this task REWRITES the source
// shard's data into the two targets, because a hash-routed shard's segments
// straddle every split boundary.
//
//	Preparing -> Fencing -> Rewriting -> Adopting -> Done
//
// Preparing is abortable; everything after the fence is forward-only.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §5.

// advanceHashPreparing seeds the rewrite work list and moves to the fence.
//
// The targets and the residues they take are decided by the trigger when it
// creates the task -- the source's residue set divided, or its last residue cut
// on one more hash bit -- so preparing only has to record which source segments
// the rewrite must process.
// The task is still abortable here: nothing outside datacoord has happened yet.
func (m *shardSplitManager) advanceRewritePreparing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	if m.meta.GetCollection(task.GetCollectionId()) == nil {
		m.abortTask(task, "collection dropped before the write fence")
		return
	}
	if len(task.GetTargets()) < 2 {
		m.abortTask(task, "a hash split needs at least two targets")
		return
	}
	if len(task.GetSources()) == 0 {
		m.abortTask(task, "a hash split needs at least one source")
		return
	}

	if m.vchannelAllocator == nil {
		// Wired during server initialization; a task that ticks before that
		// waits rather than dereferencing nil.
		logger.RatedWarn(m.ctx, 60, "no vchannel allocator wired, hash split preparing stalled")
		return
	}

	// Preempt the in-flight compactions of every source, as the relabel path
	// does for its one source. The enqueue freeze only stops NEW ones; a
	// compaction already running holds its segments in isCompacting, and the
	// rewrite skips those — so without preemption a long compaction (clustering,
	// say) leaves a segment the rewrite can never pick up, and the task cannot
	// drain. The split's own rewrite is exempt from preemption, so this cannot
	// kill work it is about to dispatch. Idempotent per tick.
	if m.preempter != nil {
		for _, source := range task.GetSources() {
			m.preempter.preemptTasksByChannel(source.GetVchannel())
		}
	}

	// Allocate the target vchannels here rather than at detection: allocation
	// consumes cluster-wide pchannel headroom, so it belongs to the task's own
	// lifecycle, where a failure aborts this task instead of the whole scan.
	collection := m.meta.GetCollection(task.GetCollectionId())
	vchannels, err := m.vchannelAllocator.AllocVirtualChannels(m.ctx, balancer.AllocVChannelParam{
		CollectionID:      task.GetCollectionId(),
		Num:               len(task.GetTargets()),
		ExistingVChannels: collection.VChannelNames,
	})
	if err != nil {
		// e.g. not enough pchannels: abort with an alert; the trigger fires
		// again once the headroom recovers.
		m.abortTask(task, "allocate target vchannels failed: "+err.Error())
		return
	}

	// Seed each source's own rewrite work list. Keeping the lists per source
	// rather than flattening them keeps every later per-source check — the
	// fence drain against that source's own T_switch, its import check — able to
	// name the segments it is waiting on.
	pending := make(map[string][]int64, len(task.GetSources()))
	total := 0
	for _, source := range task.GetSources() {
		segments := m.initialPendingSegments(source.GetVchannel())
		pending[source.GetVchannel()] = segments
		total += len(segments)
	}

	if err := m.updateTask(task, func(t *datapb.SplitShardTask) {
		for i := range t.Targets {
			if i < len(vchannels) {
				t.Targets[i].Vchannel = vchannels[i]
			}
		}
		for _, source := range t.Sources {
			source.PendingSegments = pending[source.GetVchannel()]
		}
		t.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	}); err != nil {
		logger.Warn(m.ctx, "persist the prepared hash split task failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "hash split prepared, advance to fencing",
		mlog.Int("sources", len(task.GetSources())),
		mlog.Int("targets", len(vchannels)),
		mlog.Int("sourceSegments", total))
}

// advanceHashFencing fences every source vchannel and creates the targets.
//
// The fences do NOT have to be simultaneous, and there is no distributed commit
// here. What must hold is an ordering: the routing commit must come after every
// source's fence is durable. A target's hash bucket draws keys from every source
// when the shard count changes, so the routing flip is global and cannot be
// applied one source at a time; if it went live while a source still accepted
// writes, a primary key would have two live writers on two WALs with no order
// between them, and a delete could be sequenced before the insert it removes.
//
// That ordering is cheap to guarantee because each fence is a single durable WAL
// append and a re-fence is idempotent, returning the T_switch already recorded
// (streaming.ErrSourceVChannelFenced carries it). So a coordinator crash midway
// re-runs the remaining fences and converges — no source is ever fenced twice
// with two different T_switch values.
//
// The cost is a window: between the first fence and the routing commit, the
// already-fenced sources reject writes while the routing still points at them,
// so those writes fail once the proxy's ShardFenced retries are exhausted. The
// window is kept to about one round trip by fencing all sources concurrently.
func (m *shardSplitManager) advanceFencing(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	collection := m.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		m.abortTask(task, "collection dropped during the write fence")
		return
	}

	// One collection for the whole write switch: the CreateVChannel messages
	// below embed the schema and partition set, and a DDL landing between them
	// and the routing commit would leave the targets born from a shape the
	// collection no longer has.
	lock, err := m.lockCollectionForWriteSwitch(collection)
	if err != nil {
		if isCollectionBusy(err) {
			logger.RatedInfo(m.ctx, 30, "a collection DDL holds the write-switch lock, retrying")
		} else {
			logger.RatedWarn(m.ctx, 30, "cannot take the write-switch lock", mlog.Err(err))
		}
		return
	}
	defer lock.Close()

	if !allHashSourcesFenced(task) {
		m.fenceHashSources(task)
		task = m.mustGetTask(task.GetTaskId())
		if !allHashSourcesFenced(task) {
			// Some source could not be fenced this tick. Retry on the next one:
			// past the first fence the task is forward-only, because a fence
			// marks the vchannel SPLITTED in the streamingnode's recovery info
			// and is never revoked, so there is no state to roll back to.
			logger.RatedWarn(m.ctx, 30, "not every source fenced yet, retrying",
				mlog.Int("fenced", countFencedHashSources(task)),
				mlog.Int("sources", len(task.GetSources())))
			return
		}
		logger.Info(m.ctx, "every source vchannel fenced",
			mlog.Uint64("maxSwitchTimeTick", maxHashSwitchTimeTick(task)))
	}

	// The target vchannels are created strictly after every fence, so a freshly
	// allocated barrier exceeds the greatest T_switch (the TSO is monotonic) and
	// every message of the new WALs lands after all of the fences.
	barrier, err := m.allocator.AllocTimestamp(m.ctx)
	if err != nil {
		logger.Warn(m.ctx, "allocate the barrier timestamp failed", mlog.Err(err))
		return
	}
	if maxTick := maxHashSwitchTimeTick(task); barrier <= maxTick {
		// Monotonic TSO makes this unreachable; it is checked rather than
		// assumed because a barrier at or below a source's fence would let a
		// target's WAL carry a message older than that fence, silently breaking
		// the ordering the whole fence exists to establish.
		logger.Warn(m.ctx, "barrier timestamp does not exceed the fences, retrying",
			mlog.Uint64("barrier", barrier), mlog.Uint64("maxSwitchTimeTick", maxTick))
		return
	}
	genesis, err := streaming.InitSplitTargetVChannels(m.ctx, m.wal, streaming.InitSplitTargetVChannelsParam{
		CollectionID:    task.GetCollectionId(),
		DBID:            collection.DatabaseID,
		DBName:          collection.DatabaseName,
		CollectionName:  collection.Schema.GetName(),
		Schema:          collection.Schema,
		PartitionIDs:    collection.Partitions,
		SplitTaskID:     task.GetTaskId(),
		SourceVChannels: splitSourceVChannels(task),
		BarrierTimeTick: barrier,
		Targets:         allMessageHashSplitTargets(task.GetTargets()),
		// The modulus the targets' residues are taken against. Recorded in the
		// WAL alongside them because the record is permanent while the
		// collection's modulus moves.
		RoutingModulus: task.GetRoutingModulus(),
	})
	if err != nil {
		logger.Warn(m.ctx, "create the target vchannels failed", mlog.Err(err))
		return
	}
	if err := m.seedTargetCheckpoints(genesis); err != nil {
		logger.Warn(m.ctx, "seed the target vchannel checkpoints failed", mlog.Err(err))
		return
	}

	// Routing commit: the targets become write-routable (Creating) and the
	// source becomes fenced and unroutable (Splitting). Idempotent by shard
	// state, so a retry — and a crash before the state advances below — is safe.
	if err := m.commitRouting(task, collection,
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating); err != nil {
		if m.retireOnDroppedCollection(task, err, "collection dropped before the write switch") {
			return
		}
		logger.Warn(m.ctx, "commit the hash split routing failed", mlog.Err(err))
		return
	}

	if err := m.updateTask(task, func(t *datapb.SplitShardTask) {
		t.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	}); err != nil {
		logger.Warn(m.ctx, "persist the fenced hash split task failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "target vchannels created and routing committed, advance to rewriting")
}

// seedTargetCheckpoints records each freshly created target vchannel's genesis
// position as its first channel checkpoint.
//
// Without it a target that receives no live write before adoption has no
// checkpoint at all, and GetChannelSeekPosition falls through to the earliest
// segment's DML position. On such a target the only segments are rewrite
// output, which compaction produced rather than the WAL delivered, so that
// position carries a timestamp but no message id and no WAL name. It is not
// nil, so it is returned and used — and the dispatcher built on it skips its
// Seek, leaving the delegator's streaming adaptor half-built until the first
// read panics the querynode. The likelihood grows with the target count,
// because the more targets a rehash creates the smaller each one's share of the
// live writes: `2 -> 3` never hit it and `3 -> 8` did.
//
// Only channels without a checkpoint are seeded. A retry of the write switch
// re-appends CreateVChannel and gets a LATER position, and writing that over a
// channel that has since been written to would advance the checkpoint past
// messages nobody consumed. Seeding only the empty case makes this idempotent:
// the genesis position is a floor for a channel that has none, never an
// advance for one that has.
func (m *shardSplitManager) seedTargetCheckpoints(genesis []*msgpb.MsgPosition) error {
	unseeded := lo.Filter(genesis, func(pos *msgpb.MsgPosition, _ int) bool {
		return pos != nil && m.meta.GetChannelCheckpoint(pos.GetChannelName()) == nil
	})
	if len(unseeded) == 0 {
		return nil
	}
	return m.meta.UpdateChannelCheckpoints(m.ctx, unseeded)
}

// fenceHashSources appends the SplitShard fence to every source that does not
// yet have one, and persists the T_switch each fence landed at.
//
// The fences run concurrently: they are independent appends to different WALs,
// and every source is unavailable for writes from its own fence until the
// routing commit, so serializing them would multiply that window by the number
// of sources. A source that fails is simply left unfenced for the next tick —
// partial progress is kept, since a landed fence cannot be taken back.
func (m *shardSplitManager) fenceHashSources(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)

	// Close the abort window BEFORE the first append, not after it.
	//
	// The flag records that the fence step has been ENTERED, which is the only
	// honest precondition available: a fence is a durable WAL append followed by
	// a SEPARATE persist of the tick it returns, so a crash in between leaves a
	// fenced vchannel with nothing on the task to show for it. Writing the flag
	// first means a task can never be aborted after an append it cannot account
	// for -- and aborting one would leave that vchannel marked SPLITTED with no
	// task left to finish the split, a shard nothing can write to again.
	//
	// The cost is the mirror case: a crash between this persist and the first
	// append leaves a task that never fenced yet can no longer be abandoned. That
	// is the safe direction of the trade -- the task still runs to completion,
	// where the other direction loses a shard -- and it is why the window a
	// cancel gets is the whole tick before this call, not the instant before the
	// append.
	if !task.GetFenced() {
		if err := m.updateTask(task, func(t *datapb.SplitShardTask) { t.Fenced = true }); err != nil {
			logger.Warn(m.ctx, "persist the fence intent failed, appending no fence this tick", mlog.Err(err))
			return
		}
		task = m.mustGetTask(task.GetTaskId())
	}

	type fenceOutcome struct {
		vchannel       string
		switchTimeTick uint64
	}
	outcomes := make([]fenceOutcome, len(task.GetSources()))

	var wg sync.WaitGroup
	for i, source := range task.GetSources() {
		if source.GetSwitchTimeTick() != 0 {
			continue // already fenced, T_switch recorded
		}
		wg.Add(1)
		go func(idx int, vchannel string) {
			defer wg.Done()
			// The append is idempotent — a retry on an already-fenced vchannel
			// returns ErrSourceVChannelFenced carrying the T_switch recorded by
			// the first fence, so a crash that lost the persisted value still
			// recovers the original tick instead of inventing a second one.
			result, err := streaming.SplitShard(m.ctx, m.wal, streaming.SplitShardParam{
				CollectionID:   task.GetCollectionId(),
				SourceVChannel: vchannel,
				SplitTaskID:    task.GetTaskId(),
				// Only the targets THIS source fronts: the delegator spawns a
				// child per target named here, so sending all of them to every
				// source would front each target once per source.
				Targets:        toMessageHashSplitTargets(task, vchannel),
				RoutingModulus: task.GetRoutingModulus(),
			})
			if err != nil && !errors.Is(err, streaming.ErrSourceVChannelFenced) {
				logger.Warn(m.ctx, "fence a source vchannel failed",
					mlog.String("vchannel", vchannel), mlog.Err(err))
				return
			}
			if result == nil || result.SwitchTimeTick == 0 {
				// Fenced, but T_switch is unknown — the rewrite drain and the
				// routing commit both key off it, so treat this as not yet
				// fenced and re-ask on the next tick rather than proceeding
				// with a zero tick.
				logger.Warn(m.ctx, "source vchannel fenced without a switch time tick",
					mlog.String("vchannel", vchannel))
				return
			}
			outcomes[idx] = fenceOutcome{vchannel: vchannel, switchTimeTick: result.SwitchTimeTick}
		}(i, source.GetVchannel())
	}
	wg.Wait()

	landed := make(map[string]uint64, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.vchannel != "" {
			landed[outcome.vchannel] = outcome.switchTimeTick
		}
	}
	if len(landed) == 0 {
		return
	}

	if err := m.updateTask(task, func(t *datapb.SplitShardTask) {
		for _, source := range t.Sources {
			if tick, ok := landed[source.GetVchannel()]; ok && source.SwitchTimeTick == 0 {
				source.SwitchTimeTick = tick
			}
		}
	}); err != nil {
		// The fences themselves already landed and are durable in their WALs, so
		// losing this write only costs a re-ask on the next tick, which returns
		// the same ticks.
		logger.Warn(m.ctx, "persist the landed fences failed", mlog.Err(err))
	}
}

// countFencedHashSources counts the sources whose fence has been recorded.
func countFencedHashSources(task *datapb.SplitShardTask) int {
	fenced := 0
	for _, source := range task.GetSources() {
		if source.GetSwitchTimeTick() != 0 {
			fenced++
		}
	}
	return fenced
}

// advanceHashRewriting runs one rewrite round: harvest committed plans,
// dispatch a bounded batch of the remaining source segments, and advance to
// Adopting once the source shard is fully rewritten and drained.
func (m *shardSplitManager) advanceRewritingPhase(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	dispatcher := m.hashRewriteDispatcher(task.GetTaskId())
	if dispatcher == nil {
		logger.RatedWarn(m.ctx, 60, "no rewrite dispatcher wired, hash split rewrite stalled")
		return
	}
	batchSize := paramtable.Get().DataCoordCfg.ShardSplitRelabelBatchSize.GetAsInt()
	m.advanceRewriting(task, dispatcher, batchSize)
}

// advanceHashAdopting flips the targets live and completes the task.
//
// The adoption itself is the namespace task's adoption: the targets become
// Normal (querycoord watches them and releases the source) and the source
// becomes Dropped, in one routing commit. What differs is only what the targets
// hold — rewritten segments rather than relabeled ones.
func (m *shardSplitManager) advanceAdopting(task *datapb.SplitShardTask) {
	logger := m.taskLogger(task)
	collection := m.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		// The collection was dropped after the fence; there is nothing left to
		// route. Complete the task so the source-shard freeze is lifted.
		m.finishTask(task, "collection dropped during the rewrite")
		return
	}

	// The targets go live and the source is released, in one routing commit:
	// querycoord picks the targets up, watches them, and only then releases the
	// source. Idempotent by shard state, so a crash before the task reaches Done
	// is resumed by re-committing the same states.
	if err := m.commitRouting(task, collection,
		schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal); err != nil {
		if m.retireOnDroppedCollection(task, err, "collection dropped before the routing was adopted") {
			return
		}
		logger.Warn(m.ctx, "commit the hash split adoption routing failed", mlog.Err(err))
		return
	}

	// Only now, after the sources are Dropped in the routing, may their segments
	// go. The order is what makes the handoff safe: the routing commit is what
	// takes the sources out of the serving topology, and until it lands the
	// source delegator is still the only thing answering for the whole key space.
	// Retiring first would empty a shard that is still being read.
	// Only a rewrite leaves anything to retire. A relabel MOVED its segments to
	// the targets, so the source channel is already empty by the time it drains;
	// retiring on that path could only ever drop something that was not supposed
	// to be there, which is a state to notice rather than to clean up.
	if !rewrites(task) {
		m.finishTask(task, "")
		logger.Info(m.ctx, "split routing adopted, task done")
		return
	}
	if _, err := m.meta.RetireSplitSourceSegments(m.ctx, splitSourceVChannels(task)); err != nil {
		// The routing is already committed and the targets already serve, so the
		// split is functionally done; this only leaves the rewritten-away copies
		// behind. Keep the task in Adopting so the next tick retries the retire
		// rather than declaring it done with the copies still in meta.
		logger.Warn(m.ctx, "retire the hash split source segments failed", mlog.Err(err))
		return
	}
	m.finishTask(task, "")
	logger.Info(m.ctx, "hash split routing adopted, task done")
}

// retireOnDroppedCollection completes a task whose collection rootcoord says is
// gone, and reports whether it did.
//
// The nil check at the top of each phase is not enough on its own: rootcoord
// owns collection existence, and datacoord's meta can still hold a collection
// rootcoord has already dropped. The routing commit is where that disagreement
// surfaces, and retrying cannot resolve it — the collection is not coming back.
//
// Left to retry, the task never reaches a terminal state, so it is never reaped
// and keeps its slot in the cluster-wide concurrency budget forever. With the
// default of one concurrent task that is enough to stop every future split in
// the cluster, and a restart does not clear it: the task is reloaded from meta
// and resumes the same loop. Observed in an E2E run, 69 identical retries deep.
//
// Done rather than Aborted: the task did everything that was asked of it, and
// what it was working on no longer exists. The reason is recorded either way.
func (m *shardSplitManager) retireOnDroppedCollection(task *datapb.SplitShardTask, err error, reason string) bool {
	if !errors.Is(err, merr.ErrCollectionNotFound) {
		return false
	}
	m.finishTask(task, reason)
	return true
}

// finishHashTask marks a task Done and stamps its end time, which also starts
// the retention clock for reaping.
func (m *shardSplitManager) finishTask(task *datapb.SplitShardTask, reason string) {
	logger := m.taskLogger(task)
	if err := m.updateTask(task, func(t *datapb.SplitShardTask) {
		t.State = datapb.SplitShardTaskState_SplitShardTaskDone
		t.EndTime = uint64(time.Now().Unix())
		if reason != "" {
			t.FailReason = reason
		}
	}); err != nil {
		logger.Warn(m.ctx, "persist the done hash split task failed", mlog.Err(err))
		return
	}
	logger.Info(m.ctx, "hash split task done", mlog.String("reason", reason))
}

// hashRewriteDispatcher returns the dispatcher scoped to one task, or nil when
// the compaction inspector has not been wired in yet (the manager is built
// before the inspector, as with the compaction preempter).
func (m *shardSplitManager) hashRewriteDispatcher(taskID int64) rewritePlanDispatcher {
	if m.rewriteDispatcher == nil {
		return nil
	}
	return m.rewriteDispatcher.forTask(taskID)
}

// setRewriteDispatcher wires the hash-split rewrite dispatcher in; called once
// during server initialization, after the compaction inspector exists.
func (m *shardSplitManager) setRewriteDispatcher(d *inspectorRewriteDispatcher) {
	m.rewriteDispatcher = d
}
