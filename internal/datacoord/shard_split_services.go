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
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CommitShardSplit is the datacoord half of the SplitShard broadcast ack
// callback.
//
// The broadcast reaches every replica's coordinator, so this runs on the
// primary that planned the split AND on secondaries that have never heard of
// it. It is therefore idempotent by split task id: an absent task is created
// outright, already in Redistributing, because the fence it acknowledges has
// by definition already landed.
//
// For a task that IS present, the request's T_switch fills a tick the task
// does not carry yet: the primary wrote its task before it broadcast, with no
// tick, and the tick the broadcast landed on is the one the write path fenced
// at. A tick already recorded is kept -- T_switch is the tick of the task's
// first fence, which a redelivery reports again -- and a redelivery reporting
// another one is logged, not applied. The
// state only ever moves forward: Preparing/Fencing advance to Redistributing,
// and a task already Adopting or Done is left where it is, because a
// redelivered callback must not drag a split back into a window it has left.
// An Aborted task -- which a fenced split can only be through a bug -- is
// logged at Error and rolled forward to Redistributing: the fence has landed.
//
// Errors are System: the blame for a catalog write failure, a checkpoint write
// failure or a malformed callback never lies with the request's content, and the
// caller is a coordinator callback that has nothing to fix in it either way. A
// malformed callback is ServiceInternal. A store failure keeps the code it
// arrived with (shardSplitStoreError), so a typed transient failure stays
// retriable on the wire; a bare one, which carries no code, is ServiceInternal.
// The only caller, rootcoord's SplitShard ack callback, is retried by the
// broadcaster whatever the code.
func (s *Server) CommitShardSplit(ctx context.Context, req *datapb.CommitShardSplitRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	logger := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.Int64("splitTaskID", req.GetSplitTaskId()))

	// The record is read (validate, merge) and then written (upsert), and a
	// second writer of the same id -- a redelivered callback, or a planner
	// that writes the task on the primary -- must not read between the two,
	// or its upsert drops the fields this one merged in. The lock is per task
	// id, so commits of different splits do not wait on each other.
	s.shardSplitTasks.lockTask(req.GetSplitTaskId())
	defer s.shardSplitTasks.unlockTask(req.GetSplitTaskId())

	if err := s.validateCommitShardSplit(req); err != nil {
		logger.Warn(ctx, "refused a malformed shard split commit", mlog.Err(err))
		return merr.Status(err), nil
	}

	task := s.mergeCommittedShardSplit(ctx, req)
	if err := s.shardSplitTasks.upsert(ctx, s.meta.catalog, task); err != nil {
		logger.Warn(ctx, "persist the committed shard split task failed", mlog.Err(err))
		return merr.Status(shardSplitStoreError(err, "persist the committed shard split task %d", req.GetSplitTaskId())), nil
	}

	// The mark goes before the checkpoint, as in WatchChannels: a target with
	// a checkpoint but no mark is a target the GC guard does not protect.
	if err := s.markSplitTargetsAdded(ctx, req); err != nil {
		logger.Warn(ctx, "mark the split targets added failed", mlog.Err(err))
		return merr.Status(shardSplitStoreError(err, "mark the split targets of task %d added", req.GetSplitTaskId())), nil
	}

	if err := s.seedSplitTargetCheckpoints(ctx, req); err != nil {
		logger.Warn(ctx, "seed the split targets' genesis checkpoints failed", mlog.Err(err))
		return merr.Status(shardSplitStoreError(err, "seed the split targets of task %d", req.GetSplitTaskId())), nil
	}

	// A truncate may already be waiting on a source whose checkpoint is frozen
	// at the T_switch this commit has just recorded.
	s.meta.NotifyChannelCheckpointWatchers()

	logger.Info(ctx, "recorded a committed shard split",
		mlog.Strings("sources", splitSourceVChannels(task)),
		mlog.Any("state", task.GetState()))
	return merr.Success(), nil
}

// shardSplitStoreError adds context to a catalog or checkpoint write failure
// without masking its code.
//
// merr.Wrap keeps the inner error's code and retriability, which is what a
// typed failure needs: WrapErrServiceInternalErr would relabel a retriable one
// (ServiceUnavailable, a timeout) as non-retriable ServiceInternal. A bare error
// -- what the etcd kv returns today -- carries no code to keep, and merr.Wrap
// would let it reach the wire as Unexpected, indistinguishable from an
// unhandled bug; it is classified System here instead.
func shardSplitStoreError(err error, format string, args ...any) error {
	if merr.IsMilvusError(err) || merr.IsCanceledOrTimeout(err) {
		return merr.Wrapf(err, format, args...)
	}
	return merr.WrapErrServiceInternalErr(err, format, args...)
}

// validateCommitShardSplit refuses a callback that cannot be acted on, before
// anything is written.
//
// This is the only malformed-input check either RPC has, and it is deliberately
// minimal: the sender is another coordinator, so anything wrong here is a Milvus
// bug rather than a user's, and the class stays System. What it refuses:
//
//   - task id zero, which the streamingnode fence uses as its "no split task"
//     sentinel (design doc §7).
//     Recording it would persist a real task under that id and make every later
//     "is there a task" question answer yes;
//   - anything but exactly one source and two targets. A shard split fences one
//     shard and creates two; no sources in particular would create a task that
//     reports drained the moment it is asked, retiring shards on the strength
//     of an empty loop;
//   - a source that disagrees with the one a task already stored under this id
//     names. A redelivery carries the same source, so a different one is two
//     splits sharing a task id, not a source to append;
//   - a collection id that disagrees with a task already stored under this id.
//     mergeCommittedShardSplit never overwrites CollectionId, so this would
//     otherwise be silently ignored and the two coordinators would carry
//     different records under the same id;
//   - a genesis position that meta.UpdateChannelCheckpoints would drop on the
//     floor. That filter logs a warning and returns nil, so without this check
//     the target is left unseeded while the callback reports success --- and an
//     unseeded target is exactly the state the seeding exists to prevent.
func (s *Server) validateCommitShardSplit(req *datapb.CommitShardSplitRequest) error {
	if req.GetSplitTaskId() == 0 {
		return merr.WrapErrServiceInternalMsg("shard split commit carries no split task id")
	}
	if len(req.GetSources()) != 1 || len(req.GetTargets()) != 2 {
		return merr.WrapErrServiceInternalMsg(
			"shard split commit for task %d names %d source and %d target shards, a shard split has exactly 1 and 2",
			req.GetSplitTaskId(), len(req.GetSources()), len(req.GetTargets()))
	}
	if existing, ok := s.shardSplitTasks.get(req.GetSplitTaskId()); ok {
		if existing.GetCollectionId() != req.GetCollectionId() {
			return merr.WrapErrServiceInternalMsg(
				"shard split commit for task %d claims collection %d, but the recorded task belongs to collection %d",
				req.GetSplitTaskId(), req.GetCollectionId(), existing.GetCollectionId())
		}
		source := req.GetSources()[0].GetVchannel()
		for _, recorded := range existing.GetSources() {
			if recorded.GetVchannel() != source {
				return merr.WrapErrServiceInternalMsg(
					"shard split commit for task %d names source %s, but the recorded task fences %s",
					req.GetSplitTaskId(), source, recorded.GetVchannel())
			}
		}
	}
	targets := splitTargetVChannels(req.GetTargets())
	for _, position := range req.GetTargetStartPositions() {
		if position.GetChannelName() == "" {
			return merr.WrapErrServiceInternalMsg(
				"shard split commit for task %d carries a genesis position with no channel name", req.GetSplitTaskId())
		}
		if !targets.Contain(position.GetChannelName()) {
			// Not this split's business to seed; ignored, not refused.
			continue
		}
		// Mirrors meta.UpdateChannelCheckpoints' own filter: WoodPecker
		// serializes a zero message id to nil, every other WAL does not.
		if position.GetMsgID() == nil && position.GetWALName() != commonpb.WALName_WoodPecker {
			return merr.WrapErrServiceInternalMsg(
				"shard split commit for task %d carries a genesis position with no message id for channel %s",
				req.GetSplitTaskId(), position.GetChannelName())
		}
	}
	return nil
}

// splitTargetVChannels is the set of vchannel names the targets name.
func splitTargetVChannels(targets []*datapb.SplitShardTaskTarget) typeutil.Set[string] {
	out := typeutil.NewSet[string]()
	for _, target := range targets {
		out.Insert(target.GetVchannel())
	}
	return out
}

// mergeCommittedShardSplit produces the task record the request implies,
// without touching the store.
func (s *Server) mergeCommittedShardSplit(ctx context.Context, req *datapb.CommitShardSplitRequest) *datapb.SplitShardTask {
	existing, ok := s.shardSplitTasks.get(req.GetSplitTaskId())
	if !ok {
		return &datapb.SplitShardTask{
			TaskId:         req.GetSplitTaskId(),
			CollectionId:   req.GetCollectionId(),
			Sources:        cloneSplitSources(req.GetSources()),
			Targets:        cloneSplitTargets(req.GetTargets()),
			RoutingModulus: req.GetRoutingModulus(),
			State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
			// The callback only fires once the fence has been appended, so a
			// task learned of here is fenced by construction and can no longer
			// be aborted.
			Fenced:    true,
			StartTime: uint64(time.Now().Unix()),
		}
	}

	task := proto.Clone(existing).(*datapb.SplitShardTask)
	task.Sources = mergeCommittedSplitSources(ctx, req.GetSplitTaskId(), task.GetSources(), req.GetSources())
	if len(task.GetTargets()) == 0 {
		task.Targets = cloneSplitTargets(req.GetTargets())
	}
	if task.GetRoutingModulus() == 0 {
		task.RoutingModulus = req.GetRoutingModulus()
	}
	task.Fenced = true
	switch task.GetState() {
	case datapb.SplitShardTaskState_SplitShardTaskUnknown,
		datapb.SplitShardTaskState_SplitShardTaskPreparing,
		datapb.SplitShardTaskState_SplitShardTaskFencing:
		task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	case datapb.SplitShardTaskState_SplitShardTaskAborted:
		// An abort is refused once a write switch may be in the WAL, so this
		// is a bug. The fence has landed all the same: the source takes no
		// more writes, and only the split carries its rows on. A fenced split
		// rolls forward, never stays Aborted.
		mlog.Error(ctx, "a fenced shard split commit found its task aborted, rolling it forward",
			mlog.Int64("splitTaskID", req.GetSplitTaskId()),
			mlog.FieldCollectionID(req.GetCollectionId()),
			mlog.String("abortReason", task.GetFailReason()))
		task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
		task.EndTime = 0
		task.FailReason = ""
	}
	return task
}

// mergeCommittedSplitSources merges a commit's sources into the recorded ones.
//
// validateCommitShardSplit has already refused a request whose one source
// disagrees with the recorded one, so the request's source either IS the
// recorded source or the record has none yet (a task written before its fence
// was planned). Everything the recorded source holds is kept: the commit
// carries only the source's name and tick, and replacing the entry wholesale
// would erase the fields other writers of the record own on every
// redelivery, and the fields a newer build wrote that this one does not know.
//
// That includes its switch time tick once one is recorded. T_switch is the
// tick of the task's first fence (design doc §6.1 step 3): a same-task
// re-fence reports that tick again, and the drain and the flush-state checks
// may already have been answered against it. A redelivery that reports
// another tick is therefore logged and not applied. A recorded tick of zero --
// the planner's record, written before its broadcast -- takes the request's.
func mergeCommittedSplitSources(ctx context.Context, taskID int64, recorded, committed []*datapb.SplitShardTaskSource) []*datapb.SplitShardTaskSource {
	merged := make([]*datapb.SplitShardTaskSource, 0, len(committed))
	for _, source := range committed {
		idx := slices.IndexFunc(recorded, func(r *datapb.SplitShardTaskSource) bool {
			return r.GetVchannel() == source.GetVchannel()
		})
		if idx < 0 {
			merged = append(merged, proto.Clone(source).(*datapb.SplitShardTaskSource))
			continue
		}
		kept := proto.Clone(recorded[idx]).(*datapb.SplitShardTaskSource)
		switch {
		case kept.GetSwitchTimeTick() == 0:
			kept.SwitchTimeTick = source.GetSwitchTimeTick()
		case source.GetSwitchTimeTick() != kept.GetSwitchTimeTick():
			mlog.Warn(ctx, "a redelivered shard split commit reports another switch time tick, keeping the first one",
				mlog.Int64("splitTaskID", taskID),
				mlog.String("source", kept.GetVchannel()),
				mlog.Uint64("recordedSwitchTimeTick", kept.GetSwitchTimeTick()),
				mlog.Uint64("redeliveredSwitchTimeTick", source.GetSwitchTimeTick()))
		}
		merged = append(merged, kept)
	}
	return merged
}

// markSplitTargetsAdded gives each target vchannel the channel-added mark a
// created collection's vchannels get from WatchChannels (catalog
// MarkChannelAdded), which is what the garbage collector's ChannelExists guard
// reads.
//
// The guard keeps the meta of a Dropped segment whose dml position is past the
// channel checkpoint, so a consumer replaying from that checkpoint still finds
// it in DroppedSegmentIds and filters its rows, which a compaction has already
// written into the output. Without the mark, ChannelExists is false, the guard
// treats the channel as one of a collection being dropped and the segment's
// meta goes as soon as dropTolerance passes, even with the target's checkpoint
// still behind it: the next recovery replays those rows next to the compacted
// output and duplicates them.
//
// The mark is an idempotent save of a constant, so a redelivered callback
// writes the same value again. The one value it must not overwrite is the
// removal tombstone DropVirtualChannel leaves in the same key once the target's
// data has been dropped: re-marking a dropped channel added would revive the
// guard for a channel whose checkpoint is gone, and its Dropped segments would
// never leave the meta the collection's drop waits on. A target already marked
// for removal is therefore left alone.
func (s *Server) markSplitTargetsAdded(ctx context.Context, req *datapb.CommitShardSplitRequest) error {
	for _, target := range req.GetTargets() {
		vchannel := target.GetVchannel()
		if s.meta.catalog.ShouldDropChannel(ctx, vchannel) {
			mlog.Warn(ctx, "a split target is already marked for removal, not marking it added",
				mlog.Int64("splitTaskID", req.GetSplitTaskId()), mlog.String("vchannel", vchannel))
			continue
		}
		if err := s.meta.catalog.MarkChannelAdded(ctx, vchannel); err != nil {
			return err
		}
	}
	return nil
}

// seedSplitTargetCheckpoints gives each target vchannel its first channel
// checkpoint, so the child delegators have somewhere to seek from before the
// target's own streamingnode has reported anything.
//
// Only targets with no checkpoint at all are seeded. One that already has a
// checkpoint has been consuming for a while, and writing the genesis position
// over it would rewind every reader of that shard to the start of its WAL.
func (s *Server) seedSplitTargetCheckpoints(ctx context.Context, req *datapb.CommitShardSplitRequest) error {
	targets := splitTargetVChannels(req.GetTargets())
	unseeded := make([]*msgpb.MsgPosition, 0, len(req.GetTargetStartPositions()))
	for _, position := range req.GetTargetStartPositions() {
		if !targets.Contain(position.GetChannelName()) {
			continue
		}
		if s.meta.GetChannelCheckpoint(position.GetChannelName()) != nil {
			continue
		}
		unseeded = append(unseeded, proto.Clone(position).(*msgpb.MsgPosition))
	}
	if len(unseeded) == 0 {
		return nil
	}
	return s.meta.UpdateChannelCheckpoints(ctx, unseeded)
}

// CheckShardSplitDrained reports whether the split's sources still hold
// anything the targets have not taken.
//
// Three conjuncts, per source, all of them datacoord-local:
//
//   - no segment left on the source vchannel in a non-Dropped state --- data a
//     reader could still be routed to;
//   - the source's channel checkpoint has reached its own T_switch. The fence
//     only appends a message; the streamingnode seals and reports the sealed
//     segments asynchronously afterwards, so below T_switch an empty segment
//     scan proves nothing and those segments would land on a retired shard as
//     orphans. A source with no checkpoint at all is not drained, and neither is
//     one whose T_switch is still zero --- that is a source whose fence has not
//     been recorded, i.e. one that may still be accepting writes;
//   - no unfinished import job names the source. A job still in
//     Pending/PreImporting has registered no segment in meta, so the scan
//     cannot see it, and it could allocate onto the retired shard after the
//     scan passed.
//
// The response also describes the task as recorded here -- its source and
// targets -- because the adoption message does not name the shard it retires,
// and the adoption callback may retire and adopt only what its own task names.
//
// A task datacoord has no record of, or holds only as planned (not fenced by
// CommitShardSplit), is answered recorded=false, which is neither "drained"
// nor "not drained": the SplitShard ack callback has not run here, so the
// adoption that asks is ahead of this cluster and waits for it.
func (s *Server) CheckShardSplitDrained(ctx context.Context, req *datapb.CheckShardSplitDrainedRequest) (*datapb.CheckShardSplitDrainedResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.CheckShardSplitDrainedResponse{Status: merr.Status(err)}, nil
	}
	task, ok := s.shardSplitTasks.get(req.GetSplitTaskId())
	if !ok || !task.GetFenced() {
		// Not recorded here, or only planned here: CommitShardSplit has not run
		// for it, so nothing of the split is applied on this cluster. That is an
		// answer, not an error; the caller waits for the SplitShard callback.
		return &datapb.CheckShardSplitDrainedResponse{Status: merr.Success(), Recorded: false}, nil
	}
	if task.GetCollectionId() != req.GetCollectionId() {
		// Two coordinators recorded the same task id against different
		// collections: a planning bug, and the drain of the wrong collection's
		// shards must not answer for this one.
		return &datapb.CheckShardSplitDrainedResponse{
			Status: merr.Status(merr.WrapErrServiceInternalMsg(
				"shard split task %d is recorded on collection %d, not on collection %d",
				req.GetSplitTaskId(), task.GetCollectionId(), req.GetCollectionId())),
		}, nil
	}
	targets := make([]string, 0, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		targets = append(targets, target.GetVchannel())
	}
	return &datapb.CheckShardSplitDrainedResponse{
		Status:          merr.Success(),
		Drained:         s.splitSourcesDrained(ctx, task),
		Recorded:        true,
		SourceVchannels: splitSourceVChannels(task),
		TargetVchannels: targets,
	}, nil
}

// splitSourcesDrained is the drain predicate CheckShardSplitDrained answers
// with, and the one the split manager moves a task to Adopting on and issues
// the adoption after.
func (s *Server) splitSourcesDrained(ctx context.Context, task *datapb.SplitShardTask) bool {
	return s.splitDrainBlockReason(ctx, task) == ""
}

// splitDrainBlockReason names the first drain conjunct the task's sources do
// not satisfy yet, in the order splitSourcesDrained checks them, or "" once
// they are drained. The predicate IS this function: splitSourcesDrained, the
// split manager's stall logs and every caller of either are defined by it and
// its two halves (liveSegmentBlockReason, fenceFlushBlockReason), so there is
// one drain predicate and one set of reason strings, and nothing that logs why
// a split is waiting can disagree with what CheckShardSplitDrained answers.
func (s *Server) splitDrainBlockReason(ctx context.Context, task *datapb.SplitShardTask) string {
	for _, source := range task.GetSources() {
		if reason := s.liveSegmentBlockReason(source.GetVchannel()); reason != "" {
			return reason
		}
	}
	if reason := s.fenceFlushBlockReason(task); reason != "" {
		return reason
	}
	if s.hasActiveImportOnAnyVChannel(ctx, splitSourceVChannels(task)) {
		return "an import is still in progress on a source"
	}
	return ""
}

// fenceFlushBlockReason names the first source whose fence has not been
// recorded or whose channel checkpoint has not reached it, or "" once every
// source is past its own T_switch.
//
// A zero T_switch is a source whose fence has not been recorded, i.e. one that
// may still be accepting writes: there is no tick to have caught up to, and
// comparing the checkpoint against zero would pass vacuously and collapse the
// predicate to the empty segment scan it exists to close. A source with no
// checkpoint at all has not caught up either.
//
// It is the drain without the segment scan and the import check, split out
// because a redistribution must start only once every source is past its
// T_switch (design doc §6.3 step 2.1): from then on the source's WAL is closed
// and every segment the fence sealed is in meta.
func (s *Server) fenceFlushBlockReason(task *datapb.SplitShardTask) string {
	for _, source := range task.GetSources() {
		vchannel := source.GetVchannel()
		if source.GetSwitchTimeTick() == 0 {
			return fmt.Sprintf("source %s fence not recorded yet (switch time tick is zero)", vchannel)
		}
		cp := s.meta.GetChannelCheckpoint(vchannel)
		if cp == nil {
			return fmt.Sprintf("source %s has no channel checkpoint yet", vchannel)
		}
		if cp.GetTimestamp() < source.GetSwitchTimeTick() {
			return fmt.Sprintf("source %s checkpoint %d has not reached its switch time tick %d",
				vchannel, cp.GetTimestamp(), source.GetSwitchTimeTick())
		}
	}
	return ""
}

// splitSourceFenceRecorded is the shard split's
// registry.AppendFirstReplicaRecordedChecker: whether this cluster records a
// fenced split of vchannel, i.e. a non-zero T_switch on a task naming it as its
// source.
//
// The only append-first replica on this branch is a SplitShard source, so this
// is the question a secondary's append gate asks about a split whose broadcast
// task has already been collected here. The answer is exact for the gate:
//   - the T_switch is written by CommitShardSplit from the source's append
//     result on THIS cluster, so the fence is in this cluster's WAL;
//   - a source is fenced by at most one split -- a second task's fence is
//     refused on the primary before any of its targets is appended -- so every
//     target replica naming this source waits for that one fence;
//   - task records are never removed, so a record once seen stays.
func (s *Server) splitSourceFenceRecorded(_ context.Context, vchannel string) (bool, error) {
	if s.shardSplitTasks == nil {
		return false, nil
	}
	_, ok := s.shardSplitTasks.sourceSwitchTimeTick(vchannel)
	return ok, nil
}

// channelCheckpointCovers reports whether vchannel's channel checkpoint cp
// proves every message of the vchannel at or before ts has been flushed. This
// is the check behind GetFlushState and GetFlushAllState.
//
// Normally that is cp >= ts. The fenced source of a recorded shard split is the
// exception: once its checkpoint passes the fence the flusher closes its data
// sync service, so the checkpoint stops near T_switch and a flush ts taken after
// the fence would never be reached --- the source stays in the collection's
// vchannel list until adoption, so a client waiting on flush state would wait
// for the adoption, which can be hours away. The source accepts nothing after
// its fence, so a checkpoint at or past T_switch already covers every message
// it will ever hold, and that is exactly as strong as cp >= ts. A T_switch of
// zero never counts, matching the drain predicate: a fence not on record may
// still be accepting writes.
func (s *Server) channelCheckpointCovers(vchannel string, cp *msgpb.MsgPosition, ts uint64) bool {
	if cp == nil {
		return false
	}
	if cp.GetTimestamp() >= ts {
		return true
	}
	if s.shardSplitTasks == nil {
		return false
	}
	switchTimeTick, ok := s.shardSplitTasks.sourceSwitchTimeTick(vchannel)
	return ok && cp.GetTimestamp() >= switchTimeTick
}

// liveSegmentBlockReason names the first segment still on vchannel in a
// non-Dropped state -- data a reader could still be routed to -- by id, level
// and state, so a stall log says exactly what is still there. "" once none is
// left.
func (s *Server) liveSegmentBlockReason(vchannel string) string {
	for _, segment := range s.meta.GetRealSegmentsForChannel(vchannel) {
		if segment.GetState() != commonpb.SegmentState_Dropped {
			return fmt.Sprintf("source %s still has a live segment %d (level %s, state %s)",
				vchannel, segment.GetID(), segment.GetLevel(), segment.GetState())
		}
	}
	return ""
}

// hasActiveImportOnAnyVChannel reports whether an unfinished import job targets
// any of the given vchannels. A job's target vchannels are fixed at creation,
// so this is a purely datacoord-local check that needs no import/split mutual
// exclusion.
func (s *Server) hasActiveImportOnAnyVChannel(ctx context.Context, vchannels []string) bool {
	if s.importMeta == nil || len(vchannels) == 0 {
		return false
	}
	wanted := typeutil.NewSet(vchannels...)
	jobs := s.importMeta.GetJobBy(ctx, WithoutJobStates(
		internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed))
	for _, job := range jobs {
		for _, vc := range job.GetVchannels() {
			if wanted.Contain(vc) {
				return true
			}
		}
	}
	return false
}

func cloneSplitSources(sources []*datapb.SplitShardTaskSource) []*datapb.SplitShardTaskSource {
	out := make([]*datapb.SplitShardTaskSource, 0, len(sources))
	for _, source := range sources {
		out = append(out, proto.Clone(source).(*datapb.SplitShardTaskSource))
	}
	return out
}

func cloneSplitTargets(targets []*datapb.SplitShardTaskTarget) []*datapb.SplitShardTaskTarget {
	out := make([]*datapb.SplitShardTaskTarget, 0, len(targets))
	for _, target := range targets {
		out = append(out, proto.Clone(target).(*datapb.SplitShardTaskTarget))
	}
	return out
}
