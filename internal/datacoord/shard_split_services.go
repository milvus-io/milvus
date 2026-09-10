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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitBroadcastTimeout bounds the SplitShard broadcast. The ack callback
// (CommitShardSplit above) is what actually commits the split on every
// replica that receives it, so a caller stuck past this long is almost
// certainly talking to a broadcaster/WAL that will never ack, not one that is
// merely slow.
const splitBroadcastTimeout = 60 * time.Second

// broadcastShardSplit issues a shard split's write switch as one broadcast:
// the sources are fenced, the targets' genesis and the routing post-image
// travel with them, all in the single message NewSplitShardBroadcastMessage
// builds.
//
// There is nothing left for this function to do once Broadcast acks. The
// bookkeeping -- persisting the split task, seeding the targets' checkpoints
// -- happens in the ack callback (CommitShardSplit above), which runs on
// every replica the broadcast reaches, including ones that never called this
// function. This function only has to get the message onto the wire, or
// report why it could not.
func (s *Server) broadcastShardSplit(ctx context.Context, param streaming.SplitShardParam) error {
	if err := param.Validate(); err != nil {
		return err
	}

	broadcastAPI, err := s.startBroadcastWithCollectionID(ctx, param.CollectionID)
	if err != nil {
		return merr.Wrap(err, "failed to start broadcast for shard split")
	}
	defer broadcastAPI.Close()

	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	if err != nil {
		return merr.Wrap(err, "failed to build shard split broadcast message")
	}

	broadcastCtx, cancel := context.WithTimeout(ctx, splitBroadcastTimeout)
	defer cancel()
	result, err := broadcastAPI.Broadcast(broadcastCtx, msg)
	if err != nil {
		return merr.Wrap(err, "failed to broadcast shard split")
	}

	fields := []mlog.Field{
		mlog.Int64("collectionID", param.CollectionID),
		mlog.Int64("splitTaskID", param.SplitTaskID),
		mlog.Strings("sources", param.SourceVChannels),
		mlog.Strings("targets", splitShardTargetVChannelsFromParam(param.Targets)),
	}
	// Cheap to look up (a map read on the result already in hand) and useful
	// as a cross-reference to the control channel's own log line; skipped
	// rather than treated as an error when the control channel's append
	// result is not part of the reply.
	if controlResult := result.GetAppendResult(param.ControlChannel); controlResult != nil {
		fields = append(fields, mlog.Uint64("controlChannelTick", controlResult.TimeTick))
	}
	mlog.Info(ctx, "broadcast the shard split write switch", fields...)
	return nil
}

// splitShardTargetVChannelsFromParam is the target vchannel names of a shard
// split param, for logging.
func splitShardTargetVChannelsFromParam(targets []*message.SplitShardTarget) []string {
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		out = append(out, target.GetVchannel())
	}
	return out
}

// CommitShardSplit is the datacoord half of the SplitShard broadcast ack
// callback.
//
// The broadcast reaches every replica's coordinator, so this runs on the
// primary that planned the split AND on secondaries that have never heard of
// it. It is therefore idempotent by split task id: an absent task is created
// outright, already in Redistributing, because the fence it acknowledges has
// by definition already landed.
//
// For a task that IS present, the request's T_switch wins over whatever the
// task carries. The primary wrote its task before it broadcast, so its tick is
// at best a placeholder; the tick the broadcast actually landed on is the one
// the write path fenced at, and the drain must wait for exactly that one. The
// state only ever moves forward: Preparing/Fencing advance to Redistributing,
// and a task already Adopting or beyond is left where it is, because a
// redelivered callback must not drag a split back into a window it has left.
//
// Errors are System (merr.WrapErrServiceInternal*): the blame for a catalog
// write failure, a checkpoint write failure or a malformed callback never lies
// with the request's content, and the caller is a coordinator callback that has
// nothing to fix in it either way. Note what "System" buys and what it does not:
// pkg/util/retry retries these because they are NOT InputError, but
// ErrServiceInternal is declared non-retriable, so the wire Status.Retriable bit
// is false. Callers must gate on the error class, never on Status.GetRetriable().
func (s *Server) CommitShardSplit(ctx context.Context, req *datapb.CommitShardSplitRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	logger := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.Int64("splitTaskID", req.GetSplitTaskId()))

	if err := s.validateCommitShardSplit(req); err != nil {
		logger.Warn(ctx, "refused a malformed shard split commit", mlog.Err(err))
		return merr.Status(err), nil
	}

	task := s.mergeCommittedShardSplit(req)
	if err := s.shardSplitTasks.upsert(ctx, s.meta.catalog, task); err != nil {
		logger.Warn(ctx, "persist the committed shard split task failed", mlog.Err(err))
		return merr.Status(merr.WrapErrServiceInternalErr(err, "persist the committed shard split task %d", req.GetSplitTaskId())), nil
	}

	if err := s.seedSplitTargetCheckpoints(ctx, req); err != nil {
		logger.Warn(ctx, "seed the split targets' genesis checkpoints failed", mlog.Err(err))
		return merr.Status(merr.WrapErrServiceInternalErr(err, "seed the split targets of task %d", req.GetSplitTaskId())), nil
	}

	logger.Info(ctx, "recorded a committed shard split",
		mlog.Strings("sources", splitSourceVChannels(task)),
		mlog.Any("state", task.GetState()))
	return merr.Success(), nil
}

// validateCommitShardSplit refuses a callback that cannot be acted on, before
// anything is written.
//
// This is the only malformed-input check either RPC has, and it is deliberately
// minimal: the sender is another coordinator, so anything wrong here is a Milvus
// bug rather than a user's, and the class stays System. What it refuses:
//
//   - task id zero, which §03 uses as the "no split task" sentinel of the fence.
//     Recording it would persist a real task under that id and make every later
//     "is there a task" question answer yes;
//   - no sources, which would create a task that reports drained the moment it
//     is asked, retiring shards on the strength of an empty loop;
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
	if len(req.GetSources()) == 0 {
		return merr.WrapErrServiceInternalMsg("shard split commit for task %d names no source shard", req.GetSplitTaskId())
	}
	if existing, ok := s.shardSplitTasks.get(req.GetSplitTaskId()); ok &&
		existing.GetCollectionId() != req.GetCollectionId() {
		return merr.WrapErrServiceInternalMsg(
			"shard split commit for task %d claims collection %d, but the recorded task belongs to collection %d",
			req.GetSplitTaskId(), req.GetCollectionId(), existing.GetCollectionId())
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
func (s *Server) mergeCommittedShardSplit(req *datapb.CommitShardSplitRequest) *datapb.SplitShardTask {
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
	ticks := make(map[string]uint64, len(req.GetSources()))
	for _, source := range req.GetSources() {
		ticks[source.GetVchannel()] = source.GetSwitchTimeTick()
	}
	known := typeutil.NewSet(splitSourceVChannels(task)...)
	for _, source := range task.GetSources() {
		if tick, ok := ticks[source.GetVchannel()]; ok {
			source.SwitchTimeTick = tick
		}
	}
	// A source the local task does not know about is still a source of this
	// split: recorded here rather than dropped, or its data would never be
	// waited for.
	for _, source := range req.GetSources() {
		if !known.Contain(source.GetVchannel()) {
			task.Sources = append(task.Sources, proto.Clone(source).(*datapb.SplitShardTaskSource))
			// Marked known immediately: a request that names one vchannel twice
			// must not grow two entries for it, which would then be waited on
			// twice and drift apart on the next merge.
			known.Insert(source.GetVchannel())
		}
	}
	if len(task.GetTargets()) == 0 {
		task.Targets = cloneSplitTargets(req.GetTargets())
	}
	if task.GetRoutingModulus() == 0 {
		task.RoutingModulus = req.GetRoutingModulus()
	}
	task.Fenced = true
	if task.GetState() == datapb.SplitShardTaskState_SplitShardTaskUnknown ||
		task.GetState() == datapb.SplitShardTaskState_SplitShardTaskPreparing ||
		task.GetState() == datapb.SplitShardTaskState_SplitShardTaskFencing {
		task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	}
	return task
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
// A task datacoord has no record of is a System error rather than either
// answer: "not drained" would be a lie and "drained" would retire a shard on no
// evidence. The record may simply not have arrived yet, so the caller retries.
func (s *Server) CheckShardSplitDrained(ctx context.Context, req *datapb.CheckShardSplitDrainedRequest) (*datapb.CheckShardSplitDrainedResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.CheckShardSplitDrainedResponse{Status: merr.Status(err)}, nil
	}
	task, ok := s.shardSplitTasks.get(req.GetSplitTaskId())
	if !ok {
		return &datapb.CheckShardSplitDrainedResponse{
			Status: merr.Status(merr.WrapErrServiceInternalMsg(
				"no record of shard split task %d on collection %d", req.GetSplitTaskId(), req.GetCollectionId())),
		}, nil
	}
	return &datapb.CheckShardSplitDrainedResponse{
		Status:  merr.Success(),
		Drained: s.splitSourcesDrained(ctx, task),
	}, nil
}

func (s *Server) splitSourcesDrained(ctx context.Context, task *datapb.SplitShardTask) bool {
	for _, source := range task.GetSources() {
		vchannel := source.GetVchannel()
		if s.hasLiveSegmentOnVChannel(vchannel) {
			return false
		}
		if source.GetSwitchTimeTick() == 0 {
			// The fence has not been recorded for this source, so there is no
			// tick to have caught up to and conjunct (b) below would compare
			// against zero and pass vacuously --- collapsing the predicate to
			// the empty scan it exists to close. A source that still accepts
			// writes is never drained.
			return false
		}
		cp := s.meta.GetChannelCheckpoint(vchannel)
		if cp == nil || cp.GetTimestamp() < source.GetSwitchTimeTick() {
			return false
		}
	}
	// The job list is walked once for the whole source set rather than per
	// source: a rehash holds every shard of the collection, and each scan walks
	// every unfinished job in the cluster.
	return !s.hasActiveImportOnAnyVChannel(ctx, splitSourceVChannels(task))
}

// hasLiveSegmentOnVChannel reports whether the channel still carries a segment
// in a non-Dropped state, i.e. data a reader could still be routed to.
func (s *Server) hasLiveSegmentOnVChannel(vchannel string) bool {
	for _, segment := range s.meta.GetRealSegmentsForChannel(vchannel) {
		if segment.GetState() != commonpb.SegmentState_Dropped {
			return true
		}
	}
	return false
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
