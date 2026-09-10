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
// For a task that IS present, the request's T_switch wins over whatever the
// task carries. The primary wrote its task before it broadcast, so its tick is
// at best a placeholder; the tick the broadcast actually landed on is the one
// the write path fenced at, and the drain must wait for exactly that one. The
// state only ever moves forward: Preparing/Fencing advance to Redistributing,
// and a task already Adopting or beyond is left where it is, because a
// redelivered callback must not drag a split back into a window it has left.
//
// Errors are System (merr.WrapErrServiceInternal*): the caller is a coordinator
// callback with nothing to fix in its request, and every failure here --- a
// catalog write, a checkpoint write --- is transient and must stay retriable.
func (s *Server) CommitShardSplit(ctx context.Context, req *datapb.CommitShardSplitRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	logger := mlog.With(
		mlog.Int64("collectionID", req.GetCollectionId()),
		mlog.Int64("splitTaskID", req.GetSplitTaskId()))

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
	targets := typeutil.NewSet[string]()
	for _, target := range req.GetTargets() {
		targets.Insert(target.GetVchannel())
	}
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
//     orphans. A source with no checkpoint at all is not drained;
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
