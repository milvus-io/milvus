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

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The adoption and the end of a shard split (design doc §5, §6.3 step 4).
//
// Once this cluster's drain predicate holds, the split manager issues the
// adoption itself: an AlterCollection broadcast under the shard_split_routing
// mask, naming the split task, whose post-image delists the source and moves
// both targets to Normal. The ack callback of that broadcast waits for the
// drain of every cluster it reaches while it holds the collection's keys, so
// it is never issued before this cluster's own drain holds: issued early it
// would queue every later DDL of the collection behind it.
//
// The task stays Adopting after the adoption applies, until this cluster's
// QueryCoord no longer serves the source; only then is it Done, and only then
// do the compaction freeze and the trigger's exclusion lift.

// buildAdoptionPostImage builds a split's adoption: the collection as it
// stands minus the split's source, with both of its targets moved to Normal,
// keeping their residues. Nothing else changes; an adoption changes no
// modulus. It names the split task, whose record every cluster's adoption gate
// reads its delta and its drain from.
//
// Invariant: the adoption is ONE commit. The one post-image delists the source
// AND moves both targets to Normal. The routing judge also accepts step-wise
// shapes -- the source still listed as Splitting, or a target still Creating --
// because it must recognize a redelivery of one; but a step-wise adoption
// would let QueryCoord pull the source together with Normal targets that own
// no loaded data yet, which reads the rows twice and unfreezes balance early.
// The issuer therefore never builds one.
//
// A collection that does not list both targets has not applied the split, and
// is refused.
func buildAdoptionPostImage(task *datapb.SplitShardTask, coll *splitCollection) (*messagespb.AlterCollectionMessageUpdates, error) {
	source := splitTaskSource(task)
	targets := splitTaskTargetVChannels(task)
	if source == "" || len(targets) != 2 {
		return nil, merr.WrapErrServiceInternalMsg(
			"refuse to adopt shard split %d: it names source %q and %d targets", task.GetTaskId(), source, len(targets))
	}
	for _, target := range targets {
		if !slices.Contains(coll.VirtualChannelNames, target) {
			return nil, merr.WrapErrServiceInternalMsg(
				"refuse to adopt shard split %d: collection %d does not list its target %s",
				task.GetTaskId(), coll.CollectionID, target)
		}
	}
	updates := &messagespb.AlterCollectionMessageUpdates{
		RoutingModulus: coll.RoutingModulus,
		ShardBy:        coll.ShardBy,
		SplitTaskId:    task.GetTaskId(),
	}
	for i, vchannel := range coll.VirtualChannelNames {
		if vchannel == source {
			continue
		}
		info := listedShardInfo(coll.Collection, i)
		if slices.Contains(targets, vchannel) {
			info.State = schemapb.ShardState_ShardNormal
		}
		updates.VirtualChannelNames = append(updates.VirtualChannelNames, vchannel)
		updates.PhysicalChannelNames = append(updates.PhysicalChannelNames, pchannelAt(coll.Collection, i))
		updates.ShardInfos = append(updates.ShardInfos, info)
	}
	return updates, nil
}

// adoptionBroadcastVChannels is where an adoption must be broadcast: the
// control channel, every vchannel the collection lists (the source included,
// whose own replica is what retires it) and every vchannel the post-image
// names.
func adoptionBroadcastVChannels(controlChannel string, coll *splitCollection, updates *messagespb.AlterCollectionMessageUpdates) []string {
	seen := typeutil.NewSet(controlChannel)
	vchannels := []string{controlChannel}
	for _, list := range [][]string{coll.VirtualChannelNames, updates.GetVirtualChannelNames()} {
		for _, vchannel := range list {
			if !seen.Contain(vchannel) {
				seen.Insert(vchannel)
				vchannels = append(vchannels, vchannel)
			}
		}
	}
	return vchannels
}

// issueShardSplitAdoption broadcasts a drained split's adoption under the
// collection's resource keys (design doc §6.3 step 4).
//
// Under the keys, against the meta held there:
//
//   - this cluster's drain predicate is asked again. The manager moved the
//     task to Adopting on it, but the adoption's callback holds the keys while
//     it waits for the drain, so the adoption is never sent before it holds;
//   - the post-image is judged with the adoption's own delta
//     (routing.JudgeCommit), the judgement the callback makes. One already
//     applied here -- a re-issue after the apply -- is success, and nothing is
//     sent.
//
// The broadcast is deduplicated by the task id, so a retry of one that landed
// is the same broadcast. Every error is returned for the next tick to retry.
func (s *Server) issueShardSplitAdoption(ctx context.Context, task *datapb.SplitShardTask, controlChannel string) error {
	api, coll, err := s.startSplitCollectionBroadcast(ctx, task.GetCollectionId())
	if err != nil {
		return err
	}
	defer api.Close()

	recorded, ok := s.shardSplitTasks.get(task.GetTaskId())
	if !ok || !recorded.GetFenced() {
		return merr.WrapErrServiceInternalMsg("shard split task %d is not recorded as fenced, refuse to adopt it", task.GetTaskId())
	}
	if reason := s.splitDrainBlockReason(ctx, recorded); reason != "" {
		return merr.WrapErrServiceUnavailableMsg("shard split %d is not drained on this cluster: %s", task.GetTaskId(), reason)
	}
	updates, err := buildAdoptionPostImage(recorded, coll)
	if err != nil {
		return err
	}
	delta := routing.AdoptionDelta(splitTaskSource(recorded), splitTaskTargetVChannels(recorded), true)
	switch err := routing.JudgeCommit(coll.Collection, updates, delta); {
	case err == nil:
	case errors.Is(err, routing.ErrCommitAlreadyApplied):
		return nil
	default:
		return merr.Wrapf(err, "judge the adoption of shard split %d", task.GetTaskId())
	}

	msg, err := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         coll.DBID,
			CollectionId: coll.CollectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{Updates: updates}).
		WithBroadcast(adoptionBroadcastVChannels(controlChannel, coll, updates)).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(coll.CollectionID, fmt.Sprintf("shard-split-adoption-%d", task.GetTaskId()))).
		BuildBroadcast()
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "build the adoption of shard split %d", task.GetTaskId())
	}
	if _, err := api.Broadcast(ctx, msg); err != nil {
		return merr.Wrapf(err, "broadcast the adoption of shard split %d", task.GetTaskId())
	}
	mlog.Info(ctx, "shard split adoption broadcast",
		mlog.FieldCollectionID(task.GetCollectionId()),
		mlog.Int64("splitTaskID", task.GetTaskId()),
		mlog.String("source", splitTaskSource(recorded)),
		mlog.Strings("targets", splitTaskTargetVChannels(recorded)))
	return nil
}

// splitSourceServed reports whether this cluster's QueryCoord still serves a
// split's source: whether the collection's current target still lists it as a
// shard. A collection that is not loaded, or no longer exists, is served by
// nobody. Any other failure is returned, and the caller asks again.
func (s *Server) splitSourceServed(ctx context.Context, collectionID int64, source string) (bool, error) {
	if s.mixCoord == nil {
		return false, merr.WrapErrServiceNotReadyMsg("datacoord has no mixcoord client to ask querycoord with")
	}
	resp, err := s.mixCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID: collectionID,
		// Every shard of the current target, served or not: a source listed
		// there is still QueryCoord's to release.
		WithUnserviceableShards: true,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		if errors.Is(err, merr.ErrCollectionNotLoaded) || errors.Is(err, merr.ErrCollectionNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, shard := range resp.GetShards() {
		if shard.GetChannelName() == source {
			return true, nil
		}
	}
	return false, nil
}

// advanceAdopting issues a drained split's adoption and finishes the task once
// this cluster no longer serves its source.
//
// The adoption is observed, not assumed: the source leaving rootcoord's record
// is what says it applied here, on the issuing cluster and on a secondary that
// only replays it alike. A secondary never issues it. Done then waits for
// QueryCoord: the task is Done only once this cluster's QueryCoord no longer
// serves the source, and a failed question keeps it Adopting.
func (m *shardSplitManager) advanceAdopting(task *datapb.SplitShardTask) {
	coll, ok := m.liveCollection(task, "collection dropped during adoption")
	if !ok {
		return
	}
	logger := m.taskLogger(task)
	source := splitTaskSource(task)
	if slices.Contains(coll.VirtualChannelNames, source) {
		if m.clusterIsReplicationSecondary() {
			logger.RatedInfo(m.ctx, 60, "waiting for the primary's shard split adoption to be applied here")
			return
		}
		if reason := m.coordinator.splitDrainBlockReason(m.ctx, task); reason != "" {
			logger.RatedWarn(m.ctx, 60, "an adopting shard split is not drained, not issuing its adoption", mlog.String("reason", reason))
			return
		}
		if err := m.coordinator.issueShardSplitAdoption(m.ctx, task, m.controlChannel()); err != nil {
			if !m.finishOnDroppedCollection(task, err, "collection dropped during adoption") {
				logger.RatedWarn(m.ctx, 30, "issue the shard split adoption failed, retrying", mlog.Err(err))
			}
			return
		}
		logger.RatedInfo(m.ctx, 60, "shard split adoption issued, waiting for it to apply")
		return
	}
	served, err := m.coordinator.splitSourceServed(m.ctx, task.GetCollectionId(), source)
	if err != nil {
		logger.RatedWarn(m.ctx, 30, "ask querycoord whether it still serves the split source failed, retrying", mlog.Err(err))
		return
	}
	if served {
		logger.RatedInfo(m.ctx, 60, "shard split adopted, waiting for querycoord to stop serving the source")
		return
	}
	m.finishTask(task, "")
}
