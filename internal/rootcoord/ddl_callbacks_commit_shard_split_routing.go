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

package rootcoord

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// shardSplitRoutingAlterV2AckCallback applies an AlterCollection carrying the
// shard_split_routing mask: a split's adoption, which travels like any other
// AlterCollection and is therefore also the message a secondary cluster
// receives through replication.
//
// There is exactly ONE apply path for a routing post-image,
// MetaTable.ApplyShardSplitRouting, and this callback uses it on every cluster
// alike -- a replicated adoption carries no extra trust and gets no fewer
// refusals than the primary's own. The generic meta.AlterCollection would
// write the post-image without asking whether it moves the collection forward.
//
// # Flow
//
//  1. Shape: the routing mask must travel alone (checkShardSplitRoutingAlterShape).
//  2. Judge the post-image against the collection (routing.JudgeCommit) with
//     this adoption's own delta: the source it may retire and the targets it
//     may adopt, which the message does not name and this cluster's DataCoord
//     record of the split task does. What the message alone can answer (its
//     arrays parallel, no vchannel twice, its writable shards tiling the
//     modulus) is refused before that record is asked for. Already applied -> no-op; ahead of the
//     collection (an earlier routing commit of the collection is not applied
//     here yet, or the split task is not recorded here) -> a retriable error;
//     refused -> an error; forward -> go on.
//  3. If it retires a shard, wait for THIS cluster's drain.
//  4. Apply through ApplyShardSplitRouting, which judges again under ddLock.
//  5. Broadcast the altered collection and expire the proxy caches of the
//     collection as this cluster names it (expireShardSplitRoutingCaches).
//
// Every refusal is a System error: the post-image was planned by the split
// coordinator, never typed by a user, so an incoherent one is a Milvus bug. It
// is also a wedge -- the broadcaster retries this callback until it returns
// nil while holding the collection's resource keys -- which is why a refusal is
// logged at Error naming what it blocks, and why a redelivery that has nothing
// left to write is skipped rather than refused.
func (c *DDLCallback) shardSplitRoutingAlterV2AckCallback(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	header := result.Message.Header()
	updates := result.Message.MustBody().GetUpdates()
	logger := mlog.With(
		mlog.FieldCollectionID(header.GetCollectionId()),
		mlog.Int64("splitTaskID", updates.GetSplitTaskId()),
		mlog.FieldMessage(result.Message))

	if err := checkShardSplitRoutingAlterShape(header, updates); err != nil {
		logger.Error(ctx, shardSplitRoutingWedgeLog, mlog.Err(err))
		return err
	}
	coll, delta, apply, err := c.gateShardSplitRoutingAlter(ctx, logger, result)
	if err != nil {
		return err
	}
	if apply {
		switch err := c.meta.ApplyShardSplitRouting(ctx, header.GetCollectionId(), updates, delta, result.GetMaxTimeTick()); {
		case err == nil:
		case errors.Is(err, routing.ErrCommitAlreadyApplied):
			logger.Info(ctx, "shard split routing commit already applied, only expiring caches")
		case errors.Is(err, errAlterCollectionNotFound):
			logger.Warn(ctx, "collection vanished while committing the shard split routing, ignore it")
			return nil
		case errors.Is(err, errShardSplitRoutingCollectionUnavailable):
			// Returned before BroadcastAlteredCollection, which cannot resolve a
			// Dropping collection and would fail this callback forever.
			logger.Warn(ctx, "collection is being dropped while committing the shard split routing, ignore it")
			return nil
		default:
			// A refusal here means the meta moved between the gate's snapshot and
			// the lock, which the collection's exclusive key should rule out; a
			// catalog failure is transient. Either way the broadcaster warns once
			// per retry with this error.
			return merr.Wrap(err, "apply the shard split routing commit")
		}
	}
	if err := c.broker.BroadcastAlteredCollection(ctx, header.GetCollectionId()); err != nil {
		return merr.Wrap(err, "broadcast the altered collection after the shard split routing commit")
	}
	return c.expireShardSplitRoutingCaches(ctx, logger, header, coll)
}

// expireShardSplitRoutingCaches expires the proxy caches a routing commit
// stales: the collection's own, under the name and the aliases THIS cluster
// holds for it (getCacheExpireForCollection, as the SplitShard callback does),
// plus whatever the message's header names.
//
// The header's list is an addition, never the only source. No adoption issuer
// exists on this branch to fill it, and on a secondary a rename applied
// between the primary's issue and this apply leaves it naming a collection
// this cluster no longer knows. Either way, a proxy whose cache survives keeps
// placing 1/N of the inserts on the retired source, which the name gate
// refuses. The collection is the one the gate loaded; a collection the gate
// could not find was reported by the apply before this runs.
func (c *DDLCallback) expireShardSplitRoutingCaches(ctx context.Context, logger *mlog.Logger, header *messagespb.AlterCollectionMessageHeader, coll *model.Collection) error {
	expirations := &message.CacheExpirations{
		CacheExpirations: slices.Clone(header.GetCacheExpirations().GetCacheExpirations()),
	}
	if coll != nil {
		own, err := c.getCacheExpireForCollection(ctx, coll.DBName, coll.Name)
		switch {
		case err == nil:
			for _, expiration := range own.GetCacheExpirations() {
				if !slices.ContainsFunc(expirations.CacheExpirations, func(listed *messagespb.CacheExpiration) bool {
					return proto.Equal(listed, expiration)
				}) {
					expirations.CacheExpirations = append(expirations.CacheExpirations, expiration)
				}
			}
		case errors.Is(err, merr.ErrCollectionNotFound):
			// The collection entered Dropping between the gate's lookup by id
			// and this lookup by name. Its caches are invalidated by the drop
			// itself, so only what the header names is left to expire.
			logger.Warn(ctx, "collection is being dropped, expiring only the header's caches after the shard split routing commit")
		default:
			return merr.Wrap(err, "collect the cache expirations after the shard split routing commit")
		}
	}
	return c.ExpireCaches(ctx, expirations)
}

// shardSplitRoutingWedgeLog names what a refused routing commit blocks, so the
// operator who finds it in the log knows it will not clear by itself.
const shardSplitRoutingWedgeLog = "shard split routing commit refused; the broadcaster retries it forever " +
	"and every later DDL of the collection is queued behind it until the message is repaired"

// checkShardSplitRoutingAlterShape refuses an AlterCollection that carries the
// routing mask together with anything else.
//
// A routing commit is one atomic catalog write through ApplyShardSplitRouting.
// Anything else the same message asked for -- another field mask, a dropped
// field, a load-config change, a bound index -- would need the generic apply
// path as a SECOND write, and a crash or a refusal between the two would leave
// half of the message applied. No builder produces such a message, so one that
// arrives is a bug: refused loudly rather than half-applied.
func checkShardSplitRoutingAlterShape(header *messagespb.AlterCollectionMessageHeader, updates *messagespb.AlterCollectionMessageUpdates) error {
	for _, path := range header.GetUpdateMask().GetPaths() {
		if path != message.FieldMaskCollectionShardSplitRouting {
			return merr.WrapErrServiceInternalMsg(
				"shard split routing commit of collection %d also carries the %q update; a routing commit is applied on its own",
				header.GetCollectionId(), path)
		}
	}
	if len(header.GetDroppedFieldIds()) > 0 || updates.GetAlterLoadConfig() != nil || len(updates.GetBoundFieldIndexes()) > 0 {
		return merr.WrapErrServiceInternalMsg(
			"shard split routing commit of collection %d also drops fields, alters the load config or binds indexes; "+
				"a routing commit is applied on its own", header.GetCollectionId())
	}
	return nil
}

// gateShardSplitRoutingAlter decides, on a snapshot of the collection, whether
// the routing commit should be applied at all, names the delta it may apply,
// and holds a retiring commit until THIS cluster has drained what it retires.
//
// The judgement runs here as well as under ddLock because a redelivery must
// never reach the drain gate. Once datacoord reclaims a finished split's task
// record (the reaper is not on this branch), nothing could name the delta and
// the drain could not be asked, so a post-image the collection already carries
// is recognized BEFORE datacoord is consulted, reports apply=false, and nothing
// is written. The judge can say that much without the record; anything else it
// says without the record is either a message-only refusal or "the record is
// needed" (ahead), and only then is datacoord asked.
//
// What the adoption may retire and adopt is read off this cluster's record of
// the split task (CheckShardSplitDrained describes it), never off the meta: a
// vchannel the collection lists and the post-image does not may belong to
// another split's adoption that this cluster has not applied yet, and applying
// that retirement here would skip that adoption's own drain gate. A post-image
// ahead of the collection in that way -- or one whose task this cluster has no
// record of, because its SplitShard callback has not run here -- is a
// retriable error and never reaches the drain gate.
//
// The adoption retires a shard: the meta stops routing to it and its own
// replica of this message tears its streamingnode registration down. That is
// only safe once its data has moved to the targets, and "moved" is a
// per-cluster fact -- a secondary replays the same WAL but compacts, imports
// and flushes on its own schedule. Each cluster therefore asks its own
// datacoord and refuses until the answer is yes; the broadcaster retries the
// callback with backoff. A routing commit that retires nothing has nothing to
// wait for.
//
// It returns the collection it judged (nil when the meta no longer holds it,
// which the apply then reports), the delta, and whether to apply.
func (c *DDLCallback) gateShardSplitRoutingAlter(ctx context.Context, logger *mlog.Logger, result message.BroadcastResultAlterCollectionMessageV2) (coll *model.Collection, delta routing.CommitDelta, apply bool, err error) {
	header := result.Message.Header()
	updates := result.Message.MustBody().GetUpdates()

	// allowUnavailable: a collection that is being dropped is loaded, and is
	// reported by the apply rather than here.
	coll, err = c.meta.GetCollectionByID(ctx, "", header.GetCollectionId(), typeutil.MaxTimestamp, true)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil, delta, true, nil
		}
		return nil, delta, false, merr.Wrap(err, "load the collection for the shard split routing commit")
	}
	if !coll.Available() {
		// Dropping: skip the judge and the drain gate and let the apply refuse
		// it (errShardSplitRoutingCollectionUnavailable). The drain gate in
		// particular must not be waited on: a collection being dropped may
		// never drain, and this callback holds its keys while it waits.
		return coll, delta, true, nil
	}
	// Without the task record the judge can only recognize a redelivery or a
	// malformed message; "ahead" here means "ask datacoord for the delta".
	switch err := routing.JudgeCommit(coll, updates, routing.AdoptionDelta("", nil, false)); {
	case errors.Is(err, routing.ErrCommitAlreadyApplied):
		logger.Info(ctx, "shard split routing commit already applied, skipping the drain gate and the meta apply")
		return coll, delta, false, nil
	case errors.Is(err, routing.ErrCommitAheadOfCollection):
	default:
		logger.Error(ctx, shardSplitRoutingWedgeLog, mlog.Err(err))
		return coll, delta, false, merr.Wrap(err, "refuse the shard split routing commit")
	}

	resp, err := c.mixCoord.CheckShardSplitDrained(ctx, &datapb.CheckShardSplitDrainedRequest{
		CollectionId: header.GetCollectionId(),
		SplitTaskId:  updates.GetSplitTaskId(),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return coll, delta, false, merr.Wrap(err, "ask this cluster's datacoord about the shard split task")
	}
	// A recorded task names exactly one source. A record with none would make
	// a delta with an empty source, which the judge would read as "retired
	// already" and let the adoption retire whatever the post-image delists;
	// one with several is no split this branch issues.
	if n := len(resp.GetSourceVchannels()); n > 1 || (resp.GetRecorded() && n == 0) {
		err := merr.WrapErrServiceInternalMsg(
			"shard split task %d of collection %d is recorded with %d sources; a split has one",
			updates.GetSplitTaskId(), header.GetCollectionId(), n)
		logger.Error(ctx, shardSplitRoutingWedgeLog, mlog.Err(err))
		return coll, delta, false, err
	}
	source := ""
	if len(resp.GetSourceVchannels()) == 1 {
		source = resp.GetSourceVchannels()[0]
	}
	delta = routing.AdoptionDelta(source, resp.GetTargetVchannels(), resp.GetRecorded())

	switch err := routing.JudgeCommit(coll, updates, delta); {
	case err == nil:
	case errors.Is(err, routing.ErrCommitAlreadyApplied):
		logger.Info(ctx, "shard split routing commit already applied, skipping the drain gate and the meta apply")
		return coll, delta, false, nil
	case errors.Is(err, routing.ErrCommitAheadOfCollection):
		// Not a wedge: an earlier routing commit of the collection is still
		// being applied here (its SplitShard, or another split's adoption), and
		// this one applies once it has. Reaching this at all means the two
		// callbacks ran out of order, so it is logged.
		logger.Warn(ctx, "shard split routing commit is ahead of this cluster's collection, waiting for the earlier routing commit to be applied", mlog.Err(err))
		return coll, delta, false, merr.Wrap(err, "wait for the routing commit this adoption follows")
	default:
		logger.Error(ctx, shardSplitRoutingWedgeLog, mlog.Err(err))
		return coll, delta, false, merr.Wrap(err, "refuse the shard split routing commit")
	}

	// What the commit retires is read off the meta, not off the message: a
	// vchannel the collection lists and the post-image does not. The judge has
	// just established that it is this adoption's own source, or nothing.
	var retired []string
	for _, vchannel := range coll.VirtualChannelNames {
		if !slices.Contains(updates.GetVirtualChannelNames(), vchannel) {
			retired = append(retired, vchannel)
		}
	}
	if len(retired) == 0 {
		return coll, delta, true, nil
	}
	// The retired shard's own replica is what tears its streamingnode state
	// down, and a vchannel the collection no longer names receives nothing
	// later. A retiring commit that did not reach it would leave that state --
	// and the WAL truncation it pins -- behind forever.
	broadcastVChannels := result.Message.BroadcastHeader().VChannels
	for _, vchannel := range retired {
		if !slices.Contains(broadcastVChannels, vchannel) {
			err := merr.WrapErrServiceInternalMsg(
				"shard split routing commit of collection %d retires vchannel %s but was not broadcast to it",
				header.GetCollectionId(), vchannel)
			logger.Error(ctx, shardSplitRoutingWedgeLog, mlog.Err(err))
			return coll, delta, false, err
		}
	}

	if !resp.GetDrained() {
		// Not logged here: waiting is the expected answer, and the broadcaster
		// already warns once per retry with the returned error. A transient wait
		// is a retriable System error: nothing is wrong, the split manager is
		// still moving data. The broadcaster retries the callback whatever the
		// code; the code is what the log, the span and any future caller see.
		return coll, delta, false, merr.WrapErrServiceUnavailableMsg("shard split %d of collection %d not drained on this cluster yet",
			updates.GetSplitTaskId(), header.GetCollectionId())
	}
	return coll, delta, true, nil
}
