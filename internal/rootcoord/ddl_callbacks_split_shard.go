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

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitShardV2AckCallback commits a shard split's write switch once every
// replica of its broadcast has landed: the source is fenced and the target
// genesis records exist, so the targets may become routable.
//
// The post-image travels in the message body; nothing here derives it from
// mutable meta, which is what makes a retry and a replay apply the same
// topology.
//
// # Order: judge, then datacoord, then meta, then the caches
//
// The post-image is judged against the collection (routing.JudgeCommit) BEFORE
// anything is committed. On a secondary the ack callbacks of one collection's
// routing commits are not ordered by the broadcaster -- keys are collection
// names, and a rename between two commits gives them different keys -- so a
// split can reach its callback while an earlier routing commit of the same
// collection is still retrying. Its post-image then reflects that earlier
// commit, and the judge refuses it as ahead of the collection: a retriable
// refusal, returned without recording a task, so that DataCoord holds no
// record for a split this cluster has not applied and the drain of the
// overtaken adoption is never answered on its behalf.
//
// The two commit halves are independently idempotent, so a crash between them
// is repaired by the retry -- but the order still matters, because it decides
// what a reader can observe in between. DataCoord seeds each target's first
// channel checkpoint; until it has, GetChannelSeekPosition falls back to the
// collection's creation position (timestamp 0), and QueryCoord learns the
// target vchannel exists only once the routing post-image is applied. Seeding
// first therefore closes the window in which a target could be discovered with
// no checkpoint to seek from. The reverse order has no compensating benefit:
// the meta write is not what makes the split durable -- the WAL records
// already are.
//
// Every refusal below is a System error. The post-image was derived by the split
// coordinator, not by a user request, so a bad one is a Milvus bug; and every
// transport or store failure is transient. The broadcaster retries the callback
// with backoff until it succeeds.
func (c *DDLCallback) splitShardV2AckCallback(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	header := result.Message.Header()
	postImage := result.Message.MustBody().GetRouting()
	logger := mlog.With(
		mlog.FieldCollectionID(header.GetCollectionId()),
		mlog.Int64("splitTaskID", header.GetSplitTaskId()))

	// allowUnavailable: a collection that is being dropped is loaded, so that it
	// is ignored just below rather than retried as not found.
	coll, err := c.meta.GetCollectionByID(ctx, "", header.GetCollectionId(), typeutil.MaxTimestamp, true)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// Neither half has anything left to act on: the split's collection is
			// gone, so committing its routing or its datacoord task would create
			// records for a collection that no longer exists.
			logger.Warn(ctx, "split shard committed on a collection that no longer exists, ignore it")
			return nil
		}
		return merr.Wrap(err, "load the collection for the split shard commit")
	}
	if !coll.Available() {
		// Dropping: the drop is already under way here, which on a secondary a
		// rename between the split and the drop lets happen while this callback
		// retries (their collection-name keys differ). Neither half is written.
		// The routing apply would move counters the drop has already settled
		// (ApplyShardSplitRouting refuses it too, under its lock). The datacoord
		// record would mark the targets added and seed their checkpoints, and
		// the drop tears down only the vchannels the collection lists, which do
		// not include targets whose routing was never applied, so both would
		// outlive the collection.
		logger.Warn(ctx, "split shard committed on a collection that is being dropped, ignore it",
			mlog.String("state", coll.State.String()))
		return nil
	}
	// The control channel's tick is what the routing commit below is stamped
	// with, and the result is searched for it BY NAME (GetControlChannelResult
	// keys on funcutil.IsControlChannel). A broadcast issued without a control
	// channel -- one built around SplitShardParam.Validate, which refuses a
	// plain vchannel in that role -- acks with no such entry. Dereferencing it
	// would panic mixcoord, and since the task is persisted the callback would
	// run and panic again after every restart. Refused as a retriable System
	// error instead: the message is a Milvus fault, never the request's, and the
	// broadcaster retries the callback while the log names the wedge.
	controlChannelResult := result.GetControlChannelResult()
	if controlChannelResult == nil {
		err := merr.WrapErrServiceUnavailableMsg(
			"split shard broadcast %d of collection %d was acknowledged with no control channel replica",
			result.Message.BroadcastHeader().BroadcastID, header.GetCollectionId())
		logger.Error(ctx, splitShardCommitWedgeLog, mlog.Strings("ackedVChannels", result.GetVChannelsWithoutControlChannel()), mlog.Err(err))
		return err
	}
	// Read-only assertions, BEFORE anything is committed: the very checks
	// SplitShardParam.Validate ran before the broadcast, on the same message,
	// from the same function -- names, tiling, modulus, the source Splitting and
	// the targets Creating, namespace admission, the namespace-collection
	// deferral (design §1.3). A message that got here failing them did not come
	// through the builder. Retrying cannot clear it, but
	// neither can the broadcast be abandoned with a fenced source behind it, so
	// it is logged naming the wedge and returned as a System error; CommitShardSplit
	// is not called, so no task is recorded for a message that can never commit.
	if err := streaming.ValidateSplitShardMessage(header, result.Message.MustBody()); err != nil {
		logger.Error(ctx, splitShardCommitWedgeLog, mlog.Err(err))
		return merr.Wrap(err, "refuse the split shard commit")
	}
	// Admission was answered before the fence from the genesis schema's copy of
	// the collection properties, and the routing apply answers it from the
	// meta's. A message whose copy disagrees could pass one and fail the other;
	// report the disagreement itself, before anything is committed.
	if err := routing.CheckAdmissionPropertiesAgree(result.Message.MustBody().GetGenesis().GetCollectionSchema().GetProperties(), coll.Properties); err != nil {
		logger.Error(ctx, splitShardCommitWedgeLog, mlog.Err(err))
		return merr.Wrap(err, "refuse the split shard commit")
	}

	// The split's own delta: the header's source fenced, the header's targets
	// created. Whether this cluster already recorded the task tells a source the
	// collection no longer lists apart from one it has never listed (see
	// routing.CommitDelta.Recorded); the record is read, never written, here.
	delta, err := c.splitShardDelta(ctx, coll, header)
	if err != nil {
		return err
	}
	switch err := routing.JudgeCommit(coll, postImage, delta); {
	case err == nil:
	case errors.Is(err, routing.ErrCommitAlreadyApplied):
		// Already in the meta: a redelivery. DataCoord is still told, since a
		// crash between the two halves leaves the task unrecorded; it is
		// idempotent by task id.
		logger.Info(ctx, "split shard routing already applied, committing the task at datacoord and expiring caches")
	case errors.Is(err, routing.ErrCommitAheadOfCollection):
		// Not a wedge: an earlier routing commit of the collection is still
		// being applied here, and this one applies once it has. Reaching this
		// means the two callbacks ran out of order, so it is logged.
		logger.Warn(ctx, "split shard commit is ahead of this cluster's collection, waiting for the earlier routing commit to be applied", mlog.Err(err))
		return merr.Wrap(err, "wait for the routing commit this split follows")
	default:
		logger.Error(ctx, splitShardCommitWedgeLog, mlog.Err(err))
		return merr.Wrap(err, "refuse the split shard commit")
	}

	if err := c.commitShardSplitAtDataCoord(ctx, result, postImage); err != nil {
		return err
	}
	// The task is recorded now, whatever the delta said before.
	delta.Recorded = true

	// Whether there is anything left to write is decided again inside the meta
	// table, under its lock: this callback cannot hold that lock across the
	// datacoord call above, so the answer it computed out here could be stale
	// by the time the write lands.
	//
	// The timetick stamped here is result.GetMaxTimeTick(), the max over every
	// replica of this broadcast -- the same source every other collection-meta
	// write uses (see the adoption's own routing commit,
	// ddl_callbacks_commit_shard_split_routing.go, and
	// MetaTable.AlterCollection), not the control channel replica's own tick.
	// UpdateTimestamp is also the snapshot-KV write ts (MetaTable.
	// ApplyShardSplitRouting), the cache-bypass threshold for a time-travel
	// read (MetaTable.GetCollectionByName), the proxy's guarantee-ts floor and
	// QueryCoord's schema barrier. The control channel's tick can be smaller
	// than another replica's on a secondary cluster, where the two ack
	// callbacks of the collection's concurrent broadcasts are not ordered
	// against each other and cover different vchannel sets -- stamping the
	// smaller tick would move UpdateTimestamp and the snapshot ts backwards
	// against a write that already landed.
	switch err := c.meta.ApplyShardSplitRouting(ctx, header.GetCollectionId(), postImage, delta, result.GetMaxTimeTick()); {
	case err == nil:
	case errors.Is(err, routing.ErrCommitAlreadyApplied):
		logger.Info(ctx, "split shard routing already applied, only expiring caches")
	case errors.Is(err, errAlterCollectionNotFound):
		logger.Warn(ctx, "collection vanished while committing the split shard routing, ignore it")
		return nil
	case errors.Is(err, errShardSplitRoutingCollectionUnavailable):
		// Entered Dropping after the check above, while datacoord was asked.
		// Returned before BroadcastAlteredCollection, which cannot resolve a
		// Dropping collection and would fail this callback forever.
		logger.Warn(ctx, "collection is being dropped while committing the split shard routing, ignore it")
		return nil
	default:
		return merr.Wrap(err, "apply the split shard routing")
	}
	if err := c.broker.BroadcastAlteredCollection(ctx, header.GetCollectionId()); err != nil {
		return merr.Wrap(err, "broadcast the altered collection after the split shard commit")
	}
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, coll.DBName, coll.Name)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// The collection entered Dropping between the lookup above and here:
			// it resolves by id but no longer by name. Its caches are about to be
			// invalidated by the drop itself, so there is nothing left to expire
			// and nothing a retry could achieve.
			logger.Warn(ctx, "collection is being dropped, skipping the cache expiry after the split shard commit")
			return nil
		}
		return merr.Wrap(err, "collect the cache expirations after the split shard commit")
	}
	return c.ExpireCaches(ctx, cacheExpirations)
}

// splitShardDelta names what the split may change -- its header's source and
// targets. The judge reads whether this cluster recorded the task only for a
// source the collection does not list, so datacoord is asked only then; the
// answer is read through CheckShardSplitDrained, whose response describes the
// record, and a task datacoord does not hold is recorded=false, not an error.
// A datacoord that cannot answer is a transient failure the broadcaster
// retries.
func (c *DDLCallback) splitShardDelta(ctx context.Context, coll *model.Collection, header *message.SplitShardMessageHeader) (routing.CommitDelta, error) {
	if slices.Contains(coll.VirtualChannelNames, header.GetSourceVchannel()) {
		return routing.SplitDelta(header.GetSourceVchannel(), header.GetTargetVchannels(), false), nil
	}
	resp, err := c.mixCoord.CheckShardSplitDrained(ctx, &datapb.CheckShardSplitDrainedRequest{
		CollectionId: header.GetCollectionId(),
		SplitTaskId:  header.GetSplitTaskId(),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return routing.CommitDelta{}, merr.Wrap(err, "ask this cluster's datacoord whether the shard split task is recorded")
	}
	return routing.SplitDelta(header.GetSourceVchannel(), header.GetTargetVchannels(), resp.GetRecorded()), nil
}

// splitShardCommitWedgeLog names what a refused SplitShard commit blocks. The
// source is already fenced when the callback runs, so the operator who finds it
// in the log must know it will not clear by itself.
const splitShardCommitWedgeLog = "split shard commit refused after the source was fenced; the broadcaster retries it forever, " +
	"the residues it moves stay unwritable and every later DDL of the collection is queued behind it until the message is repaired"

// commitShardSplitAtDataCoord is the datacoord half: the source's T_switch as
// the source's StreamingNode recorded it and each target's genesis position,
// read off the broadcast result rather than off any local plan, and each
// target's residues and the modulus, read off the routing post-image -- their
// only copy.
//
// Idempotent at datacoord by split task id, so a retry -- including one on a
// secondary that has never heard of the task -- converges on the same record.
func (c *DDLCallback) commitShardSplitAtDataCoord(ctx context.Context, result message.BroadcastResultSplitShardMessageV2, postImage *message.AlterCollectionMessageUpdates) error {
	header := result.Message.Header()

	source := header.GetSourceVchannel()
	sourceResult := result.Results[source]
	if sourceResult == nil {
		// The broadcaster acks only once every vchannel has been appended, so
		// a missing entry is an internal bug. Recording a source with tick
		// zero would make the drain wait for a fence that never happened.
		return merr.WrapErrServiceInternalMsg("split shard broadcast result is missing source vchannel %s", source)
	}
	switchTimeTick, err := splitShardSwitchTimeTick(sourceResult)
	if err != nil {
		mlog.Error(ctx, "split shard source replica was acknowledged without its fence time tick",
			mlog.FieldCollectionID(header.GetCollectionId()),
			mlog.Int64("splitTaskID", header.GetSplitTaskId()),
			mlog.FieldVChannel(source),
			mlog.Uint64("appendTimeTick", sourceResult.TimeTick),
			mlog.Err(err))
		return err
	}
	sources := []*datapb.SplitShardTaskSource{{
		Vchannel:       source,
		SwitchTimeTick: switchTimeTick,
	}}

	targets := make([]*datapb.SplitShardTaskTarget, 0, len(header.GetTargetVchannels()))
	positions := make([]*msgpb.MsgPosition, 0, len(header.GetTargetVchannels()))
	for _, vchannel := range header.GetTargetVchannels() {
		appendResult := result.Results[vchannel]
		if appendResult == nil {
			return merr.WrapErrServiceInternalMsg("split shard broadcast result is missing target vchannel %s", vchannel)
		}
		targets = append(targets, &datapb.SplitShardTaskTarget{
			Vchannel: vchannel,
			Buckets:  streaming.SplitShardTargetBuckets(postImage, vchannel),
		})
		positions = append(positions, streaming.SplitTargetGenesisPosition(vchannel, appendResult.MessageID, appendResult.TimeTick))
	}

	resp, err := c.mixCoord.CommitShardSplit(ctx, &datapb.CommitShardSplitRequest{
		CollectionId:         header.GetCollectionId(),
		SplitTaskId:          header.GetSplitTaskId(),
		Sources:              sources,
		Targets:              targets,
		TargetStartPositions: positions,
		RoutingModulus:       postImage.GetRoutingModulus(),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return merr.Wrap(err, "commit the shard split at datacoord")
	}
	return nil
}

// splitShardSwitchTimeTick reads T_switch from the source replica's extra
// append response (SplitShardExtraResponse), which the source's StreamingNode
// sets to the tick of the task's FIRST fence record.
//
// The append result's own TimeTick is deliberately not a fallback: when the
// broadcaster re-drove a source replica it had not persisted, the recorded
// append is the re-fence, whose tick is later than T_switch. The source's data
// sync service may already have drained past the first fence and closed, so a
// drain gate waiting for that later tick would never open. A result without the
// extra response is refused instead -- as a retriable System error: it is a
// Milvus fault (a node that does not report it, or an ack that lost it), never
// the request's, and the broadcaster retries the callback while it is logged.
func splitShardSwitchTimeTick(sourceResult *message.AppendResult) (uint64, error) {
	if sourceResult.Extra == nil {
		return 0, merr.WrapErrServiceUnavailableMsg("split shard source replica carries no extra append response")
	}
	resp := &message.SplitShardExtraResponse{}
	if err := sourceResult.Extra.UnmarshalTo(resp); err != nil {
		return 0, merr.WrapErrServiceUnavailableMsg(
			"split shard source replica carries an extra append response of type %s, not a split shard one",
			sourceResult.Extra.GetTypeUrl())
	}
	if resp.GetSplitTimeTick() == 0 {
		return 0, merr.WrapErrServiceUnavailableMsg("split shard source replica reports a zero fence time tick")
	}
	return resp.GetSplitTimeTick(), nil
}
