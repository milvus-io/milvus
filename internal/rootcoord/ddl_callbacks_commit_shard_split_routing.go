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
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastCommitShardSplitRouting commits a shard-split routing change into the
// collection meta. It reuses the alter-collection DDL machinery: the routing
// topology rides in an AlterCollection message under the shard-split routing
// field mask, so the existing broadcast -> ack -> meta_table.AlterCollection ->
// Collection.ApplyUpdates path persists it atomically and invalidates the proxy
// caches. The broadcast reaches every shard of the new topology (existing shards
// plus the split targets) so the streamingnode shard managers converge on the
// new routing version.
func (c *Core) broadcastCommitShardSplitRouting(ctx context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
	if req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, collection name is empty")
	}
	vchannels := req.GetVirtualChannelNames()
	if len(vchannels) == 0 {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, no vchannels provided")
	}
	if len(vchannels) != len(req.GetPhysicalChannelNames()) || len(vchannels) != len(req.GetShardInfos()) {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, channel and shard-info arrays must be parallel")
	}

	// Refuse a topology that does not tile the key space. Deriving it here is the
	// last point at which a bad split plan is still only a rejected DDL: once
	// committed, a gap silently drops the writes of the residues nobody claims,
	// and an overlap sends one key to two shards. The same derivation the write
	// path will do is therefore done first, over the same writable-shard filter.
	writable, err := routing.ShardsFromMeta(vchannels, req.GetShardInfos())
	if err != nil {
		return merr.Wrap(merr.WrapErrParameterInvalidMsg("commit shard split routing failed"), err.Error())
	}
	if _, err := routing.Derive(req.GetRoutingModulus(), vchannels, writable); err != nil {
		return merr.Wrap(merr.WrapErrParameterInvalidMsg("commit shard split routing failed"), err.Error())
	}

	// The collection's own resource keys, taken here rather than inherited from
	// the caller. The write switch is one SplitShard broadcast that commits its
	// routing from its own ack callback, so nothing holds the collection's
	// exclusive key across this call any more: what reaches this RPC is the
	// adoption, a stand-alone DDL that has to serialize against every other
	// collection DDL exactly like an AlterCollection does -- it REPLACES the
	// whole vchannel list, so a rename or a schema change interleaved with it
	// would be written against a topology that no longer exists.
	broadcaster, err := c.startBroadcastWithCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	// The name resolved above is not enough to identify the collection the caller
	// meant. A commit is retried, and a collection can be dropped and recreated
	// under the same name in between; since this DDL REPLACES the whole vchannel
	// list, landing a stale topology on a new collection of the same name would
	// overwrite its channels with channels it does not own.
	if req.GetCollectionId() != 0 && coll.CollectionID != req.GetCollectionId() {
		return merr.WrapErrParameterInvalidMsg(
			"commit shard split routing failed, collection %q is now id %d, not the requested %d",
			req.GetCollectionName(), coll.CollectionID, req.GetCollectionId())
	}

	updates := routingUpdatesFromRequest(req)
	if err := checkRoutingCommitAgainstMeta(coll, updates); err != nil {
		return err
	}

	// A commit that delists a vchannel is an adoption, and its ack callback asks
	// THIS cluster's datacoord whether the named split has drained. With no task
	// to name there is no answer datacoord can give, so the callback would refuse
	// the commit forever -- and the broadcast, once appended, holds the
	// collection's exclusive key until its callback succeeds, queueing every
	// later DDL of the collection behind it with no way out. Refused here, while
	// it is still only a failed RPC. System, not input: the request comes from
	// the split coordinator.
	if updates.GetSplitTaskId() == 0 && routingCommitDelistsAVChannel(coll, updates) {
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q retires a vchannel without naming the split task it belongs to",
			coll.Name)
	}

	// Idempotent by committed topology: if the collection already carries exactly
	// the requested vchannels, each at the requested lifecycle state and owning
	// the requested residues, the routing is already committed and this is a
	// no-op. Otherwise the whole topology is (re)applied atomically below. There
	// is no version counter — the source fence plus per-shard state drive the
	// write switch, so a routing change is identified by the topology it sets,
	// not by a monotonic epoch.
	//
	// The residues and the modulus are part of the comparison, not just the
	// states: a commit that re-expresses the same shards at a doubled modulus
	// leaves every state alone, and comparing states only would report it as
	// already committed and silently drop it.
	if routingCommitAlreadyApplied(coll, updates) {
		return errIgnoredAlterCollection
	}

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	header := &messagespb.AlterCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{message.FieldMaskCollectionShardSplitRouting},
		},
		CacheExpirations: cacheExpirations,
	}
	// Broadcast to the control channel, to every vchannel the collection has
	// today, and to every vchannel the new topology names -- deduplicated.
	//
	// The union is what makes the delisted source reachable. An adoption commit
	// drops the fenced source from the vchannel list, and the source's OWN
	// replica of this message is what retires it: the shard interceptor, the
	// recovery storage and the flusher each read `messageutil.RetiresVChannel`
	// off the replica they receive and tear their registration down. Broadcasting
	// only to the post-image would delist the source from the meta while leaving
	// its streamingnode state alive forever, with no later message able to reach
	// it -- a vchannel no longer in the collection receives nothing.
	//
	// The current list also carries the bystander shards, which need the new
	// routing version even though the split did not touch them.
	channels := make([]string, 0, len(coll.VirtualChannelNames)+len(vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	seen := typeutil.NewSet(channels...)
	for _, list := range [][]string{coll.VirtualChannelNames, vchannels} {
		for _, vchannel := range list {
			if seen.Contain(vchannel) {
				continue
			}
			seen.Insert(vchannel)
			channels = append(channels, vchannel)
		}
	}
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterCollectionMessageBody{Updates: updates}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// routingUpdatesFromRequest lifts the five routing fields out of the RPC request
// into the post-image shape a SplitShard message body already carries, so the
// commit checks below have exactly one input type. The RPC and the ack callback
// commit the same thing by two routes; they must not diverge in what they check.
func routingUpdatesFromRequest(req *rootcoordpb.CommitShardSplitRoutingRequest) *messagespb.AlterCollectionMessageUpdates {
	return &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  req.GetVirtualChannelNames(),
		PhysicalChannelNames: req.GetPhysicalChannelNames(),
		ShardInfos:           req.GetShardInfos(),
		RoutingModulus:       req.GetRoutingModulus(),
		ShardBy:              req.GetShardBy(),
		SplitTaskId:          req.GetSplitTaskId(),
	}
}

// routingCommitAlreadyApplied reports whether the collection already carries
// exactly the topology the post-image commits: the same vchannels, each at the
// same lifecycle state and owning the same residues, against the same modulus. A
// shard_by the post-image leaves empty is not compared, since an empty one means
// "nothing to back-fill" rather than "clear it".
func routingCommitAlreadyApplied(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates) bool {
	vchannels := updates.GetVirtualChannelNames()
	if len(coll.VirtualChannelNames) != len(vchannels) || coll.RoutingModulus != updates.GetRoutingModulus() {
		return false
	}
	if updates.GetShardBy() != "" && coll.ShardBy != updates.GetShardBy() {
		return false
	}
	for i, vchannel := range vchannels {
		info, ok := coll.ShardInfos[vchannel]
		if !ok {
			return false
		}
		want := updates.GetShardInfos()[i]
		if info.State != want.GetState() || !slices.Equal(info.Buckets, want.GetHashRouting().GetBuckets()) {
			return false
		}
	}
	return true
}

// shardSplitRoutingSuperseded reports whether the collection has already moved
// AT OR BEYOND the topology this post-image commits, so that applying it would
// move the collection BACKWARDS rather than forward.
//
// It is the predicate that separates the one refusal a retrying ack callback
// must not make from every other one. A routing commit is delivered by a
// callback the broadcaster retries until it returns nil, holding the
// collection's resource keys the whole time; a redelivery that arrives after a
// LATER commit finished the split therefore has to end the callback, not fail
// it. Reported as "nothing left to do", never as an error.
//
// "At or beyond" per shard: a vchannel the collection does not carry is not
// behind the post-image (it is either a target this commit would create or one a
// later commit already retired), and a vchannel it does carry must be at a state
// the post-image's own state could legally have advanced TO -- equal included,
// since the lifecycle allows staying put. The modulus only ever grows, so a
// collection at a smaller modulus has not overtaken anything.
//
// Callers must ask routingCommitAlreadyApplied and the forward check first; see
// MetaTable.ApplyShardSplitRouting for why the order is not free.
func shardSplitRoutingSuperseded(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates) bool {
	if coll.RoutingModulus < updates.GetRoutingModulus() {
		return false
	}
	for i, vchannel := range updates.GetVirtualChannelNames() {
		current, ok := coll.ShardInfos[vchannel]
		if !ok {
			continue
		}
		if !shardStateMayAdvance(updates.GetShardInfos()[i].GetState(), current.State) {
			return false
		}
	}
	return true
}

// routingCommitDelistsAVChannel reports whether the post-image drops a vchannel
// the collection has today -- the signature of a split's adoption, which retires
// the source it drops.
func routingCommitDelistsAVChannel(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates) bool {
	return slices.ContainsFunc(coll.VirtualChannelNames, func(vchannel string) bool {
		return !slices.Contains(updates.GetVirtualChannelNames(), vchannel)
	})
}

// checkRoutingCommitAgainstMeta refuses a commit that would move the collection
// BACKWARDS.
//
// The commit takes no collection lock (see above) and relies on its caller
// serializing it. That assumption is not enforceable from here, and this RPC is
// on the wire, so the one failure it must not have is a lost update: a retry of
// the write-switch commit arriving after the adoption commit would otherwise
// un-adopt the split, putting the released source back to fenced and the adopted
// targets back to not-yet-serviceable. Checking the transition against the meta
// makes a late duplicate a rejected DDL instead of a silent regression, without
// needing the lock.
// namespaceShardBy is the shard_by expression the split coordinator emits for a
// collection it routes by namespace. Kept in step with datacoord's shardByOf.
const namespaceShardBy = "hash(" + common.NamespaceFieldName + ")"

func checkRoutingCommitAgainstMeta(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates) error {
	// Routing is not revocable. Once a collection has been split, its shards own
	// residues and only the modulus says what those residues mean; a commit that
	// zeroes it would leave the collection reading as never-split and route by
	// position over a channel list that now contains retired sources -- writes
	// landing on shards that do not own them, and on fenced ones that reject
	// them. A modulus may grow (a doubling) or stay, never return to zero.
	//
	// System, not input, in both callers: the RPC's caller is datacoord's split
	// manager and the callback's input is a WAL message that same manager wrote,
	// so a revocation is a planning bug and never a user request's content --
	// the same reasoning the namespace branch below already spells out.
	if coll.RoutingModulus != 0 && updates.GetRoutingModulus() == 0 {
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q routes at modulus %d and a commit cannot take it back to none",
			coll.Name, coll.RoutingModulus)
	}

	// The namespace routing key is valid only for a collection whose rows have
	// ALWAYS been placed by it, and that is one configuration, not every
	// namespace collection: the proxy places by namespace only when
	// namespace.sharding.enabled=true AND namespace.mode=partition_key, and
	// sharding.enabled is written as false at create time unless the request
	// set it. A default namespace collection therefore has every row spread
	// over all shards by primary key, and back-filling hash($namespace_id) onto
	// it would send a namespace's NEW rows to one shard while its existing rows
	// stay everywhere -- a delete routed by the namespace hash then reaches one
	// shard and silently misses the rest. Both properties are immutable after
	// creation, so placement history is decidable from the collection's own
	// properties, and this is the last point before the key is persisted.
	//
	// System, not input, and not retriable: the request comes from the split
	// coordinator, a plan that names this key for this collection is a
	// planning bug, and asking again gets the same answer.
	if updates.GetShardBy() == namespaceShardBy {
		enabled, err := common.IsNamespaceShardingEnabled(coll.Properties...)
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "commit shard split routing failed, collection %q has a malformed %s",
				coll.Name, common.NamespaceShardingEnabledKey)
		}
		if !enabled || !common.IsNamespaceModePartitionKey(coll.Properties...) {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, collection %q cannot route by %s: its rows are placed by primary key "+
					"(namespace.sharding.enabled=%t, namespace.mode=%s), so it must split under hash(pk) or not at all",
				coll.Name, namespaceShardBy, enabled, common.GetNamespaceMode(coll.Properties...))
		}
	}

	for i, vchannel := range updates.GetVirtualChannelNames() {
		current, ok := coll.ShardInfos[vchannel]
		if !ok {
			// A vchannel the collection does not have yet: a split target being
			// created. Any state is a valid start.
			continue
		}
		to := updates.GetShardInfos()[i].GetState()
		if !shardStateMayAdvance(current.State, to) {
			// System for the same reason as above. Note what this does NOT
			// distinguish: a post-image a later commit has overtaken looks
			// exactly like an incoherent one from here. The callback separates
			// the two with shardSplitRoutingSuperseded; the RPC does not need
			// to, because its caller can act on the refusal.
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q cannot go from %s back to %s",
				vchannel, current.State.String(), to.String())
		}
	}

	// The loop above only sees vchannels the post-image NAMES. A vchannel it
	// drops is checked by no state transition at all -- it simply ceases to
	// exist -- which is the one way this DDL can retire a live shard silently:
	// its residues would have no owner, its unmoved data would be unreachable,
	// and there is no later message that could reach it to say so, because a
	// vchannel the collection no longer names receives nothing.
	//
	// A shard may therefore only be delisted from the one state that means "this
	// shard has stopped taking writes and its data is being moved": Splitting.
	// Anything else -- Normal, Creating, or a shard already retired by an
	// earlier commit -- is a planning bug.
	for _, vchannel := range coll.VirtualChannelNames {
		if slices.Contains(updates.GetVirtualChannelNames(), vchannel) {
			continue
		}
		current, ok := coll.ShardInfos[vchannel]
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q cannot be retired: the collection carries no shard info for it, "+
					"so nothing records that it has stopped taking writes", vchannel)
		}
		if current.State != schemapb.ShardState_ShardSplitting {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q cannot be retired from %s: only a fenced shard may be delisted",
				vchannel, current.State.String())
		}
	}
	return nil
}

// shardStateMayAdvance reports whether a shard may move from one lifecycle state
// to another. Staying put is always allowed, which is what makes a retry of the
// same commit a no-op rather than a rejection.
//
// The lifecycle only ever runs one way. A source is fenced (Normal ->
// Splitting) and later released (Splitting -> Dropped); the fence is recorded in
// the WAL and is permanent, so there is no way back to Normal. A target is
// created writable and later adopted (Creating -> Normal).
//
// A target is NOT abandonable. It is write-routable from the moment the write
// switch publishes it -- `routing.ShardsFromMeta` admits a Creating shard
// precisely so its residues take writes before it is serviceable for reads --
// so moving one to Dropped would discard rows that were already accepted, and
// the residues it owns would have no shard at all. A split that cannot finish is
// finished forward, by adopting the targets; there is no state transition that
// undoes it.
func shardStateMayAdvance(from, to schemapb.ShardState) bool {
	if from == to {
		return true
	}
	switch from {
	case schemapb.ShardState_ShardNormal:
		return to == schemapb.ShardState_ShardSplitting
	case schemapb.ShardState_ShardCreating:
		return to == schemapb.ShardState_ShardNormal
	case schemapb.ShardState_ShardSplitting:
		return to == schemapb.ShardState_ShardDropped
	default:
		// Dropped, and any state a later version adds that this one does not know
		// how to advance.
		return false
	}
}

// checkShardSplitAdoptionDrained gates a split's adoption on THIS cluster
// having drained the sources it retires.
//
// The adoption is the commit whose post-image no longer names a vchannel the
// broadcast reached: applying it retires that source everywhere -- the meta
// stops routing to it, and its own replica of this message tears its
// streamingnode registration down. That is only safe once the source's data has
// actually moved to the targets, and "moved" is a per-cluster fact: a secondary
// replays the same WAL but compacts, imports and flushes on its own schedule, so
// the primary's drain says nothing about it. Each cluster therefore asks its own
// datacoord, and refuses the commit until the answer is yes; the broadcaster
// retries the callback with backoff.
//
// Two redeliveries are deliberately NOT gated, and for them the gate reports
// skip=true: the caller must then apply NOTHING, not merely skip the drain
// question.
//
//   - a post-image the collection already carries;
//   - a post-image a LATER routing commit has already overtaken.
//
// datacoord reclaims a split's task record once the split is done, so either
// redelivery would ask about a task id nobody knows any more -- an answer that
// is a System error by design, and one this callback would then retry forever,
// wedging every later DDL of the collection behind it. And a superseded
// post-image must not be WRITTEN either: applying it would put the retired
// source back and un-adopt the targets the later commit adopted. That is why
// this returns a skip flag rather than nil -- nil would let the apply run.
//
// A routing commit that delists nothing -- the write switch, which publishes the
// targets while the source keeps serving -- has nothing to drain and nothing to
// skip.
func (c *DDLCallback) checkShardSplitAdoptionDrained(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) (skip bool, err error) {
	header := result.Message.Header()
	updates := result.Message.MustBody().GetUpdates()
	if !slices.ContainsFunc(result.Message.BroadcastHeader().VChannels, func(vchannel string) bool {
		return messageutil.RetiresVChannel(header, updates, vchannel)
	}) {
		return false, nil
	}

	// allowUnavailable: a collection mid-split is available anyway, and one that
	// is being dropped is reported by the apply below rather than here.
	coll, err := c.meta.GetCollectionByID(ctx, "", header.GetCollectionId(), typeutil.MaxTimestamp, true)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return false, nil
		}
		return false, merr.Wrap(err, "load the collection for the shard split adoption")
	}
	if routingCommitAlreadyApplied(coll, updates) {
		mlog.Info(ctx, "shard split adoption already applied, skipping the drain gate and the meta apply",
			mlog.FieldMessage(result.Message))
		return true, nil
	}
	if shardSplitRoutingSuperseded(coll, updates) {
		mlog.Warn(ctx, "shard split adoption superseded by a later routing commit, skipping the drain gate and the meta apply",
			mlog.FieldMessage(result.Message))
		return true, nil
	}

	resp, err := c.mixCoord.CheckShardSplitDrained(ctx, &datapb.CheckShardSplitDrainedRequest{
		CollectionId: header.GetCollectionId(),
		SplitTaskId:  updates.GetSplitTaskId(),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return false, merr.Wrap(err, "ask this cluster's datacoord whether the shard split has drained")
	}
	if !resp.GetDrained() {
		mlog.Info(ctx, "shard split adoption is waiting for this cluster to drain the source",
			mlog.FieldCollectionID(header.GetCollectionId()),
			mlog.Int64("splitTaskID", updates.GetSplitTaskId()))
		return false, merr.WrapErrServiceInternalMsg("shard split %d of collection %d not drained on this cluster yet",
			updates.GetSplitTaskId(), header.GetCollectionId())
	}
	return false, nil
}
