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

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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

	// No collection lock here, deliberately. This is the last step of a split's
	// write switch, and its only caller — datacoord's split manager — already
	// holds the collection's exclusive resource key across the whole fence ->
	// create -> commit span, precisely so no DDL can change the collection
	// underneath the target vchannels it just created. Taking the same exclusive
	// key again from inside that span would block on the caller and deadlock:
	// the broadcaster's locker is one process-wide instance, and its acquire does
	// not honour a context.
	//
	// The routing commit is therefore protected by its caller, not by itself.
	// Reaching it from anywhere else would be a bug, and an unlocked one.
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx)
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

	if err := checkRoutingCommitAgainstMeta(coll, req); err != nil {
		return err
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
	if routingCommitAlreadyApplied(coll, req) {
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
	updates := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  vchannels,
		PhysicalChannelNames: req.GetPhysicalChannelNames(),
		ShardInfos:           req.GetShardInfos(),
		RoutingModulus:       req.GetRoutingModulus(),
		ShardBy:              req.GetShardBy(),
	}

	// Broadcast to every shard of the new topology plus the control channel, so
	// all streamingnode shard managers (including the new split targets) and the
	// proxy caches pick up the new routing version.
	channels := make([]string, 0, len(vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, vchannels...)
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

// routingCommitAlreadyApplied reports whether the collection already carries
// exactly the topology the request commits: the same vchannels, each at the same
// lifecycle state and owning the same residues, against the same modulus. A
// shard_by the request leaves empty is not compared, since an empty one means
// "nothing to back-fill" rather than "clear it".
func routingCommitAlreadyApplied(coll *model.Collection, req *rootcoordpb.CommitShardSplitRoutingRequest) bool {
	vchannels := req.GetVirtualChannelNames()
	if len(coll.VirtualChannelNames) != len(vchannels) || coll.RoutingModulus != req.GetRoutingModulus() {
		return false
	}
	if req.GetShardBy() != "" && coll.ShardBy != req.GetShardBy() {
		return false
	}
	for i, vchannel := range vchannels {
		info, ok := coll.ShardInfos[vchannel]
		if !ok {
			return false
		}
		want := req.GetShardInfos()[i]
		if info.State != want.GetState() || !slices.Equal(info.Buckets, want.GetHashRouting().GetBuckets()) {
			return false
		}
	}
	return true
}

// checkRoutingCommitAgainstMeta refuses a commit that would move the collection
// BACKWARDS.
//
// The commit takes no collection lock (see above) and relies on its caller
// serialising it. That assumption is not enforceable from here, and this RPC is
// on the wire, so the one failure it must not have is a lost update: a retry of
// the write-switch commit arriving after the adoption commit would otherwise
// un-adopt the split, putting the released source back to fenced and the adopted
// targets back to not-yet-serviceable. Checking the transition against the meta
// makes a late duplicate a rejected DDL instead of a silent regression, without
// needing the lock.
func checkRoutingCommitAgainstMeta(coll *model.Collection, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
	// Routing is not revocable. Once a collection has been split, its shards own
	// residues and only the modulus says what those residues mean; a commit that
	// zeroes it would leave the collection reading as never-split and route by
	// position over a channel list that now contains retired sources -- writes
	// landing on shards that do not own them, and on fenced ones that reject
	// them. A modulus may grow (a doubling) or stay, never return to zero.
	if coll.RoutingModulus != 0 && req.GetRoutingModulus() == 0 {
		return merr.WrapErrParameterInvalidMsg(
			"commit shard split routing failed, collection %q routes at modulus %d and a commit cannot take it back to none",
			req.GetCollectionName(), coll.RoutingModulus)
	}

	for i, vchannel := range req.GetVirtualChannelNames() {
		current, ok := coll.ShardInfos[vchannel]
		if !ok {
			// A vchannel the collection does not have yet: a split target being
			// created. Any state is a valid start.
			continue
		}
		to := req.GetShardInfos()[i].GetState()
		if !shardStateMayAdvance(current.State, to) {
			return merr.WrapErrParameterInvalidMsg(
				"commit shard split routing failed, shard %q cannot go from %s back to %s",
				vchannel, current.State.String(), to.String())
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
// created writable and later adopted (Creating -> Normal), or abandoned if the
// split is aborted before adoption (Creating -> Dropped). Dropped is terminal.
func shardStateMayAdvance(from, to schemapb.ShardState) bool {
	if from == to {
		return true
	}
	switch from {
	case schemapb.ShardState_ShardNormal:
		return to == schemapb.ShardState_ShardSplitting
	case schemapb.ShardState_ShardCreating:
		return to == schemapb.ShardState_ShardNormal || to == schemapb.ShardState_ShardDropped
	case schemapb.ShardState_ShardSplitting:
		return to == schemapb.ShardState_ShardDropped
	default:
		// Dropped, and any state a later version adds that this one does not know
		// how to advance.
		return false
	}
}
