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

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Reclaiming the vchannels a split retired.
//
// A split retires its sources but the routing commit only ever APPENDS, so a
// retired source stays in the collection's arrays forever carrying
// ShardDropped. It is dead as a shard — no predicate, so nothing routes to it;
// dropped from the read set; its segments retired at adoption; its delegator
// released — and yet it still costs the one thing a shard count is capped by.
//
// The allocator derives occupancy straight from the list it is handed:
//
//	occupied[funcutil.ToPhysicalChannel(vchannel)] = struct{}{}
//	...
//	if _, ok := occupied[channel.id.Name]; ok { continue }
//
// and the caller hands it collection.VChannelNames, retired entries included.
// So each doubling costs +1 live shard and +2 held pchannels, permanently:
// from two shards, the default sixteen pchannels are exhausted at about eight
// live shards rather than sixteen (§11).
//
// Removing the entry is all it takes to give the slot back, because the routing
// commit is a wholesale replace on both sides. rootcoord rebuilds
// VirtualChannelNames, PhysicalChannelNames and the ShardInfos map from the
// arrays it is sent (model.Collection.ApplyUpdates), so a SHORTER list removes
// the vchannel from all three at once and recomputes ShardsNum. Nothing in that
// path needed changing.
//
// The slot alone is not the whole job, and stopping there would be worse than
// doing nothing. The streamingnode indexes its shard manager by COLLECTION id,
// one entry per pchannel, so once the slot is free a later vchannel of the same
// collection can be allocated onto that pchannel — find the retired entry still
// sitting there, skip its own registration with nothing but a warning, and
// inherit the retired shard's SPLITTED state, leaving the new shard
// permanently unwritable. And DropCollection is broadcast to exactly
// collection.VirtualChannelNames, so a vchannel removed from that list first
// would never receive a teardown of any kind: its streamingnode state would
// outlive the collection itself.
//
// So each reclaim appends DropVChannel FIRST and drops the vchannel from the
// collection only once that succeeds. Ordered that way, a failure between the
// two leaves a torn-down vchannel still listed — the next sweep re-appends
// (the teardown is idempotent) and finishes the job. The reverse order would
// leave an orphan nothing can reach.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §3.5.

// reclaimRetiredVChannelsOnce sweeps every collection that carries a retired
// shard and gives the slots back.
func (m *shardSplitManager) reclaimRetiredVChannelsOnce() {
	if m.router == nil {
		// Wired during server initialization; a tick before that waits rather
		// than dereferencing nil.
		return
	}
	for _, collectionID := range m.meta.ListCollections() {
		if err := m.reclaimRetiredVChannels(collectionID); err != nil {
			mlog.With(mlog.FieldComponent("shard-split-manager"),
				mlog.FieldCollectionID(collectionID)).
				RatedWarn(m.ctx, 60, "reclaim retired vchannels failed, retrying next tick",
					mlog.Err(err))
		}
	}
}

// reclaimRetiredVChannels drops one collection's reclaimable retired vchannels.
//
// A no-op unless something is actually reclaimable, so the common case costs a
// scan of the shard map and no RPC.
func (m *shardSplitManager) reclaimRetiredVChannels(collectionID int64) error {
	collection := m.meta.GetCollection(collectionID)
	if collection == nil {
		return nil
	}
	reclaimable := m.reclaimableVChannels(collection)
	if len(reclaimable) == 0 {
		return nil
	}

	kept := make([]string, 0, len(collection.VChannelNames)-len(reclaimable))
	for _, vchannel := range collection.VChannelNames {
		if !reclaimable.Contain(vchannel) {
			kept = append(kept, vchannel)
		}
	}
	// Refuse to empty a collection. Unreachable while a routable shard is never
	// reclaimable, but the arrays are the collection's only description of its
	// own topology and committing an empty one would be unrecoverable, so this
	// is checked rather than argued.
	if len(kept) == 0 {
		return merr.WrapErrServiceInternalMsg(
			"refuse to reclaim every vchannel of collection %d", collectionID)
	}

	// Only now, with the survivors known to be non-empty, is the teardown safe
	// to append. It is irreversible: doing it before that check would tear down
	// the collection's last vchannel and then refuse to commit, leaving the
	// collection pointing at a channel the streamingnode no longer has.
	//
	// WAL side first, meta side second. DropCollection is broadcast to exactly
	// collection.VirtualChannelNames, so a vchannel dropped from that list
	// before its teardown would never receive one at all. Ordered this way a
	// failure in between leaves a torn-down vchannel still listed, and the next
	// sweep re-appends — the teardown is idempotent — and finishes.
	for _, vchannel := range reclaimable.Collect() {
		if err := streaming.DropSplitVChannel(m.ctx, m.wal, streaming.DropSplitVChannelParam{
			CollectionID: collectionID,
			DBID:         collection.DatabaseID,
			VChannel:     vchannel,
		}); err != nil {
			return errors.Wrapf(err, "tear down retired vchannel %s failed", vchannel)
		}
	}

	pchannels := make([]string, len(kept))
	shardInfos := make([]*schemapb.CollectionShardInfo, len(kept))
	for i, vchannel := range kept {
		pchannels[i] = funcutil.ToPhysicalChannel(vchannel)
		info := collection.ShardInfos[vchannel]
		if info == nil {
			// Every surviving shard must keep the exact predicate it already
			// has: this commit retires channels, it does not re-derive routing.
			// A shard the meta cannot describe is one this sweep must not touch.
			return merr.WrapErrServiceInternalMsg(
				"refuse to reclaim: collection %d has no shard info for surviving vchannel %s",
				collectionID, vchannel)
		}
		cloned := proto.Clone(info).(*schemapb.CollectionShardInfo)
		cloned.VchannelName = vchannel
		shardInfos[i] = cloned
	}

	logger := mlog.With(mlog.FieldComponent("shard-split-manager"),
		mlog.FieldCollectionID(collectionID))
	if err := m.router.CommitShardSplitRouting(m.ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               collection.DatabaseName,
		CollectionName:       collection.Schema.GetName(),
		CollectionId:         collectionID,
		VirtualChannelNames:  kept,
		PhysicalChannelNames: pchannels,
		ShardInfos:           shardInfos,
		// Unchanged: reclamation retires dead channels, it never converts a
		// collection between range and hash routing.
		RoutingModulus: collection.RoutingModulus,
	}); err != nil {
		return err
	}

	m.refreshCachedTopology(collectionID, kept, shardInfos, collection.RoutingModulus)
	logger.Info(m.ctx, "reclaimed retired split vchannels",
		mlog.Strings("reclaimed", reclaimable.Collect()),
		mlog.Int("heldBefore", len(collection.VChannelNames)),
		mlog.Int("heldAfter", len(kept)))
	return nil
}

// reclaimableVChannels is the set of a collection's vchannels that a split
// retired and nothing needs any more.
//
// Three conditions, each guarding a different way a premature reclaim would
// hurt:
//
//   - **ShardDropped.** Only a retired source qualifies. A Splitting source is
//     mid-window and still fronts its children; a routable shard is live.
//   - **No live split task references it.** While a task still names it as a
//     source, `getRealSegmentsForSplitFamily` merges the targets' segments into
//     its recovery view (§9) and querycoord may still hold it in the read set —
//     the handover of §8 completes after datacoord's FSM does. A task is dropped
//     from the cache only once it has been terminal for the retention window, so
//     waiting for that is also the delay that lets the handover finish.
//   - **No LIVE segment on the channel.** Adoption retires the source's
//     segments, so anything still in a live state means adoption has not
//     finished draining and removing the vchannel would orphan reachable data.
//     Dropped segments do NOT block: they are the evidence adoption completed,
//     not a sign it did not. They linger in meta until GC runs, which is far
//     longer than a split takes, so counting them would make a retired source
//     effectively unreclaimable. Their cleanup is unaffected either way --
//     recycleDroppedSegments iterates the segments themselves by state and
//     derives the channel from each segment's InsertChannel, never from the
//     collection's vchannel list.
//
// A split's provenance -- which sources a target was carved from, and which one
// fronted its reads -- is not in the collection meta at all: it lives in the
// split task, and is discarded with it. So reclamation has nothing to clear
// here. The earlier design carried the two fields on every shard info, which
// meant an adopted target kept naming sources that no longer existed until a
// sweep pruned them; keeping provenance where its lifetime already ends removes
// both the field and the sweep.

func (m *shardSplitManager) reclaimableVChannels(collection *collectionInfo) typeutil.Set[string] {
	reclaimable := typeutil.NewSet[string]()
	for _, vchannel := range collection.VChannelNames {
		info := collection.ShardInfos[vchannel]
		if info == nil || info.GetState() != schemapb.ShardState_ShardDropped {
			continue
		}
		if m.hasActiveTaskOnVChannel(vchannel) || m.referencedByAnyTask(vchannel) {
			continue
		}
		if m.hasLiveSegment(vchannel) {
			continue
		}
		reclaimable.Insert(vchannel)
	}
	return reclaimable
}

// hasLiveSegment reports whether the channel still carries a segment in a
// non-Dropped state, i.e. data a reader could still be routed to.
func (m *shardSplitManager) hasLiveSegment(vchannel string) bool {
	for _, segment := range m.meta.GetRealSegmentsForChannel(vchannel) {
		if segment.GetState() != commonpb.SegmentState_Dropped {
			return true
		}
	}
	return false
}

// referencedByAnyTask reports whether ANY task still in the cache names the
// vchannel, terminal ones included.
//
// hasActiveTaskOnVChannel only covers active tasks, and the window that matters
// here opens exactly when a task goes terminal: datacoord calls the split Done
// before querycoord has released the source, and the source leaves the read set
// only once every target is serving. The task lingers until the retention
// window reaps it, so treating any surviving task as a reference is what keeps
// this sweep from racing the handover.
func (m *shardSplitManager) referencedByAnyTask(vchannel string) bool {
	referenced := false
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if slices.Contains(splitSourceVChannels(task), vchannel) {
			referenced = true
			return false
		}
		for _, target := range task.GetTargets() {
			if target.GetVchannel() == vchannel {
				referenced = true
				return false
			}
		}
		return true
	})
	return referenced
}
