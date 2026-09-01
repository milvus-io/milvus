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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Routing commit for a hash-routed (primary-key) shard split.
//
// It has the same shape as the namespace split's commit — send the full
// post-split topology, applied idempotently by shard state — and differs only
// in the predicate each target carries: a hash bucket instead of a key range.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §3.1.

// hashSplitFrontingHosts assigns every target exactly one source to front its
// reads during the split window, round-robin over the task's sources.
//
// The assignment must exist because the fronting source spawns the target
// in-process and merges its results into its own, while a read fans out to every
// source: if two sources fronted one target, that target's post-fence rows would
// be returned twice. With a single source the question does not arise, which is
// why it only appears once a rehash gives every target N sources.
//
// It is a pure function of the task's persisted source and target order, and it
// feeds BOTH the per-source fence messages and the routing commit — so the
// streamingnode's live spawn and a querynode's rebuild from meta cannot disagree
// about who fronts what, whatever order they happen in.
func hashSplitFrontingHosts(task *datapb.SplitShardTask) map[string]string {
	sources := splitSourceVChannels(task)
	hosts := make(map[string]string, len(task.GetTargets()))
	if len(sources) == 0 {
		return hosts
	}
	for i, target := range task.GetTargets() {
		hosts[target.GetVchannel()] = sources[i%len(sources)]
	}
	return hosts
}

// toMessageHashSplitTargets converts the targets one source must front into the
// fence message's target list.
//
// Only the vchannel names matter to the consumers: the SplitShard message tells
// the source streamingnode which shards to front, and the fence itself is
// per-vchannel. The residues ride along as permanent provenance in the WAL, but
// nothing reads them there -- routing reaches the cluster through the routing
// commit, which is also where it must be authoritative.
//
// The list is the source's OWN targets, not all of them: the delegator spawns a
// child for every target the message names (`ProcessSplitShard`), so sending the
// full list to every source is what would multiply the fronting.
func toMessageHashSplitTargets(task *datapb.SplitShardTask, sourceVChannel string) []*message.SplitShardTarget {
	hosts := hashSplitFrontingHosts(task)
	converted := make([]*message.SplitShardTarget, 0, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		if hosts[target.GetVchannel()] != sourceVChannel {
			continue
		}
		converted = append(converted, &message.SplitShardTarget{
			Vchannel: target.GetVchannel(),
		})
	}
	return converted
}

// allMessageHashSplitTargets lists every target, for the steps that address the
// targets themselves rather than one source's fronting duty (creating the
// vchannels).
func allMessageHashSplitTargets(targets []*datapb.SplitShardTaskTarget) []*message.SplitShardTarget {
	converted := make([]*message.SplitShardTarget, 0, len(targets))
	for _, target := range targets {
		converted = append(converted, &message.SplitShardTarget{
			Vchannel: target.GetVchannel(),
		})
	}
	return converted
}

// targetShardInfoPB builds the CollectionShardInfo of one split target, carrying
// the residues it owns. Relabeling and rewriting splits build it identically --
// they differ in how the DATA moves, not in how the keys are described.
func targetShardInfoPB(state schemapb.ShardState, target *datapb.SplitShardTaskTarget) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state}
	if len(target.GetBuckets()) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: append([]uint64(nil), target.GetBuckets()...)},
		}
	}
	return si
}

// carryThroughShardInfoPB copies an untouched shard's routing meta forward
// unchanged, whatever predicate variant it carries.
//
// A split of one shard must leave every other shard's routing exactly as it
// was; copying the oneof wholesale (rather than reading one variant out and
// rebuilding it) is what keeps that true as new variants are added.
func carryThroughShardInfoPB(info *schemapb.CollectionShardInfo) *schemapb.CollectionShardInfo {
	if info == nil {
		return &schemapb.CollectionShardInfo{State: schemapb.ShardState_ShardNormal}
	}
	return &schemapb.CollectionShardInfo{
		State:                info.GetState(),
		LastTruncateTimeTick: info.GetLastTruncateTimeTick(),
		Routing:              info.GetRouting(),
	}
}

// commitRouting commits a hash split's routing change into the collection
// meta via rootcoord: the source shard moves to sourceState and every target to
// targetState carrying its predicate, and the collection stays hash-routed.
//
// The full post-split topology is sent and rootcoord applies it idempotently by
// shard state, so a retry — and a crash between the commit and the task's state
// advance — is safe.
func (m *shardSplitManager) commitRouting(
	task *datapb.SplitShardTask,
	collection *collectionInfo,
	sourceState, targetState schemapb.ShardState,
) error {
	if m.router == nil {
		// The routing committer is wired during server initialization; a task
		// that ticks before that must wait rather than dereference nil. Erroring
		// keeps the task in its current state, so the next tick retries.
		return merr.WrapErrServiceInternalMsg("routing committer not wired yet")
	}

	// The precondition that makes the multi-source fence safe: a source may only
	// be retired once its own fence is recorded. The routing flip is global — a
	// target's bucket draws keys from every source — so committing it while any
	// source still accepted writes would give one primary key two live writers
	// on two WALs, with no order between them to resolve an insert against a
	// later delete. Checking it here rather than trusting the caller's sequencing
	// makes the invariant enforced instead of assumed.
	if !allHashSourcesFenced(task) {
		return merr.WrapErrServiceInternalMsg(
			"refuse to commit hash split routing: %d of %d sources are not fenced",
			len(task.GetSources())-countFencedHashSources(task), len(task.GetSources()))
	}

	targets := task.GetTargets()
	sourceVChannels := splitSourceVChannels(task)
	isSource := typeutil.NewSet(sourceVChannels...)

	// The topology BEFORE this split, expressed as residues at the modulus it
	// routed by. A never-split collection describes itself implicitly -- shard i
	// owns residue i at modulus N -- and residuesOf makes both forms the same
	// thing, which is what lets an untouched shard be carried across a doubling
	// without a special case for "it had no residues yet".
	before, err := residuesOf(collection)
	if err != nil {
		return err
	}
	// The modulus AFTER this split, decided when the task was planned. It differs
	// from `before` only when the split had to double it.
	//
	// Read from the task, never re-derived: the commit grows the collection's
	// vchannel list, so a modulus computed from the meta the commit is about to
	// change would disagree with the residues the planner already handed the
	// targets -- and a topology built from two different moduli does not tile.
	// A task with none is a planning bug, refused rather than guessed at.
	after := task.GetRoutingModulus()
	if after == 0 {
		return merr.WrapErrServiceInternalMsg(
			"refuse to commit shard split routing: task %d carries no routing modulus", task.GetTaskId())
	}
	targetByVChannel := make(map[string]*datapb.SplitShardTaskTarget, len(targets))
	for _, target := range targets {
		targetByVChannel[target.GetVchannel()] = target
	}

	// The full new vchannel list: the collection's current vchannels plus any
	// target not already present (already present on a retry, e.g. the adoption
	// commit after the write-switch commit).
	vchannels := make([]string, len(collection.VChannelNames))
	copy(vchannels, collection.VChannelNames)
	for _, target := range targets {
		if !slices.Contains(vchannels, target.GetVchannel()) {
			vchannels = append(vchannels, target.GetVchannel())
		}
	}

	pchannels := make([]string, len(vchannels))
	shardInfos := make([]*schemapb.CollectionShardInfo, len(vchannels))
	for i, vchannel := range vchannels {
		pchannels[i] = funcutil.ToPhysicalChannel(vchannel)
		switch {
		case isSource.Contain(vchannel):
			// A source is fenced (Splitting) then released (Dropped). Its key
			// space now belongs to the targets, so it carries no predicate —
			// which is also what keeps the remaining shards an exact cover once
			// the routing derivation filters it out.
			shardInfos[i] = &schemapb.CollectionShardInfo{State: sourceState}
		case targetByVChannel[vchannel] != nil:
			// The target's residues are already expressed at `after`: the planner
			// produced them together with that modulus.
			shardInfos[i] = targetShardInfoPB(targetState, targetByVChannel[vchannel])
		default:
			// An untouched shard. Carry its state through, and re-express the
			// residues it already owns at the collection's modulus after this
			// split.
			//
			// Both halves of that matter. A never-split collection stores no
			// residues at all: its shards are hash % shardNum by position, and
			// the write path reads "no shard has residues" as exactly that. But
			// the moment ONE shard is split, the targets get explicit residues --
			// and a shard still carrying none then contributes nothing, so the
			// derived table has a gap and the write path cannot build it at all.
			// Once a collection describes routing anywhere, every live shard must.
			//
			// And when the split doubled the modulus, an untouched shard's
			// residue r at M covers {r, r+M} at 2M. Writing r alone would hand
			// half of its keys to nobody -- the same gap, arrived at from the
			// other direction. Rebase is what keeps "this split touched one
			// shard" true while the arithmetic underneath it changes for
			// everyone.
			shardInfos[i] = carryThroughShardInfoPB(collection.ShardInfos[vchannel])
			// A vchannel `before` does not know is an already-retired source from
			// an earlier split: it owns nothing and stays that way.
			if own, err := before.of(vchannel); err == nil {
				rebased, err := routing.Rebase(own, before.modulus, after)
				if err != nil {
					return err
				}
				shardInfos[i].Routing = &schemapb.CollectionShardInfo_HashRouting{
					HashRouting: &schemapb.HashRouting{Buckets: rebased},
				}
			}
		}
		// Every shard carries its own vchannel so consumers need not rely on
		// positional alignment with the parallel virtual_channel_names array.
		shardInfos[i].VchannelName = vchannel
	}

	if err := m.router.CommitShardSplitRouting(m.ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               collection.DatabaseName,
		CollectionName:       collection.Schema.GetName(),
		CollectionId:         task.GetCollectionId(),
		VirtualChannelNames:  vchannels,
		PhysicalChannelNames: pchannels,
		ShardInfos:           shardInfos,
		RoutingModulus:       after,
		// Back-filled here and nowhere else: a collection created before shard_by
		// existed declares nothing, and its first split is the moment the routing
		// key stops being inferred from collection properties and becomes a
		// recorded fact. A collection that already declares one sends empty,
		// which rootcoord leaves alone.
		ShardBy: shardByOf(collection),
	}); err != nil {
		return err
	}

	m.refreshCachedTopology(task.GetCollectionId(), vchannels, shardInfos, after)
	return nil
}

// shardByOf returns the routing-key expression to back-fill.
//
// It is always sent; rootcoord writes it only when the collection declares none,
// so a later split of a collection that already has one leaves it alone. Sending
// it unconditionally keeps this function a pure description of the collection
// rather than a decision that depends on what the meta happens to hold.
//
// The expression names which value gets hashed. A namespace collection routes by
// its namespace; every other collection routes by its primary key. Both were
// previously inferred from collection properties on every read, which made the
// routing key a function of mutable state; recording it at the first split
// freezes it.
func shardByOf(collection *collectionInfo) string {
	if collection.Schema.GetEnableNamespace() {
		return "hash(" + common.NamespaceFieldName + ")"
	}
	return "hash(" + primaryFieldNameOf(collection.Schema) + ")"
}

// primaryFieldNameOf returns the collection's primary key field name.
func primaryFieldNameOf(schema *schemapb.CollectionSchema) string {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field.GetName()
		}
	}
	return ""
}

// refreshCachedTopology updates datacoord's cached view of a collection's shard
// topology to the one just committed.
//
// Nothing else does it. BroadcastAlteredCollection — datacoord's handler for a
// collection change — refreshes only Properties and Schema, so after a routing
// commit the cache still describes the PRE-split topology. Every later decision
// that reads it is then made against a collection that no longer exists: the
// shard-count reconciler compares the requested count against the old shard set
// and starts the same rehash again, forever; and the next routing commit builds
// its vchannel list from the stale names, dropping the shards the previous split
// created.
//
// Refreshing from what was just committed is safe because the commit is the
// authority: rootcoord applies the full topology as sent, idempotently by shard
// state, so the arrays here are exactly what the meta now holds.
func (m *shardSplitManager) refreshCachedTopology(
	collectionID int64,
	vchannels []string,
	shardInfos []*schemapb.CollectionShardInfo,
	modulus uint64,
) {
	cloned := m.meta.GetClonedCollectionInfo(collectionID)
	if cloned == nil {
		return
	}
	cloned.VChannelNames = vchannels
	cloned.RoutingModulus = modulus
	cloned.ShardInfos = buildShardInfoMap(vchannels, shardInfos)
	m.meta.AddCollection(cloned)
}
