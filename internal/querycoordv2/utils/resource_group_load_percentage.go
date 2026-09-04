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

package utils

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// LoadPercentageByResourceGroup answers a narrower question than the
// collection-wide load percentage CollectionObserver maintains: how loaded is
// collectionID, restricted to the replica(s) that live in resource group
// rgName. Deployments that load each resource group independently need this,
// because two resource groups on the same collection can legitimately sit at
// very different progress -- one at 100%, another at 0% -- and a
// collection-wide average would report the same misleading figure to both.
//
// It is a free function over the three read-only stores it needs -- the meta
// (for replicas and load registration), the target manager, and the
// distribution manager -- rather than a method, so that both Server and
// CollectionObserver can call it. CollectionObserver holds exactly these three
// and does not import the querycoordv2 root package.
//
// rgName == "" means "every replica of the collection", which is the
// collection-wide question and the behavior every upstream caller wants: an
// empty resource group is not a filter that matches nothing, it is the absence
// of a filter. This keeps the resource-group concept inert for callers that do
// not use it.
//
// # Outcomes
//
// Five distinct outcomes are all spelled "the collection isn't ready in this
// resource group", and callers must be able to tell them apart. The
// percentage alone separates the first two; the error separates the rest,
// and the error outcomes differ in whether waiting can ever help, and in
// whose fault the state is:
//
//   - -1, nil error: rgName has no replica of this collection at all. There
//     is nothing to report a percentage for. Terminal once the load has been
//     registered -- but NOT during the load-startup window: job_load.go
//     spawns the replicas before it registers the collection, in a separate
//     meta commit, and the registration check below runs before the replica
//     scan, so a poll landing between the two commits reads -1 for a group
//     that already holds replicas. The window is one etcd commit wide and
//     the next poll answers correctly; it is left as-is rather than
//     special-cased because telling it apart from a registered collection
//     with no partitions -- which must answer -1, so that this figure agrees
//     with what GetShardLeaders will do -- would mean layering an Exist check
//     back on top of the registration test these surfaces just unified on.
//     A caller treating -1 as terminal should confirm it across two polls.
//   - 0, nil error: rgName has a replica of this collection, but that replica
//     has not picked up any of the collection's current load targets yet.
//     This is a real, distinct state from "no replica" -- it means loading is
//     underway but has not made progress, not that the resource group is
//     unrelated to the collection. 0 is never truncation: any progress at
//     all reads at least 1 (see replicaLoadPercentage), so 0 means exactly
//     "none". It ALSO covers the empty-target window:
//     the current target is persisted only on a graceful Stop
//     (Server.SaveCurrentTarget), so after an ungraceful restart
//     TargetManager.Recover finds nothing and NextTargetFirst reads empty
//     until the target observer rebuilds it -- a fully loaded resource group
//     reads 0 for that window. Waiting is the right response either way,
//     which is why the two share a value; the sibling surfaces do separate
//     them, as ShardLeadersReasonNoChannelTarget and
//     ErrCollectionOnRecovering, so a caller that needs to tell "still
//     loading" from "targets not rebuilt yet" should ask one of those.
//   - -1, ErrServiceNotReady (1, retriable): the coordinator's own read
//     stores are not wired up yet, so no answer about any resource group can
//     be computed (for a named group this includes the resource manager; the
//     unscoped form never consults it, so it never fails on it). This is the state initQueryCoord passes through --
//     initMeta assigns the meta before the distribution and target managers
//     -- and it is neither of the two above: nothing is known, rather than
//     something being known to be absent. Reusing a bare -1 for it would tell
//     a caller "this resource group holds no replica", which is a claim this
//     function is in no position to make. The sentinel is the retriable one
//     because the state resolves on its own within the init window, and it
//     mirrors the ShardLeadersReasonCoordinatorNotReady that
//     ShardLeaderReadinessByResourceGroup reports for the same window.
//   - -1, ErrCollectionNotLoaded (101, non-retriable): the collection is not
//     registered as loaded and GlobalFailedLoadCache holds a recorded reason
//     -- the load failed and is not coming back on its own. The recorded
//     cause is kept in the message.
//   - -1, ErrResourceGroupNotFound (300, InputError): rgName names a resource
//     group that does not exist. This is the request's own content forcing
//     the branch -- a misspelled name -- and it is checked before anything
//     about the collection, because the alternative is the replica scan,
//     where an unknown group is indistinguishable from an existing group
//     that holds nothing and reads as the terminal bare -1: the caller would
//     be told to stop for the wrong reason, with no way to learn it
//     misspelled the group. rgName == "" is the absence of a filter, never a
//     name to validate.
//
// The ErrServiceNotReady and ErrCollectionNotLoaded rows MUST NOT be conflated, which is why the failed-load error is
// normalized here rather than returned verbatim. FailedLoadCache stores
// whatever error the failing load task recorded, and a load can fail with a
// retriable sentinel -- ErrServiceNotReady when a target query node was
// restarting during LoadSegments, ErrServiceUnavailable -- which is the very
// code the init window above uses to mean "retry, this fixes itself". A
// caller written to this contract would then retry a terminally failed load
// until the cache entry expires 24h later. ShowLoadCollections and
// ShowLoadPartitions normalize the same cache the same way, so this also
// restores the parity with them that this comment claims.
//
// # What the figure measures
//
// The percentage is a LIVE target-coverage figure: the fraction of the
// collection's current work set -- its channel targets plus sealed-segment
// targets, pooled across partitions -- that the selected replica carries
// right now. The walk is the same one CollectionObserver performs (find the
// delegators that carry each target, check replica membership), but the
// aggregation is restricted to the selected replica(s) instead of summing
// across every replica; that summing step is what made the collection-wide
// figure wrong for this use case.
//
// The figure agrees with the collection-wide ShowLoadCollections number at
// the endpoints -- -1 when nothing is here, and both reach 100 exactly when
// every target is carried -- but is NOT the same number in between, in two
// deliberate ways:
//
//   - Intermediate values weight differently. The observer computes each
//     partition separately (its own segments plus the channel targets) and
//     CalculateLoadPercentage averages the partitions, so a small partition
//     counts as much as a large one; this figure pools every target, so each
//     target counts once. On a multi-partition collection mid-load the two
//     can differ widely while both agree at 0 and at 100.
//   - This figure can drop back below 100 in steady state. The observer's
//     number is persisted per partition and never regresses once it reaches
//     100, while this one is recomputed against the live target set: when a
//     freshly flushed segment or compaction output lands in the next target,
//     the figure reports the not-yet-loaded remainder until the replica picks
//     it up. That is the point -- it answers "is this resource group carrying
//     everything currently asked of it" -- but it is also why == 100 is a
//     transient on a collection under ingestion, and why the figure is
//     progress rather than a gate (see the division of labor below), where
//     ShowLoadCollections would keep saying 100.
//
// # Division of labor with readiness
//
// 100 IS NOT A SERVABILITY VERDICT, AND IT IS NOT MEANT TO BE ONE:
// ShardLeaderReadinessByResourceGroup is the servability gate; this figure is
// progress. Ready already implies the group's delegators carry every sealed
// segment of the current target -- a delegator is serviceable only once
// loadedRatio >= 1.0 && syncedByCoord (querynodev2/delegator/distribution.go)
// -- so there is nothing left for the percentage to add to that verdict.
//
// Do NOT gate a switchover on percentage == 100 in addition to Ready. This
// figure is measured against NextTargetFirst and re-arms below 100 whenever a
// flush or compaction output lands in the next target, so on a collection
// under continuous ingestion == 100 is a transient, and an AND-gate can spin
// indefinitely on a group that has been able to serve the whole time. Use the
// percentage to answer "how far along is this group", and Ready to answer
// "may I route to it".
//
// The two answer different questions, and here is where they deliberately
// disagree. This figure counts query-invisible replicas (see the note at the
// selection loop); readiness and the GetShardLeaders routing path both
// exclude them, because a leader the proxy can never be routed to cannot
// serve. A resource group whose replicas are all still query-invisible
// therefore reads 100 here while readiness says Ready=false -- and while the
// group does not appear in the GetShardLeaders answer at all, since its
// leaders are dropped before the per-leader resource-group tag is applied.
// That is a normal product state, not a corner case: UpdateLoadConfig with
// needWaitRGReady spawns a new group's replicas WithQueryInvisible, and
// promotion is global and all-or-nothing, so the new group can finish
// carrying every target of its own while promotion stays blocked on some
// unrelated replica. It is also exactly the window where the percentage is
// useful -- "how much longer" for a group readiness cannot yet call Ready.
// A caller acting on 100 alone would cut traffic to a group that cannot
// answer. TestShardLeaderReadinessByRG_QueryInvisibleReplicaDoesNotCount and
// TestLoadPercentageByResourceGroup_InvisibleReplicaCounts pin the two halves.
//
// The two are also measured against DIFFERENT target scopes, which a caller
// reading both must expect rather than read as a contradiction. This figure
// uses NextTargetFirst -- "is this group carrying everything currently asked
// of it" -- while readiness uses CurrentTarget -- "can it serve what the
// collection is currently expected to hold". So when a next-target re-pull
// adds a channel that has not been promoted, this figure drops below 100
// while readiness, which cannot see that channel yet, can still say Ready.
// Both answers are correct for their own question; they are not two views of
// one number, which is one more reason not to AND them.
//
// # Multiple replicas
//
// When more than one replica is selected -- either because Spawn put several
// replicas of the collection in rgName, or because rgName is empty and every
// replica is selected -- this returns the minimum percentage across them. A
// caller deciding whether a resource group can be trusted to serve queries
// wants the laggard, not the average or the best replica: reporting the
// furthest-behind replica means the resource group is only called "ready"
// once every replica in it actually is.
//
// This is a read-only composition over existing querycoord state. It adds no
// proto field, no persisted state, and no change to the replica model.
func LoadPercentageByResourceGroup(
	ctx context.Context,
	m *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	dist *meta.DistributionManager,
	collectionID int64,
	rgName string,
) (int32, error) {
	if m == nil || targetMgr == nil || dist == nil {
		return -1, merr.WrapErrServiceNotReadyMsg("querycoord read stores are not wired up yet")
	}

	// Only the scoped form consults the resource manager, so only the scoped
	// form requires it: rgName == "" is the absence of a filter and must stay
	// answerable by a caller that does not use resource groups at all -- the
	// inertness this surface promises a few lines up. A resource group that
	// does not exist is the request's own content forcing this branch, so it
	// is an input error -- which ErrResourceGroupNotFound (300) already is --
	// rather than the terminal "-1, this group holds no replica" the replica
	// scan below would answer, telling the caller to stop for the wrong
	// reason. It runs before the registration check: the name is wrong
	// whatever the collection's state.
	if rgName != "" {
		if m.ResourceManager == nil {
			return -1, merr.WrapErrServiceNotReadyMsg("querycoord resource manager is not wired up yet")
		}
		if !m.ContainResourceGroup(ctx, rgName) {
			return -1, merr.WrapErrResourceGroupNotFound(rgName)
		}
	}

	// The load-registration check comes BEFORE the replica scan: the terminal
	// failed-load state is the one CollectionObserver.observeTimeout leaves
	// behind, with the collection registration AND every replica record
	// removed and only the GlobalFailedLoadCache entry remaining. Scanning
	// replicas first would turn that state into a bare (-1, nil) and swallow
	// the recorded failure.
	//
	// The test is CalculateLoadPercentage(...) < 0, NOT m.Exist: Exist checks
	// only the collection map, while calculateLoadPercentage additionally
	// requires a non-empty partition set and otherwise falls through to -1.
	// The two disagree on a collection record that has no partitions, which
	// job_load.go leaves behind whenever the incoming partition set is
	// disjoint from the loaded one -- RemovePartition is an independent etcd
	// commit that does not touch the collection key, so the window is
	// observable concurrently and survives a crash inside it, with Recover
	// restoring the Loaded record over zero partitions. Exist would call that
	// state loaded while GetShardLeaders (which gates on checkLoadStatus,
	// i.e. this same figure) calls it not loaded, and the caller would be told
	// to cut traffic over to a collection whose routing is then refused with a
	// non-retriable 101. This is also the test ShowLoadCollections has always
	// used.
	if m.CalculateLoadPercentage(ctx, collectionID) < 0 {
		// Defense in depth, not a tolerance this function can promise:
		// GlobalFailedLoadCache is the LAST piece initQueryCoord wires and
		// Get dereferences a nil receiver, so a caller reaching a Server
		// mid-Init would panic here. Server's own entry point gates on
		// CheckHealthy, which is what actually orders this against Init; the
		// check below only keeps a direct utils-level caller (the observers,
		// and the tests) from panicking. In that window the collection reads
		// as not loaded, without the recorded-failure detail.
		if meta.GlobalFailedLoadCache != nil {
			if err := meta.GlobalFailedLoadCache.Get(collectionID); err != nil {
				// Normalized to ErrCollectionNotLoaded rather than returned
				// verbatim, exactly as ShowLoadCollections does with the same
				// cache. FailedLoadCache stores whatever code the failing load
				// task recorded, and those include retriable sentinels --
				// ErrServiceNotReady when a target query node was restarting,
				// ErrServiceUnavailable -- which is the SAME code this
				// function returns for the init window above, where it means
				// "self-heals in a moment, retry". Returning the cached error
				// as-is would make a load that is never coming back
				// indistinguishable from one that is, and the caller would
				// retry until the cache entry expires 24h later. The recorded
				// cause is kept in the message.
				return -1, merr.WrapErrCollectionNotLoaded(collectionID, err.Error())
			}
		}
		return -1, nil
	}

	// Query-invisible replicas (load-config updates spawn replicas invisible
	// until every one of them is serviceable) are deliberately included: this
	// is a progress figure, and those replicas are exactly the ones whose
	// progress the load-config path is waiting on.
	// ShardLeaderReadinessByResourceGroup, by contrast, excludes them to
	// match the routing surface -- see the division of labor on this
	// function: this asymmetry is why 100 here does not by itself mean
	// servable, and why readiness, not this figure, is the gate.
	var replicas []*meta.Replica
	for _, replica := range m.GetByCollection(ctx, collectionID) {
		if rgName == "" || replica.GetResourceGroup() == rgName {
			replicas = append(replicas, replica)
		}
	}
	if len(replicas) == 0 {
		return -1, nil
	}

	// The targets are read ONCE for every selected replica, not per replica.
	// A promotion landing between two per-replica reads would have the
	// min-across-replicas comparison below weigh two different denominators
	// against each other -- the same class of inconsistency that two
	// unsynchronized reads of any shared store produce.
	//
	// This covers the target and distribution reads, NOT the whole figure.
	// The replica set was read further up, in a third independent read, so a
	// TransferReplica committing between there and here leaves a replica in
	// the loop that no longer belongs to rgName -- and since the result is a
	// min, a transferred-away laggard drags it down. That needs an operator
	// action landing inside one call and self-corrects on the next poll, so
	// it is left as-is; but this is a best-effort composition over three
	// reads, not a snapshot of one.
	//
	// NextTargetFirst, not NextTarget: promotion clears the next target until
	// the observer re-pulls it ~10s later, and a plain NextTarget read in that
	// window sees an empty target and reports 0 - so a fully loaded, serving
	// resource group would flap 100/0 on every promotion to any caller of
	// GetLoadPercentageByResourceGroup. CollectionObserver reads NextTarget;
	// this deliberately does not.
	channelTargets := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTargetFirst)
	segmentTargets := targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.NextTargetFirst)

	// One distribution lookup per CHANNEL, shared by every replica and by the
	// segment walk. ChannelDistManager.GetByFilter with no node filter walks
	// every node's channel collection under an RLock and allocates a result
	// slice, so doing it per segment costs that once per sealed segment per
	// replica -- tens of thousands of times on a large collection, on a path a
	// caller waiting for a resource group polls. A collection has single-digit
	// channels.
	delegators := prefetchDelegatorsByChannel(dist, channelTargets, segmentTargets)

	percentage := int32(100)
	for _, replica := range replicas {
		if p := replicaLoadPercentage(replica, channelTargets, segmentTargets, delegators); p < percentage {
			percentage = p
		}
	}
	return percentage, nil
}

// prefetchDelegatorsByChannel resolves every channel named by the target set
// -- the channel targets themselves plus the insert channel of every sealed
// segment target -- to its delegators, once per distinct channel.
//
// It exists so replicaLoadPercentage never touches the distribution manager:
// one snapshot serves every replica, which keeps the min-across-replicas
// comparison consistent for the same reason reading the targets once does.
func prefetchDelegatorsByChannel(
	dist *meta.DistributionManager,
	channelTargets map[string]*meta.DmChannel,
	segmentTargets map[int64]*datapb.SegmentInfo,
) map[string][]*meta.DmChannel {
	names := typeutil.NewSet[string]()
	for _, channel := range channelTargets {
		names.Insert(channel.GetChannelName())
	}
	for _, segment := range segmentTargets {
		names.Insert(segment.GetInsertChannel())
	}

	delegators := make(map[string][]*meta.DmChannel, names.Len())
	for name := range names {
		delegators[name] = dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(name))
	}
	return delegators
}

// replicaLoadPercentage is the per-replica analog of what
// CollectionObserver.observePartitionLoadStatus computes for a whole
// collection: for each target it checks whether one of replica's own nodes
// carries it. The percentage is the fraction of targets this one replica
// already carries; no aggregation across other replicas happens here.
//
// The target set and the channel->delegators map are passed in rather than
// read here, so every replica is measured against one snapshot of both. See
// the notes at the call site for why each matters.
func replicaLoadPercentage(
	replica *meta.Replica,
	channelTargets map[string]*meta.DmChannel,
	segmentTargets map[int64]*datapb.SegmentInfo,
	delegators map[string][]*meta.DmChannel,
) int32 {
	targetNum := len(channelTargets) + len(segmentTargets)
	if targetNum == 0 {
		return 0
	}

	loadedCount := 0
	for _, channel := range channelTargets {
		for _, delegator := range delegators[channel.GetChannelName()] {
			if replica.Contains(delegator.Node) {
				loadedCount++
				break
			}
		}
	}
	for _, segment := range segmentTargets {
		for _, delegator := range delegators[segment.GetInsertChannel()] {
			if replica.Contains(delegator.Node) && delegator.View.Segments[segment.GetID()] != nil {
				loadedCount++
				break
			}
		}
	}

	percentage := int32(loadedCount * 100 / targetNum)
	if percentage == 0 && loadedCount > 0 {
		// Integer division truncates 1 of 200 to 0, and 0 is contractually
		// "carries none of the targets yet" -- the value a caller reads as
		// "nothing has started", distinct from -1. Round any progress up to 1
		// so 0 keeps that exact meaning. The top end needs no symmetric
		// guard: 4999 of 5000 already truncates to 99, and 100 is reached
		// only when every target is carried.
		percentage = 1
	}
	return percentage
}
