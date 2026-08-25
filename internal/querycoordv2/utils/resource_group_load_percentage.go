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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
// Four distinct outcomes are all spelled "the collection isn't ready in this
// resource group", and callers must be able to tell them apart. The
// percentage alone separates the first two; the error separates the rest,
// and the two error outcomes differ in whether waiting can ever help:
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
//     unrelated to the collection. It ALSO covers the empty-target window:
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
//     be computed. This is the state initQueryCoord passes through --
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
//
// The last two MUST NOT be conflated, which is why the failed-load error is
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
//     everything currently asked of it", which is what a caller gating a
//     switchover on == 100 actually needs -- but a caller must expect the
//     gate to re-arm whenever new work lands, where ShowLoadCollections
//     would keep saying 100.
//
// 100 IS NOT A SERVABILITY VERDICT, and a caller gating a switchover must
// pair it with ShardLeaderReadinessByResourceGroup rather than act on it
// alone. This figure counts query-invisible replicas (see the note at the
// selection loop); readiness and the GetShardLeaders routing path both
// exclude them, because a leader the proxy can never be routed to cannot
// serve. A resource group whose replicas are all still query-invisible
// therefore reads 100 here while readiness says Ready=false -- and while the
// group does not appear in the GetShardLeaders answer at all, since its
// leaders are dropped before the per-leader resource-group tag is applied.
//
// That is a normal product state, not a corner case: UpdateLoadConfig with
// needWaitRGReady spawns a new group's replicas WithQueryInvisible, and
// promotion is global and all-or-nothing, so the new group can finish
// carrying every target of its own while promotion stays blocked on some
// unrelated replica. A caller acting on 100 alone would cut traffic to a
// group that cannot answer, and would keep retrying it for as long as that
// unrelated replica stays unserviceable, instead of staying on the old one.
// TestInvisibleOnlyResourceGroupReadsFullButNotServable pins the three-way
// state.
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
	// match the routing surface -- see the pairing rule on this function:
	// this asymmetry is why 100 here does not by itself mean servable.
	var replicas []*meta.Replica
	for _, replica := range m.GetByCollection(ctx, collectionID) {
		if rgName == "" || replica.GetResourceGroup() == rgName {
			replicas = append(replicas, replica)
		}
	}
	if len(replicas) == 0 {
		return -1, nil
	}

	percentage := int32(100)
	for _, replica := range replicas {
		if p := replicaLoadPercentage(ctx, targetMgr, dist, replica); p < percentage {
			percentage = p
		}
	}
	return percentage, nil
}

// replicaLoadPercentage is the per-replica analog of what
// CollectionObserver.observePartitionLoadStatus computes for a whole
// collection: it reads the collection's segment and channel targets and, for
// each target, checks whether one of replica's own nodes carries it. The
// percentage is the fraction of targets this one replica already carries; no
// aggregation across other replicas happens here.
//
// The targets are read with meta.NextTargetFirst, NOT the meta.NextTarget the
// observer uses (collection_observer.go). That is a deliberate divergence,
// not an inherited choice: NextTarget reads the next target alone, and
// UpdateCollectionCurrentTarget clears it on promotion until the observer
// re-pulls it, so a NextTarget read in that window would report 0 for a fully
// loaded group. NextTargetFirst falls back to the current target and closes
// exactly that window. See the note at the read itself.
func replicaLoadPercentage(
	ctx context.Context,
	targetMgr meta.TargetManagerInterface,
	dist *meta.DistributionManager,
	replica *meta.Replica,
) int32 {
	collectionID := replica.GetCollectionID()
	// NextTargetFirst, not NextTarget: promotion clears the next target until
	// the observer re-pulls it ~10s later, and a plain NextTarget read in that
	// window sees an empty target and reports 0 - so a fully loaded, serving
	// resource group would flap 100/0 on every promotion to any caller of
	// GetLoadPercentageByResourceGroup.
	channelTargets := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTargetFirst)
	segmentTargets := targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.NextTargetFirst)

	targetNum := len(channelTargets) + len(segmentTargets)
	if targetNum == 0 {
		return 0
	}

	loadedCount := 0
	for _, channel := range channelTargets {
		for _, delegator := range dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(channel.GetChannelName())) {
			if replica.Contains(delegator.Node) {
				loadedCount++
				break
			}
		}
	}
	for _, segment := range segmentTargets {
		for _, delegator := range dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(segment.GetInsertChannel())) {
			if replica.Contains(delegator.Node) && delegator.View.Segments[segment.GetID()] != nil {
				loadedCount++
				break
			}
		}
	}

	return int32(loadedCount * 100 / targetNum)
}
