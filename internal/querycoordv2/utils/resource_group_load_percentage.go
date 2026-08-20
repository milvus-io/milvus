package utils

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
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
// Two distinct outcomes are both spelled "the collection isn't ready in this
// resource group", and callers must be able to tell them apart:
//
//   - -1: rgName has no replica of this collection at all. There is nothing
//     to report a percentage for.
//   - 0: rgName has a replica of this collection, but that replica has not
//     picked up any of the collection's current load targets yet. This is a
//     real, distinct state from "no replica" -- it means loading is underway
//     but has not made progress, not that the resource group is unrelated to
//     the collection.
//
// When the collection itself is not currently registered as loaded (for
// example the load request failed), this surfaces the recorded
// GlobalFailedLoadCache error, matching the convention already used by
// ShowLoadCollections and ShowLoadPartitions to turn a negative percentage
// into an actionable error for the caller.
//
// The percentage is computed the same way CollectionObserver computes a
// partition's load percentage -- by walking the collection's segment and
// channel targets, finding the delegators that carry each one, and checking
// which replica those delegators belong to -- except the aggregation step is
// restricted to the selected replica(s) instead of summing across every
// replica of the collection. That summing-across-replicas step is precisely
// what made the old, collection-wide figure wrong for this use case; removing
// it, rather than the walk itself, is the entire fix.
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
		return -1, nil
	}

	// The load-registration check comes BEFORE the replica scan: the terminal
	// failed-load state is the one CollectionObserver.observeTimeout leaves
	// behind, with the collection registration AND every replica record
	// removed and only the GlobalFailedLoadCache entry remaining. Scanning
	// replicas first would turn that state into a bare (-1, nil) and swallow
	// the recorded failure.
	if !m.Exist(ctx, collectionID) {
		if err := meta.GlobalFailedLoadCache.Get(collectionID); err != nil {
			return -1, err
		}
		return -1, nil
	}

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
// collection: it reads the collection's segment and channel targets from
// meta.NextTarget -- the target the observer itself measures progress
// against, i.e. the one that gives "load percentage" its meaning in this
// codebase -- and, for each target, checks whether one of replica's own
// nodes carries it. The percentage is the fraction of targets this one
// replica already carries; no aggregation across other replicas happens
// here.
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
