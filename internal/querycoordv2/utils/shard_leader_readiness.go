package utils

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

// ShardLeaderReadiness is the answer type of
// ShardLeaderReadinessByResourceGroup. It lives in this package - querycoord's
// computation layer - so that both Server and the observers, which hold the
// same read-only stores and cannot import the querycoordv2 root package, can
// consume it.
type ShardLeaderReadiness struct {
	Ready         bool
	Reason        string
	TotalShards   int
	UnreadyShards []string
}

// The reason strings are part of the answer: callers compare against these
// constants rather than parsing prose, so treat their values as an API.
const (
	ShardLeadersReasonCoordinatorNotReady      = "coordinator query meta is not ready"
	ShardLeadersReasonNoReplicaInResourceGroup = "no replica of the collection lives in this resource group"
	ShardLeadersReasonCollectionNotLoaded      = "the collection is not registered as loaded"
	ShardLeadersReasonNoChannelTarget          = "the collection has no shard in the current target, it may be recovering"
	ShardLeadersReasonShardsWithoutLeader      = "some shards have no serviceable leader in this resource group"
)

// ShardLeaderReadinessByResourceGroup answers a narrower question than
// GetShardLeaders: can the replicas that live in resource group rgName serve
// every shard of collectionID right now.
//
// GetShardLeaders cannot be made to answer this. Its result type,
// querypb.ShardLeadersList, carries only the channel name and the node ids,
// addresses and serviceable flags of the leaders on it; the builder flattens
// every replica of the collection into that one list per channel, so the
// replica each leader belongs to - and with it the resource group - is
// discarded before the answer leaves the coordinator. Recovering the mapping
// afterwards by intersecting the node ids with a resource group's node set
// does not work either, because a replica may borrow nodes from another
// resource group (querycoord models exactly that as num_outgoing_node /
// num_incoming_node), so node-set membership is not replica membership, and
// the two diverge precisely during the rebalance windows a readiness check
// exists to catch.
//
// This function therefore keeps the replica in hand throughout: it selects the
// replicas whose own resource group is rgName, and asks whether each shard has
// a serviceable leader inside one of THOSE replicas. Only query-visible
// replicas count as able to serve, matching the IsQueryVisible filter every
// GetShardLeaders path applies -- a leader the proxy can never be routed to
// must not make its resource group look ready.
//
// It does not reuse checkLoadStatus, which gates the GetShardLeaders path.
// That gate is collection-wide by construction - it reads
// CalculateLoadPercentage(collectionID) and then short-circuits to "ready"
// whenever the collection's own status is LoadStatus_Loaded - so under
// per-resource-group loading it passes as soon as ANY resource group finishes,
// which is the same admission bug from a second direction. The gate here is
// derived only from the leaders of the selected replicas: nothing about
// another resource group's progress, and nothing about the collection's
// aggregate status, can make this report ready.
//
// rgName == "" means "every replica of the collection", matching
// LoadPercentageByResourceGroup: an empty resource group is the absence of a
// filter, not a filter that matches nothing.
//
// It is a free function over the read-only stores it needs, rather than a
// method on Server, for the same reason LoadPercentageByResourceGroup is: the
// observers hold these stores and cannot import the querycoordv2 root package.
// It reads state and computes; it writes nothing, allocates no persistent
// state, and adds no proto field.
func ShardLeaderReadinessByResourceGroup(
	ctx context.Context,
	m *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	collectionID int64,
	rgName string,
) (ShardLeaderReadiness, error) {
	if m == nil || targetMgr == nil || dist == nil || nodeMgr == nil {
		return ShardLeaderReadiness{Reason: ShardLeadersReasonCoordinatorNotReady}, nil
	}

	// The load-registration check comes BEFORE the replica scan, and surfaces
	// a recorded load failure rather than making the caller wait out its
	// timeout on a load that is never coming back, matching what
	// LoadPercentageByResourceGroup does with the same cache. The order
	// matters: the terminal failed-load state is the one
	// CollectionObserver.observeTimeout leaves behind, with the collection
	// registration AND every replica record removed, and a no-replica early
	// return would swallow the recorded failure into "nothing is loading
	// here".
	if !m.Exist(ctx, collectionID) {
		return ShardLeaderReadiness{Reason: ShardLeadersReasonCollectionNotLoaded},
			meta.GlobalFailedLoadCache.Get(collectionID)
	}

	inRG := 0
	var replicas []*meta.Replica
	for _, replica := range m.GetByCollection(ctx, collectionID) {
		if rgName != "" && replica.GetResourceGroup() != rgName {
			continue
		}
		inRG++
		// Only query-visible replicas can serve: both the native and the
		// resource-group-scoped GetShardLeaders filter on IsQueryVisible, so
		// a leader on a not-yet-promoted load-config replica is one no query
		// can be routed to, and counting it would report Ready for a resource
		// group whose scoped GetShardLeaders answer is empty. The replica
		// still counts as "living here" -- its shards are merely unready
		// until tryPromoteReadyLoadConfigReplicas flips it visible -- so it
		// keeps the resource group out of the no-replica bucket below, whose
		// meaning is "waiting will never help".
		if replica.IsQueryVisible() {
			replicas = append(replicas, replica)
		}
	}
	if inRG == 0 {
		return ShardLeaderReadiness{Reason: ShardLeadersReasonNoReplicaInResourceGroup}, nil
	}

	// CurrentTarget, not NextTarget: a shard is only servable once the leader
	// is serving what the collection is currently expected to hold, which is
	// the same target the native shard-leader path reads.
	channels := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		return ShardLeaderReadiness{Reason: ShardLeadersReasonNoChannelTarget}, nil
	}

	readiness := ShardLeaderReadiness{TotalShards: len(channels)}
	for _, channel := range channels {
		if !hasServiceableLeaderInReplicas(dist, nodeMgr, channel.GetChannelName(), replicas) {
			readiness.UnreadyShards = append(readiness.UnreadyShards, channel.GetChannelName())
		}
	}
	if len(readiness.UnreadyShards) > 0 {
		// channels arrives as a map, so its iteration order is random; sort so
		// that one state always prints one line.
		sort.Strings(readiness.UnreadyShards)
		readiness.Reason = ShardLeadersReasonShardsWithoutLeader
		return readiness, nil
	}

	readiness.Ready = true
	return readiness, nil
}

// hasServiceableLeaderInReplicas reports whether channelName has a leader that
// can serve a query inside one of replicas. The three conditions are the ones
// GetShardLeadersWithChannelsAndReplicaFilter applies per leader when the
// caller does not accept unserviceable shards: the replica has a leader for
// the shard at all, that leader is serviceable, and the node it sits on is one
// the coordinator still knows about.
func hasServiceableLeaderInReplicas(
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	channelName string,
	replicas []*meta.Replica,
) bool {
	for _, replica := range replicas {
		leader := dist.ChannelDistManager.GetShardLeader(channelName, replica)
		if leader == nil || !leader.IsServiceable() {
			continue
		}
		if nodeMgr.Get(leader.Node) == nil {
			continue
		}
		return true
	}
	return false
}
