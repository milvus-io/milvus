package utils

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/extension"
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
// a serviceable leader inside one of THOSE replicas.
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
) (extension.ShardLeaderReadiness, error) {
	if m == nil || targetMgr == nil || dist == nil || nodeMgr == nil {
		return extension.ShardLeaderReadiness{Reason: extension.ShardLeadersReasonCoordinatorNotReady}, nil
	}

	var replicas []*meta.Replica
	for _, replica := range m.GetByCollection(ctx, collectionID) {
		if rgName == "" || replica.GetResourceGroup() == rgName {
			replicas = append(replicas, replica)
		}
	}
	if len(replicas) == 0 {
		return extension.ShardLeaderReadiness{Reason: extension.ShardLeadersReasonNoReplicaInResourceGroup}, nil
	}

	// A replica record can outlive the load registration, for instance when the
	// load failed. Surface the recorded failure rather than making the caller
	// wait out its timeout on a load that is never coming back, matching what
	// LoadPercentageByResourceGroup does with the same cache.
	if !m.Exist(ctx, collectionID) {
		return extension.ShardLeaderReadiness{Reason: extension.ShardLeadersReasonCollectionNotLoaded},
			meta.GlobalFailedLoadCache.Get(collectionID)
	}

	// CurrentTarget, not NextTarget: a shard is only servable once the leader
	// is serving what the collection is currently expected to hold, which is
	// the same target the native shard-leader path reads.
	channels := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		return extension.ShardLeaderReadiness{Reason: extension.ShardLeadersReasonNoChannelTarget}, nil
	}

	readiness := extension.ShardLeaderReadiness{TotalShards: len(channels)}
	for _, channel := range channels {
		if !hasServiceableLeaderInReplicas(dist, nodeMgr, channel.GetChannelName(), replicas) {
			readiness.UnreadyShards = append(readiness.UnreadyShards, channel.GetChannelName())
		}
	}
	if len(readiness.UnreadyShards) > 0 {
		// channels arrives as a map, so its iteration order is random; sort so
		// that one state always prints one line.
		sort.Strings(readiness.UnreadyShards)
		readiness.Reason = extension.ShardLeadersReasonShardsWithoutLeader
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
