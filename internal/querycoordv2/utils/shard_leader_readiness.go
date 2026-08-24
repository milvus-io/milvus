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
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
// replicas count as able to serve, matching the IsQueryVisible filter both
// GetShardLeaders ROUTING paths apply -- a leader the proxy can never be
// routed to must not make its resource group look ready. Not every caller of
// the shard-leader machinery filters: checkCollectionQueryable, on the
// CheckHealth path, goes through GetShardLeadersWithChannels with no replica
// filter and does count leaders on query-invisible replicas. That asymmetry
// predates this PR and is untouched by it; readiness follows routing because
// routing is what its answer is about.
//
// It does not reuse checkLoadStatus, which gates the GetShardLeaders path.
// That gate is collection-wide by construction - it reads
// CalculateLoadPercentage(collectionID) and then short-circuits to "ready"
// whenever the collection's own status is LoadStatus_Loaded. The status is
// only ever set once the collection-wide AVERAGE reaches 100, so a group
// finishing first does not arm it -- but nothing ever disarms it either:
// UpdateLoadConfigJob spawns replicas for a newly added resource group
// without touching the collection's status, so once a collection has been
// Loaded even once, the short-circuit stays permanently armed and the gate
// answers "ready" for every later resource group from the moment its
// replicas exist. That is the same admission bug from a second direction. The gate here is
// derived only from the leaders of the selected replicas: nothing about
// another resource group's progress, and nothing about the collection's
// aggregate status, can make this report ready.
//
// rgName == "" means "every replica of the collection", matching
// LoadPercentageByResourceGroup: an empty resource group is the absence of a
// filter, not a filter that matches nothing.
//
// At that one scope, this verdict must NOT be paired with scoped
// GetShardLeaders the way a named group's can. This function never runs the
// collection-wide full-load gate, for any rgName -- that is the whole point
// of it -- while GetShardLeadersByResourceGroup("") hands the request back to
// the unscoped path, gate and all, because the unscoped answer has to stay
// byte-identical to what it was before the resource-group field existed. Both
// constraints are deliberate and neither can give way, so "" is the one scope
// where the two surfaces are gated differently: here it is a pure
// shard-coverage verdict, and a mid-load collection can read Ready=true while
// the matching strict route is still refused with the retriable
// ErrCollectionNotFullyLoaded. A caller gating a switchover wants a NAMED
// group anyway, which is the case the pairing is built for.
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
	// The test is CalculateLoadPercentage(...) < 0, NOT m.Exist, so that all
	// three resource-group-scoped surfaces and the scoped GetShardLeaders gate
	// answer "is this collection loaded" the same way -- see the longer note
	// in LoadPercentageByResourceGroup. Exist would report Ready for a
	// collection record left with zero partitions, a state whose routing the
	// scoped GetShardLeaders path refuses with a non-retriable 101, which is
	// exactly the disagreement the invariant above forbids.
	if m.CalculateLoadPercentage(ctx, collectionID) < 0 {
		// Defense in depth, not a tolerance this function can promise --
		// see the matching note in LoadPercentageByResourceGroup.
		// GlobalFailedLoadCache is the LAST piece initQueryCoord wires and
		// Get dereferences a nil receiver; Server's entry point gates on
		// CheckHealthy, and this check only keeps a direct utils-level
		// caller from panicking.
		//
		// A recorded failure is normalized to ErrCollectionNotLoaded for the
		// same reason LoadPercentageByResourceGroup normalizes it: the cache
		// stores whatever code the failing load recorded, including retriable
		// sentinels, and a terminal failure must not be reported with one.
		var failedLoadErr error
		if meta.GlobalFailedLoadCache != nil {
			if err := meta.GlobalFailedLoadCache.Get(collectionID); err != nil {
				failedLoadErr = merr.WrapErrCollectionNotLoaded(collectionID, err.Error())
			}
		}
		return ShardLeaderReadiness{Reason: ShardLeadersReasonCollectionNotLoaded}, failedLoadErr
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
