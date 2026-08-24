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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// scopedServer builds a healthy Server over the shard-leader fixture. The
// status has to be set explicitly: GetShardLeaders refuses before it looks at
// anything else, and a zero Server reports StateCode_Initializing.
func scopedServer(f *shardLeaderReadinessFixture) *Server {
	server := f.server()
	server.ctx = context.Background()
	server.status.Store(int32(commonpb.StateCode_Healthy))
	return server
}

func leaderNodeIDs(t *testing.T, resp *querypb.GetShardLeadersResponse, channel string) []int64 {
	t.Helper()
	for _, shard := range resp.GetShards() {
		if shard.GetChannelName() == channel {
			return shard.GetNodeIds()
		}
	}
	t.Fatalf("no shard %q in the response", channel)
	return nil
}

// TestGetShardLeadersRestrictedToOneResourceGroup is the routing half of
// per-resource-group isolation, at the only place it can be enforced. The
// collection is loaded into two resource groups and both have a serviceable
// leader on the one shard; the unscoped answer lists both, so a query made
// ready on rg-b would be free to land on rg-a's query node. Scoping the request
// must leave exactly the leader of the named group.
func TestGetShardLeadersRestrictedToOneResourceGroup(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", true)

	server := scopedServer(f)
	ctx := context.Background()

	unscoped, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, int32(0), unscoped.GetStatus().GetCode())
	assert.ElementsMatch(t, []int64{10, 11}, leaderNodeIDs(t, unscoped, "100-dmc0"),
		"a request that names no resource group must keep the collection-wide answer it always had")

	scopedToA, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), scopedToA.GetStatus().GetCode())
	assert.Equal(t, []int64{10}, leaderNodeIDs(t, scopedToA, "100-dmc0"),
		"a request scoped to rg-a must be answered with rg-a's leader alone")

	scopedToB, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-b",
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), scopedToB.GetStatus().GetCode())
	assert.Equal(t, []int64{11}, leaderNodeIDs(t, scopedToB, "100-dmc0"),
		"a request scoped to rg-b must be answered with rg-b's leader alone, not with whichever replica happens to be listed first")
}

// TestGetShardLeadersScopedToAResourceGroupWithNoLeader states the failure the
// scope is there to produce. rg-b holds a replica but no leader of the shard;
// unscoped, rg-a's leader hides that. A query scoped to rg-b must come back
// with no usable leader rather than with a node that belongs to another group.
func TestGetShardLeadersScopedToAResourceGroupWithNoLeader(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.registerNode(11) // rg-b's node is up; it just holds no leader

	server := scopedServer(f)
	ctx := context.Background()

	// WithUnserviceableShards is what the proxy sends, so the shard comes back
	// listed but empty rather than as an error.
	scopedToB, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		ResourceGroup:           "rg-b",
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), scopedToB.GetStatus().GetCode())
	assert.Empty(t, leaderNodeIDs(t, scopedToB, "100-dmc0"),
		"rg-b has no leader of the shard: rg-a's leader must not be offered as one")

	scopedToA, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		ResourceGroup:           "rg-a",
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{10}, leaderNodeIDs(t, scopedToA, "100-dmc0"),
		"the group that does hold the leader must still be served it")
}

// TestGetShardLeadersScopedToAnUnknownResourceGroup pins that a scope naming a
// group the collection is not loaded into is served no leaders at all, rather
// than falling back to the collection-wide list.
//
// It is deliberately the LOOSE form: "not loaded" is the name of a specific
// sentinel in this contract (ErrCollectionNotLoaded, 101, non-retriable) and
// the loose path cannot reach it for this state -- the registered-at-all
// check passes, the name refusal is skipped because the caller accepts
// unserviceable shards, and the builder returns the shard listed but empty.
// The refusal for the same state belongs to the strict form and is
// ErrReplicaNotFound; it is pinned by the test directly below.
func TestGetShardLeadersScopedToAnUnknownResourceGroup(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putLeader(100, 10, "100-dmc0", true)

	resp, err := scopedServer(f).GetShardLeaders(context.Background(), &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		ResourceGroup:           "rg-nowhere",
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.Empty(t, leaderNodeIDs(t, resp, "100-dmc0"),
		"a resource group the collection was never loaded into must be served no leaders at all")
}

// The strict form (unserviceable shards not accepted) refuses an unknown
// resource group by name, not with ChannelNotAvailable: the channel is fine -
// a sibling group may be serving it right now - and a retry will not change
// the answer until someone loads the collection into this group.
func TestGetShardLeadersStrictScopeRefusesAnUnheldResourceGroupByName(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putLeader(100, 10, "100-dmc0", true)

	resp, err := scopedServer(f).GetShardLeaders(context.Background(), &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-nowhere",
	})
	require.NoError(t, err)
	require.NotEqual(t, int32(0), resp.GetStatus().GetCode())
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrReplicaNotFound,
		"the refusal must keep the replica-not-found code so callers can branch on it")
	reason := resp.GetStatus().GetReason()
	assert.Contains(t, reason, "rg-nowhere",
		"the refusal must name the resource group, not blame the channel")
	assert.Contains(t, reason, "collection 100",
		"the refusal must name the collection as a collection")
	assert.NotContains(t, reason, "replica=100",
		"the collection id must not masquerade as a replica id in the message")
}

// TestGetShardLeadersScopedSeesThroughSiblingResourceGroupLag pins the strict
// scoped form to the state this feature exists for: one resource group at
// 100%, a sibling at 0%, so the collection-wide load percentage sits at 50 and
// the collection's status is still Loading -- yet the current target is
// already promoted, because shouldUpdateCurrentTarget pools ready delegators
// across replicas. In that state the three surfaces must tell one story:
//
//   - the native, unscoped strict answer keeps refusing with
//     ErrCollectionNotFullyLoaded (collection-wide semantics, unchanged);
//   - readiness for the leading group says Ready;
//   - the strict scoped answer for the leading group must AGREE with
//     readiness and serve its leader -- gating it on the collection-wide
//     percentage would refuse rg-a until rg-b finishes, which inverts the
//     purpose of the scope;
//   - the strict scoped answer for the lagging group is the retriable
//     ErrCollectionNotFullyLoaded, scoped to that group: it holds a replica,
//     so waiting helps, which rules out the terminal name-refusal
//     (ReplicaNotFound) and equally rules out the non-retriable per-channel
//     ChannelNotAvailable -- the channel is fine, the group is just still
//     coming up.
func TestGetShardLeadersScopedSeesThroughSiblingResourceGroupLag(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadingCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.registerNode(11) // rg-b's node is up; it just carries nothing yet

	require.EqualValues(t, 50, f.meta.CalculateLoadPercentage(ctx, 100),
		"the fixture must reproduce the state in which the collection-wide full-load gate fails")

	server := scopedServer(f)

	unscoped, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{CollectionID: 100})
	require.NoError(t, err)
	require.NotEqual(t, int32(0), unscoped.GetStatus().GetCode(),
		"the unscoped strict answer keeps its collection-wide gate")
	assert.ErrorIs(t, merr.Error(unscoped.GetStatus()), merr.ErrCollectionNotFullyLoaded)

	require.True(t, f.readiness(t, 100, "rg-a").Ready,
		"the readiness surface must call the fully-loaded group ready")

	scopedToA, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), scopedToA.GetStatus().GetCode(),
		"the strict scoped answer must agree with readiness: rg-a can serve, a lagging sibling group must not veto it")
	assert.Equal(t, []int64{10}, leaderNodeIDs(t, scopedToA, "100-dmc0"))

	scopedToB, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-b",
	})
	require.NoError(t, err)
	require.NotEqual(t, int32(0), scopedToB.GetStatus().GetCode())
	assert.ErrorIs(t, merr.Error(scopedToB.GetStatus()), merr.ErrCollectionNotFullyLoaded,
		"a group that holds a replica and is still coming up must be refused with the load-progress code, the same family the unscoped shape uses for the same physical state")
	assert.NotErrorIs(t, merr.Error(scopedToB.GetStatus()), merr.ErrChannelNotAvailable,
		"the channel is not unavailable - a sibling group is serving it right now")
	assert.True(t, scopedToB.GetStatus().GetRetriable(),
		"the wire status must say retriable: the gRPC wrapper re-issues the call only when this bit is set, and this state self-heals")
	assert.Contains(t, scopedToB.GetStatus().GetReason(), "rg-b",
		"the refusal must name the group whose progress the caller is waiting on")
	assert.Contains(t, scopedToB.GetStatus().GetReason(), "100-dmc0",
		"the refusal must name the shard that is not covered yet")
}

// TestGetShardLeadersStrictScopeRefusalIsRetriableOnTheWire is the same
// contract stated at the sentinel level rather than through the fixture, so
// that a future change of sentinel cannot quietly satisfy the assertions above
// while flipping the bit callers actually branch on. merr.Status copies
// IsRetryableErr onto the wire, so these two sentinels are what decide whether
// the generic client wrapper waits or gives up.
func TestGetShardLeadersStrictScopeRefusalIsRetriableOnTheWire(t *testing.T) {
	assert.True(t, merr.IsRetryableErr(merr.ErrCollectionNotFullyLoaded),
		"the refusal the scoped strict path uses for a group that is still coming up must be retriable")
	assert.False(t, merr.IsRetryableErr(merr.ErrChannelNotAvailable),
		"ChannelNotAvailable is not retriable, which is why the scoped path must not use it for a load-progress state")
	assert.False(t, merr.IsRetryableErr(merr.ErrReplicaNotFound),
		"the name-refusal is terminal: no amount of waiting puts a replica in a group nobody loaded into")
}

// TestGetShardLeadersScopedToTheGroupThatLostItsLeader keeps the coverage
// refusal honest in the other direction: a fully loaded collection whose named
// group once had the shard but whose leader is no longer serviceable is the
// same physical state as "still coming up" from the caller's point of view --
// the group holds a replica, and the coordinator will rebalance -- so it takes
// the same retriable answer rather than the collection-wide 100% gate's
// verdict that everything is fine.
func TestGetShardLeadersScopedToTheGroupThatLostItsLeader(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", false) // rg-b's leader is not serviceable

	server := scopedServer(f)

	unscoped, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{CollectionID: 100})
	require.NoError(t, err)
	require.Equal(t, int32(0), unscoped.GetStatus().GetCode(),
		"collection-wide, the shard is served: rg-a's leader covers it")

	scopedToB, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-b",
	})
	require.NoError(t, err)
	require.NotEqual(t, int32(0), scopedToB.GetStatus().GetCode(),
		"a fully loaded collection does not make a group ready whose own leader cannot serve")
	assert.ErrorIs(t, merr.Error(scopedToB.GetStatus()), merr.ErrCollectionNotFullyLoaded)
	assert.True(t, scopedToB.GetStatus().GetRetriable())
}

// TestGetShardLeadersStrictScopeQueryInvisibleReplicaIsNotTerminal pins the
// split the strict scoped path makes between "does this group hold a replica"
// and "can this group serve". A load-config replica is spawned query-invisible
// with a serviceable leader; routing can never reach it, so the group cannot
// be served its leader -- but the collection DOES live here, and
// tryPromoteReadyLoadConfigReplicas will flip it visible, so the refusal must
// be the retriable load-progress one and NOT the terminal name-refusal.
// Counting the invisible replica in only one of the two places is what makes
// this test fail in both directions: as ReplicaNotFound if it is dropped from
// the holds scan, and as a served leader if it is kept in the coverage scan.
func TestGetShardLeadersStrictScopeQueryInvisibleReplicaIsNotTerminal(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putInvisibleReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", true) // serviceable, but on an invisible replica

	resp, err := scopedServer(f).GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-b",
	})
	require.NoError(t, err)
	require.NotEqual(t, int32(0), resp.GetStatus().GetCode(),
		"a leader on a query-invisible replica is one no query can be routed to")
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotFullyLoaded,
		"the collection does live in rg-b, so the refusal is load progress, not a missing replica")
	assert.NotErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrReplicaNotFound,
		"the terminal name-refusal means waiting will never help, which is false here")
	assert.True(t, resp.GetStatus().GetRetriable())

	require.False(t, f.readiness(t, 100, "rg-b").Ready,
		"readiness must tell the same story: the group is not ready, but it is not empty either")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, f.readiness(t, 100, "rg-b").Reason)
}

// TestZeroPartitionCollectionReadsNotLoadedOnEveryScopedSurface pins that the
// three surfaces this PR adds answer "is this collection loaded" the same
// way. They used to disagree: the two utils surfaces tested m.Exist, which
// checks only the collection map, while scoped GetShardLeaders gates on
// checkLoadStatus, i.e. CalculateLoadPercentage, which additionally requires
// a non-empty partition set and otherwise falls through to -1.
//
// The disagreement is not theoretical -- see the fixture helper for the
// job_load.go window that produces a collection record with zero partitions,
// concurrently and across a crash. Under the old test, readiness reported
// Ready=true and the percentage reported 100 for a collection whose scoped
// routing is refused with ErrCollectionNotLoaded (101, non-retriable, so the
// gRPC layer will not even resend): a caller gating a switchover on the first
// two would cut traffic over and then have every route permanently refused.
//
// This test fails on every one of the three surfaces if any of them reverts
// to m.Exist.
func TestZeroPartitionCollectionReadsNotLoadedOnEveryScopedSurface(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollectionWithoutPartitions(t, 100, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putLeader(100, 10, "100-dmc0", true)

	readiness := f.readiness(t, 100, "rg-a")
	assert.False(t, readiness.Ready,
		"a collection with no partitions must not be called ready, whatever its leaders look like")
	assert.Equal(t, utils.ShardLeadersReasonCollectionNotLoaded, readiness.Reason)

	percentage, err := utils.LoadPercentageByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, 100, "rg-a")
	require.NoError(t, err)
	assert.EqualValues(t, -1, percentage,
		"the progress figure must agree with the routing surface, not report 100 for a collection that cannot be routed to")

	resp, err := scopedServer(f).GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotLoaded,
		"the routing surface's verdict is the one the other two now match")
}

// TestGetShardLeadersScopedOnRecoveringCollection pins the fourth refusal of
// the scoped strict form, which the contract comment and the design doc table
// once omitted: a collection registered as loaded but holding no channel in
// the current target -- what a collection under recovery looks like -- is
// refused with the retriable ErrCollectionOnRecovering (106), not with any of
// the three resource-group-specific codes.
//
// The check runs before the with_unserviceable_shards branch, so the loose
// form reaches it too; both shapes are asserted here so the ordering is
// pinned and not just described.
func TestGetShardLeadersScopedOnRecoveringCollection(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	// Registered as loaded, with a partition, but no target was ever built.
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 100, Status: querypb.LoadStatus_Loaded},
		LoadPercentage:     100,
	}))
	require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: 100, PartitionID: 1000, Status: querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
	}))
	f.putReplica(t, 100, "rg-a", 10)

	server := scopedServer(f)

	strict, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(strict.GetStatus()), merr.ErrCollectionOnRecovering,
		"a collection with no channel in the current target is recovering, not missing a replica")
	assert.NotErrorIs(t, merr.Error(strict.GetStatus()), merr.ErrCollectionNotFullyLoaded)
	assert.True(t, strict.GetStatus().GetRetriable())

	loose, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		ResourceGroup:           "rg-a",
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(loose.GetStatus()), merr.ErrCollectionOnRecovering,
		"the recovering check precedes the with_unserviceable_shards branch, so the loose form gets it too")

	// And the group that holds nothing is still refused by name, ahead of any
	// channel reasoning -- the ordering the contract depends on.
	unheld, err := server.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-nowhere",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(unheld.GetStatus()), merr.ErrReplicaNotFound)
}

// TestInvisibleOnlyResourceGroupReadsFullButNotServable nails the one state
// where the three surfaces deliberately disagree, in a single fixture, so the
// pairing rule they carry is pinned rather than only asserted in prose.
//
// The percentage counts query-invisible replicas (it is a progress figure,
// and those replicas are exactly what the load-config path waits on) while
// readiness and scoped routing exclude them (a leader the proxy can never be
// routed to cannot serve). A group whose replicas are all still invisible
// therefore reads 100 while being unable to answer a single query.
//
// This is a normal product state, not a corner case: UpdateLoadConfig with
// needWaitRGReady spawns the new group's replicas WithQueryInvisible, and
// promotion is global and all-or-nothing, so rg-b can finish carrying every
// target of its own while promotion stays blocked on an unrelated replica --
// modelled here by rg-c, whose invisible replica has no serviceable leader.
// A caller acting on the percentage alone would cut traffic to rg-b and then
// retry it for as long as rg-c stays unserviceable.
func TestInvisibleOnlyResourceGroupReadsFullButNotServable(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putInvisibleReplica(t, 100, "rg-b", 11)
	f.putInvisibleReplica(t, 100, "rg-c", 12) // the unrelated replica blocking promotion
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", true) // rg-b carries the target, but invisibly
	f.registerNode(12)                     // rg-c carries nothing: promotion cannot happen

	percentage, err := utils.LoadPercentageByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, 100, "rg-b")
	require.NoError(t, err)
	assert.EqualValues(t, 100, percentage,
		"the progress figure counts the invisible replica: rg-b really has carried every target asked of it")

	readiness := f.readiness(t, 100, "rg-b")
	assert.False(t, readiness.Ready,
		"readiness excludes invisible replicas, so the same group is not servable")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, readiness.Reason)

	resp, err := scopedServer(f).GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-b",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotFullyLoaded,
		"scoped routing agrees with readiness, not with the percentage")
	assert.True(t, resp.GetStatus().GetRetriable(),
		"the refusal stays retriable: promotion is what the caller is waiting on")
	assert.NotErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrReplicaNotFound,
		"rg-b does hold a replica -- this is not the terminal bucket")
}

// TestGetShardLeadersByResourceGroupEmptyScopeIsTheUnscopedAnswer pins the
// third of the empty-string contracts this PR establishes. The proto field and
// both sibling surfaces define "" as the absence of a filter; a literal
// comparison inside GetShardLeadersByResourceGroup would make this the one
// place where an unset field means "no replica matches", so an empty scope
// must hand the request back to the unscoped path -- gate included, since the
// scoped gate is only justified by a named group.
//
// The fixture is mid-load precisely because that is where the two gates
// disagree: a named group at 100% is served, while "" must keep the
// collection-wide refusal.
func TestGetShardLeadersByResourceGroupEmptyScopeIsTheUnscopedAnswer(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadingCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.registerNode(11)

	empty, err := utils.GetShardLeadersByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, f.nodeMgr, 100, "", false)
	assert.Nil(t, empty)
	assert.ErrorIs(t, err, merr.ErrCollectionNotFullyLoaded,
		"an empty scope must take the collection-wide gate, not the scoped one")
	assert.NotErrorIs(t, err, merr.ErrReplicaNotFound,
		"an empty scope must not be read as a group literally named \"\", which no replica lives in")

	native, nativeErr := utils.GetShardLeadersWithReplicaFilter(ctx, f.meta, f.targetMgr, f.dist, f.nodeMgr, 100, false,
		func(replica *meta.Replica) bool { return replica.IsQueryVisible() })
	assert.Equal(t, nativeErr.Error(), err.Error(),
		"an empty scope must be answered by the unscoped path verbatim")
	assert.Equal(t, native, empty)

	looseEmpty, err := utils.GetShardLeadersByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, f.nodeMgr, 100, "", true)
	require.NoError(t, err)
	looseNative, err := utils.GetShardLeadersWithReplicaFilter(ctx, f.meta, f.targetMgr, f.dist, f.nodeMgr, 100, true,
		func(replica *meta.Replica) bool { return replica.IsQueryVisible() })
	require.NoError(t, err)
	assert.Equal(t, looseNative, looseEmpty,
		"the loose shape agrees too: an empty scope is the absence of a filter in both forms")
}

// TestGetShardLeadersStrictScopeUnloadedCollectionKeepsErrorFamily pins the
// error family for a collection that is not loaded at all: the scoped strict
// shape must answer ErrCollectionNotLoaded exactly like the unscoped shape,
// not ErrReplicaNotFound. The proxy's retry policy branches on
// errors.Is(err, merr.ErrCollectionNotLoaded) (lb_policy.go), so a scoped
// request answering a different family for the same state would send a future
// caller into the wrong retry bucket.
func TestGetShardLeadersStrictScopeUnloadedCollectionKeepsErrorFamily(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	// Nothing registered: the collection is not loaded and has no replicas.

	resp, err := scopedServer(f).GetShardLeaders(context.Background(), &querypb.GetShardLeadersRequest{
		CollectionID:  100,
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	require.NotEqual(t, int32(0), resp.GetStatus().GetCode())
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotLoaded,
		"an unloaded collection must answer not-loaded for the scoped shape exactly as for the unscoped one")
	assert.NotErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrReplicaNotFound,
		"the name-refusal is for a loaded collection missing from this group, not for an unloaded collection")
}
