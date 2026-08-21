package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
// group the collection is not loaded into is answered as "not loaded" rather
// than falling back to the collection-wide list.
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
//   - the strict scoped answer for the lagging group is the per-channel,
//     retriable ErrChannelNotAvailable: the group holds a replica, so
//     waiting helps, and neither the name-refusal (ReplicaNotFound) nor the
//     collection-wide NotFullyLoaded is the right shape.
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
	assert.ErrorIs(t, merr.Error(scopedToB.GetStatus()), merr.ErrChannelNotAvailable,
		"the lagging group is refused per channel - retriable, and loading is exactly what a retry waits for")
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
