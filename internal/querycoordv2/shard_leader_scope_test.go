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
