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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// shardOf returns the one ShardLeadersList for channel, failing if absent.
func shardOf(t *testing.T, resp *querypb.GetShardLeadersResponse, channel string) *querypb.ShardLeadersList {
	t.Helper()
	for _, shard := range resp.GetShards() {
		if shard.GetChannelName() == channel {
			return shard
		}
	}
	t.Fatalf("no shard %q in the response", channel)
	return nil
}

// TestShardLeadersCarryTheirReplicaResourceGroup is the routing half of
// per-resource-group isolation. The response flattens every replica of the
// collection into one list per channel, so the replica each leader belongs to
// -- and with it the resource group -- is the one thing a caller cannot
// recover afterwards. Tagging it on the wire is what makes a caller able to
// route within a group at all.
func TestShardLeadersCarryTheirReplicaResourceGroup(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", true)

	resp, err := f.server().GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.GetStatus().GetCode())

	shard := shardOf(t, resp, "100-dmc0")
	byNode := map[int64]string{}
	for i, id := range shard.GetNodeIds() {
		byNode[id] = shard.GetResourceGroups()[i]
	}
	assert.Equal(t, map[int64]string{10: "rg-a", 11: "rg-b"}, byNode,
		"each leader must carry the resource group of the replica it leads")
}

// TestShardLeaderResourceGroupsStayIndexAligned pins the property the whole
// parallel-array shape rests on: resource_groups[i] describes node_ids[i].
//
// The fixture makes that falsifiable rather than incidental. Node 12 carries a
// leader but is never registered with the NodeManager, so the builder drops it
// at the `info != nil` check -- after it has already decided the leader is
// usable. An implementation that appends the resource group anywhere but
// inside that same branch produces a longer resource_groups than node_ids and
// shifts every tag after the dropped entry, which is exactly how a caller
// silently routes to the wrong group.
func TestShardLeaderResourceGroupsStayIndexAligned(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-gone", 12)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", true)
	// A leader on a node the coordinator does not know about: recorded in the
	// distribution, deliberately NOT registered with the NodeManager.
	f.dist.ChannelDistManager.Update(12, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "100-dmc0"},
		Node:         12,
		View: &meta.LeaderView{
			ID: 12, CollectionID: 100, Channel: "100-dmc0",
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	resp, err := f.server().GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	shard := shardOf(t, resp, "100-dmc0")

	require.Len(t, shard.GetResourceGroups(), len(shard.GetNodeIds()),
		"resource_groups must stay the same length as node_ids even when a leader is dropped")
	require.Len(t, shard.GetServiceable(), len(shard.GetNodeIds()))
	require.Len(t, shard.GetNodeAddrs(), len(shard.GetNodeIds()))

	assert.NotContains(t, shard.GetNodeIds(), int64(12),
		"the unregistered node must not be served at all")
	assert.NotContains(t, shard.GetResourceGroups(), "rg-gone",
		"and its resource group must not be left behind, shifting the tags after it")

	for i, id := range shard.GetNodeIds() {
		switch id {
		case 10:
			assert.Equal(t, "rg-a", shard.GetResourceGroups()[i])
		case 11:
			assert.Equal(t, "rg-b", shard.GetResourceGroups()[i])
		default:
			t.Fatalf("unexpected node %d in the response", id)
		}
	}
}

// TestShardLeaderResourceGroupTagIsNotAServabilityVerdict pins the boundary
// of what the tag can answer, because the natural misreading is expensive.
//
// A query-invisible replica -- what UpdateLoadConfig spawns for a newly added
// resource group -- is filtered out before the list is built, so its group
// does not appear in the response at all. A caller cannot therefore read
// "rg-b is absent" as "rg-b holds no replica": here rg-b holds one and is
// coming up. Readiness is the surface that tells those apart, and it is
// asserted alongside so the pairing is pinned rather than described.
func TestShardLeaderResourceGroupTagIsNotAServabilityVerdict(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putInvisibleReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.putLeader(100, 11, "100-dmc0", true) // serviceable, but on an invisible replica

	resp, err := f.server().GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		CollectionID:            100,
		WithUnserviceableShards: true,
	})
	require.NoError(t, err)
	shard := shardOf(t, resp, "100-dmc0")

	assert.Equal(t, []string{"rg-a"}, shard.GetResourceGroups(),
		"a query-invisible replica is filtered out before the tag is applied")
	assert.NotContains(t, shard.GetNodeIds(), int64(11))

	// The tag cannot say why rg-b is missing; readiness can.
	readiness := f.readiness(t, 100, "rg-b")
	assert.False(t, readiness.Ready)
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, readiness.Reason,
		"rg-b holds a replica and is coming up -- waiting helps, which absence from the response cannot convey")

	f.putResourceGroup(t, "rg-nowhere") // exists, nobody loaded into it
	absent := f.readiness(t, 100, "rg-nowhere")
	assert.Equal(t, utils.ShardLeadersReasonNoReplicaInResourceGroup, absent.Reason,
		"and the group that truly holds nothing reads differently -- the distinction the tag alone loses")
}
