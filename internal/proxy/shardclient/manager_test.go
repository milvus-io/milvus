// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// singleShardResp builds a GetShardLeadersResponse for one channel served by one node.
func singleShardResp(channel string, nodeID int64, addr string) *querypb.GetShardLeadersResponse {
	return &querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: channel,
				NodeIds:     []int64{nodeID},
				NodeAddrs:   []string{addr},
				Serviceable: []bool{true},
			},
		},
	}
}

func expectShardLeaders(mixCoord *mocks.MockMixCoordClient, collectionID int64, resp *querypb.GetShardLeadersResponse) {
	mixCoord.EXPECT().GetShardLeaders(mock.Anything,
		mock.MatchedBy(func(req *querypb.GetShardLeadersRequest) bool {
			return req.GetCollectionID() == collectionID
		}),
	).Return(resp, nil).Maybe()
}

// TestShardCacheAliasRepointResolvesByCollectionID reproduces the concurrent-AlterAlias
// stale-alias race (issue #51533). The shard leader cache must key its entries by the
// cluster-unique collection id, not by the mutable collection/alias name. Once an alias is
// repointed from an old collection id to a new one, a request that resolves the alias to the
// NEW id must never be served the OLD collection's shard leaders.
func TestShardCacheAliasRepointResolvesByCollectionID(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	const (
		db          = "default"
		alias       = "orders"
		oldID int64 = 100
		newID int64 = 200
		oldCh       = "old_ch_100v0"
		newCh       = "new_ch_200v0"
	)

	mixCoord := mocks.NewMockMixCoordClient(t)
	expectShardLeaders(mixCoord, oldID, singleShardResp(oldCh, 7, "10.0.0.7:21123"))
	expectShardLeaders(mixCoord, newID, singleShardResp(newCh, 12, "10.0.0.12:21123"))

	mgr := NewShardClientMgr(mixCoord)

	// 1. alias -> old collection (100). A request fills the cache for id 100.
	leaders, err := mgr.GetShardLeaderList(ctx, db, alias, oldID, true)
	require.NoError(t, err)
	require.Equal(t, []string{oldCh}, leaders)

	// 2. AlterAlias repoints "orders" -> new collection (200): Layer 1 (meta cache) now resolves
	//    the alias to id 200 while the shard cache still holds id 100's entry. The request for the
	//    alias, resolved to the NEW id, must be served the NEW collection's channels — not id 100's.
	leaders, err = mgr.GetShardLeaderList(ctx, db, alias, newID, true)
	require.NoError(t, err)
	require.Equal(t, []string{newCh}, leaders,
		"alias resolved to new collection id %d must not be served old collection %d's channels", newID, oldID)

	// 3. GetShard for the new id must return the new collection's leader (node 12), not node 7.
	nodes, err := mgr.GetShard(ctx, true, db, alias, newID, newCh)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, int64(12), nodes[0].NodeID)
}

// TestParseShardLeaderListCarriesResourceGroup pins that the per-leader
// resource group reaches the cache, index-aligned with the node it describes.
// The cache is what every routing decision reads, so a tag that is dropped or
// shifted here is one no caller can recover.
func TestParseShardLeaderListCarriesResourceGroup(t *testing.T) {
	shards := parseShardLeaderList2QueryNode(context.Background(), []*querypb.ShardLeadersList{
		{
			ChannelName:    "dmc0",
			NodeIds:        []int64{1, 2},
			NodeAddrs:      []string{"addr1", "addr2"},
			Serviceable:    []bool{true, false},
			ResourceGroups: []string{"rg-a", "rg-b"},
		},
	})

	assert.Equal(t, []NodeInfo{
		{NodeID: 1, Address: "addr1", Serviceable: true, ResourceGroup: "rg-a"},
		{NodeID: 2, Address: "addr2", Serviceable: false, ResourceGroup: "rg-b"},
	}, shards["dmc0"])
}

// TestParseShardLeaderListToleratesMissingResourceGroups pins the rolling
// upgrade shape: a coordinator built before resource_groups existed fills the
// other three arrays and leaves this one empty. proto3 gives the proxy no
// other signal, and proxy and coordinator deploy separately, so this is a
// reachable state on every upgrade -- not a malformed response.
//
// Indexing resource_groups without the length guard panics here, on every
// cache refresh, for the whole upgrade window.
func TestParseShardLeaderListToleratesMissingResourceGroups(t *testing.T) {
	assert.NotPanics(t, func() {
		shards := parseShardLeaderList2QueryNode(context.Background(), []*querypb.ShardLeadersList{
			{
				ChannelName: "dmc0",
				NodeIds:     []int64{1, 2},
				NodeAddrs:   []string{"addr1", "addr2"},
				Serviceable: []bool{true, true},
				// ResourceGroups deliberately absent
			},
		})

		assert.Equal(t, []NodeInfo{
			{NodeID: 1, Address: "addr1", Serviceable: true, ResourceGroup: ""},
			{NodeID: 2, Address: "addr2", Serviceable: true, ResourceGroup: ""},
		}, shards["dmc0"],
			"an unknown resource group must read as empty, leaving the rest of the entry intact")
	})
}

// TestParseShardLeaderListNeutralizesShortResourceGroups covers the OTHER way
// the array can be short: non-empty but not parallel. That is a coordinator
// bug rather than the documented old-coordinator downgrade. A leader dropped
// from one array but not the others shifts every tag after it, so the tags
// that ARE present are of unknown alignment -- FilterByResourceGroup would
// route on them into the wrong group. The whole array is therefore dropped
// to the documented "unknown" (an unknown entry never matches a named group,
// so a scoped request gets a retriable refusal instead of a wrong node), the
// rest of each entry is kept, and it must not panic: a proxy that crashes on
// a malformed response takes the whole query path down with it.
func TestParseShardLeaderListNeutralizesShortResourceGroups(t *testing.T) {
	assert.NotPanics(t, func() {
		shards := parseShardLeaderList2QueryNode(context.Background(), []*querypb.ShardLeadersList{
			{
				ChannelName:    "dmc0",
				NodeIds:        []int64{1, 2},
				NodeAddrs:      []string{"addr1", "addr2"},
				Serviceable:    []bool{true, true},
				ResourceGroups: []string{"rg-a"}, // one short: which node is rg-a is unknowable
			},
		})

		assert.Equal(t, []NodeInfo{
			{NodeID: 1, Address: "addr1", Serviceable: true, ResourceGroup: ""},
			{NodeID: 2, Address: "addr2", Serviceable: true, ResourceGroup: ""},
		}, shards["dmc0"],
			"a misaligned array is neutralized to unknown for every entry, never trusted partially")
	})
}

// TestGetShardLeadersReadsWholeTableOnce pins the contract of the one-read
// accessor the scoped ExecuteOneChannel pre-pass relies on: with the cache
// warm and withCache=true it answers from the cache without a coordinator
// call; withCache=false is exactly one GetShardLeaders RPC that replaces the
// whole collection entry -- every channel refreshed at once -- which is what
// entitles the pre-pass to refuse on an empty result.
func TestGetShardLeadersReadsWholeTableOnce(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	const collectionID int64 = 300

	mixCoord := mocks.NewMockMixCoordClient(t)
	calls := 0
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.MatchedBy(func(req *querypb.GetShardLeadersRequest) bool {
		return req.GetCollectionID() == collectionID
	})).RunAndReturn(func(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
		calls++
		return &querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{ChannelName: "ch0", NodeIds: []int64{1}, NodeAddrs: []string{"a"}, Serviceable: []bool{true}, ResourceGroups: []string{"rg-a"}},
				{ChannelName: "ch1", NodeIds: []int64{2}, NodeAddrs: []string{"b"}, Serviceable: []bool{true}, ResourceGroups: []string{"rg-b"}},
			},
		}, nil
	})
	mgr := NewShardClientMgr(mixCoord)

	table, err := mgr.GetShardLeaders(ctx, true, "db", "coll", collectionID)
	require.NoError(t, err)
	require.Equal(t, 1, calls, "a cold cache is filled with one call")
	assert.Equal(t, map[string][]NodeInfo{
		"ch0": {{NodeID: 1, Address: "a", Serviceable: true, ResourceGroup: "rg-a"}},
		"ch1": {{NodeID: 2, Address: "b", Serviceable: true, ResourceGroup: "rg-b"}},
	}, table, "every channel of the collection, tags included, in one read")

	_, err = mgr.GetShardLeaders(ctx, true, "db", "coll", collectionID)
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "a warm cache is served without a coordinator call")

	_, err = mgr.GetShardLeaders(ctx, false, "db", "coll", collectionID)
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "withCache=false is exactly one refreshing call")
}
