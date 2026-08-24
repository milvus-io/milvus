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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
