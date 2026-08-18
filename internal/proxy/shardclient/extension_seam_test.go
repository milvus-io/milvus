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

package shardclient

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type seamTestProvider struct{ caps extension.Capabilities }

func (seamTestProvider) Name() string                           { return "shardclient-seam-test" }
func (seamTestProvider) Requires() []extension.CapabilityID     { return nil }
func (p seamTestProvider) Capabilities() extension.Capabilities { return p.caps }

func installProxyExtension(t *testing.T) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(seamTestProvider{
		caps: extension.Capabilities{ProxyExt: extension.NoopProxyExtension{}},
	}))
}

func noProvider(t *testing.T) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
}

// coordRecorder answers GetShardLeaders with a different query node per
// resource group, and records the scope every request carried. Answering
// differently per scope is what lets a test tell "routed to the right group"
// apart from "asked the right question and then ignored the answer".
type coordRecorder struct {
	mu     sync.Mutex
	scopes []string
}

func (c *coordRecorder) install(t *testing.T) *mocks.MockMixCoordClient {
	t.Helper()
	coord := mocks.NewMockMixCoordClient(t)
	coord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			c.mu.Lock()
			c.scopes = append(c.scopes, req.GetResourceGroup())
			c.mu.Unlock()
			return &querypb.GetShardLeadersResponse{
				Status: merr.Success(),
				Shards: []*querypb.ShardLeadersList{{
					ChannelName: "channel-1",
					NodeIds:     []int64{nodeIDFor(req.GetResourceGroup())},
					NodeAddrs:   []string{"addr-" + req.GetResourceGroup()},
					Serviceable: []bool{true},
				}},
			}, nil
		}).Maybe()
	return coord
}

func (c *coordRecorder) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.scopes...)
}

func nodeIDFor(resourceGroup string) int64 {
	switch resourceGroup {
	case "rg-a":
		return 11
	case "rg-b":
		return 22
	default:
		return 99
	}
}

func scopedContext(resourceGroup string) context.Context {
	return extension.WithQueryResourceGroup(context.Background(), resourceGroup)
}

// TestShardLookupIsUnscopedWithNoProvider is the inertness proof for the
// routing half: a stock binary must go on asking the collection-wide question,
// so the request it sends must carry no resource group even when something put
// one on the context.
func TestShardLookupIsUnscopedWithNoProvider(t *testing.T) {
	noProvider(t)

	recorder := &coordRecorder{}
	mgr := NewShardClientMgr(recorder.install(t))
	defer mgr.Close()

	_, err := mgr.GetShardLeaderList(scopedContext("rg-a"), "db", "coll", 100, true)
	require.NoError(t, err)

	assert.Equal(t, []string{""}, recorder.seen(),
		"a stock binary must ask for every replica of the collection, whatever a context claims")
}

// TestShardLookupCarriesTheScopeToTheCoordinator pins that the scope reaches
// the only place that can apply it. Filtering after the fact is impossible: the
// response flattens every replica into one list per channel, so the resource
// group each leader belonged to is gone.
func TestShardLookupCarriesTheScopeToTheCoordinator(t *testing.T) {
	installProxyExtension(t)

	recorder := &coordRecorder{}
	mgr := NewShardClientMgr(recorder.install(t))
	defer mgr.Close()

	_, err := mgr.GetShardLeaderList(scopedContext("rg-a"), "db", "coll", 100, true)
	require.NoError(t, err)

	assert.Equal(t, []string{"rg-a"}, recorder.seen(),
		"the coordinator must be asked about the resource group the query was scoped to")
}

// TestShardLookupKeepsScopesApart is the isolation assertion. Two resource
// groups serving the same collection get different leaders, and a cache that
// ignored the scope would hand the second one the first one's answer - a query
// made ready on rg-b served by rg-a's query nodes.
func TestShardLookupKeepsScopesApart(t *testing.T) {
	installProxyExtension(t)

	recorder := &coordRecorder{}
	mgr := NewShardClientMgr(recorder.install(t))
	defer mgr.Close()

	fromA, err := mgr.GetShard(scopedContext("rg-a"), true, "db", "coll", 100, "channel-1")
	require.NoError(t, err)
	fromB, err := mgr.GetShard(scopedContext("rg-b"), true, "db", "coll", 100, "channel-1")
	require.NoError(t, err)

	require.Len(t, fromA, 1)
	require.Len(t, fromB, 1)
	assert.Equal(t, int64(11), fromA[0].NodeID, "a query scoped to rg-a must be routed to rg-a's leader")
	assert.Equal(t, int64(22), fromB[0].NodeID,
		"a query scoped to rg-b must not be served rg-a's cached leader; that is the isolation this key exists for")

	scopes := recorder.seen()
	sort.Strings(scopes)
	assert.Equal(t, []string{"rg-a", "rg-b"}, scopes,
		"each scope must produce its own lookup rather than reusing another scope's answer")
}

// TestShardLookupStillCachesWithinAScope guards the other direction: keying by
// scope must not turn every request into a coordinator round trip.
//
// Both entry points are driven, and they must share the one cached answer.
// A lookup that stores under the scoped key but reads under an unscoped one
// still returns correct leaders - it just refetches every single time - so
// correctness assertions alone cannot see it. Counting the round trips can.
func TestShardLookupStillCachesWithinAScope(t *testing.T) {
	installProxyExtension(t)

	recorder := &coordRecorder{}
	mgr := NewShardClientMgr(recorder.install(t))
	defer mgr.Close()

	for i := 0; i < 3; i++ {
		_, err := mgr.GetShardLeaderList(scopedContext("rg-a"), "db", "coll", 100, true)
		require.NoError(t, err)
		_, err = mgr.GetShard(scopedContext("rg-a"), true, "db", "coll", 100, "channel-1")
		require.NoError(t, err)
	}

	assert.Equal(t, []string{"rg-a"}, recorder.seen(),
		"repeated lookups in one scope must be served from the cache, whichever entry point asks: "+
			"a read key that does not match the write key silently refetches on every query")
}

// TestInvalidateShardLeaderCacheDropsEveryScope is the eviction half. A
// collection whose leaders moved has moved them for every scope; leaving one
// scope's copy behind keeps routing queries scoped to it at query nodes that no
// longer serve them.
func TestInvalidateShardLeaderCacheDropsEveryScope(t *testing.T) {
	installProxyExtension(t)

	recorder := &coordRecorder{}
	mgr := NewShardClientMgr(recorder.install(t))
	defer mgr.Close()

	for _, scope := range []string{"rg-a", "rg-b"} {
		_, err := mgr.GetShardLeaderList(scopedContext(scope), "db", "coll", 100, true)
		require.NoError(t, err)
	}
	// Another collection, to prove the eviction is scoped to the ids it was given.
	_, err := mgr.GetShardLeaderList(scopedContext("rg-a"), "db", "other", 101, true)
	require.NoError(t, err)

	mgr.InvalidateShardLeaderCache([]int64{100})

	assert.Nil(t, mgr.getCachedShardLeaders(shardLeaderCacheKey{collectionID: 100, resourceGroup: "rg-a"}, "test"),
		"the invalidated collection must lose its rg-a entry")
	assert.Nil(t, mgr.getCachedShardLeaders(shardLeaderCacheKey{collectionID: 100, resourceGroup: "rg-b"}, "test"),
		"the invalidated collection must lose every scope, not only the one that happens to be looked up first")
	assert.NotNil(t, mgr.getCachedShardLeaders(shardLeaderCacheKey{collectionID: 101, resourceGroup: "rg-a"}, "test"),
		"a collection nobody invalidated must keep its cached leaders")

	mgr.leaderMut.RLock()
	_, indexed := mgr.scopedKeys[100]
	mgr.leaderMut.RUnlock()
	assert.False(t, indexed,
		"the scope index must go with the entries it points at, or it accumulates forever")
}

// TestRoutingResourceGroupIgnoresTheContextWithNoProvider is the unit-level
// inertness assertion behind TestShardLookupIsUnscopedWithNoProvider: the
// context is not even consulted, so a stock binary pays an atomic load rather
// than a context walk on every search.
func TestRoutingResourceGroupIgnoresTheContextWithNoProvider(t *testing.T) {
	noProvider(t)
	assert.Equal(t, "", routingResourceGroup(scopedContext("rg-a")))

	installProxyExtension(t)
	assert.Equal(t, "rg-a", routingResourceGroup(scopedContext("rg-a")))
	assert.Equal(t, "", routingResourceGroup(context.Background()),
		"a request nothing scoped must stay unscoped even with a provider installed")
}
