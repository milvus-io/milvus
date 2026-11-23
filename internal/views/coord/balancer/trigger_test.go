package balancer

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestResolveCacheScope(t *testing.T) {
	c := balancercache.New(nil)
	a, b := cacheShard(1, 10), cacheShard(1, 20)
	a1, b1 := a, b
	a1.VChannel = "by-dev-rootcoord-dml_0_1v1"
	b1.VChannel = a1.VChannel
	residual, unrelated := cacheShard(1, 30), cacheShard(2, 40)
	c.PublishLoadConfig(1, &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}, {ReplicaID: 20}}}, 1)
	c.PublishDataView(1, api.PrepareCollectionDataView(&CollectionDataView{CollectionID: 1, Shards: []*ShardDataView{{VChannel: a.VChannel}, {VChannel: a1.VChannel}}}))
	c.PublishShard(a, cacheStats(101, 100, 0, coordview.SegmentStateUp, true))
	c.PublishShard(residual, cacheStats(102, 100, 100, coordview.SegmentStateReady, true))
	c.PublishShard(unrelated, cacheStats(201, 200, 100, coordview.SegmentStateUp, true))
	for _, tc := range []struct {
		name    string
		pending TriggerScope
		want    []qviews.ShardID
	}{
		{"collection expands replicas and channels including residuals", TriggerScope{DirtyCollections: []int64{1}}, []qviews.ShardID{a, a1, b, b1, residual}},
		{"direct shard does not expand siblings", TriggerScope{DirtyShards: []qviews.ShardID{a}}, []qviews.ShardID{a}},
		{"node includes zero row placements", TriggerScope{DirtyNodes: []int64{100}}, []qviews.ShardID{a, residual}},
		{"missing node", TriggerScope{DirtyNodes: []int64{999}}, nil},
		{"missing collection", TriggerScope{DirtyCollections: []int64{999}}, nil},
		{"merged scopes deduplicate", TriggerScope{DirtyCollections: []int64{1}, DirtyShards: []qviews.ShardID{a, unrelated}, DirtyNodes: []int64{100}}, []qviews.ShardID{a, a1, b, b1, residual, unrelated}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.want, discoveredIDs(c, tc.pending))
		})
	}
	c.PublishLoadConfig(1, nil, 2)
	require.Empty(t, discoveredIDs(c, TriggerScope{DirtyCollections: []int64{1}}), "explicit release bypasses shard discovery")
}

func setOf[T comparable](values ...T) map[T]struct{} {
	set := make(map[T]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func discoveredIDs(c *balancercache.Cache, scope TriggerScope) []qviews.ShardID {
	b := &DefaultBalancer{cache: c}
	found := make(map[qviews.ShardID]struct{})
	for part := range b.discover(scope) {
		for _, id := range part.ids {
			found[id] = struct{}{}
		}
	}
	return slices.Collect(maps.Keys(found))
}
