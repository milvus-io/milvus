package balancer

import (
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
		pending triggerBatch
		want    []qviews.ShardID
	}{
		{"collection expands replicas and channels including residuals", triggerBatch{dirtyColls: setOf[int64](1)}, []qviews.ShardID{a, a1, b, b1, residual}},
		{"direct shard does not expand siblings", triggerBatch{dirtyShards: setOf(a)}, []qviews.ShardID{a}},
		{"node includes zero row placements", triggerBatch{dirtyNodes: setOf[int64](100)}, []qviews.ShardID{a, residual}},
		{"missing node", triggerBatch{dirtyNodes: setOf[int64](999)}, nil},
		{"missing collection", triggerBatch{dirtyColls: setOf[int64](999)}, nil},
		{"merged scopes deduplicate", triggerBatch{dirtyColls: setOf[int64](1), dirtyShards: setOf(a, unrelated), dirtyNodes: setOf[int64](100)}, []qviews.ShardID{a, a1, b, b1, residual, unrelated}},
		{"full includes desired and residual", triggerBatch{full: true}, []qviews.ShardID{a, a1, b, b1, residual, unrelated}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.want, resolveCacheScope(c, tc.pending))
		})
	}
	c.PublishLoadConfig(1, nil, 2)
	require.ElementsMatch(t, []qviews.ShardID{a, residual}, resolveCacheScope(c, triggerBatch{dirtyColls: setOf[int64](1)}), "release retains actual shards without expanding removed replicas")
}

func TestTriggerQueueCoalescesAndPreservesSuccessorWork(t *testing.T) {
	q := newTriggerQueue()
	a, b := cacheShard(1, 10), cacheShard(2, 20)
	q.add(TriggerScope{DirtyShards: []qviews.ShardID{a, a}, DirtyCollections: []int64{1}})
	q.add(TriggerScope{NodeChanged: true, DirtyNodes: []int64{100}})
	<-q.signalCh()
	first := q.takePending()
	require.False(t, first.full)
	require.Equal(t, setOf(a), first.dirtyShards)
	require.Equal(t, setOf[int64](1), first.dirtyColls)
	require.Equal(t, setOf[int64](100), first.dirtyNodes)
	q.add(TriggerScope{DirtyShards: []qviews.ShardID{b}})
	<-q.signalCh()
	require.Equal(t, setOf(a), first.dirtyShards, "detached batch cannot absorb new events")
	require.Equal(t, setOf(b), q.takePending().dirtyShards)
	require.True(t, q.takePending().empty())
	for _, scopes := range [][]TriggerScope{nil, {{NodeChanged: true}}} {
		q.add(scopes...)
		<-q.signalCh()
		require.True(t, q.takePending().full)
	}
}

func setOf[T comparable](values ...T) map[T]struct{} {
	set := make(map[T]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}
