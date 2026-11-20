package balancer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestShardQueueReprioritizesAndDeduplicates(t *testing.T) {
	a, b, c := cacheShard(1, 10), cacheShard(2, 20), cacheShard(3, 30)
	q := shardQueue{items: make(map[qviews.ShardID]*queuedShard)}
	wait := syncutil.NewFuture[error]()
	q.submit([]scoredShard{{a, 10}, {b, -5}, {c, 0}}, 1, &shardCompletion{future: wait})
	q.submit([]scoredShard{{a, -8}, {b, 0}}, 3, nil)
	q.submit([]scoredShard{{a, 18}, {b, -5}}, 2, nil)
	require.Len(t, q.items, 3)
	first, groups := q.take(2)
	require.Equal(t, []qviews.ShardID{b, c}, first, "equal scores retain arrival order")
	for group, n := range groups {
		group.finish(n, context.DeadlineExceeded)
	}
	require.False(t, wait.Ready())
	second, groups := q.take(2)
	require.Equal(t, []qviews.ShardID{a}, second)
	for group, n := range groups {
		group.finish(n, nil)
	}
	result, err := wait.GetWithContext(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, result, context.DeadlineExceeded)
	require.Empty(t, q.items)
}

func TestPriorityBatchColdQueryDuringTenThousandRecoveries(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, nil, nil))
	cfg := DefaultBalanceConfig()
	cfg.ShardBatchSize = 8
	c.UpdateBalanceConfig(cfg)
	c.MarkReady()
	recovering := make([]qviews.ShardID, 10_000)
	for i := range recovering {
		recovering[i] = qviews.ShardID{ReplicaID: 10, VChannel: fmt.Sprintf("p_1v%d", i)}
	}
	cold := cacheShard(2, 20)
	var batches [][]qviews.ShardID
	policy := completionTestPolicy(func(_ balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		batches = append(batches, dirty)
		if len(batches) == 1 {
			auto := cfgFor(2, 20, nil, nil)
			auto.AutoLoad = true
			c.PublishLoadConfig(2, auto, 1)
			c.PublishDataView(2, cacheData(2, cold.VChannel, 1))
		}
		return nil
	})
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, policy, nil)
	b.Trigger(TriggerScope{DirtyShards: recovering})
	require.NoError(t, b.Reconcile(t.Context()))
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, batches[0], 8)
	require.Len(t, batches[1], 8)
	require.Equal(t, cold, batches[1][0])
	require.Less(t, len(b.ready.items), len(recovering)-15, "recovery discovery is still in progress")
}

func TestPriorityBatchDefersLayoutExpansion(t *testing.T) {
	c := scanTestCache(6)
	cfg := DefaultBalanceConfig()
	cfg.ShardBatchSize = 2
	c.UpdateBalanceConfig(cfg)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	reg := emptyRegistry(t)
	b := NewDefaultBalancer(c, reg, nil, nil)
	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{cacheShard(1, 10)}})
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, reg.CollectionShards(1), 1)
	require.Empty(t, b.ready.items, "layout changes submit a collection scope for discovery")
	require.NoError(t, b.Reconcile(t.Context()))
	require.Len(t, reg.CollectionShards(1), 3)
}

func TestPeriodicProducerUpdatesQueueDuringPlacement(t *testing.T) {
	params := paramtable.Get()
	setBalanceParam(t, params, params.QueryViewCfg.BalancerReconcileInterval.Key, "20ms")
	c := scanTestCache(1)
	t.Cleanup(c.Close)
	id := cacheShard(1, 10)
	started, gate := make(chan struct{}), make(chan struct{})
	var first sync.Once
	policy := completionTestPolicy(func(_ balancercache.Reader, _ []qviews.ShardID) *BalancePlan {
		first.Do(func() {
			close(started)
			<-gate
		})
		return nil
	})
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, policy, nil)
	b.Start(t.Context())
	t.Cleanup(b.Stop)
	t.Cleanup(func() { close(gate) })
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	// Only the timer can discover the new source and rescore the queued shard.
	c.SetNotifier(nil)
	cfg := c.GetCollection(1).LoadConfig().Clone()
	cfg.AutoLoad = true
	c.PublishLoadConfig(1, cfg, 2)
	scoreIs := func(want int32) bool {
		b.submitMu.Lock()
		defer b.submitMu.Unlock()
		entry := b.ready.items[id]
		return entry != nil && entry.priority == want && len(b.ready.items) == 1
	}
	require.Eventually(t, func() bool { return scoreIs(12) }, 5*time.Second, time.Millisecond)
	c.PublishShard(id, &coordview.ShardStats{MaybeUped: true})
	require.Eventually(t, func() bool { return scoreIs(2) }, 5*time.Second, time.Millisecond)
}

func TestPriorityBatchAppliesPreparingInQueueOrder(t *testing.T) {
	c := scanTestCache(2)
	t.Cleanup(c.Close)
	cfg := c.GetCollection(1).LoadConfig().Clone()
	cfg.AutoLoad = true
	c.PublishLoadConfig(1, cfg, 2)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	channels := c.GetCollection(1).DataView().Shards
	recovery := qviews.ShardID{ReplicaID: 10, VChannel: channels[0].VChannel}
	cold := qviews.ShardID{ReplicaID: 10, VChannel: channels[1].VChannel}
	c.PublishShard(recovery, &coordview.ShardStats{MaybeUped: true})
	registry := emptyRegistry(t)
	b := NewDefaultBalancer(c, registry, nil, nil)
	var scores []int32
	var applied []string
	var original func(*coordview.ShardViewManager, context.Context, *qviews.QueryViewAtCoordBuilder) error
	patch := mockey.Mock((*coordview.ShardViewManager).AddPreparing).Origin(&original).To(func(m *coordview.ShardViewManager, ctx context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
		meta := builder.Build().GetMeta()
		scores = append(scores, meta.GetControl().GetPreparationPriority())
		applied = append(applied, meta.GetVchannel())
		return original(m, ctx, builder)
	}).Build()
	defer patch.UnPatch()

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{recovery, cold}})
	require.NoError(t, b.Reconcile(t.Context()))
	require.Equal(t, []string{cold.VChannel, recovery.VChannel}, applied)
	require.Equal(t, []int32{12, 2}, scores)
}
