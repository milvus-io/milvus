package balancer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Observe entry without replacing the real readiness wait.
func observeReadyWait(t *testing.T) <-chan struct{} {
	t.Helper()
	entered := make(chan struct{})
	var once sync.Once
	var original func(*balancercache.Cache, context.Context) error
	patch := mockey.Mock((*balancercache.Cache).WaitForReady).Origin(&original).To(func(c *balancercache.Cache, ctx context.Context) error {
		once.Do(func() { close(entered) })
		return original(c, ctx)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return entered
}

func TestBalancerWaitsForRecoveryWithoutLosingWork(t *testing.T) {
	registry := emptyRegistry(t)
	existing := cacheShard(1, 10)
	added := existing
	added.VChannel = "by-dev-rootcoord-dml_0_1v1"
	addShardWithPreparingView(t, registry, existing, map[int64]map[int64][]int64{1: {1: {1000}}})
	before := registry.Get(existing).Stats().PreparingVersion
	config := policyTestConfig()
	config.TickerInterval = time.Hour
	c := balancercache.New(config)
	t.Cleanup(registry.RegisterPublicationListener(c.PublishShard))
	b := NewDefaultBalancer(c, registry, nil)
	waiting := observeReadyWait(t)
	var calls atomic.Int64
	var original func(*DefaultBalancePolicy, balancercache.Reader, []qviews.ShardID) *BalancePlan
	patch := mockey.Mock((*DefaultBalancePolicy).Plan).Origin(&original).To(func(p *DefaultBalancePolicy, reader balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		calls.Add(1)
		return original(p, reader, dirty)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{existing}})
	b.Start(t.Context())
	t.Cleanup(b.Stop)
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not wait for recovery")
	}
	params := paramtable.Get()
	setBalanceParam(t, params, params.QueryViewCfg.BalancerAutoBalance.Key, "false")
	require.False(t, c.GetBalanceConfig().AutoBalance, "configuration refresh is not blocked by recovery readiness")
	// No config has recovered yet: planning now would incorrectly release the
	// existing view. Work arriving during the wait must also survive.
	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{added}})
	require.Never(t, func() bool { return calls.Load() != 0 }, 20*time.Millisecond, time.Millisecond)
	require.Equal(t, before, registry.Get(existing).Stats().PreparingVersion)
	b.queue.mu.Lock()
	full := b.queue.full
	_, existingPending := b.queue.dirtyShards[existing]
	_, addedPending := b.queue.dirtyShards[added]
	b.queue.mu.Unlock()
	require.True(t, full)
	require.True(t, existingPending)
	require.True(t, addedPending)

	// Suppress all subsequent wakeups, including MarkReady's full request.
	// The readiness barrier itself must resume the already-running reconcile.
	c.SetNotifier(nil)
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil,
		shardDataView(existing.VChannel, 1, 1000), shardDataView(added.VChannel, 1, 1001))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.MarkReady()
	require.Eventually(t, func() bool {
		manager := registry.Get(added)
		return manager != nil && manager.Stats().PreparingVersion != nil
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, before, registry.Get(existing).Stats().PreparingVersion, "recovered preparing view must not be released or replaced")
	require.Equal(t, int64(1), calls.Load(), "queued work is coalesced into one batch")
}

func TestBalancerStopCancelsRecoveryWait(t *testing.T) {
	registry := emptyRegistry(t)
	c := newTestCache(cfgFor(1, 10, nil, nil))
	shard := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	t.Cleanup(registry.RegisterPublicationListener(c.PublishShard))
	b := NewDefaultBalancer(c, registry, nil)
	waiting := observeReadyWait(t)
	b.Start(t.Context())
	t.Cleanup(b.Stop)
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("controller did not wait for recovery")
	}
	stopped := make(chan struct{})
	go func() { b.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop blocked on cache readiness")
	}
	require.False(t, c.Ready())
	require.Nil(t, registry.Get(shard), "shutdown must not run planning or apply")
	require.True(t, b.queue.takePending().full, "canceled wait does not consume work")
	c.MarkReady()
	b.Start(t.Context())
	require.Eventually(t, func() bool {
		manager := registry.Get(shard)
		return manager != nil && manager.Stats().PreparingVersion != nil
	}, 5*time.Second, time.Millisecond)
}
