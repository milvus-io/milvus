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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newBalanceParams() *paramtable.ComponentParam {
	p := &paramtable.ComponentParam{}
	p.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files([]string{})))
	return p
}

func setBalanceParam(t *testing.T, p *paramtable.ComponentParam, key, value string) {
	t.Helper()
	require.NoError(t, p.Save(key, value))
	t.Cleanup(func() { require.NoError(t, p.Reset(key)) })
}

func TestBalanceConfigSourcePublicationAndValidation(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	require.Equal(t, DefaultBalanceConfig(), c.GetBalanceConfig())
	require.True(t, b.queue.takePending().empty(), "identical defaults do not request a full pass")
	pinned := newPlanningContext(c)
	key := p.QueryViewCfg.BalancerStickinessWeight.Key
	require.NoError(t, p.Save(key, "3"))
	require.Equal(t, 3.0, c.GetBalanceConfig().StickinessWeight)
	require.Equal(t, 1.0, pinned.GetBalanceConfig().StickinessWeight, "an existing batch retains its config")
	require.True(t, b.queue.takePending().full)
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(key, "3"))
	require.Same(t, previous, c.GetBalanceConfig())
	require.True(t, b.queue.takePending().empty())

	for _, value := range []string{"-1", "NaN", "+Inf", "invalid"} {
		require.NoError(t, p.Save(key, value))
		require.Same(t, previous, c.GetBalanceConfig())
		require.True(t, b.queue.takePending().empty())
	}
	require.NoError(t, p.Save(key, "3"))
	for _, key := range []string{p.QueryViewCfg.BalancerStickyRowsScale.Key, p.QueryViewCfg.BalancerTargetRowsPerShardNode.Key} {
		for _, value := range []string{"0", "-1", "1.5", "9223372036854775808"} {
			require.NoError(t, p.Save(key, value))
			require.Same(t, previous, c.GetBalanceConfig())
		}
		require.NoError(t, p.Reset(key))
	}
	intervalKey := p.QueryViewCfg.BalancerReconcileInterval.Key
	for _, value := range []string{"0s", "-1s", "NaN", "10", "1e100s", "9223372036854775807s"} {
		require.NoError(t, p.Save(intervalKey, value))
		require.Same(t, previous, c.GetBalanceConfig())
	}
	require.NoError(t, p.Save(intervalKey, "10ms"))
	require.Equal(t, 10*time.Millisecond, c.GetBalanceConfig().TickerInterval)
	require.True(t, b.queue.takePending().empty(), "interval-only refresh must not request full optimization")

	require.NoError(t, p.Save(p.QueryViewCfg.BalancerAutoBalance.Key, "false"))
	require.False(t, c.GetBalanceConfig().AutoBalance)
	require.True(t, b.queue.takePending().full)
	previous = c.GetBalanceConfig()
	require.NoError(t, p.Save(p.QueryViewCfg.BalancerAutoBalance.Key, "invalid"))
	require.Same(t, previous, c.GetBalanceConfig())
	stop()
	require.NoError(t, p.Save(p.QueryViewCfg.BalancerAutoBalance.Key, "true"))
	require.Same(t, previous, c.GetBalanceConfig(), "unregistration prevents further updates")
}

func TestBalanceConfigIndependentOfQueryCoord(t *testing.T) {
	p := newBalanceParams()
	// Legacy settings must not affect startup or subsequent QueryView refreshes.
	require.NoError(t, p.Save(p.QueryCoordCfg.AutoBalance.Key, "false"))
	for key, value := range map[string]string{
		"queryCoord.queryView.fullReconsileInterval":    "30",
		"queryCoord.queryView.fullReconcileInterval":    "40",
		"queryCoord.queryView.balance.stickinessWeight": "9",
	} {
		require.NoError(t, p.Save(key, value))
	}
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	require.Equal(t, DefaultBalanceConfig(), c.GetBalanceConfig())
	require.True(t, b.queue.takePending().empty())

	require.NoError(t, p.Save(p.QueryViewCfg.BalancerAutoBalance.Key, "false"))
	require.False(t, c.GetBalanceConfig().AutoBalance)
	require.True(t, b.queue.takePending().full)
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(p.QueryCoordCfg.AutoBalance.Key, "true"))
	require.Same(t, previous, c.GetBalanceConfig())
	require.True(t, b.queue.takePending().empty())
	require.True(t, p.QueryCoordCfg.AutoBalance.GetAsBool())

	for raw, want := range map[string]time.Duration{
		"500ms": 500 * time.Millisecond,
		"1m":    time.Minute,
		"1.5s":  1500 * time.Millisecond,
	} {
		require.NoError(t, p.Save(p.QueryViewCfg.BalancerReconcileInterval.Key, raw))
		require.Equal(t, want, c.GetBalanceConfig().TickerInterval)
		require.True(t, b.queue.takePending().empty())
	}
	require.NoError(t, p.Reset(p.QueryViewCfg.BalancerReconcileInterval.Key))
	require.Equal(t, time.Minute, c.GetBalanceConfig().TickerInterval)
}

func TestBalanceConfigConcurrentUpdates(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	var wg sync.WaitGroup
	for key, value := range map[string]string{
		p.QueryViewCfg.BalancerNodeLoadWeight.Key:         "2",
		p.QueryViewCfg.BalancerFanoutWeight.Key:           "4",
		p.QueryViewCfg.BalancerStickyRowsScale.Key:        "5000",
		p.QueryViewCfg.BalancerTargetRowsPerShardNode.Key: "6000",
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Save(key, value); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got, valid := readBalanceConfig(p)
	require.True(t, valid)
	require.Equal(t, got, c.GetBalanceConfig())
	require.Equal(t, 2.0, got.NodeLoadWeight)
	require.Equal(t, 4.0, got.FanoutWeight)
	require.Equal(t, int64(5000), got.StickyRowsScale)
	require.Equal(t, int64(6000), got.TargetRowsPerShardNode)
}

func TestBalanceConfigRejectsZeroAndOverflowWeightSum(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	keys := []string{p.QueryViewCfg.BalancerStickinessWeight.Key, p.QueryViewCfg.BalancerNodeLoadWeight.Key, p.QueryViewCfg.BalancerFanoutWeight.Key}
	require.NoError(t, p.Save(keys[0], "0"))
	require.NoError(t, p.Save(keys[1], "0"))
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(keys[2], "0"))
	require.Same(t, previous, c.GetBalanceConfig())
	require.NoError(t, p.Save(keys[0], "1.7e308"))
	previous = c.GetBalanceConfig()
	require.NoError(t, p.Save(keys[1], "1.7e308"))
	require.Same(t, previous, c.GetBalanceConfig())
}

func TestBalanceLoopRefreshesLongTimerAndUnwatchesOnStop(t *testing.T) {
	p := paramtable.Get()
	key := p.QueryViewCfg.BalancerReconcileInterval.Key
	setBalanceParam(t, p, key, "1h")
	c := balancercache.New(nil)
	c.MarkReady()
	b := NewDefaultBalancer(c, nil, nil)
	var calls atomic.Int64
	patch := mockey.Mock((*DefaultBalancer).Reconcile).To(func(controller *DefaultBalancer, _ context.Context) error {
		controller.queue.takePending()
		calls.Add(1)
		return nil
	}).Build()
	defer patch.UnPatch()
	b.Start(t.Context())
	defer b.Stop()
	require.Eventually(t, func() bool { return calls.Load() > 0 && c.GetBalanceConfig().TickerInterval == time.Hour }, time.Second, time.Millisecond)
	before := calls.Load()
	require.NoError(t, p.Save(key, "10ms"))
	require.Eventually(t, func() bool { return calls.Load() > before }, time.Second, time.Millisecond)
	b.Stop()
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(key, "50s"))
	require.Same(t, previous, c.GetBalanceConfig())
}
