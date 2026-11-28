package balancer

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
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

func TestBalancerPeriodicScanDeadlineOnConfigChange(t *testing.T) {
	params := paramtable.Get()
	interval := params.QueryViewCfg.BalancerReconcileInterval.Key
	setBalanceParam(t, params, interval, "1m")
	synctest.Test(t, func(t *testing.T) {
		cache := scanTestCache(1)
		defer cache.Close()
		var passes atomic.Int32
		b := NewDefaultBalancer(cache, &coordview.ShardViewRegistry{}, balancePolicyFunc(func(balancercache.Reader, []qviews.ShardID) *BalancePlan {
			passes.Add(1)
			return nil
		}), nil)
		b.Start(t.Context())
		defer b.Stop()
		synctest.Wait()
		require.EqualValues(t, 1, passes.Load(), "startup scan")

		time.Sleep(59 * time.Second)
		setBalanceParam(t, params, params.QueryViewCfg.BalancerShardBatchSize.Key, "8")
		synctest.Wait()
		require.EqualValues(t, 8, cache.GetBalanceConfig().ShardBatchSize)
		require.EqualValues(t, 1, passes.Load())
		time.Sleep(time.Second)
		synctest.Wait()
		require.EqualValues(t, 2, passes.Load(), "a batch-size change must preserve the original deadline")

		require.NoError(t, params.Save(interval, "5s"))
		synctest.Wait()
		time.Sleep(4 * time.Second)
		synctest.Wait()
		require.EqualValues(t, 2, passes.Load())
		time.Sleep(time.Second)
		synctest.Wait()
		require.EqualValues(t, 3, passes.Load(), "the new interval controls the next deadline")
	})
}

func TestBalanceConfigSourcePublicationAndValidation(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	require.Equal(t, DefaultBalanceConfig(), c.GetBalanceConfig())
	require.True(t, !takeScanRequest(b), "identical defaults do not request a full pass")
	batchSize := p.QueryViewCfg.BalancerShardBatchSize.Key
	require.NoError(t, p.Save(batchSize, "8"))
	require.EqualValues(t, 8, c.GetBalanceConfig().ShardBatchSize)
	require.NoError(t, p.Save(batchSize, "0"))
	require.EqualValues(t, 8, c.GetBalanceConfig().ShardBatchSize)
	require.NoError(t, p.Reset(batchSize))
	require.True(t, !takeScanRequest(b), "changing the bound needs no full scan")
	pinned := newPlanningContext(c)
	key := p.QueryViewCfg.BalancerMoveCostPerUnit.Key
	require.NoError(t, p.Save(key, "3"))
	require.Equal(t, 3.0, c.GetBalanceConfig().MoveCostPerUnit)
	require.Equal(t, 0.02, pinned.GetBalanceConfig().MoveCostPerUnit, "an existing batch retains its config")
	require.True(t, takeScanRequest(b))
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(key, "3"))
	require.Same(t, previous, c.GetBalanceConfig())
	require.True(t, !takeScanRequest(b))

	for _, value := range []string{"-1", "NaN", "+Inf", "invalid"} {
		require.NoError(t, p.Save(key, value))
		require.Same(t, previous, c.GetBalanceConfig())
		require.True(t, !takeScanRequest(b))
	}
	require.NoError(t, p.Save(key, "3"))
	for _, key := range []string{p.QueryViewCfg.BalancerResourceGroupAbsoluteTolerance.Key, p.QueryViewCfg.BalancerTargetLogicalSizePerNode.Key} {
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
	require.Equal(t, 10*time.Millisecond, c.GetBalanceConfig().ReconcileInterval)
	require.True(t, !takeScanRequest(b), "interval-only refresh must not request full optimization")

	require.NoError(t, p.Save(p.QueryViewCfg.BalancerOptimizationEnabled.Key, "false"))
	require.False(t, c.GetBalanceConfig().OptimizationEnabled)
	require.True(t, takeScanRequest(b))
	previous = c.GetBalanceConfig()
	require.NoError(t, p.Save(p.QueryViewCfg.BalancerOptimizationEnabled.Key, "invalid"))
	require.Same(t, previous, c.GetBalanceConfig())
	stop()
	require.NoError(t, p.Save(p.QueryViewCfg.BalancerOptimizationEnabled.Key, "true"))
	require.Same(t, previous, c.GetBalanceConfig(), "unregistration prevents further updates")
}

func TestBalanceConfigIndependentOfQueryCoord(t *testing.T) {
	p := newBalanceParams()
	// QueryCoord balancing must not affect QueryView startup or refreshes.
	require.NoError(t, p.Save(p.QueryCoordCfg.AutoBalance.Key, "false"))

	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	require.Equal(t, DefaultBalanceConfig(), c.GetBalanceConfig())
	require.True(t, !takeScanRequest(b))

	require.NoError(t, p.Save(p.QueryViewCfg.BalancerOptimizationEnabled.Key, "false"))
	require.False(t, c.GetBalanceConfig().OptimizationEnabled)
	require.True(t, takeScanRequest(b))
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(p.QueryCoordCfg.AutoBalance.Key, "true"))
	require.Same(t, previous, c.GetBalanceConfig())
	require.True(t, !takeScanRequest(b))
	require.True(t, p.QueryCoordCfg.AutoBalance.GetAsBool())

	for raw, want := range map[string]time.Duration{
		"500ms": 500 * time.Millisecond,
		"1m":    time.Minute,
		"1.5s":  1500 * time.Millisecond,
	} {
		require.NoError(t, p.Save(p.QueryViewCfg.BalancerReconcileInterval.Key, raw))
		require.Equal(t, want, c.GetBalanceConfig().ReconcileInterval)
		require.True(t, !takeScanRequest(b))
	}
	require.NoError(t, p.Reset(p.QueryViewCfg.BalancerReconcileInterval.Key))
	require.Equal(t, time.Minute, c.GetBalanceConfig().ReconcileInterval)
}

func TestBalanceConfigConcurrentUpdates(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	var wg sync.WaitGroup
	for key, value := range map[string]string{
		p.QueryViewCfg.BalancerResourceGroupWeight.Key:            "2",
		p.QueryViewCfg.BalancerFanoutWeight.Key:                   "4",
		p.QueryViewCfg.BalancerResourceGroupAbsoluteTolerance.Key: "5000",
		p.QueryViewCfg.BalancerTargetLogicalSizePerNode.Key:       "6000",
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
	require.Equal(t, 2.0, got.ResourceGroupWeight)
	require.Equal(t, 4.0, got.FanoutWeight)
	require.Equal(t, int64(5000), got.ResourceGroupAbsoluteTolerance)
	require.Equal(t, int64(6000), got.TargetLogicalSizePerNode)
}

func TestBalanceConfigRejectsZeroAndOverflowWeightSum(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	keys := []string{p.QueryViewCfg.BalancerShardConcentrationWeight.Key, p.QueryViewCfg.BalancerCollectionConcentrationWeight.Key, p.QueryViewCfg.BalancerResourceGroupWeight.Key, p.QueryViewCfg.BalancerFanoutWeight.Key}
	for _, key := range keys[:3] {
		require.NoError(t, p.Save(key, "0"))
	}
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(keys[3], "0"))
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
	b := NewDefaultBalancer(c, nil, nil, nil)
	var calls atomic.Int64
	patch := mockey.Mock((*DefaultBalancer).dispatch).To(func(controller *DefaultBalancer, source discoverySource) int {
		if job, source := controller.discovery.next(source); job != nil {
			controller.discovery.finish(source, job)
		}
		calls.Add(1)
		return -1
	}).Build()
	defer patch.UnPatch()
	b.Start(t.Context())
	defer b.Stop()
	require.Eventually(t, func() bool { return calls.Load() > 0 && c.GetBalanceConfig().ReconcileInterval == time.Hour }, time.Second, time.Millisecond)
	before := calls.Load()
	require.NoError(t, p.Save(key, "10ms"))
	require.Eventually(t, func() bool { return calls.Load() > before }, time.Second, time.Millisecond)
	b.Stop()
	previous := c.GetBalanceConfig()
	require.NoError(t, p.Save(key, "50s"))
	require.Same(t, previous, c.GetBalanceConfig())
}

func TestDataViewCostConfigDoesNotTriggerBalance(t *testing.T) {
	p := newBalanceParams()
	c := balancercache.New(nil)
	b := NewDefaultBalancer(c, nil, nil, nil)
	stop := b.watchBalanceConfig(t.Context(), p)
	defer stop()
	previous := c.GetBalanceConfig()
	for key, value := range map[string]string{
		p.QueryViewCfg.LogicalSizeSegmentBaseCost.Key: "1000",
		p.QueryViewCfg.LogicalSizeSource.Key:          "insertBytes",
		p.QueryViewCfg.LogicalSizeBytesPerUnit.Key:    "128KB",
	} {
		require.NoError(t, p.Save(key, value))
		require.Same(t, previous, c.GetBalanceConfig())
		require.True(t, !takeScanRequest(b), "only a future DataView publication changes scheduling size")
	}
	require.NoError(t, p.Save("queryView.balancer.scoring.targetLogicalSizePerNode", "200000"))
	require.Equal(t, int64(200000), c.GetBalanceConfig().TargetLogicalSizePerNode)
	require.True(t, takeScanRequest(b))
}

func takeScanRequest(b *DefaultBalancer) bool {
	job, source := b.discovery.next(periodicScan)
	if job == nil {
		return false
	}
	b.discovery.finish(source, job)
	return job.scope.NodeChanged
}
