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
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type testClock struct{ t time.Time }

func (c *testClock) now() time.Time                { return c.t }
func (c *testClock) advance(d time.Duration)       { c.t = c.t.Add(d) }
func (c *testClock) ago(d time.Duration) time.Time { return c.t.Add(-d) }

func newTestBalancer(t *testing.T) (*LatencyWeightedBalancer, *testClock) {
	paramtable.Init()
	b := NewLatencyWeightedBalancer(nil)
	clock := &testClock{t: testEpoch}
	b.now = clock.now
	b.rng = rand.New(rand.NewSource(1))
	return b, clock
}

const testKey = "ch|search/Bounded"

// failInARow reports enough failed attempts in a row for them to count: the
// replica gets the penalty of a request without deadline, 60s.
func failInARow(b *LatencyWeightedBalancer, clock *testClock, key string, node int64, err error) {
	for i := 0; i < failuresBeforePenalty; i++ {
		observe(b, clock, key, node, time.Millisecond, err)
	}
}

// observe reports one attempt of key on node that took d.
func observe(b *LatencyWeightedBalancer, clock *testClock, key string, node int64, d time.Duration, err error) {
	b.ObserveAttempt(context.Background(), key, node, false, clock.ago(d), err)
}

func shares(b *LatencyWeightedBalancer, clock *testClock, key string, nodes ...int64) []float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sharesLocked(clock.now(), key, nodes, loadLatencyConfig())
}

func TestLatencyWeightedShares(t *testing.T) {
	cases := []struct {
		name          string
		fast, slow    time.Duration
		wantSlowShare float64
	}{
		{"equal", 10 * time.Millisecond, 10 * time.Millisecond, 0.5},
		{"close", 10 * time.Millisecond, 12 * time.Millisecond, 0.4547},
		{"three times", 10 * time.Millisecond, 30 * time.Millisecond, 0.2505},
		{"slow but working", 10 * time.Millisecond, 3 * time.Second, 0.0043},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, clock := newTestBalancer(t)
			observe(b, clock, testKey, 1, tc.fast, nil)
			observe(b, clock, testKey, 2, tc.slow, nil)
			s := shares(b, clock, testKey, 1, 2)
			assert.InDelta(t, tc.wantSlowShare, s[1], 0.0005)
			assert.InDelta(t, 1, s[0]+s[1], 1e-9)
		})
	}
}

func TestLatencyWeightedUnknownAndSingle(t *testing.T) {
	b, clock := newTestBalancer(t)
	// no statistics: even shares
	assert.InDelta(t, 1.0/3, shares(b, clock, testKey, 1, 2, 3)[2], 1e-9)
	// a new replica gets the mean of the others
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	assert.InDelta(t, 0.5, shares(b, clock, testKey, 1, 2)[1], 1e-9)
	// a single replica gets everything, failing or not
	observe(b, clock, testKey, 3, time.Second, merr.ErrServiceUnavailable)
	assert.Equal(t, []float64{1}, shares(b, clock, testKey, 3))
	node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{3})
	require.NoError(t, err)
	assert.Equal(t, int64(3), node)
}

// In-flight attempts weigh a replica down by their number, not by how long
// they have been running: a heavy request does not push traffic away.
func TestLatencyWeightedInflight(t *testing.T) {
	b, clock := newTestBalancer(t)
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	b.mu.Lock()
	for i := 0; i < 3; i++ {
		b.replicaLocked(testKey, 2).begin()
	}
	b.mu.Unlock()
	assert.InDelta(t, 0.2, shares(b, clock, testKey, 1, 2)[1], 0.001)
	clock.advance(time.Minute)
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	assert.InDelta(t, 0.2, shares(b, clock, testKey, 1, 2)[1], 0.001)
}

// An attempt overtaken by a hedge counts as failed: once is nothing, in a row
// it moves the traffic away, and a replica that answers in between is not
// affected however often it is overtaken.
func TestLatencyWeightedOvertaken(t *testing.T) {
	b, clock := newTestBalancer(t)
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	for i := 1; i < failuresBeforePenalty; i++ {
		b.ObserveOvertaken(context.Background(), testKey, 2, clock.ago(210*time.Millisecond))
		assert.InDelta(t, 0.5, shares(b, clock, testKey, 1, 2)[1], 0.001)
	}
	b.ObserveOvertaken(context.Background(), testKey, 2, clock.ago(210*time.Millisecond))
	assert.Less(t, shares(b, clock, testKey, 1, 2)[1], 0.002)

	b2, clock2 := newTestBalancer(t)
	for i := 0; i < 20; i++ {
		observe(b2, clock2, testKey, 1, 10*time.Millisecond, nil)
		observe(b2, clock2, testKey, 2, 10*time.Millisecond, nil)
		b2.ObserveOvertaken(context.Background(), testKey, 2, clock2.ago(210*time.Millisecond))
	}
	assert.InDelta(t, 0.5, shares(b2, clock2, testKey, 1, 2)[1], 0.001)
}

// Failures in a row move the traffic away, even after a long healthy history;
// after recovery the share comes back as the penalty decays.
func TestLatencyWeightedFailureAndRecovery(t *testing.T) {
	b, clock := newTestBalancer(t)
	for i := 0; i < 1000; i++ {
		observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
		observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	}
	failInARow(b, clock, testKey, 2, merr.ErrServiceUnavailable)
	assert.Less(t, shares(b, clock, testKey, 1, 2)[1], 0.002, "down to about the minimum share")

	// recovered: probes succeed while the 60s penalty decays (10s time constant)
	var last float64
	for i := 0; i < 12; i++ {
		clock.advance(10 * time.Second)
		observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
		observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
		share := shares(b, clock, testKey, 1, 2)[1]
		assert.GreaterOrEqual(t, share, last)
		last = share
	}
	assert.Greater(t, last, 0.45)
}

// A failure on a replica that keeps answering other requests of its class (an
// error during a segment handoff, a heavy request timing out) does not move
// the traffic; failuresBeforePenalty failures in a row do, errors and
// timeouts alike.
func TestLatencyWeightedConsecutiveFailures(t *testing.T) {
	b, clock := newTestBalancer(t)
	for i := 0; i < 100; i++ {
		observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
		observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	}
	// expired in real time: the test clock is not what contexts use
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	timeout := func(node int64) {
		b.ObserveAttempt(expired, testKey, node, false, clock.ago(5*time.Second), context.DeadlineExceeded)
	}
	for i := 0; i < 20; i++ {
		timeout(2)
		observe(b, clock, testKey, 2, time.Millisecond, merr.ErrServiceUnavailable)
		observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	}
	for i := 0; i < failuresBeforePenalty-1; i++ {
		timeout(2)
	}
	assert.InDelta(t, 0.5, shares(b, clock, testKey, 1, 2)[1], 0.01)
	timeout(2)
	assert.Less(t, shares(b, clock, testKey, 1, 2)[1], 0.004, "5s against 10ms")
}

// The minimum share keeps probing a failing replica.
func TestLatencyWeightedSelectionFollowsShares(t *testing.T) {
	b, clock := newTestBalancer(t)
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	failInARow(b, clock, testKey, 2, merr.ErrServiceUnavailable)
	picked := map[int64]int{}
	for i := 0; i < 20000; i++ {
		node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{1, 2})
		require.NoError(t, err)
		picked[node]++
		b.ReleaseSelection(testKey, node)
	}
	assert.Greater(t, picked[2], 5)
	assert.Less(t, picked[2], 80)
}

// Statistics are per class, and an error every replica returns within a class
// leaves the shares of the class unchanged, while the same error from one
// replica only moves the traffic away from it. This is how a time tick lag is
// handled without telling its causes apart.
func TestLatencyWeightedClasses(t *testing.T) {
	b, clock := newTestBalancer(t)
	strong, bounded := "ch|search/Strong", "ch|search/Bounded"
	lag := merr.Error(merr.Status(errors.Wrap(merr.ErrChannelTSafeStalled, "lag(3s) max(3s)")))
	for _, key := range []string{strong, bounded} {
		for _, node := range []int64{1, 2} {
			observe(b, clock, key, node, 10*time.Millisecond, nil)
		}
	}
	// the WAL lags: Strong fails on every replica
	failInARow(b, clock, strong, 1, lag)
	failInARow(b, clock, strong, 2, lag)
	assert.InDelta(t, 0.5, shares(b, clock, strong, 1, 2)[1], 0.001)
	assert.InDelta(t, 0.5, shares(b, clock, bounded, 1, 2)[1], 0.001, "Bounded is not touched")

	// one replica's consumer is stuck: only it lags
	b2, clock2 := newTestBalancer(t)
	for _, node := range []int64{1, 2} {
		observe(b2, clock2, strong, node, 10*time.Millisecond, nil)
	}
	failInARow(b2, clock2, strong, 2, lag)
	assert.Less(t, shares(b2, clock2, strong, 1, 2)[1], 0.002)
}

func TestLatencyWeightedIgnoredOutcomes(t *testing.T) {
	b, clock := newTestBalancer(t)
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	observe(b, clock, testKey, 2, 10*time.Millisecond, nil)
	observe(b, clock, testKey, 2, time.Millisecond, merr.WrapErrParameterInvalidMsg("bad"))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	b.ObserveAttempt(canceled, testKey, 2, false, clock.ago(time.Millisecond), context.Canceled)
	assert.InDelta(t, 0.5, shares(b, clock, testKey, 1, 2)[1], 0.001)
}

func TestLatencyWeightedSelectionRelease(t *testing.T) {
	b, clock := newTestBalancer(t)
	inflight := func(node int64) int {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.replicas[replicaKey{testKey, node}].inflight
	}
	node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{7})
	require.NoError(t, err)
	assert.Equal(t, 1, inflight(node))
	// a result of an attempt the balancer did not select releases nothing
	b.ObserveAttempt(context.Background(), testKey, node, false, clock.now(), nil)
	assert.Equal(t, 1, inflight(node))
	b.ObserveAttempt(context.Background(), testKey, node, true, clock.now(), nil)
	assert.Equal(t, 0, inflight(node))
	b.ReleaseSelection(testKey, node)
	b.ReleaseSelection("unknown", node)
	assert.Equal(t, 0, inflight(node))
}

func TestLatencyWeightedUnreachable(t *testing.T) {
	b, _ := newTestBalancer(t)
	b.metricsMap.Insert(1, &CostMetrics{})
	m, _ := b.metricsMap.Get(1)
	m.unavailable.Store(true)
	for i := 0; i < 50; i++ {
		node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{1, 2})
		require.NoError(t, err)
		assert.Equal(t, int64(2), node)
	}
	_, err := b.SelectNodeForKey(context.Background(), testKey, []int64{1})
	assert.Error(t, err)
}

func TestLatencyWeightedHedgeDelay(t *testing.T) {
	b, clock := newTestBalancer(t)
	d, ok := b.HedgeDelay(testKey)
	require.True(t, ok)
	assert.Equal(t, 200*time.Millisecond, d, "the minimum delay without requests")
	// a few requests are enough: the p99 of a handful is their maximum
	for i := 0; i < 5; i++ {
		b.ObserveRequest(testKey, 300*time.Millisecond)
	}
	d, _ = b.HedgeDelay(testKey)
	assert.Equal(t, 300*time.Millisecond, d)
	// never below the minimum delay
	b.ObserveRequest("fast", time.Millisecond)
	d, _ = b.HedgeDelay("fast")
	assert.Equal(t, 200*time.Millisecond, d)
	// attempts do not feed the hedge delay, requests do
	observe(b, clock, "other", 1, 5*time.Second, nil)
	d, _ = b.HedgeDelay("other")
	assert.Equal(t, 200*time.Millisecond, d)

	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.LatencyWeightedHedgeEnabled.Key, "false")
	defer pt.Reset(pt.ProxyCfg.LatencyWeightedHedgeEnabled.Key)
	_, ok = b.HedgeDelay(testKey)
	assert.False(t, ok)
}

// Hedges are cut to the probe rate when too few of the recent ones won.
func TestLatencyWeightedAllowHedge(t *testing.T) {
	b, clock := newTestBalancer(t)
	assert.True(t, b.AllowHedge(testKey), "no hedge seen")
	allowed := func(key string) int {
		n := 0
		for i := 0; i < 4000; i++ {
			if b.AllowHedge(key) {
				n++
			}
		}
		return n
	}
	// fewer than ten hedges: always
	for i := 0; i < 9; i++ {
		b.ObserveHedge(testKey, false)
	}
	assert.Equal(t, 4000, allowed(testKey))
	// ten losses: the probe rate
	b.ObserveHedge(testKey, false)
	assert.InDelta(t, 4000*hedgeProbeRate, allowed(testKey), 80)
	// 4 of 14 won: still below 30%
	for i := 0; i < 4; i++ {
		b.ObserveHedge(testKey, true)
	}
	assert.InDelta(t, 4000*hedgeProbeRate, allowed(testKey), 80)
	// 5 of 15 won: always
	b.ObserveHedge(testKey, true)
	assert.Equal(t, 4000, allowed(testKey))
	// outcomes older than the hedge window do not hold hedges back
	for i := 0; i < 100; i++ {
		b.ObserveHedge("lost", false)
	}
	assert.Less(t, allowed("lost"), 600)
	clock.advance(hedgeWindow + time.Second)
	assert.Equal(t, 4000, allowed("lost"))
	// per key
	assert.True(t, b.AllowHedge("other"))

	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.LatencyWeightedHedgeMinWinRate.Key, "0.1")
	defer pt.Reset(pt.ProxyCfg.LatencyWeightedHedgeMinWinRate.Key)
	for i := 0; i < 9; i++ {
		b.ObserveHedge("rare", false)
	}
	b.ObserveHedge("rare", true)
	assert.Equal(t, 4000, allowed("rare"), "1 of 10 meets a 10% minimum")
}

func TestLatencyWeightedSwitch(t *testing.T) {
	b, _ := newTestBalancer(t)
	assert.True(t, b.Enabled())
	assert.NotNil(t, latencyWeighted(b))
	assert.Nil(t, latencyWeighted(NewRoundRobinBalancer()))
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.LatencyWeightedEnabled.Key, "false")
	defer pt.Reset(pt.ProxyCfg.LatencyWeightedEnabled.Key)
	assert.False(t, b.Enabled())
	assert.Nil(t, latencyWeighted(b))
	// behaves as look_aside
	node, err := b.SelectNode(context.Background(), []int64{1, 2}, 1)
	require.NoError(t, err)
	b.CancelWorkload(node, 1)
}

func TestLatencyWeightedCleanup(t *testing.T) {
	b, clock := newTestBalancer(t)
	observe(b, clock, testKey, 1, 10*time.Millisecond, nil)
	b.ObserveRequest(testKey, 10*time.Millisecond)
	b.ObserveHedge("hedged", true)
	count := func() (int, int) {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.replicas), len(b.keys)
	}
	clock.advance(statsIdleTimeout / 2)
	b.cleanup()
	r, k := count()
	assert.Equal(t, 1, r)
	assert.Equal(t, 2, k)
	clock.advance(statsIdleTimeout)
	b.cleanup()
	r, k = count()
	assert.Equal(t, 0, r)
	assert.Equal(t, 0, k)
}

func TestLatencyWeightedConfigDefaults(t *testing.T) {
	paramtable.Init()
	assert.Equal(t, latencyConfig{
		enabled:         true,
		decay:           10 * time.Second,
		minShare:        0.001,
		hedgeEnabled:    true,
		hedgeMinDelay:   200 * time.Millisecond,
		hedgeMinWinRate: 0.3,
	}, loadLatencyConfig())
}

func TestStatsKey(t *testing.T) {
	assert.Equal(t, "ch", statsKey(ChannelWorkload{Channel: "ch"}))
	assert.Equal(t, "ch|search/Bounded", statsKey(ChannelWorkload{Channel: "ch", StatsClass: "search/Bounded"}))
}

// Use the real heartbeat transitions; do not seed metricsMap in the test.
func TestLatencyWeightedHeartbeatLifecycle(t *testing.T) {
	b, _ := newTestBalancer(t)
	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.RetryTimesOnHealthCheck.Key, "2")
	pt.Save(pt.CommonCfg.SessionTTL.Key, "1")
	pt.Save(pt.ProxyCfg.HealthCheckTimeout.Key, "100")
	t.Cleanup(func() {
		pt.Reset(pt.ProxyCfg.RetryTimesOnHealthCheck.Key)
		pt.Reset(pt.CommonCfg.SessionTTL.Key)
		pt.Reset(pt.ProxyCfg.HealthCheckTimeout.Key)
	})
	ctx := context.Background()
	nodes := []NodeInfo{{NodeID: 1, Serviceable: true}}
	selectNode := func() error {
		b.RegisterNodeInfo(nodes)
		node, err := b.SelectNodeForKey(ctx, testKey, []int64{1})
		if err == nil {
			b.ReleaseSelection(testKey, node)
		}
		return err
	}
	require.NoError(t, selectNode())
	require.True(t, b.metricsMap.Contain(1), "selection must register heartbeat health state")
	b.trySetQueryNodeUnReachable(1, merr.ErrServiceUnavailable)
	require.NoError(t, selectNode(), "one failure is below the configured threshold")
	b.trySetQueryNodeUnReachable(1, merr.ErrServiceUnavailable)
	require.ErrorIs(t, selectNode(), merr.ErrServiceUnavailable)
	for i := 2; i < 10; i++ {
		b.trySetQueryNodeUnReachable(1, merr.ErrServiceUnavailable)
	}
	require.False(t, b.metricsMap.Contain(1), "heartbeat cleanup removes the record")
	require.False(t, b.knownNodeInfos.Contain(1))
	require.ErrorIs(t, selectNode(), merr.ErrServiceUnavailable, "rediscovery must preserve unreachable state")
	require.True(t, b.knownNodeInfos.Contain(1), "rediscovery must resume heartbeats")
	b.trySetQueryNodeReachable(1)
	require.NoError(t, selectNode(), "healthy heartbeat restores eligibility")
	b.trySetQueryNodeUnReachable(1, merr.ErrServiceUnavailable)
	b.trySetQueryNodeUnReachable(1, merr.ErrServiceUnavailable)
	require.ErrorIs(t, selectNode(), merr.ErrServiceUnavailable)
	b.UpdateCostMetrics(1, &internalpb.CostAggregation{})
	require.NoError(t, selectNode(), "successful request also restores eligibility")
}

// A healthy heartbeat can finish after cleanup removed the cost record.
func TestLatencyWeightedRediscoveryAfterHealthyHeartbeat(t *testing.T) {
	for _, threshold := range []string{"0", "2"} {
		t.Run(threshold, func(t *testing.T) {
			b, _ := newTestBalancer(t)
			pt := paramtable.Get()
			pt.Save(pt.ProxyCfg.RetryTimesOnHealthCheck.Key, threshold)
			pt.Save(pt.CommonCfg.SessionTTL.Key, "1")
			pt.Save(pt.ProxyCfg.HealthCheckTimeout.Key, "100")
			t.Cleanup(func() {
				pt.Reset(pt.ProxyCfg.RetryTimesOnHealthCheck.Key)
				pt.Reset(pt.CommonCfg.SessionTTL.Key)
				pt.Reset(pt.ProxyCfg.HealthCheckTimeout.Key)
			})
			b.RegisterNodeInfo([]NodeInfo{{NodeID: 1, Serviceable: true}})
			for i := 0; i < 10; i++ {
				b.trySetQueryNodeUnReachable(1, merr.ErrServiceUnavailable)
			}
			require.False(t, b.metricsMap.Contain(1))
			b.trySetQueryNodeReachable(1)
			b.RegisterNodeInfo([]NodeInfo{{NodeID: 1, Serviceable: true}})
			node, err := b.SelectNodeForKey(context.Background(), testKey, []int64{1})
			require.NoError(t, err, "zero failures must not mark a recovered node unreachable")
			b.ReleaseSelection(testKey, node)
		})
	}
}
