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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	// hedgeQuantile is the request latency quantile after which a hedge is sent.
	hedgeQuantile = 0.99
	// replicaMinLatency avoids dividing by zero for sub-millisecond samples.
	replicaMinLatency = 100 * time.Microsecond
	// statsCleanupInterval is how often unused statistics are dropped, and
	// statsIdleTimeout how long they are kept without use.
	statsCleanupInterval = time.Minute
	statsIdleTimeout     = 10 * time.Minute
	// latencyWeightedRetryInterval is the initial pause before retrying a
	// failed attempt on another replica. The retry utility caps subsequent
	// intervals at twice this value.
	latencyWeightedRetryInterval = 10 * time.Millisecond
)

// latencyConfig holds the tunables of the latency_weighted policy.
type latencyConfig struct {
	enabled         bool
	decay           time.Duration
	minShare        float64
	hedgeEnabled    bool
	hedgeMinDelay   time.Duration
	hedgeMinWinRate float64
}

func loadLatencyConfig() latencyConfig {
	p := &paramtable.Get().ProxyCfg
	c := latencyConfig{
		enabled:         p.LatencyWeightedEnabled.GetAsBool(),
		decay:           p.LatencyWeightedDecay.GetAsDurationByParse(),
		minShare:        p.LatencyWeightedMinShare.GetAsFloat(),
		hedgeEnabled:    p.LatencyWeightedHedgeEnabled.GetAsBool(),
		hedgeMinDelay:   p.LatencyWeightedHedgeMinDelay.GetAsDurationByParse(),
		hedgeMinWinRate: p.LatencyWeightedHedgeMinWinRate.GetAsFloat(),
	}
	if c.decay <= 0 {
		c.decay = 10 * time.Second
	}
	if c.minShare < 0 || c.minShare >= 1 {
		c.minShare = 0.001
	}
	if c.hedgeMinDelay < 0 {
		c.hedgeMinDelay = 200 * time.Millisecond
	}
	if c.hedgeMinWinRate <= 0 || c.hedgeMinWinRate > 1 {
		c.hedgeMinWinRate = 0.3
	}
	return c
}

type replicaKey struct {
	key  string
	node int64
}

// keyStats is what is known about a statistics key over all its replicas.
type keyStats struct {
	// latency of its requests, for the hedge delay
	requests latencySamples
	// outcomes of its hedges, for the hedge win rate
	hedges hedgeOutcomes
}

// LatencyWeightedBalancer spreads the attempts of a statistics key (see
// statsKey: a channel and a request class) over its replicas in inverse
// proportion to their recent latency and in-flight attempts, keeping a
// minimum share on every replica as probes. See replicaStats for what the
// latency of a replica is made of and classifyAttempt for how outcomes are
// recorded. It also decides when a hedged attempt is sent (see
// LBPolicyImpl.executeHedged); hedges cover the probes and the requests caught
// on a replica that just failed.
//
// The health check and node registration of the look-aside balancer are
// reused and its unreachable marks honored. With the switch off it behaves as
// look_aside.
type LatencyWeightedBalancer struct {
	*LookAsideBalancer

	mu       sync.Mutex
	replicas map[replicaKey]*replicaStats
	keys     map[string]*keyStats
	rng      *rand.Rand

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// replaceable in tests
	now func() time.Time
}

func NewLatencyWeightedBalancer(clientMgr ShardClientMgr) *LatencyWeightedBalancer {
	return &LatencyWeightedBalancer{
		LookAsideBalancer: NewLookAsideBalancer(clientMgr),
		replicas:          make(map[replicaKey]*replicaStats),
		keys:              make(map[string]*keyStats),
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		closeCh:           make(chan struct{}),
		now:               time.Now,
	}
}

func (b *LatencyWeightedBalancer) Start(ctx context.Context) {
	b.LookAsideBalancer.Start(ctx)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		ticker := time.NewTicker(statsCleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-b.closeCh:
				return
			case <-ticker.C:
				b.cleanup()
			}
		}
	}()
}

func (b *LatencyWeightedBalancer) Close() {
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.wg.Wait()
	})
	b.LookAsideBalancer.Close()
}

// cleanup drops statistics not used for statsIdleTimeout.
func (b *LatencyWeightedBalancer) cleanup() {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, s := range b.replicas {
		if s.inflight == 0 && now.Sub(s.lastUsed) > statsIdleTimeout {
			delete(b.replicas, k)
		}
	}
	for k, s := range b.keys {
		if now.Sub(s.requests.lastUsed()) > statsIdleTimeout && now.Sub(s.hedges.lastUsed()) > statsIdleTimeout {
			delete(b.keys, k)
		}
	}
}

// Enabled reports the switch.
func (b *LatencyWeightedBalancer) Enabled() bool {
	return loadLatencyConfig().enabled
}

// SelectNode serves callers that do not know the channel, and look_aside when
// the switch is off.
func (b *LatencyWeightedBalancer) SelectNode(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
	if !b.Enabled() {
		return b.LookAsideBalancer.SelectNode(ctx, availableNodes, nq)
	}
	return b.SelectNodeForKey(ctx, "", availableNodes)
}

// CancelWorkload releases the look_aside workload when the switch is off. With
// the switch on, selections are released by ObserveAttempt.
func (b *LatencyWeightedBalancer) CancelWorkload(node int64, nq int64) {
	if !b.Enabled() {
		b.LookAsideBalancer.CancelWorkload(node, nq)
	}
}

func (b *LatencyWeightedBalancer) replicaLocked(key string, node int64) *replicaStats {
	k := replicaKey{key, node}
	s, ok := b.replicas[k]
	if !ok {
		s = &replicaStats{}
		b.replicas[k] = s
	}
	return s
}

func (b *LatencyWeightedBalancer) keyLocked(key string) *keyStats {
	s, ok := b.keys[key]
	if !ok {
		s = &keyStats{}
		b.keys[key] = s
	}
	return s
}

// sharesLocked returns the share of attempts of each candidate. A candidate's
// weight is 1 / (latency * (in-flight + 1)); a candidate without statistics
// gets the mean latency of the others, so a new replica starts with an even
// share; every candidate keeps at least minShare.
func (b *LatencyWeightedBalancer) sharesLocked(now time.Time, key string, candidates []int64, cfg latencyConfig) []float64 {
	n := len(candidates)
	shares := make([]float64, n)
	if n == 1 {
		shares[0] = 1
		return shares
	}
	latencies := make([]time.Duration, n)
	known := make([]bool, n)
	inflight := make([]int, n)
	var knownSum time.Duration
	knownCount := 0
	for i, node := range candidates {
		s, ok := b.replicas[replicaKey{key, node}]
		if !ok {
			continue
		}
		inflight[i] = s.inflight
		if latencies[i], known[i] = s.latency(now, cfg.decay); known[i] {
			knownSum += latencies[i]
			knownCount++
		}
	}
	fallback := replicaMinLatency
	if knownCount > 0 {
		fallback = knownSum / time.Duration(knownCount)
	}
	var total float64
	for i := range candidates {
		latency := latencies[i]
		if !known[i] {
			latency = fallback
		}
		if latency < replicaMinLatency {
			latency = replicaMinLatency
		}
		shares[i] = 1 / (latency.Seconds() * float64(inflight[i]+1))
		total += shares[i]
	}
	floor := cfg.minShare
	if floor*float64(n) > 1 {
		floor = 1 / float64(n)
	}
	for i := range shares {
		shares[i] = floor + (1-floor*float64(n))*shares[i]/total
	}
	return shares
}

// SelectNodeForKey selects the node to send an attempt of key to. The
// selection counts as in flight until ObserveAttempt (selected) or
// ReleaseSelection.
func (b *LatencyWeightedBalancer) SelectNodeForKey(ctx context.Context, key string, availableNodes []int64) (int64, error) {
	candidates := make([]int64, 0, len(availableNodes))
	for _, node := range availableNodes {
		m, ok := b.metricsMap.Get(node)
		if !ok {
			m = &CostMetrics{}
			// Heartbeat cleanup removes metrics but retains consecutive failures.
			// Rediscovery must not make an unreachable node healthy again.
			if failures, ok := b.failedHeartBeatCounter.Get(node); ok &&
				failures.Load() >= max(int64(1), paramtable.Get().ProxyCfg.RetryTimesOnHealthCheck.GetAsInt64()) {
				m.unavailable.Store(true)
			}
			m, _ = b.metricsMap.GetOrInsert(node, m)
		}
		if m.unavailable.Load() {
			continue
		}
		candidates = append(candidates, node)
	}
	if len(candidates) == 0 {
		return -1, merr.WrapErrServiceUnavailable("all available nodes are unreachable")
	}
	cfg := loadLatencyConfig()
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	shares := b.sharesLocked(now, key, candidates, cfg)
	target := candidates[len(candidates)-1]
	r := b.rng.Float64()
	for i, share := range shares {
		if r < share {
			target = candidates[i]
			break
		}
		r -= share
	}
	s := b.replicaLocked(key, target)
	s.begin()
	s.lastUsed = now
	return target, nil
}

// ReleaseSelection gives back a selection that was not used for an attempt.
func (b *LatencyWeightedBalancer) ReleaseSelection(key string, node int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.replicas[replicaKey{key, node}]; ok {
		s.end()
	}
}

// ObserveAttempt reports the outcome of one attempt of key on node that
// started at start. ctx is the attempt's context. selected tells whether the
// node was chosen by SelectNodeForKey for this attempt.
func (b *LatencyWeightedBalancer) ObserveAttempt(ctx context.Context, key string, node int64, selected bool, start time.Time, err error) {
	now := b.now()
	outcome, latency := classifyAttempt(ctx, start, now, err)
	b.observeAttempt(ctx, key, node, selected, now, outcome, latency, err)
}

func (b *LatencyWeightedBalancer) observeAttempt(ctx context.Context, key string, node int64, selected bool, now time.Time, outcome attemptOutcome, latency time.Duration, err error) {
	cfg := loadLatencyConfig()
	b.mu.Lock()
	s := b.replicaLocked(key, node)
	if selected {
		s.end()
	}
	s.lastUsed = now
	penalized := false
	switch outcome {
	case attemptSucceeded:
		s.succeed(now, latency, cfg.decay)
	case attemptFailed:
		penalized = s.fail(now, latency, cfg.decay)
	}
	b.mu.Unlock()
	if penalized {
		log.Ctx(ctx).WithRateGroup("proxy.LatencyWeightedBalancer", 1, 60).RatedInfo(5, "replica attempts failed in a row, penalized",
			zap.String("key", key),
			zap.Int64("nodeID", node),
			zap.Duration("penalty", latency),
			zap.Error(err))
	}
}

// ObserveOvertaken records negative feedback while the primary is unfinished
// and another replica has completed the same request. attemptFeedback owns the
// per-attempt deduplication: a later error is not another failure, while a later
// success still supplies its latency and resets consecutive failures. Dispatched
// attempts keep running until completion or cancellation of the outer request.
func (b *LatencyWeightedBalancer) ObserveOvertaken(ctx context.Context, key string, node int64, start time.Time) {
	cfg := loadLatencyConfig()
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.replicaLocked(key, node)
	s.lastUsed = now
	s.fail(now, attemptTimeout(ctx, start, now.Sub(start)), cfg.decay)
}

// ObserveRequest reports how long a successful channel request of key took
// until its first result.
func (b *LatencyWeightedBalancer) ObserveRequest(key string, latency time.Duration) {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keyLocked(key).requests.add(now, latency)
}

// HedgeDelay returns how long to wait for an attempt of key before hedging:
// the p99 of the requests of key (see latencySamples.quantile), at least
// hedge.minDelay. It returns false when hedging is off.
func (b *LatencyWeightedBalancer) HedgeDelay(key string) (time.Duration, bool) {
	cfg := loadLatencyConfig()
	if !cfg.hedgeEnabled {
		return 0, false
	}
	now := b.now()
	b.mu.Lock()
	var samples []time.Duration
	if s, ok := b.keys[key]; ok {
		samples = s.requests.snapshot(now)
	}
	b.mu.Unlock()
	delay := cfg.hedgeMinDelay
	if q, ok := latencyQuantile(samples, hedgeQuantile); ok && q > delay {
		delay = q
	}
	return delay, true
}

// AllowHedge holds back the hedges of key when they do not pay off: when
// fewer than hedge.minWinRate of its recent hedges won (see hedgeOutcomes),
// only one hedge in 1/hedgeProbeRate is sent. A hedge that loses only added
// load: when every replica is slow, as under overall overload, hedges stop
// winning and are cut to the probe rate, while after a replica fault they win
// and keep going. The probes let the win rate recover, and outcomes older than
// the window do not count, so hedging is never switched off for good. With too
// few recent hedges to tell, a hedge is always sent.
func (b *LatencyWeightedBalancer) AllowHedge(key string) bool {
	cfg := loadLatencyConfig()
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.keys[key]
	if !ok {
		return true
	}
	rate, ok := s.hedges.winRate(now)
	if !ok || rate >= cfg.hedgeMinWinRate {
		return true
	}
	return b.rng.Float64() < hedgeProbeRate
}

// ObserveHedge reports whether a hedge of key that was sent won.
func (b *LatencyWeightedBalancer) ObserveHedge(key string, won bool) {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keyLocked(key).hedges.add(now, won)
}
