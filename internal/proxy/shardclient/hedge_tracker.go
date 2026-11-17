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
	"math"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	// defaultHedgeMinWinRate suppresses speculative hedges when recent win rate
	// drops below this threshold, keeping hedgeProbeRate probes active.
	defaultHedgeMinWinRate = 0.3
	// hedgeMinInflight is the minimum concurrent hedge allowance on a Proxy
	// when hedging is enabled, so low-QPS workloads can still hedge.
	hedgeMinInflight = 2
)

type hedgeConfig struct {
	enabled          bool
	minDelay         time.Duration
	maxInflightRatio float64
}

func loadHedgeConfig() hedgeConfig {
	p := &paramtable.Get().ProxyCfg
	c := hedgeConfig{
		enabled:          p.HedgingEnabled.GetAsBool(),
		minDelay:         p.HedgingMinDelay.GetAsDurationByParse(),
		maxInflightRatio: p.HedgingMaxInflightRatio.GetAsFloat(),
	}
	if c.minDelay < 0 {
		c.minDelay = 200 * time.Millisecond
	}
	if c.maxInflightRatio < 0 {
		c.maxInflightRatio = 0
	} else if c.maxInflightRatio > 1 {
		c.maxInflightRatio = 1
	}
	return c
}

// keyStats is what is known about a statistics key over all its replicas.
type keyStats struct {
	// latency of its requests, for the hedge delay
	requests latencySamples
	// outcomes of its hedges, for the hedge win rate
	hedges hedgeOutcomes
}

// hedgeTracker manages per-Proxy hedged read timing, win-rate suppression, and
// concurrent in-flight hedge budget independently of the replica selection policy.
type hedgeTracker struct {
	mu   sync.Mutex
	keys map[string]*keyStats
	rng  *rand.Rand

	inflightPrimaries atomic.Int64
	inflightHedges    atomic.Int64

	now             func() time.Time
	cleanupInterval time.Duration

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newHedgeTracker() *hedgeTracker {
	return &hedgeTracker{
		keys:            make(map[string]*keyStats),
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		now:             time.Now,
		cleanupInterval: statsCleanupInterval,
		closeCh:         make(chan struct{}),
	}
}

func (t *hedgeTracker) Start(ctx context.Context) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(t.cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.closeCh:
				return
			case <-ticker.C:
				t.cleanup()
			}
		}
	}()
}

func (t *hedgeTracker) Close() {
	t.closeOnce.Do(func() {
		close(t.closeCh)
	})
	t.wg.Wait()
}

func (t *hedgeTracker) keyLocked(key string) *keyStats {
	s, ok := t.keys[key]
	if !ok {
		s = &keyStats{}
		t.keys[key] = s
	}
	return s
}

func (t *hedgeTracker) cleanup() {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, s := range t.keys {
		if now.Sub(s.requests.lastUsed()) > statsIdleTimeout && now.Sub(s.hedges.lastUsed()) > statsIdleTimeout {
			delete(t.keys, k)
		}
	}
}

// ObserveRequest reports how long a successful channel request of key took
// until its first result.
func (t *hedgeTracker) ObserveRequest(key string, latency time.Duration) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keyLocked(key).requests.add(now, latency)
}

// HedgeDelay returns how long to wait for an attempt of key before hedging:
// the p99 of the requests of key (see latencySamples.quantile), at least
// hedging.minDelay. It returns false when hedging is off.
func (t *hedgeTracker) HedgeDelay(key string) (time.Duration, bool) {
	cfg := loadHedgeConfig()
	if !cfg.enabled {
		return 0, false
	}
	now := t.now()
	t.mu.Lock()
	var samples []time.Duration
	if s, ok := t.keys[key]; ok {
		samples = s.requests.snapshot(now)
	}
	t.mu.Unlock()
	delay := cfg.minDelay
	if q, ok := latencyQuantile(samples, hedgeQuantile); ok && q > delay {
		delay = q
	}
	return delay, true
}

// AllowHedge holds back the hedges of key when they do not pay off: when
// fewer than defaultHedgeMinWinRate of its recent hedges won, only
// hedgeProbeRate of eligible hedges are sent.
func (t *hedgeTracker) AllowHedge(key string) bool {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.keys[key]
	if !ok {
		return true
	}
	rate, ok := s.hedges.winRate(now)
	if !ok || rate >= defaultHedgeMinWinRate {
		return true
	}
	return t.rng.Float64() < hedgeProbeRate
}

// ObserveHedge reports whether a sent hedge of key won.
func (t *hedgeTracker) ObserveHedge(key string, won bool) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keyLocked(key).hedges.add(now, won)
}

func (t *hedgeTracker) EnterPrimary() func() {
	t.inflightPrimaries.Inc()
	var once sync.Once
	return func() {
		once.Do(func() {
			t.inflightPrimaries.Dec()
		})
	}
}

func (t *hedgeTracker) hedgeInflightLimit(cfg hedgeConfig) int64 {
	if !cfg.enabled || cfg.maxInflightRatio <= 0 {
		return 0
	}
	primaries := t.inflightPrimaries.Load()
	limit := int64(math.Ceil(cfg.maxInflightRatio * float64(primaries)))
	if limit < hedgeMinInflight {
		limit = hedgeMinInflight
	}
	return limit
}

func (t *hedgeTracker) CanAcquireHedge() bool {
	return t.inflightHedges.Load() < t.hedgeInflightLimit(loadHedgeConfig())
}

func (t *hedgeTracker) TryAcquireHedge() (func(), bool) {
	cfg := loadHedgeConfig()
	for {
		limit := t.hedgeInflightLimit(cfg)
		current := t.inflightHedges.Load()
		if current >= limit {
			return nil, false
		}
		if t.inflightHedges.CompareAndSwap(current, current+1) {
			var once sync.Once
			return func() {
				once.Do(func() {
					t.inflightHedges.Dec()
				})
			}, true
		}
	}
}
