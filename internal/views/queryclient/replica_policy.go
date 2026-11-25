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
package queryclient

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	replicaMinLatency = 100 * time.Microsecond
)

// ReplicaSelectionConfig is sampled once per shard request. Changing the live
// configuration never changes which policy owns an outstanding selection.
type ReplicaSelectionConfig struct {
	LatencyWeighted bool
	Decay           time.Duration
	MinShare        float64
	HedgeEnabled    bool
	HedgeMinDelay   time.Duration
}

func (c ReplicaSelectionConfig) normalized() ReplicaSelectionConfig {
	if c.Decay <= 0 {
		c.Decay = 10 * time.Second
	}
	if math.IsNaN(c.MinShare) || c.MinShare < 0 || c.MinShare >= 1 {
		c.MinShare = 0.001
	}
	if c.HedgeMinDelay < 0 {
		c.HedgeMinDelay = 200 * time.Millisecond
	}
	return c
}

type replicaPolicyKey struct {
	channel string
	class   string
}

type replicaPolicyStats struct {
	replicas map[int64]*replicaStats
	requests latencySamples
	hedges   hedgeOutcomes
	lastUsed time.Time
}

type replicaPolicy struct {
	mu      sync.Mutex
	keys    map[replicaPolicyKey]*replicaPolicyStats
	rng     *rand.Rand
	cleaned time.Time
}

func newReplicaPolicy() *replicaPolicy {
	return &replicaPolicy{keys: make(map[replicaPolicyKey]*replicaPolicyStats), rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (p *replicaPolicy) statsLocked(key replicaPolicyKey, now time.Time) *replicaPolicyStats {
	if now.Sub(p.cleaned) > time.Minute {
		for key, stats := range p.keys {
			for id, replica := range stats.replicas {
				if replica.inflight == 0 && now.Sub(replica.lastUsed) > 10*time.Minute {
					delete(stats.replicas, id)
				}
			}
			if now.Sub(stats.lastUsed) <= 10*time.Minute {
				continue
			}
			idle := true
			for _, replica := range stats.replicas {
				idle = idle && replica.inflight == 0
			}
			if idle {
				delete(p.keys, key)
			}
		}
		p.cleaned = now
	}
	stats := p.keys[key]
	if stats == nil {
		stats = &replicaPolicyStats{replicas: make(map[int64]*replicaStats)}
		p.keys[key] = stats
	}
	stats.lastUsed = now
	return stats
}

// selectReplica is called only with eligible candidates from the live directory.
// It reserves no work: begin pairs the selected replica with exactly one attempt.
func (p *replicaPolicy) selectReplica(key replicaPolicyKey, candidates []qviews.Replica, cfg ReplicaSelectionConfig) qviews.Replica {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	stats := p.statsLocked(key, now)
	latencies := make([]time.Duration, len(candidates))
	var sum time.Duration
	known := 0
	for i, candidate := range candidates {
		if replica := stats.replicas[candidate.ReplicaID]; replica != nil {
			if latency, ok := replica.mean(now, cfg.Decay); ok {
				latencies[i] = max(latency, replicaMinLatency)
				sum += latencies[i]
				known++
			}
		}
	}
	fallback := replicaMinLatency
	if known > 0 {
		fallback = sum / time.Duration(known)
	}
	weights := make([]float64, len(candidates))
	var total float64
	for i, candidate := range candidates {
		latency := latencies[i]
		if latency == 0 {
			latency = fallback
		}
		inflight := 0
		if replica := stats.replicas[candidate.ReplicaID]; replica != nil {
			inflight = replica.inflight
			latency = max(latency, replica.currentPenalty(now, cfg.Decay))
		}
		weights[i] = 1 / (latency.Seconds() * float64(inflight+1))
		total += weights[i]
	}
	floor := min(cfg.MinShare, 1/float64(len(candidates)))
	draw := p.rng.Float64()
	for i, weight := range weights {
		draw -= floor + (1-floor*float64(len(candidates)))*weight/total
		if draw < 0 {
			return candidates[i]
		}
	}
	return candidates[len(candidates)-1]
}

// replicaObservation serializes completion and overtaking. One attempt can
// contribute at most one negative observation, regardless of their ordering.
type replicaObservation struct {
	mu       sync.Mutex
	policy   *replicaPolicy
	key      replicaPolicyKey
	replica  int64
	config   ReplicaSelectionConfig
	start    time.Time
	finished bool
	failed   bool
	workers  int
}

func (p *replicaPolicy) begin(key replicaPolicyKey, replicaID int64, cfg ReplicaSelectionConfig) *replicaObservation {
	now := time.Now()
	p.mu.Lock()
	stats := p.statsLocked(key, now)
	if stats.replicas[replicaID] == nil {
		stats.replicas[replicaID] = &replicaStats{}
	}
	stats.replicas[replicaID].begin()
	stats.replicas[replicaID].lastUsed = now
	p.mu.Unlock()
	return &replicaObservation{policy: p, key: key, replica: replicaID, config: cfg, start: now}
}

func (o *replicaObservation) workerStarted(node qviews.WorkNode) func() {
	if o == nil || node.NodeType() != qviews.NodeTypeQueryNode {
		return func() {}
	}
	o.mu.Lock()
	o.workers++
	o.mu.Unlock()
	return func() {
		o.mu.Lock()
		o.workers--
		o.mu.Unlock()
	}
}

func (o *replicaObservation) overtaken(ctx context.Context) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	// Planning shares the primary WAL owner. Its stalls are not evidence
	// against a particular replica's workers.
	if o.finished || o.failed || o.workers == 0 || ctx.Err() != nil {
		return
	}
	o.failed = true
	o.policy.mu.Lock()
	defer o.policy.mu.Unlock()
	now := time.Now()
	stats := o.policy.statsLocked(o.key, now).replicas[o.replica]
	stats.lastUsed = now
	stats.fail(now, replicaAttemptPenalty(ctx, o.start, now), o.config.Decay)
}

func (o *replicaObservation) finish(ctx context.Context, err error, workerFailure, success bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finished = true
	now := time.Now()
	o.policy.mu.Lock()
	defer o.policy.mu.Unlock()
	stats := o.policy.statsLocked(o.key, now).replicas[o.replica]
	stats.lastUsed = now
	stats.end()
	if success {
		stats.succeed(now, now.Sub(o.start), o.config.Decay)
	} else if !o.failed && workerFailure && !errors.Is(ctx.Err(), context.Canceled) &&
		merr.GetErrorType(ToMilvusError(err)) != merr.InputError && !isOverloadErr(err) {
		o.failed = true
		stats.fail(now, replicaAttemptPenalty(ctx, o.start, now), o.config.Decay)
	}
}

// isOverloadErr tells whether err is a replica refusing a request because its
// read queue is too long. It says the replica has more work than it gets
// done, not that it is failing: the request may try another replica once, and
// the refusal does not count against this one.
func isOverloadErr(err error) bool {
	return errors.Is(ToMilvusError(err), merr.ErrServiceTooManyRequests)
}

func replicaAttemptPenalty(ctx context.Context, start, now time.Time) time.Duration {
	penalty := time.Minute
	if deadline, ok := ctx.Deadline(); ok {
		penalty = deadline.Sub(start)
	}
	return min(max(penalty, now.Sub(start)), time.Hour)
}

func (p *replicaPolicy) hedgeDelay(key replicaPolicyKey, cfg ReplicaSelectionConfig) time.Duration {
	p.mu.Lock()
	stats := p.keys[key]
	var samples latencySamples
	if stats != nil {
		samples = stats.requests
		samples.ring = append([]latencySample(nil), samples.ring...)
	}
	p.mu.Unlock()
	delay := cfg.HedgeMinDelay
	if quantile, ok := samples.quantile(time.Now(), 0.99); ok {
		delay = max(delay, quantile)
	}
	return delay
}

func (p *replicaPolicy) allowHedge(key replicaPolicyKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := p.keys[key]
	if stats == nil {
		return true
	}
	rate, enough := stats.hedges.winRate(time.Now())
	return !enough || rate >= defaultHedgeMinWinRate || p.rng.Float64() < hedgeProbeRate
}

func (p *replicaPolicy) completed(key replicaPolicyKey, latency time.Duration, success, hedgeSent, hedgeWon bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	stats := p.statsLocked(key, now)
	if success {
		stats.requests.add(now, latency)
	}
	if hedgeSent {
		stats.hedges.add(now, hedgeWon)
	}
}
