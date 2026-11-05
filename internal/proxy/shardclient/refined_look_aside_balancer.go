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
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RefinedCostMetrics keeps the per-node cost metrics of the refined
// look-aside balancer. Besides the raw cost aggregation, the service time
// and response time are EWMA-smoothed so that millisecond-rounding noise and
// single-sample outliers no longer flip the steering decision (fix C). The
// smoothed values are kept in float64 (packed as bits) so that sub-millisecond
// samples (1-4ms) survive the integer-millisecond domain of the wire metrics.
type RefinedCostMetrics struct {
	cost               atomic.Pointer[internalpb.CostAggregation]
	serviceTimeSmooth  atomic.Uint64 // math.Float64bits of the EWMA smoothed service time (ms)
	responseTimeSmooth atomic.Uint64 // math.Float64bits of the EWMA smoothed response time (ms)
	executingNQ        atomic.Int64
	ts                 atomic.Int64
}

// defaultWorkloadMetricSmoothingFactor is the smoothing factor used when the
// configured value is out of the (0,1] range.
const defaultWorkloadMetricSmoothingFactor = 0.2

// refinedMetricsCleanupInterval is how often the balancer sweeps the smoothed
// metrics of nodes that the health loop has evicted from knownNodeInfos.
const refinedMetricsCleanupInterval = time.Minute

// RefinedLookAsideBalancer is the look-aside balancer variant carrying two
// fixes for the sub-millisecond herding issue (see
// docs/design-docs/design_docs/20261019-look-aside-balancer-fix-design.md):
//
//   - B: a 1ms floor on the service time used by the workload term, so that a
//     quantized-to-zero ServiceTime no longer collapses the workload score to
//     0 and the steering loop keeps its termination condition.
//   - C: EWMA smoothing of the service/response time samples, damping the
//     millisecond-rounding noise and outliers that used to mis-trigger
//     steering.
//
// The idx control logic, the metric expiry rule and the steering semantics of
// the original LookAsideBalancer are kept unchanged. The health check loop,
// round-robin index and node reachability state are reused from the embedded
// LookAsideBalancer; only the scoring-related state (smoothed metrics) and
// methods are overridden.
type RefinedLookAsideBalancer struct {
	*LookAsideBalancer

	// refinedMetricsMap keeps the per-node EWMA-smoothed cost metrics; the
	// availability/heartbeat state stays in the embedded balancer's metricsMap.
	refinedMetricsMap     *typeutil.ConcurrentMap[int64, *RefinedCostMetrics]
	metricSmoothingFactor float64

	closeCh         chan struct{}
	closeOnce       sync.Once
	cleanupWg       sync.WaitGroup
	cleanupInterval atomic.Duration
}

func NewRefinedLookAsideBalancer(clientMgr ShardClientMgr) *RefinedLookAsideBalancer {
	factor := paramtable.Get().ProxyCfg.WorkloadMetricSmoothingFactor.GetAsFloat()
	if factor <= 0 || factor > 1 {
		mlog.Warn(context.TODO(), "invalid workload metric smoothing factor, fall back to default",
			mlog.Float64("factor", factor),
			mlog.Float64("default", defaultWorkloadMetricSmoothingFactor))
		factor = defaultWorkloadMetricSmoothingFactor
	}

	balancer := &RefinedLookAsideBalancer{
		LookAsideBalancer:     NewLookAsideBalancer(clientMgr),
		refinedMetricsMap:     typeutil.NewConcurrentMap[int64, *RefinedCostMetrics](),
		metricSmoothingFactor: factor,
		closeCh:               make(chan struct{}),
	}
	balancer.cleanupInterval.Store(refinedMetricsCleanupInterval)
	return balancer
}

// Start starts the embedded balancer's health check loop and the periodic
// sweep of evicted nodes' smoothed metrics.
func (b *RefinedLookAsideBalancer) Start(ctx context.Context) {
	b.LookAsideBalancer.Start(ctx)
	b.cleanupWg.Add(1)
	go b.cleanupRefinedMetricsLoop(ctx)
}

// Close stops the health check loop and the metrics sweep.
func (b *RefinedLookAsideBalancer) Close() {
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.cleanupWg.Wait()
	})
	b.LookAsideBalancer.Close()
}

// cleanupRefinedMetricsLoop periodically drops the smoothed metrics of nodes
// the embedded health loop has evicted. The embedded loop removes offline
// nodes from its own maps and from knownNodeInfos, but it cannot reach
// refinedMetricsMap, so without this sweep the map would grow with historical
// node IDs on QN restarts or scaling.
func (b *RefinedLookAsideBalancer) cleanupRefinedMetricsLoop(ctx context.Context) {
	defer b.cleanupWg.Done()
	ticker := time.NewTicker(b.cleanupInterval.Load())
	defer ticker.Stop()
	for {
		select {
		case <-b.closeCh:
			return
		case <-ticker.C:
			b.cleanupEvictedMetrics()
		}
	}
}

// cleanupEvictedMetrics removes the smoothed metrics of nodes that are no
// longer registered. A node removed here is permanently gone (evicted after
// repeated health-check failures), so any in-flight request on it completes
// and its CancelWorkload becomes a no-op — the same behavior as the original
// balancer, whose metrics entry is removed on eviction too.
func (b *RefinedLookAsideBalancer) cleanupEvictedMetrics() {
	b.refinedMetricsMap.Range(func(node int64, metrics *RefinedCostMetrics) bool {
		if !b.knownNodeInfos.Contain(node) {
			b.refinedMetricsMap.Remove(node)
		}
		return true
	})
}

func (b *RefinedLookAsideBalancer) SelectNode(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
	targetNode := int64(-1)
	defer func() {
		if targetNode != -1 {
			metrics, _ := b.refinedMetricsMap.GetOrInsert(targetNode, &RefinedCostMetrics{})
			metrics.executingNQ.Add(nq)
			// keep an entry in the embedded balancer's metrics map so its
			// health loop can mark this node unavailable even if it never
			// returned cost metrics (e.g. proxy restarted while the QN is down)
			b.metricsMap.GetOrInsert(targetNode, &CostMetrics{})
		}
	}()

	// after assign n request, try to assign the task to a query node which has much less workload
	idx := b.idx.Load()
	if idx%b.checkWorkloadRequestNum != 0 {
		for i := 0; i < len(availableNodes); i++ {
			node := availableNodes[(int(idx)+i)%len(availableNodes)]
			targetMetrics, ok := b.metricsMap.Get(node)
			if !ok || !targetMetrics.unavailable.Load() {
				targetNode = node
				break
			}
		}

		if targetNode == -1 {
			return targetNode, merr.WrapErrServiceUnavailable("all available nodes are unreachable")
		}

		b.idx.Inc()
		return targetNode, nil
	}

	// compute each query node's workload score, select the one with least workload score
	minScore := int64(math.MaxInt64)
	maxScore := int64(0)
	nowTs := time.Now().UnixMilli()
	for i := 0; i < len(availableNodes); i++ {
		node := availableNodes[(int(idx)+i)%len(availableNodes)]
		score := int64(0)
		// availability is tracked by the embedded balancer's health loop
		targetMetrics, baseOK := b.metricsMap.Get(node)
		if baseOK && targetMetrics.unavailable.Load() {
			continue
		}

		metrics, ok := b.refinedMetricsMap.Get(node)
		if ok {
			executingNQ := metrics.executingNQ.Load()
			// for multi-replica cases, when there are no task which waiting in queue,
			// the response time will effect the score, to prevent the score based on a too old metrics
			// we expire the cost metrics if no task in queue.
			if executingNQ != 0 || nowTs-metrics.ts.Load() <= b.metricExpireInterval {
				score = b.calculateScore(node, metrics.cost.Load(), executingNQ,
					b.ms(metrics.serviceTimeSmooth.Load()), b.ms(metrics.responseTimeSmooth.Load()))
			}
		}

		if score < minScore || targetNode == -1 {
			minScore = score
			targetNode = node
		}
		if score > maxScore {
			maxScore = score
		}
	}

	if minScore <= 0 || float64(maxScore-minScore)/float64(minScore) <= b.workloadToleranceFactor {
		// if all query node has nearly same workload, just fall back to round_robin
		b.idx.Inc()
	}

	if targetNode == -1 {
		return targetNode, merr.WrapErrServiceUnavailable("all available nodes are unreachable")
	}

	return targetNode, nil
}

// when task canceled, should reduce executing total nq cost
func (b *RefinedLookAsideBalancer) CancelWorkload(node int64, nq int64) {
	metrics, ok := b.refinedMetricsMap.Get(node)
	if ok {
		metrics.executingNQ.Sub(nq)
	}
}

// UpdateCostMetrics caches the latest query node cost metrics and updates the
// EWMA-smoothed service/response time (fix C). The embedded balancer's raw
// metrics (ts/availability) are kept fresh for the inherited health loop.
func (b *RefinedLookAsideBalancer) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	if cost == nil {
		return
	}
	b.LookAsideBalancer.UpdateCostMetrics(node, cost)
	metrics, ok := b.refinedMetricsMap.Get(node)
	if !ok {
		metrics = &RefinedCostMetrics{}
		b.refinedMetricsMap.Insert(node, metrics)
	}
	nowTs := time.Now().UnixMilli()
	// Seed the smoothed value with the fresh sample when the previous one has
	// expired (or never existed): 0 cannot mark "unseeded" because it is
	// ServiceTime's normal value. Otherwise apply the EWMA.
	if nowTs-metrics.ts.Load() > b.metricExpireInterval {
		metrics.serviceTimeSmooth.Store(math.Float64bits(float64(cost.GetServiceTime())))
		metrics.responseTimeSmooth.Store(math.Float64bits(float64(cost.GetResponseTime())))
	} else {
		metrics.serviceTimeSmooth.Store(math.Float64bits(
			b.ewma(math.Float64frombits(metrics.serviceTimeSmooth.Load()), float64(cost.GetServiceTime()))))
		metrics.responseTimeSmooth.Store(math.Float64bits(
			b.ewma(math.Float64frombits(metrics.responseTimeSmooth.Load()), float64(cost.GetResponseTime()))))
	}
	metrics.cost.Store(cost)
	metrics.ts.Store(nowTs)
}

// ewma computes prev + (sample-prev) * alpha. The value is kept in float64 so
// that sub-millisecond samples (1-4ms) survive the integer-millisecond domain
// of the wire metrics, which would truncate them to 0.
func (b *RefinedLookAsideBalancer) ewma(prev, sample float64) float64 {
	return prev + (sample-prev)*b.metricSmoothingFactor
}

// ms rounds a float64-bit-packed millisecond value to the integer-millisecond
// domain used by calculateScore.
func (b *RefinedLookAsideBalancer) ms(bits uint64) int64 {
	return int64(math.Round(math.Float64frombits(bits)))
}

// minServiceTimeMs is the lower bound applied to the service time used by the
// workload term (fix B). Sub-millisecond requests have their ServiceTime
// truncated to 0 by millisecond rounding, which collapsed the workload score
// to 0 and removed the steering loop's termination condition. Flooring the
// workload service time at 1ms restores the score's response to executingNQ.
const minServiceTimeMs int64 = 1

// calculateScore computes the query node's workload score from the EWMA
// smoothed service/response time.
// https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/suresh
func (b *RefinedLookAsideBalancer) calculateScore(node int64, cost *internalpb.CostAggregation,
	executingNQ int64, serviceTime int64, responseTime int64,
) int64 {
	pow3 := func(n int64) int64 {
		return n * n * n
	}

	// no valid sample (including the only-TotalNQ case): keep the original behavior
	if cost == nil || responseTime == 0 {
		return pow3(executingNQ)
	}

	// executeSpeed keeps the real (smoothed) service time so that rounding
	// error is not amplified
	executeSpeed := responseTime - serviceTime
	if executingNQ < 0 {
		mlog.Warn(context.TODO(), "unexpected executing nq value",
			mlog.Int64("executingNQ", executingNQ))
		return executeSpeed
	}

	if cost.GetTotalNQ() < 0 {
		mlog.Warn(context.TODO(), "unexpected total nq value",
			mlog.Int64("totalNq", cost.GetTotalNQ()))
		return executeSpeed
	}

	// B: quantization floor, applied to the workload term only
	if serviceTime < minServiceTimeMs {
		serviceTime = minServiceTimeMs
	}

	// workload := math.Pow(float64(1+cost.GetTotalNQ()+executingNQ), 3.0) * float64(serviceTime)
	workload := pow3(1+cost.GetTotalNQ()+executingNQ) * serviceTime
	if workload < 0 {
		return math.MaxInt64
	}

	return executeSpeed + workload
}
