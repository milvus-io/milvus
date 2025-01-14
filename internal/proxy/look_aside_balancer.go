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

package proxy

import (
	"context"
	"math"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type CostMetrics struct {
	cost        atomic.Pointer[internalpb.CostAggregation]
	executingNQ atomic.Int64
	ts          atomic.Int64
	unavailable atomic.Bool
}

type LookAsideBalancer struct {
	clientMgr shardClientMgr

	knownNodeInfos *typeutil.ConcurrentMap[int64, nodeInfo]
	metricsMap     *typeutil.ConcurrentMap[int64, *CostMetrics]
	// query node id -> number of consecutive heartbeat failures
	failedHeartBeatCounter *typeutil.ConcurrentMap[int64, *atomic.Int64]

	// idx for round_robin
	idx atomic.Int64

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// param for replica selection
	metricExpireInterval    int64
	checkWorkloadRequestNum int64
	workloadToleranceFactor float64
}

func NewLookAsideBalancer(clientMgr shardClientMgr) *LookAsideBalancer {
	balancer := &LookAsideBalancer{
		clientMgr:              clientMgr,
		knownNodeInfos:         typeutil.NewConcurrentMap[int64, nodeInfo](),
		metricsMap:             typeutil.NewConcurrentMap[int64, *CostMetrics](),
		failedHeartBeatCounter: typeutil.NewConcurrentMap[int64, *atomic.Int64](),
		closeCh:                make(chan struct{}),
	}

	balancer.metricExpireInterval = Params.ProxyCfg.CostMetricsExpireTime.GetAsInt64()
	balancer.checkWorkloadRequestNum = Params.ProxyCfg.CheckWorkloadRequestNum.GetAsInt64()
	balancer.workloadToleranceFactor = Params.ProxyCfg.WorkloadToleranceFactor.GetAsFloat()

	return balancer
}

func (b *LookAsideBalancer) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.checkQueryNodeHealthLoop(ctx)
}

func (b *LookAsideBalancer) Close() {
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.wg.Wait()
	})
}

func (b *LookAsideBalancer) RegisterNodeInfo(nodeInfos []nodeInfo) {
	for _, node := range nodeInfos {
		b.knownNodeInfos.Insert(node.nodeID, node)
	}
}

func (b *LookAsideBalancer) SelectNode(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
	targetNode := int64(-1)
	defer func() {
		if targetNode != -1 {
			metrics, _ := b.metricsMap.GetOrInsert(targetNode, &CostMetrics{})
			metrics.executingNQ.Add(nq)
		}
	}()

	// after assign n request, try to assign the task to a query node which has much less workload
	idx := b.idx.Load()
	if idx%b.checkWorkloadRequestNum != 0 {
		for i := 0; i < len(availableNodes); i++ {
			targetNode = availableNodes[int(idx)%len(availableNodes)]
			targetMetrics, ok := b.metricsMap.Get(targetNode)
			if !ok || !targetMetrics.unavailable.Load() {
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
		metrics, ok := b.metricsMap.Get(node)
		if ok {
			if metrics.unavailable.Load() {
				continue
			}

			executingNQ := metrics.executingNQ.Load()
			// for multi-replica cases, when there are no task which waiting in queue,
			// the response time will effect the score, to prevent the score based on a too old metrics
			// we expire the cost metrics if no task in queue.
			if executingNQ != 0 || nowTs-metrics.ts.Load() <= b.metricExpireInterval {
				score = b.calculateScore(node, metrics.cost.Load(), executingNQ)
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

	if float64(maxScore-minScore)/float64(minScore) <= b.workloadToleranceFactor {
		// if all query node has nearly same workload, just fall back to round_robin
		b.idx.Inc()
	}

	if targetNode == -1 {
		return targetNode, merr.WrapErrServiceUnavailable("all available nodes are unreachable")
	}

	return targetNode, nil
}

// when task canceled, should reduce executing total nq cost
func (b *LookAsideBalancer) CancelWorkload(node int64, nq int64) {
	metrics, ok := b.metricsMap.Get(node)
	if ok {
		metrics.executingNQ.Sub(nq)
	}
}

// UpdateCostMetrics used for cache some metrics of recent search/query cost
func (b *LookAsideBalancer) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	// cache the latest query node cost metrics for updating the score
	if cost != nil {
		metrics, ok := b.metricsMap.Get(node)
		if !ok {
			metrics = &CostMetrics{}
			b.metricsMap.Insert(node, metrics)
		}
		metrics.cost.Store(cost)
		metrics.ts.Store(time.Now().UnixMilli())
		metrics.unavailable.CompareAndSwap(true, false)
	}
}

// calculateScore compute the query node's workload score
// https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/suresh
func (b *LookAsideBalancer) calculateScore(node int64, cost *internalpb.CostAggregation, executingNQ int64) int64 {
	pow3 := func(n int64) int64 {
		return n * n * n
	}

	if cost == nil || cost.GetResponseTime() == 0 {
		return pow3(executingNQ)
	}

	executeSpeed := cost.ResponseTime - cost.ServiceTime
	if executingNQ < 0 {
		log.Warn("unexpected executing nq value",
			zap.Int64("executingNQ", executingNQ))
		return executeSpeed
	}

	if cost.GetTotalNQ() < 0 {
		log.Warn("unexpected total nq value",
			zap.Int64("totalNq", cost.GetTotalNQ()))
		return executeSpeed
	}

	// workload := math.Pow(float64(1+cost.GetTotalNQ()+executingNQ), 3.0) * float64(cost.ServiceTime)
	workload := pow3(1+cost.GetTotalNQ()+executingNQ) * cost.ServiceTime
	if workload < 0 {
		return math.MaxInt64
	}

	return executeSpeed + workload
}

func (b *LookAsideBalancer) checkQueryNodeHealthLoop(ctx context.Context) {
	log := log.Ctx(ctx).WithRateGroup("proxy.LookAsideBalancer", 1, 60)
	defer b.wg.Done()

	checkHealthInterval := Params.ProxyCfg.CheckQueryNodeHealthInterval.GetAsDuration(time.Millisecond)
	ticker := time.NewTicker(checkHealthInterval)
	defer ticker.Stop()
	log.Info("Start check query node health loop")
	pool := conc.NewDefaultPool[any]()
	for {
		select {
		case <-b.closeCh:
			log.Info("check query node health loop exit")
			return

		case <-ticker.C:
			var futures []*conc.Future[any]
			now := time.Now()
			b.knownNodeInfos.Range(func(node int64, info nodeInfo) bool {
				futures = append(futures, pool.Submit(func() (any, error) {
					metrics, ok := b.metricsMap.Get(node)
					if !ok || now.UnixMilli()-metrics.ts.Load() > checkHealthInterval.Milliseconds() {
						checkTimeout := Params.ProxyCfg.HealthCheckTimeout.GetAsDuration(time.Millisecond)
						ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
						defer cancel()

						if node == -1 {
							panic("let it panic")
						}

						qn, err := b.clientMgr.GetClient(ctx, info)
						if err != nil {
							// get client from clientMgr failed, which means this qn isn't a shard leader anymore, skip it's health check
							b.trySetQueryNodeUnReachable(node, err)
							log.RatedInfo(10, "get client failed", zap.Int64("node", node), zap.Error(err))
							return struct{}{}, nil
						}

						resp, err := qn.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
						if err != nil {
							b.trySetQueryNodeUnReachable(node, err)
							log.RatedWarn(10, "get component status failed, set node unreachable", zap.Int64("node", node), zap.Error(err))
							return struct{}{}, nil
						}

						if resp.GetState().GetStateCode() != commonpb.StateCode_Healthy {
							b.trySetQueryNodeUnReachable(node, merr.ErrServiceUnavailable)
							log.RatedWarn(10, "component status unhealthy, set node unreachable", zap.Int64("node", node), zap.Error(err))

							return struct{}{}, nil
						}
					}

					// check health successfully, try set query node reachable
					b.trySetQueryNodeReachable(node)
					return struct{}{}, nil
				}))

				return true
			})
			conc.AwaitAll(futures...)
		}
	}
}

func (b *LookAsideBalancer) trySetQueryNodeUnReachable(node int64, err error) {
	failures, ok := b.failedHeartBeatCounter.Get(node)
	if !ok {
		failures = atomic.NewInt64(0)
	}
	failures.Inc()
	b.failedHeartBeatCounter.Insert(node, failures)

	log.Info("get component status failed",
		zap.Int64("node", node),
		zap.Int64("times", failures.Load()),
		zap.Error(err))

	if failures.Load() < Params.ProxyCfg.RetryTimesOnHealthCheck.GetAsInt64() {
		return
	}

	// if the total time of consecutive heartbeat failures reach the session.ttl, remove the offline query node
	limit := Params.CommonCfg.SessionTTL.GetAsDuration(time.Second).Seconds() /
		Params.ProxyCfg.HealthCheckTimeout.GetAsDuration(time.Millisecond).Seconds()
	if failures.Load() > Params.ProxyCfg.RetryTimesOnHealthCheck.GetAsInt64() && float64(failures.Load()) >= limit {
		log.Info("the heartbeat failures has reach it's upper limit, remove the query node",
			zap.Int64("nodeID", node))
		// stop the heartbeat
		b.metricsMap.Remove(node)
		b.knownNodeInfos.Remove(node)
		return
	}

	metrics, ok := b.metricsMap.Get(node)
	if ok {
		metrics.unavailable.Store(true)
	}
}

func (b *LookAsideBalancer) trySetQueryNodeReachable(node int64) {
	// once heartbeat succeed, clear failed counter
	failures, ok := b.failedHeartBeatCounter.Get(node)
	if ok {
		failures.Store(0)
	}

	metrics, ok := b.metricsMap.Get(node)
	if !ok || metrics.unavailable.CompareAndSwap(true, false) {
		log.Info("component recuperated, set node reachable", zap.Int64("node", node))
	}
}
