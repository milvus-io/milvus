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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	checkQueryNodeHealthInterval = 500 * time.Millisecond
)

type LookAsideBalancer struct {
	clientMgr shardClientMgr

	// query node -> workload latest metrics
	metricsMap *typeutil.ConcurrentMap[int64, *internalpb.CostAggregation]

	// query node -> last update metrics ts
	metricsUpdateTs *typeutil.ConcurrentMap[int64, int64]

	// query node -> total nq of requests which already send but response hasn't received
	executingTaskTotalNQ *typeutil.ConcurrentMap[int64, *atomic.Int64]

	unreachableQueryNodes *typeutil.ConcurrentSet[int64]

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewLookAsideBalancer(clientMgr shardClientMgr) *LookAsideBalancer {
	balancer := &LookAsideBalancer{
		clientMgr:             clientMgr,
		metricsMap:            typeutil.NewConcurrentMap[int64, *internalpb.CostAggregation](),
		metricsUpdateTs:       typeutil.NewConcurrentMap[int64, int64](),
		executingTaskTotalNQ:  typeutil.NewConcurrentMap[int64, *atomic.Int64](),
		unreachableQueryNodes: typeutil.NewConcurrentSet[int64](),
		closeCh:               make(chan struct{}),
	}

	balancer.wg.Add(1)
	go balancer.checkQueryNodeHealthLoop()
	return balancer
}

func (b *LookAsideBalancer) Close() {
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.wg.Wait()
	})
}

func (b *LookAsideBalancer) SelectNode(availableNodes []int64, cost int64) (int64, error) {
	targetNode := int64(-1)
	targetScore := float64(math.MaxFloat64)
	for _, node := range availableNodes {
		if b.unreachableQueryNodes.Contain(node) {
			continue
		}

		cost, _ := b.metricsMap.Get(node)
		executingNQ, ok := b.executingTaskTotalNQ.Get(node)
		if !ok {
			executingNQ = atomic.NewInt64(0)
			b.executingTaskTotalNQ.Insert(node, executingNQ)
		}

		score := b.calculateScore(cost, executingNQ.Load())
		if targetNode == -1 || score < targetScore {
			targetScore = score
			targetNode = node
		}
	}

	// update executing task cost
	totalNQ, ok := b.executingTaskTotalNQ.Get(targetNode)
	if !ok {
		totalNQ = atomic.NewInt64(0)
	}
	totalNQ.Add(cost)

	return targetNode, nil
}

// when task canceled, should reduce executing total nq cost
func (b *LookAsideBalancer) CancelWorkload(node int64, nq int64) {
	totalNQ, ok := b.executingTaskTotalNQ.Get(node)
	if ok {
		totalNQ.Sub(nq)
	}
}

// UpdateCostMetrics used for cache some metrics of recent search/query cost
func (b *LookAsideBalancer) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {
	// cache the latest query node cost metrics for updating the score
	b.metricsMap.Insert(node, cost)
	b.metricsUpdateTs.Insert(node, time.Now().UnixMilli())
}

// calculateScore compute the query node's workload score
// https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/suresh
func (b *LookAsideBalancer) calculateScore(cost *internalpb.CostAggregation, executingNQ int64) float64 {
	if cost == nil || cost.ResponseTime == 0 {
		return float64(executingNQ)
	}
	return float64(cost.ResponseTime) - float64(1)/float64(cost.ServiceTime) + math.Pow(float64(1+cost.TotalNQ+executingNQ), 3.0)/float64(cost.ServiceTime)
}

func (b *LookAsideBalancer) checkQueryNodeHealthLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(checkQueryNodeHealthInterval)
	defer ticker.Stop()
	log.Info("Start check query node health loop")
	for {
		select {
		case <-b.closeCh:
			log.Info("check query node health loop exit")
			return

		case <-ticker.C:
			now := time.Now().UnixMilli()
			b.metricsUpdateTs.Range(func(node int64, lastUpdateTs int64) bool {
				if now-lastUpdateTs > checkQueryNodeHealthInterval.Milliseconds() {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()

					checkHealthFailed := func(err error) bool {
						log.Warn("query node check health failed, add it to unreachable nodes list",
							zap.Int64("nodeID", node),
							zap.Error(err))
						b.unreachableQueryNodes.Insert(node)
						return true
					}

					qn, err := b.clientMgr.GetClient(ctx, node)
					if err != nil {
						return checkHealthFailed(err)
					}

					resp, err := qn.GetComponentStates(ctx)
					if err != nil {
						return checkHealthFailed(err)
					}

					if resp.GetState().GetStateCode() != commonpb.StateCode_Healthy {
						return checkHealthFailed(merr.WrapErrNodeOffline(node))
					}

					// check health successfully, update check health ts
					b.metricsUpdateTs.Insert(node, time.Now().Local().UnixMilli())
					b.unreachableQueryNodes.Remove(node)
				}

				return true
			})
		}
	}
}
