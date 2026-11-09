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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type RefinedLookAsideBalancerSuite struct {
	suite.Suite

	clientMgr *MockShardClientManager
	balancer  *RefinedLookAsideBalancer
}

func (suite *RefinedLookAsideBalancerSuite) SetupTest() {
	paramtable.Init()
	suite.clientMgr = NewMockShardClientManager(suite.T())
	suite.balancer = NewRefinedLookAsideBalancer(suite.clientMgr)
	suite.balancer.Start(context.Background())

	qn := mocks.NewMockQueryNodeClient(suite.T())
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Maybe()
}

func (suite *RefinedLookAsideBalancerSuite) TearDownTest() {
	suite.balancer.Close()
}

func (suite *RefinedLookAsideBalancerSuite) TestUpdateMetrics() {
	costMetrics := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      1,
	}

	suite.balancer.UpdateCostMetrics(1, costMetrics)

	// raw metrics are mirrored to the embedded balancer so the inherited
	// health check loop keeps fresh ts/availability
	raw, ok := suite.balancer.metricsMap.Get(1)
	suite.True(ok)
	suite.True(time.Now().UnixMilli()-raw.ts.Load() <= 5)

	metrics, ok := suite.balancer.refinedMetricsMap.Get(1)
	suite.True(ok)
	suite.Equal(costMetrics, metrics.cost.Load())
	suite.True(time.Now().UnixMilli()-metrics.ts.Load() <= 5)
}

func (suite *RefinedLookAsideBalancerSuite) TestUpdateMetricsEwma() {
	// EWMA with the default smoothing factor 0.2. The first sample seeds the
	// smoothed value, keeping sub-millisecond precision instead of truncating
	// it to 0.
	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 100, ServiceTime: 100})
	metrics, ok := suite.balancer.refinedMetricsMap.Get(1)
	suite.True(ok)
	suite.Equal(100.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()))
	suite.Equal(100.0, math.Float64frombits(metrics.responseTimeSmooth.Load()))

	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 100, ServiceTime: 100})
	suite.InDelta(100.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()), 1e-6)
	suite.InDelta(100.0, math.Float64frombits(metrics.responseTimeSmooth.Load()), 1e-6)

	// a single low sample must not collapse the smoothed value
	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 5, ServiceTime: 5})
	suite.InDelta(81.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()), 1e-6)
	suite.InDelta(81.0, math.Float64frombits(metrics.responseTimeSmooth.Load()), 1e-6)

	// a single outlier sample must not dominate the smoothed value
	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 100, ServiceTime: 100})
	suite.InDelta(84.8, math.Float64frombits(metrics.serviceTimeSmooth.Load()), 1e-6)
	suite.InDelta(84.8, math.Float64frombits(metrics.responseTimeSmooth.Load()), 1e-6)

	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 5, ServiceTime: 5})
	suite.InDelta(68.84, math.Float64frombits(metrics.serviceTimeSmooth.Load()), 1e-6)
	suite.InDelta(68.84, math.Float64frombits(metrics.responseTimeSmooth.Load()), 1e-6)
}

func (suite *RefinedLookAsideBalancerSuite) TestEwma() {
	// prev + (sample - prev) * alpha, kept in float64
	suite.InDelta(0.2, suite.balancer.ewma(0, 1), 1e-6)
	suite.InDelta(20.0, suite.balancer.ewma(0, 100), 1e-6)
	suite.InDelta(36.0, suite.balancer.ewma(20, 100), 1e-6)
	suite.InDelta(29.8, suite.balancer.ewma(36, 5), 1e-6)
}

func (suite *RefinedLookAsideBalancerSuite) TestUpdateMetricsSeedWhenExpired() {
	// A fresh sample after an expired (or never-existing) one seeds the
	// smoothed value directly instead of blending with a stale value.
	metrics, _ := suite.balancer.refinedMetricsMap.GetOrInsert(1, &RefinedCostMetrics{})
	metrics.ts.Store(time.Now().UnixMilli() - 10000)
	metrics.serviceTimeSmooth.Store(math.Float64bits(100))
	metrics.responseTimeSmooth.Store(math.Float64bits(100))

	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 1, ServiceTime: 1})
	suite.Equal(1.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()))
	suite.Equal(1.0, math.Float64frombits(metrics.responseTimeSmooth.Load()))
}

func (suite *RefinedLookAsideBalancerSuite) TestUpdateMetricsZeroSampleOutlierDamped() {
	// 0 is ServiceTime's normal value, so it cannot be used as the "unseeded"
	// marker: after a run of 0ms samples, a 20ms outlier must still be damped
	// by the EWMA (to 4 with alpha 0.2) instead of seeding straight to 20.
	metrics, _ := suite.balancer.refinedMetricsMap.GetOrInsert(1, &RefinedCostMetrics{})
	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 0, ServiceTime: 0})
	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 0, ServiceTime: 0})
	suite.Equal(0.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()))

	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{ResponseTime: 20, ServiceTime: 20})
	suite.InDelta(4.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()), 1e-6)
	suite.InDelta(4.0, math.Float64frombits(metrics.responseTimeSmooth.Load()), 1e-6)
}

func (suite *RefinedLookAsideBalancerSuite) TestInvalidSmoothingFactorFallback() {
	params := paramtable.Get()

	params.Save(params.ProxyCfg.WorkloadMetricSmoothingFactor.Key, "0")
	balancer := NewRefinedLookAsideBalancer(suite.clientMgr)
	suite.Equal(defaultWorkloadMetricSmoothingFactor, balancer.metricSmoothingFactor)
	balancer.Close()

	params.Save(params.ProxyCfg.WorkloadMetricSmoothingFactor.Key, "2")
	balancer = NewRefinedLookAsideBalancer(suite.clientMgr)
	suite.Equal(defaultWorkloadMetricSmoothingFactor, balancer.metricSmoothingFactor)
	balancer.Close()

	params.Save(params.ProxyCfg.WorkloadMetricSmoothingFactor.Key, "0.5")
	balancer = NewRefinedLookAsideBalancer(suite.clientMgr)
	suite.Equal(0.5, balancer.metricSmoothingFactor)
	balancer.Close()

	params.Reset(params.ProxyCfg.WorkloadMetricSmoothingFactor.Key)
}

func (suite *RefinedLookAsideBalancerSuite) TestCalculateScore() {
	costMetrics1 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      1,
	}

	costMetrics2 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  2,
		TotalNQ:      1,
	}

	costMetrics3 := &internalpb.CostAggregation{
		ResponseTime: 10,
		ServiceTime:  1,
		TotalNQ:      1,
	}

	costMetrics4 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      0,
	}

	// ServiceTime >= 1 keeps the original score, the floor only acts on zero service time
	score1 := suite.balancer.calculateScore(-1, costMetrics1, 0, 1, 5)
	score2 := suite.balancer.calculateScore(-1, costMetrics2, 0, 2, 5)
	score3 := suite.balancer.calculateScore(-1, costMetrics3, 0, 1, 10)
	score4 := suite.balancer.calculateScore(-1, costMetrics4, 0, 1, 5)
	suite.Equal(int64(12), score1)
	suite.Equal(int64(19), score2)
	suite.Equal(int64(17), score3)
	suite.Equal(int64(5), score4)

	score5 := suite.balancer.calculateScore(-1, costMetrics1, 5, 1, 5)
	score6 := suite.balancer.calculateScore(-1, costMetrics2, 5, 2, 5)
	score7 := suite.balancer.calculateScore(-1, costMetrics3, 5, 1, 10)
	score8 := suite.balancer.calculateScore(-1, costMetrics4, 5, 1, 5)
	suite.Equal(int64(347), score5)
	suite.Equal(int64(689), score6)
	suite.Equal(int64(352), score7)
	suite.Equal(int64(220), score8)

	// test score overflow
	costMetrics5 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      math.MaxInt64,
	}

	score9 := suite.balancer.calculateScore(-1, costMetrics5, math.MaxInt64, 1, 5)
	suite.Equal(int64(math.MaxInt64), score9)

	// test unexpected negative nq value
	costMetrics6 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      -1,
	}
	score10 := suite.balancer.calculateScore(-1, costMetrics6, math.MaxInt64, 1, 5)
	suite.Equal(int64(4), score10)
	costMetrics7 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      1,
	}
	score11 := suite.balancer.calculateScore(-1, costMetrics7, -1, 1, 5)
	suite.Equal(int64(4), score11)

	// no valid sample: keep the original early-return behavior
	suite.Equal(int64(0), suite.balancer.calculateScore(-1, nil, 0, 0, 0))
	suite.Equal(int64(8), suite.balancer.calculateScore(-1, nil, 2, 0, 0))
	suite.Equal(int64(0), suite.balancer.calculateScore(-1, &internalpb.CostAggregation{TotalNQ: 0}, 0, 0, 0))
}

func (suite *RefinedLookAsideBalancerSuite) TestCalculateScoreServiceTimeFloor() {
	// Fix B: a ServiceTime quantized to 0 must not collapse the workload term.
	// The score grows cubically with executingNQ (before the fix it was a
	// constant).
	cost := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  0,
		TotalNQ:      0,
	}
	score0 := suite.balancer.calculateScore(-1, cost, 0, 0, 5)
	score1 := suite.balancer.calculateScore(-1, cost, 1, 0, 5)
	score2 := suite.balancer.calculateScore(-1, cost, 2, 0, 5)
	score3 := suite.balancer.calculateScore(-1, cost, 3, 0, 5)
	suite.Equal(int64(6), score0)
	suite.Equal(int64(13), score1)
	suite.Equal(int64(32), score2)
	suite.Equal(int64(69), score3)
	suite.True(score3 > score2 && score2 > score1 && score1 > score0,
		"score must grow with executingNQ, got %d %d %d %d", score0, score1, score2, score3)

	// executeSpeed keeps the real service time, not the floored one
	suite.Equal(int64(13), suite.balancer.calculateScore(-1, cost, 1, 0, 5))
	suite.Equal(int64(36), suite.balancer.calculateScore(-1, cost, 1, 4, 8))
}

func (suite *RefinedLookAsideBalancerSuite) TestCalculateScoreSubMillisecondFloor() {
	// A sub-millisecond node reports ServiceTime truncated to 0 but keeps a
	// 1ms ResponseTime. With the float64 EWMA the smoothed response time
	// stays 1 (>0), so the floor (fix B) applies instead of the
	// responseTime == 0 early return, and the score grows with executingNQ.
	cost := &internalpb.CostAggregation{
		ResponseTime: 1,
		ServiceTime:  0,
		TotalNQ:      0,
	}
	score0 := suite.balancer.calculateScore(-1, cost, 0, 0, 1)
	score1 := suite.balancer.calculateScore(-1, cost, 1, 0, 1)
	score2 := suite.balancer.calculateScore(-1, cost, 2, 0, 1)
	suite.True(score2 > score1 && score1 > score0,
		"score must grow with executingNQ, got %d %d %d", score0, score1, score2)
}

func (suite *RefinedLookAsideBalancerSuite) TestSelectNodeSubMillisecondNoHerding() {
	// Force every SelectNode through the check branch
	suite.balancer.checkWorkloadRequestNum = 1
	suite.balancer.metricExpireInterval = 1000
	// True sub-millisecond scenario: ServiceTime quantizes to 0, nodes report
	// different ResponseTime and node 1 has ~6 requests in flight. On the
	// original balancer the workload term collapses to 0, the score never
	// responds to executingNQ and node 1 (lowest RT) herds all 300 requests
	// (see TestOldLookAsideHerdsWithoutFix). The floor (fix B) restores the
	// response to executingNQ, so the traffic spreads.
	rts := map[int64]int64{1: 1, 2: 2, 3: 2}
	for node, rt := range rts {
		suite.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{
			ResponseTime: rt,
			ServiceTime:  0,
			TotalNQ:      0,
		})
	}
	// the EWMA keeps the response time above 0 so the floor applies
	// instead of the responseTime == 0 early return
	for node, rt := range rts {
		metrics, _ := suite.balancer.refinedMetricsMap.Get(node)
		suite.Equal(float64(rt), math.Float64frombits(metrics.responseTimeSmooth.Load()))
		suite.Equal(0.0, math.Float64frombits(metrics.serviceTimeSmooth.Load()))
	}
	metrics1, _ := suite.balancer.refinedMetricsMap.Get(int64(1))
	metrics1.executingNQ.Store(6)

	counter := make(map[int64]int64)
	for i := 0; i < 300; i++ {
		node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 1)
		suite.NoError(err)
		counter[node]++
	}

	// No single node is persistently selected: the lowest-RT node must not
	// dominate and every node receives a fair share.
	suite.True(counter[1] <= 150, "node 1 (lowest RT) must not dominate, got %d", counter[1])
	for node := 2; node <= 3; node++ {
		suite.True(counter[int64(node)] >= 60, "node %d should receive a fair share, got %d", node, counter[int64(node)])
	}
}

func (suite *RefinedLookAsideBalancerSuite) TestOldLookAsideHerdsWithoutFix() {
	// Control for TestSelectNodeSubMillisecondNoHerding: the same scenario on
	// the original LookAsideBalancer. With ServiceTime 0 the workload term
	// collapses to 0, the score does not respond to executingNQ, and node 1
	// (lowest RT) is selected on every check.
	old := NewLookAsideBalancer(suite.clientMgr)
	defer old.Close()
	old.checkWorkloadRequestNum = 1
	old.metricExpireInterval = 1000
	for node, rt := range map[int64]int64{1: 1, 2: 2, 3: 2} {
		old.UpdateCostMetrics(node, &internalpb.CostAggregation{
			ResponseTime: rt,
			ServiceTime:  0,
			TotalNQ:      0,
		})
	}
	metrics1, _ := old.metricsMap.Get(int64(1))
	metrics1.executingNQ.Store(6)

	counter := make(map[int64]int64)
	for i := 0; i < 300; i++ {
		node, err := old.SelectNode(context.TODO(), []int64{1, 2, 3}, 1)
		suite.NoError(err)
		counter[node]++
	}

	suite.Equal(int64(300), counter[1])
}

func (suite *RefinedLookAsideBalancerSuite) TestSelectNodeSteeringConverges() {
	// Node 1 is equally fast (same RT) but overloaded with 20 in-flight
	// requests that complete over time. The steering loop must shed its
	// traffic while it is overloaded and take it back once it drains. The
	// original balancer keeps round-robining through node 1 during the
	// overload (score does not respond to executingNQ); the floor (fix B)
	// makes the overloaded node's score rise so it is skipped.
	suite.balancer.checkWorkloadRequestNum = 1
	suite.balancer.metricExpireInterval = 1000
	for _, node := range []int64{1, 2, 3} {
		suite.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{
			ResponseTime: 5,
			ServiceTime:  0,
			TotalNQ:      0,
		})
	}
	metrics1, _ := suite.balancer.refinedMetricsMap.Get(int64(1))
	metrics1.executingNQ.Store(20)

	counter := make(map[int64]int64)
	earlyPicks := 0
	for i := 0; i < 300; i++ {
		// node 1's outstanding requests complete over time and drain its load
		if metrics1.executingNQ.Load() > 0 {
			metrics1.executingNQ.Sub(1)
		}
		node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 1)
		suite.NoError(err)
		suite.balancer.CancelWorkload(node, 1)
		counter[node]++
		if i < 20 && node == 1 {
			earlyPicks++
		}
	}

	// while overloaded, node 1 is shed (the original balancer picks it ~7
	// times in the same window by round-robin); once drained it receives a
	// fair share again
	suite.True(earlyPicks <= 4, "overloaded node 1 must be shed early, got %d picks in first 20", earlyPicks)
	suite.True(counter[1] >= 60, "node 1 should receive traffic after recovering, got %d", counter[1])
	suite.True(counter[1] <= 150, "node 1 should not dominate after recovering, got %d", counter[1])
}

func (suite *RefinedLookAsideBalancerSuite) TestSelectNodeFastPathSkipsUnavailable() {
	for _, node := range []int64{1, 2, 3} {
		suite.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{})
	}
	metrics1, _ := suite.balancer.metricsMap.Get(int64(1))
	metrics1.unavailable.Store(true)
	metrics2, _ := suite.balancer.metricsMap.Get(int64(2))
	metrics2.unavailable.Store(true)

	suite.balancer.idx.Store(1)

	node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 1)
	suite.NoError(err)
	suite.Equal(int64(3), node)
}

func (suite *RefinedLookAsideBalancerSuite) TestSelectNodeFastPathAllUnavailable() {
	for _, node := range []int64{1, 2, 3} {
		suite.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{})
		metrics, _ := suite.balancer.metricsMap.Get(node)
		metrics.unavailable.Store(true)
	}

	suite.balancer.idx.Store(1)

	node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 1)
	suite.ErrorIs(err, merr.ErrServiceUnavailable)
	suite.Equal(int64(-1), node)
}

func (suite *RefinedLookAsideBalancerSuite) TestCancelWorkload() {
	node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 10)
	suite.NoError(err)
	suite.balancer.CancelWorkload(node, 10)

	metrics, ok := suite.balancer.refinedMetricsMap.Get(node)
	suite.True(ok)
	suite.Equal(int64(0), metrics.executingNQ.Load())
}

func (suite *RefinedLookAsideBalancerSuite) TestCheckHealthLoop() {
	qn := mocks.NewMockQueryNodeClient(suite.T())
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Maybe()
	qn2 := mocks.NewMockQueryNodeClient(suite.T())
	qn2.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
	}, nil).Maybe()
	suite.clientMgr.ExpectedCalls = nil
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ni NodeInfo) (types.QueryNodeClient, error) {
		if ni.NodeID == 1 {
			return qn, nil
		}

		if ni.NodeID == 2 {
			return qn2, nil
		}
		return nil, errors.New("unexpected node")
	}).Maybe()

	metrics1 := &CostMetrics{}
	metrics1.ts.Store(time.Now().UnixMilli())
	metrics1.unavailable.Store(true)
	suite.balancer.metricsMap.Insert(1, metrics1)
	suite.balancer.RegisterNodeInfo([]NodeInfo{
		{
			NodeID: 1,
		},
	})
	metrics2 := &CostMetrics{}
	metrics2.ts.Store(time.Now().UnixMilli())
	metrics2.unavailable.Store(true)
	suite.balancer.metricsMap.Insert(2, metrics2)
	suite.balancer.RegisterNodeInfo([]NodeInfo{
		{
			NodeID: 2,
		},
	})
	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(1)
		return ok && metrics.unavailable.Load()
	}, 5*time.Second, 100*time.Millisecond)
	targetNode, err := suite.balancer.SelectNode(context.Background(), []int64{1}, 1)
	suite.ErrorIs(err, merr.ErrServiceUnavailable)
	suite.Equal(int64(-1), targetNode)

	suite.balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{})
	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(1)
		return ok && !metrics.unavailable.Load()
	}, 3*time.Second, 100*time.Millisecond)

	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(2)
		return ok && !metrics.unavailable.Load()
	}, 5*time.Second, 100*time.Millisecond)
}

func (suite *RefinedLookAsideBalancerSuite) TestHealthLoopMarksUnavailableWithoutCostMetrics() {
	// Simulates the proxy restarting while the query node is down: the node
	// has never returned cost metrics, so only the entry created by
	// SelectNode exists in the embedded metrics map. The inherited health
	// loop must still be able to mark it unavailable after the configured
	// number of failed health checks.
	node, err := suite.balancer.SelectNode(context.TODO(), []int64{1}, 1)
	suite.NoError(err)
	suite.Equal(int64(1), node)
	suite.balancer.RegisterNodeInfo([]NodeInfo{
		{
			NodeID: 1,
		},
	})

	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(1)
		return ok && metrics.unavailable.Load()
	}, 30*time.Second, 100*time.Millisecond)

	targetNode, err := suite.balancer.SelectNode(context.Background(), []int64{1}, 1)
	suite.ErrorIs(err, merr.ErrServiceUnavailable)
	suite.Equal(int64(-1), targetNode)
}

func (suite *RefinedLookAsideBalancerSuite) TestCleanupEvictedMetrics() {
	for node := int64(1); node <= 3; node++ {
		suite.balancer.UpdateCostMetrics(node, &internalpb.CostAggregation{
			ResponseTime: 5,
			ServiceTime:  1,
			TotalNQ:      1,
		})
	}
	suite.balancer.RegisterNodeInfo([]NodeInfo{{NodeID: 1}, {NodeID: 2}, {NodeID: 3}})

	// node 2 is evicted by the health loop (removed from knownNodeInfos); the
	// sweep must drop its smoothed metrics while keeping the live nodes.
	suite.balancer.knownNodeInfos.Remove(2)
	suite.balancer.cleanupEvictedMetrics()

	_, ok := suite.balancer.refinedMetricsMap.Get(2)
	suite.False(ok)
	for _, node := range []int64{1, 3} {
		_, ok := suite.balancer.refinedMetricsMap.Get(node)
		suite.True(ok)
	}
}

func (suite *RefinedLookAsideBalancerSuite) TestCleanupLoopEvictsRefinedMetrics() {
	// The sweep loop drops the smoothed metrics of evicted nodes. Use a
	// dedicated balancer so the tiny sweep interval is set before Start:
	// the running loop reads the interval once when it boots, so mutating it
	// after Start would either race the loop or never take effect.
	balancer := NewRefinedLookAsideBalancer(suite.clientMgr)
	balancer.cleanupInterval.Store(50 * time.Millisecond)
	balancer.Start(context.Background())
	defer balancer.Close()

	balancer.UpdateCostMetrics(1, &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      1,
	})
	balancer.RegisterNodeInfo([]NodeInfo{{NodeID: 1}})
	_, ok := balancer.refinedMetricsMap.Get(1)
	suite.True(ok)

	balancer.knownNodeInfos.Remove(1)
	suite.Eventually(func() bool {
		_, ok := balancer.refinedMetricsMap.Get(1)
		return !ok
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRefinedLookAsideBalancerSuite(t *testing.T) {
	suite.Run(t, new(RefinedLookAsideBalancerSuite))
}
