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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type LookAsideBalancerSuite struct {
	suite.Suite

	clientMgr *MockShardClientManager
	balancer  *LookAsideBalancer
}

func (suite *LookAsideBalancerSuite) SetupTest() {
	suite.clientMgr = NewMockShardClientManager(suite.T())
	suite.balancer = NewLookAsideBalancer(suite.clientMgr)
	suite.balancer.Start(context.Background())

	qn := mocks.NewMockQueryNodeClient(suite.T())
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn, nil).Maybe()
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Maybe()
}

func (suite *LookAsideBalancerSuite) TearDownTest() {
	suite.balancer.Close()
}

func (suite *LookAsideBalancerSuite) TestUpdateMetrics() {
	costMetrics := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      1,
	}

	suite.balancer.UpdateCostMetrics(1, costMetrics)

	metrics, ok := suite.balancer.metricsMap.Get(1)
	suite.True(ok)
	suite.True(time.Now().UnixMilli()-metrics.ts.Load() <= 5)
}

func (suite *LookAsideBalancerSuite) TestCalculateScore() {
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

	score1 := suite.balancer.calculateScore(-1, costMetrics1, 0)
	score2 := suite.balancer.calculateScore(-1, costMetrics2, 0)
	score3 := suite.balancer.calculateScore(-1, costMetrics3, 0)
	score4 := suite.balancer.calculateScore(-1, costMetrics4, 0)
	suite.Equal(int64(12), score1)
	suite.Equal(int64(19), score2)
	suite.Equal(int64(17), score3)
	suite.Equal(int64(5), score4)

	score5 := suite.balancer.calculateScore(-1, costMetrics1, 5)
	score6 := suite.balancer.calculateScore(-1, costMetrics2, 5)
	score7 := suite.balancer.calculateScore(-1, costMetrics3, 5)
	score8 := suite.balancer.calculateScore(-1, costMetrics4, 5)
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

	score9 := suite.balancer.calculateScore(-1, costMetrics5, math.MaxInt64)
	suite.Equal(int64(math.MaxInt64), score9)

	// test unexpected negative nq value
	costMetrics6 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      -1,
	}
	score12 := suite.balancer.calculateScore(-1, costMetrics6, math.MaxInt64)
	suite.Equal(int64(4), score12)
	costMetrics7 := &internalpb.CostAggregation{
		ResponseTime: 5,
		ServiceTime:  1,
		TotalNQ:      1,
	}
	score13 := suite.balancer.calculateScore(-1, costMetrics7, -1)
	suite.Equal(int64(4), score13)
}

func (suite *LookAsideBalancerSuite) TestSelectNode() {
	type testcase struct {
		name         string
		costMetrics  map[int64]*internalpb.CostAggregation
		executingNQ  map[int64]int64
		requestCount int
		result       map[int64]int64
	}

	cases := []testcase{
		{
			name: "qn with empty metrics",
			costMetrics: map[int64]*internalpb.CostAggregation{
				1: {},
				2: {},
				3: {},
			},

			executingNQ:  map[int64]int64{},
			requestCount: 100,
			result:       map[int64]int64{1: 34, 2: 33, 3: 33},
		},
		{
			name: "each qn has same cost metrics",
			costMetrics: map[int64]*internalpb.CostAggregation{
				1: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},
				2: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},

				3: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},
			},

			executingNQ:  map[int64]int64{1: 0, 2: 0, 3: 0},
			requestCount: 100,
			result:       map[int64]int64{1: 34, 2: 33, 3: 33},
		},
		{
			name: "each qn has different service time",
			costMetrics: map[int64]*internalpb.CostAggregation{
				1: {
					ResponseTime: 30,
					ServiceTime:  20,
					TotalNQ:      0,
				},
				2: {
					ResponseTime: 50,
					ServiceTime:  40,
					TotalNQ:      0,
				},

				3: {
					ResponseTime: 70,
					ServiceTime:  60,
					TotalNQ:      0,
				},
			},

			executingNQ:  map[int64]int64{1: 0, 2: 0, 3: 0},
			requestCount: 100,
			result:       map[int64]int64{1: 40, 2: 32, 3: 28},
		},
		{
			name: "one qn has task in queue",
			costMetrics: map[int64]*internalpb.CostAggregation{
				1: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},
				2: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},

				3: {
					ResponseTime: 100,
					ServiceTime:  1,
					TotalNQ:      20,
				},
			},

			executingNQ:  map[int64]int64{1: 0, 2: 0, 3: 0},
			requestCount: 100,
			result:       map[int64]int64{1: 40, 2: 40, 3: 20},
		},

		{
			name: "qn with executing task",
			costMetrics: map[int64]*internalpb.CostAggregation{
				1: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},
				2: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},

				3: {
					ResponseTime: 5,
					ServiceTime:  1,
					TotalNQ:      0,
				},
			},

			executingNQ:  map[int64]int64{1: 0, 2: 0, 3: 20},
			requestCount: 100,
			result:       map[int64]int64{1: 40, 2: 40, 3: 20},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			for node, cost := range c.costMetrics {
				suite.balancer.UpdateCostMetrics(node, cost)
			}

			for node, executingNQ := range c.executingNQ {
				metrics, _ := suite.balancer.metricsMap.Get(node)
				metrics.executingNQ.Store(executingNQ)
			}
			counter := make(map[int64]int64)
			for i := 0; i < c.requestCount; i++ {
				node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 1)
				suite.NoError(err)
				counter[node]++
			}

			for node, result := range c.result {
				suite.True(math.Abs(float64(result-counter[node])) <= float64(1))
			}
		})
	}
}

func (suite *LookAsideBalancerSuite) TestCancelWorkload() {
	node, err := suite.balancer.SelectNode(context.TODO(), []int64{1, 2, 3}, 10)
	suite.NoError(err)
	suite.balancer.CancelWorkload(node, 10)

	metrics, ok := suite.balancer.metricsMap.Get(node)
	suite.True(ok)
	suite.Equal(int64(0), metrics.executingNQ.Load())
}

func (suite *LookAsideBalancerSuite) TestCheckHealthLoop() {
	qn := mocks.NewMockQueryNodeClient(suite.T())
	qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Maybe()
	qn2 := mocks.NewMockQueryNodeClient(suite.T())
	qn2.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
	}, nil).Maybe()
	suite.clientMgr.ExpectedCalls = nil
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ni nodeInfo) (types.QueryNodeClient, error) {
		if ni.nodeID == 1 {
			return qn, nil
		}

		if ni.nodeID == 2 {
			return qn2, nil
		}
		return nil, errors.New("unexpected node")
	}).Maybe()

	metrics1 := &CostMetrics{}
	metrics1.ts.Store(time.Now().UnixMilli())
	metrics1.unavailable.Store(true)
	suite.balancer.metricsMap.Insert(1, metrics1)
	suite.balancer.RegisterNodeInfo([]nodeInfo{
		{
			nodeID: 1,
		},
	})
	metrics2 := &CostMetrics{}
	metrics2.ts.Store(time.Now().UnixMilli())
	metrics2.unavailable.Store(true)
	suite.balancer.metricsMap.Insert(2, metrics2)
	suite.balancer.knownNodeInfos.Insert(2, nodeInfo{})
	suite.balancer.RegisterNodeInfo([]nodeInfo{
		{
			nodeID: 2,
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

func (suite *LookAsideBalancerSuite) TestGetClientFailed() {
	metrics1 := &CostMetrics{}
	metrics1.ts.Store(time.Now().UnixMilli())
	metrics1.unavailable.Store(true)
	suite.balancer.metricsMap.Insert(2, metrics1)
	suite.balancer.RegisterNodeInfo([]nodeInfo{
		{
			nodeID: 2,
		},
	})

	// test get shard client from client mgr return nil
	suite.clientMgr.ExpectedCalls = nil
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("shard client not found"))
	// expected stopping the health check after failure times reaching the limit
	suite.Eventually(func() bool {
		return !suite.balancer.metricsMap.Contain(2)
	}, 30*time.Second, 1*time.Second)
}

func (suite *LookAsideBalancerSuite) TestNodeRecover() {
	// mock qn down for a while and then recover
	qn3 := mocks.NewMockQueryNodeClient(suite.T())
	suite.clientMgr.ExpectedCalls = nil
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn3, nil)
	qn3.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Abnormal,
		},
	}, nil).Times(3)

	qn3.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
	}, nil)

	metrics1 := &CostMetrics{}
	metrics1.ts.Store(time.Now().UnixMilli())
	suite.balancer.metricsMap.Insert(3, metrics1)
	suite.balancer.RegisterNodeInfo([]nodeInfo{
		{
			nodeID: 3,
		},
	})
	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(3)
		return ok && metrics.unavailable.Load()
	}, 5*time.Second, 100*time.Millisecond)

	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(3)
		return ok && !metrics.unavailable.Load()
	}, 5*time.Second, 100*time.Millisecond)
}

func (suite *LookAsideBalancerSuite) TestNodeOffline() {
	Params.Save(Params.CommonCfg.SessionTTL.Key, "10")
	Params.Save(Params.ProxyCfg.HealthCheckTimeout.Key, "1000")
	// mock qn down for a while and then recover
	qn3 := mocks.NewMockQueryNodeClient(suite.T())
	suite.clientMgr.ExpectedCalls = nil
	suite.clientMgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(qn3, nil)
	qn3.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Abnormal,
		},
	}, nil)

	metrics1 := &CostMetrics{}
	metrics1.ts.Store(time.Now().UnixMilli())
	suite.balancer.metricsMap.Insert(3, metrics1)
	suite.balancer.RegisterNodeInfo([]nodeInfo{
		{
			nodeID: 3,
		},
	})
	suite.Eventually(func() bool {
		metrics, ok := suite.balancer.metricsMap.Get(3)
		return ok && metrics.unavailable.Load()
	}, 5*time.Second, 100*time.Millisecond)

	suite.Eventually(func() bool {
		_, ok := suite.balancer.metricsMap.Get(3)
		return !ok
	}, 10*time.Second, 100*time.Millisecond)
}

func BenchmarkSelectNode_QNWithSameWorkload(b *testing.B) {
	balancer := NewLookAsideBalancer(nil)

	ctx := context.Background()
	nodeList := make([]int64, 0o0)

	metrics := &internalpb.CostAggregation{
		ResponseTime: 100,
		ServiceTime:  100,
		TotalNQ:      100,
	}
	for i := 0; i < 16; i++ {
		nodeID := int64(10000 + i)
		nodeList = append(nodeList, nodeID)
	}
	cost := int64(7)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			node, _ := balancer.SelectNode(ctx, nodeList, cost)
			balancer.CancelWorkload(node, cost)
			balancer.UpdateCostMetrics(node, metrics)
		}
	})
}

func BenchmarkSelectNode_QNWithDifferentWorkload(b *testing.B) {
	balancer := NewLookAsideBalancer(nil)

	ctx := context.Background()
	nodeList := make([]int64, 0o0)

	metrics := &internalpb.CostAggregation{
		ResponseTime: 100,
		ServiceTime:  100,
		TotalNQ:      100,
	}

	heavyMetric := &internalpb.CostAggregation{
		ResponseTime: 1000,
		ServiceTime:  1000,
		TotalNQ:      1000,
	}
	for i := 0; i < 16; i++ {
		nodeID := int64(10000 + i)
		nodeList = append(nodeList, nodeID)
	}
	cost := int64(7)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			node, _ := balancer.SelectNode(ctx, nodeList, cost)
			balancer.CancelWorkload(node, cost)
			if i%2 == 0 {
				balancer.UpdateCostMetrics(node, heavyMetric)
			} else {
				balancer.UpdateCostMetrics(node, metrics)
			}
			i++
		}
	})
}

func TestLookAsideBalancerSuite(t *testing.T) {
	suite.Run(t, new(LookAsideBalancerSuite))
}
