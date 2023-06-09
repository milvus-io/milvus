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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type LookAsideBalancerSuite struct {
	suite.Suite

	clientMgr *MockShardClientManager
	balancer  *LookAsideBalancer
}

func (suite *LookAsideBalancerSuite) SetupTest() {
	suite.clientMgr = NewMockShardClientManager(suite.T())
	suite.balancer = NewLookAsideBalancer(suite.clientMgr)

	qn := types.NewMockQueryNode(suite.T())
	suite.clientMgr.EXPECT().GetClient(mock.Anything, int64(1)).Return(qn, nil).Maybe()
	qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, errors.New("fake error")).Maybe()
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

	lastUpdateTs, ok := suite.balancer.metricsUpdateTs.Get(1)
	suite.True(ok)
	suite.True(time.Now().UnixMilli()-lastUpdateTs <= 5)
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

	score1 := suite.balancer.calculateScore(costMetrics1, 0)
	score2 := suite.balancer.calculateScore(costMetrics2, 0)
	score3 := suite.balancer.calculateScore(costMetrics3, 0)
	score4 := suite.balancer.calculateScore(costMetrics4, 0)
	suite.Equal(float64(12), score1)
	suite.Equal(float64(8.5), score2)
	suite.Equal(float64(17), score3)
	suite.Equal(float64(5), score4)

	score5 := suite.balancer.calculateScore(costMetrics1, 5)
	score6 := suite.balancer.calculateScore(costMetrics2, 5)
	score7 := suite.balancer.calculateScore(costMetrics3, 5)
	score8 := suite.balancer.calculateScore(costMetrics4, 5)
	suite.Equal(float64(347), score5)
	suite.Equal(float64(176), score6)
	suite.Equal(float64(352), score7)
	suite.Equal(float64(220), score8)
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
			result:       map[int64]int64{1: 27, 2: 34, 3: 39},
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
		{
			name: "qn with empty metrics",
			costMetrics: map[int64]*internalpb.CostAggregation{
				1: {},
				2: {},
				3: {},
			},

			executingNQ:  map[int64]int64{1: 0, 2: 0, 3: 0},
			requestCount: 100,
			result:       map[int64]int64{1: 34, 2: 33, 3: 33},
		},
	}

	for _, c := range cases {
		suite.Run(c.name, func() {
			for node, cost := range c.costMetrics {
				suite.balancer.UpdateCostMetrics(node, cost)
			}

			for node, executingNQ := range c.executingNQ {
				suite.balancer.executingTaskTotalNQ.Insert(node, atomic.NewInt64(executingNQ))
			}

			counter := make(map[int64]int64)
			for i := 0; i < c.requestCount; i++ {
				node, err := suite.balancer.SelectNode([]int64{1, 2, 3}, 1)
				suite.NoError(err)
				counter[node]++
			}

			for node, result := range c.result {
				suite.Equal(result, counter[node])
			}
		})
	}
}

func (suite *LookAsideBalancerSuite) TestCancelWorkload() {
	node, err := suite.balancer.SelectNode([]int64{1, 2, 3}, 10)
	suite.NoError(err)
	suite.balancer.CancelWorkload(node, 10)

	executingNQ, ok := suite.balancer.executingTaskTotalNQ.Get(node)
	suite.True(ok)
	suite.Equal(int64(0), executingNQ.Load())
}

func (suite *LookAsideBalancerSuite) TestCheckHealthLoop() {
	qn2 := types.NewMockQueryNode(suite.T())
	suite.clientMgr.EXPECT().GetClient(mock.Anything, int64(2)).Return(qn2, nil)
	qn2.EXPECT().GetComponentStates(mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
	}, nil)

	suite.balancer.metricsUpdateTs.Insert(1, time.Now().UnixMilli())
	suite.balancer.metricsUpdateTs.Insert(2, time.Now().UnixMilli())
	suite.Eventually(func() bool {
		return suite.balancer.unreachableQueryNodes.Contain(1)
	}, 2*time.Second, 100*time.Millisecond)

	suite.Eventually(func() bool {
		return !suite.balancer.unreachableQueryNodes.Contain(2)
	}, 3*time.Second, 100*time.Millisecond)
}

func TestLookAsideBalancerSuite(t *testing.T) {
	suite.Run(t, new(LookAsideBalancerSuite))
}
