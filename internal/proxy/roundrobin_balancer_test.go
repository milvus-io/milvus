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
	"testing"

	"github.com/stretchr/testify/suite"
)

type RoundRobinBalancerSuite struct {
	suite.Suite

	balancer *RoundRobinBalancer
}

func (s *RoundRobinBalancerSuite) SetupTest() {
	s.balancer = NewRoundRobinBalancer()
	s.balancer.Start(context.Background())
}

func (s *RoundRobinBalancerSuite) TestRoundRobin() {
	availableNodes := []int64{1, 2}
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)

	workload, ok := s.balancer.nodeWorkload.Get(1)
	s.True(ok)
	s.Equal(int64(2), workload.Load())
	workload, ok = s.balancer.nodeWorkload.Get(1)
	s.True(ok)
	s.Equal(int64(2), workload.Load())

	s.balancer.SelectNode(context.TODO(), availableNodes, 3)
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)
	s.balancer.SelectNode(context.TODO(), availableNodes, 1)

	workload, ok = s.balancer.nodeWorkload.Get(1)
	s.True(ok)
	s.Equal(int64(5), workload.Load())
	workload, ok = s.balancer.nodeWorkload.Get(1)
	s.True(ok)
	s.Equal(int64(5), workload.Load())
}

func (s *RoundRobinBalancerSuite) TestNoAvailableNode() {
	availableNodes := []int64{}
	_, err := s.balancer.SelectNode(context.TODO(), availableNodes, 1)
	s.Error(err)
}

func (s *RoundRobinBalancerSuite) TestCancelWorkload() {
	availableNodes := []int64{101}
	_, err := s.balancer.SelectNode(context.TODO(), availableNodes, 5)
	s.NoError(err)
	workload, ok := s.balancer.nodeWorkload.Get(101)
	s.True(ok)
	s.Equal(int64(5), workload.Load())
	s.balancer.CancelWorkload(101, 5)
	s.Equal(int64(0), workload.Load())
}

func TestRoundRobinBalancerSuite(t *testing.T) {
	suite.Run(t, new(RoundRobinBalancerSuite))
}
