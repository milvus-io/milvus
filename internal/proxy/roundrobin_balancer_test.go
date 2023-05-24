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

	"github.com/stretchr/testify/suite"
)

type RoundRobinBalancerSuite struct {
	suite.Suite

	balancer *RoundRobinBalancer
}

func (s *RoundRobinBalancerSuite) SetupTest() {
	s.balancer = NewRoundRobinBalancer()
}

func (s *RoundRobinBalancerSuite) TestRoundRobin() {
	availableNodes := []int64{1, 2}
	s.balancer.SelectNode(availableNodes, 1)
	s.balancer.SelectNode(availableNodes, 1)
	s.balancer.SelectNode(availableNodes, 1)
	s.balancer.SelectNode(availableNodes, 1)

	s.Equal(int64(2), s.balancer.nodeWorkload[1])
	s.Equal(int64(2), s.balancer.nodeWorkload[2])

	s.balancer.SelectNode(availableNodes, 3)
	s.balancer.SelectNode(availableNodes, 1)
	s.balancer.SelectNode(availableNodes, 1)
	s.balancer.SelectNode(availableNodes, 1)

	s.Equal(int64(5), s.balancer.nodeWorkload[1])
	s.Equal(int64(5), s.balancer.nodeWorkload[2])
}

func (s *RoundRobinBalancerSuite) TestNoAvailableNode() {
	availableNodes := []int64{}
	_, err := s.balancer.SelectNode(availableNodes, 1)
	s.Error(err)
}

func TestRoundRobinBalancerSuite(t *testing.T) {
	suite.Run(t, new(RoundRobinBalancerSuite))
}
