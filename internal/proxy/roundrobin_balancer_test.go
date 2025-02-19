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

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type RoundRobinBalancerSuite struct {
	suite.Suite

	balancer *RoundRobinBalancer
}

func (s *RoundRobinBalancerSuite) SetupTest() {
	s.balancer = NewRoundRobinBalancer()
	s.balancer.Start(context.Background())
}

func TestSelectNode(t *testing.T) {
	balancer := NewRoundRobinBalancer()

	// Test case 1: Empty availableNodes
	_, err1 := balancer.SelectNode(context.Background(), []int64{}, 0)
	if err1 != merr.ErrNodeNotAvailable {
		t.Errorf("Expected ErrNodeNotAvailable, got %v", err1)
	}

	// Test case 2: Non-empty availableNodes
	availableNodes := []int64{1, 2, 3}
	selectedNode2, err2 := balancer.SelectNode(context.Background(), availableNodes, 0)
	if err2 != nil {
		t.Errorf("Expected no error, got %v", err2)
	}
	if selectedNode2 < 1 || selectedNode2 > 3 {
		t.Errorf("Expected a node in the range [1, 3], got %d", selectedNode2)
	}

	// Test case 3: Boundary case
	availableNodes = []int64{1}
	selectedNode3, err3 := balancer.SelectNode(context.Background(), availableNodes, 0)
	if err3 != nil {
		t.Errorf("Expected no error, got %v", err3)
	}
	if selectedNode3 != 1 {
		t.Errorf("Expected 1, got %d", selectedNode3)
	}
}

func TestRoundRobinBalancerSuite(t *testing.T) {
	suite.Run(t, new(RoundRobinBalancerSuite))
}
