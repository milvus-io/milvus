// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type NodeManagerSuite struct {
	suite.Suite

	nodeManager *NodeManager
}

func (s *NodeManagerSuite) SetupTest() {
	s.nodeManager = NewNodeManager()
}

func (s *NodeManagerSuite) TearDownTest() {
}

func (s *NodeManagerSuite) TestNodeOperation() {
	s.nodeManager.Add(NewNodeInfo(ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	s.nodeManager.Add(NewNodeInfo(ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	s.nodeManager.Add(NewNodeInfo(ImmutableNodeInfo{
		NodeID:   3,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	s.NotNil(s.nodeManager.Get(1))
	s.Len(s.nodeManager.GetAll(), 3)
	s.nodeManager.Remove(1)
	s.Nil(s.nodeManager.Get(1))
	s.Len(s.nodeManager.GetAll(), 2)

	s.nodeManager.Stopping(2)
	s.True(s.nodeManager.IsStoppingNode(2))
	node := s.nodeManager.Get(2)
	node.SetState(NodeStateNormal)
	s.False(s.nodeManager.IsStoppingNode(2))
}

func (s *NodeManagerSuite) TestResourceExhaustion() {
	nodeID := int64(1)
	s.nodeManager.Add(NewNodeInfo(ImmutableNodeInfo{NodeID: nodeID}))

	s.Run("mark_exhausted", func() {
		s.nodeManager.MarkResourceExhaustion(nodeID, 10*time.Minute)
		s.True(s.nodeManager.IsResourceExhausted(nodeID))
	})

	s.Run("auto_clear_after_expiry", func() {
		s.nodeManager.MarkResourceExhaustion(nodeID, 1*time.Millisecond)
		time.Sleep(2 * time.Millisecond)
		s.False(s.nodeManager.IsResourceExhausted(nodeID))
	})

	s.Run("invalid_node", func() {
		s.False(s.nodeManager.IsResourceExhausted(999))
	})
}

func (s *NodeManagerSuite) TestNodeInfo() {
	node := NewNodeInfo(ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	})
	s.Equal(int64(1), node.ID())
	s.Equal("localhost", node.Addr())
	node.setChannelCnt(1)
	node.setSegmentCnt(1)
	s.Equal(1, node.ChannelCnt())
	s.Equal(1, node.SegmentCnt())

	node.UpdateStats(WithSegmentCnt(5))
	node.UpdateStats(WithChannelCnt(5))
	s.Equal(5, node.ChannelCnt())
	s.Equal(5, node.SegmentCnt())

	node.SetLastHeartbeat(time.Now())
	s.NotNil(node.LastHeartbeat())
}

func TestNodeManagerSuite(t *testing.T) {
	suite.Run(t, new(NodeManagerSuite))
}
