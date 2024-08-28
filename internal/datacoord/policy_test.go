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

package datacoord

import (
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

func TestPolicySuite(t *testing.T) {
	suite.Run(t, new(PolicySuite))
}

func getChannel(name string, collID int64) *StateChannel {
	return &StateChannel{
		Name:         name,
		CollectionID: collID,
		Info: &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{ChannelName: name, CollectionID: collID},
		},
		Schema: &schemapb.CollectionSchema{Name: "coll1"},
	}
}

func getChannels(ch2Coll map[string]int64) map[string]RWChannel {
	ret := make(map[string]RWChannel)
	for k, v := range ch2Coll {
		ret[k] = getChannel(k, v)
	}
	return ret
}

type PolicySuite struct {
	suite.Suite

	mockStore *MockRWChannelStore
}

func (s *PolicySuite) SetupSubTest() {
	s.mockStore = NewMockRWChannelStore(s.T())
}

func (s *PolicySuite) TestAvgBalanceChannelPolicy() {
	s.Run("test even distribution", func() {
		// even distribution should have not results
		evenDist := []*NodeChannelInfo{
			{100, getChannels(map[string]int64{"ch1": 1, "ch2": 1})},
			{101, getChannels(map[string]int64{"ch3": 2, "ch4": 2})},
			{102, getChannels(map[string]int64{"ch5": 3, "ch6": 3})},
		}

		opSet := AvgBalanceChannelPolicy(evenDist)
		s.Nil(opSet)
	})
	s.Run("test uneven with conservative effect", func() {
		uneven := []*NodeChannelInfo{
			{100, getChannels(map[string]int64{"ch1": 1, "ch2": 1})},
			{NodeID: 101},
		}

		opSet := AvgBalanceChannelPolicy(uneven)
		s.Equal(opSet.Len(), 1)
		for _, op := range opSet.Collect() {
			s.True(lo.Contains([]string{"ch1", "ch2"}, op.GetChannelNames()[0]))
		}
	})
	s.Run("test uneven with zero", func() {
		uneven := []*NodeChannelInfo{
			{100, getChannels(map[string]int64{"ch1": 1, "ch2": 1, "ch3": 1})},
			{NodeID: 101},
		}

		opSet := AvgBalanceChannelPolicy(uneven)
		s.NotNil(opSet)
		s.Equal(1, opSet.Len())

		for _, op := range opSet.Collect() {
			s.Equal(Release, op.Type)
			s.EqualValues(100, op.NodeID)
			s.Equal(1, len(op.GetChannelNames()))
			s.True(lo.Contains([]string{"ch1", "ch2", "ch3"}, op.GetChannelNames()[0]))
		}
		log.Info("test OpSet", zap.Any("opset", opSet))
	})
}

type AssignByCountPolicySuite struct {
	suite.Suite

	curCluster Assignments
}

func TestAssignByCountPolicySuite(t *testing.T) {
	suite.Run(t, new(AssignByCountPolicySuite))
}

func (s *AssignByCountPolicySuite) SetupSubTest() {
	s.curCluster = []*NodeChannelInfo{
		{1, getChannels(map[string]int64{"ch-1": 1, "ch-2": 1, "ch-3": 1})},
		{2, getChannels(map[string]int64{"ch-4": 1, "ch-5": 1, "ch-6": 4})},
		{NodeID: 3, Channels: map[string]RWChannel{}},
	}
}

func (s *AssignByCountPolicySuite) TestWithoutUnassignedChannels() {
	s.Run("balance without exclusive", func() {
		opSet := AvgAssignByCountPolicy(s.curCluster, nil, nil)
		s.NotNil(opSet)

		s.Equal(2, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			s.True(lo.Contains([]int64{1, 2}, op.NodeID))
		}
	})
	s.Run("balance with exclusive", func() {
		execlusiveNodes := []int64{1, 2}
		opSet := AvgAssignByCountPolicy(s.curCluster, nil, execlusiveNodes)
		s.NotNil(opSet)

		s.Equal(2, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			if op.NodeID == bufferID {
				s.Equal(Watch, op.Type)
			} else {
				s.True(lo.Contains([]int64{1, 2}, op.NodeID))
				s.Equal(Delete, op.Type)
			}
		}
	})
	s.Run("extreme cases", func() {
		m := make(map[string]int64)
		for i := 0; i < 100; i++ {
			m[fmt.Sprintf("ch-%d", i)] = 1
		}
		s.curCluster = []*NodeChannelInfo{
			{NodeID: 1, Channels: getChannels(m)},
			{NodeID: 2, Channels: map[string]RWChannel{}},
			{NodeID: 3, Channels: map[string]RWChannel{}},
			{NodeID: 4, Channels: map[string]RWChannel{}},
			{NodeID: 5, Channels: map[string]RWChannel{}},
		}

		execlusiveNodes := []int64{4, 5}
		opSet := AvgAssignByCountPolicy(s.curCluster, nil, execlusiveNodes)
		s.NotNil(opSet)
	})
}

func (s *AssignByCountPolicySuite) TestWithUnassignedChannels() {
	s.Run("one unassigned channel", func() {
		unassigned := NewNodeChannelInfo(bufferID, getChannel("new-ch-1", 1))

		opSet := AvgAssignByCountPolicy(s.curCluster, unassigned, nil)
		s.NotNil(opSet)

		s.Equal(1, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			if op.NodeID == bufferID {
				s.Equal(Delete, op.Type)
			} else {
				s.EqualValues(3, op.NodeID)
			}
		}
	})

	s.Run("three unassigned channel", func() {
		unassigned := NewNodeChannelInfo(bufferID,
			getChannel("new-ch-1", 1),
			getChannel("new-ch-2", 1),
			getChannel("new-ch-3", 1),
		)

		opSet := AvgAssignByCountPolicy(s.curCluster, unassigned, nil)
		s.NotNil(opSet)

		s.Equal(3, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			if op.NodeID == bufferID {
				s.Equal(Delete, op.Type)
			}
		}
		s.Equal(2, opSet.Len())

		nodeIDs := lo.FilterMap(opSet.Collect(), func(op *ChannelOp, _ int) (int64, bool) {
			return op.NodeID, op.NodeID != bufferID
		})
		s.ElementsMatch([]int64{3}, nodeIDs)
	})

	s.Run("three unassigned channel with execlusiveNodes", func() {
		unassigned := NewNodeChannelInfo(bufferID,
			getChannel("new-ch-1", 1),
			getChannel("new-ch-2", 1),
			getChannel("new-ch-3", 1),
		)

		opSet := AvgAssignByCountPolicy(s.curCluster, unassigned, []int64{1, 2})
		s.NotNil(opSet)

		s.Equal(3, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			if op.NodeID == bufferID {
				s.Equal(Delete, op.Type)
			}
		}
		s.Equal(2, opSet.Len())

		nodeIDs := lo.FilterMap(opSet.Collect(), func(op *ChannelOp, _ int) (int64, bool) {
			return op.NodeID, op.NodeID != bufferID
		})
		s.ElementsMatch([]int64{3}, nodeIDs)
	})
	s.Run("67 unassigned with 33 in node1, none in node2,3", func() {
		var unassignedChannels []RWChannel
		m1 := make(map[string]int64)
		for i := 0; i < 33; i++ {
			m1[fmt.Sprintf("ch-%d", i)] = 1
		}
		for i := 33; i < 100; i++ {
			unassignedChannels = append(unassignedChannels, getChannel(fmt.Sprintf("ch-%d", i), 1))
		}
		s.curCluster = []*NodeChannelInfo{
			{NodeID: 1, Channels: getChannels(m1)},
			{NodeID: 2, Channels: map[string]RWChannel{}},
			{NodeID: 3, Channels: map[string]RWChannel{}},
		}

		unassigned := NewNodeChannelInfo(bufferID, unassignedChannels...)
		opSet := AvgAssignByCountPolicy(s.curCluster, unassigned, nil)
		s.NotNil(opSet)

		s.Equal(67, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			if op.NodeID == bufferID {
				s.Equal(Delete, op.Type)
			}
		}
		s.Equal(4, opSet.Len())

		nodeIDs := lo.FilterMap(opSet.Collect(), func(op *ChannelOp, _ int) (int64, bool) {
			return op.NodeID, op.NodeID != bufferID
		})
		s.ElementsMatch([]int64{3, 2}, nodeIDs)
	})

	s.Run("toAssign from nodeID = 1", func() {
		var unassigned *NodeChannelInfo
		for _, info := range s.curCluster {
			if info.NodeID == int64(1) {
				unassigned = info
			}
		}
		s.Require().NotNil(unassigned)

		opSet := AvgAssignByCountPolicy(s.curCluster, unassigned, []int64{1, 2})
		s.NotNil(opSet)

		s.Equal(3, opSet.GetChannelNumber())
		for _, op := range opSet.Collect() {
			if op.NodeID == int64(1) {
				s.Equal(Delete, op.Type)
			}
		}
		s.Equal(2, opSet.Len())

		nodeIDs := lo.FilterMap(opSet.Collect(), func(op *ChannelOp, _ int) (int64, bool) {
			return op.NodeID, true
		})
		s.ElementsMatch([]int64{3, 1}, nodeIDs)
	})

	s.Run("assign to reach average", func() {
		curCluster := []*NodeChannelInfo{
			{1, getChannels(map[string]int64{"ch-1": 1, "ch-2": 1, "ch-3": 1})},
			{2, getChannels(map[string]int64{"ch-4": 1, "ch-5": 1, "ch-6": 4, "ch-7": 4, "ch-8": 4})},
		}
		unassigned := NewNodeChannelInfo(bufferID,
			getChannel("new-ch-1", 1),
			getChannel("new-ch-2", 1),
			getChannel("new-ch-3", 1),
		)

		opSet := AvgAssignByCountPolicy(curCluster, unassigned, nil)
		s.NotNil(opSet)

		s.Equal(3, opSet.GetChannelNumber())
		s.Equal(2, opSet.Len())
		for _, op := range opSet.Collect() {
			if op.Type == Delete {
				s.Equal(int64(bufferID), op.NodeID)
			}

			if op.Type == Watch {
				s.Equal(int64(1), op.NodeID)
			}
		}
	})
}
