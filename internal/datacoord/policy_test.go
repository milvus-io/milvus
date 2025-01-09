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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
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

func (s *PolicySuite) TestBufferChannelAssignPolicy() {
	s.Run("Test no channels in bufferID", func() {
		s.mockStore.EXPECT().GetBufferChannelInfo().Return(nil)

		opSet := BufferChannelAssignPolicy(s.mockStore, 1)
		s.Nil(opSet)
	})

	s.Run("Test channels remain in bufferID", func() {
		ch2Colls := map[string]int64{
			"ch1": 1,
			"ch2": 1,
			"ch3": 2,
		}
		info := &NodeChannelInfo{NodeID: bufferID, Channels: getChannels(ch2Colls)}
		s.mockStore.EXPECT().GetBufferChannelInfo().Return(info)

		var testNodeID int64 = 100
		opSet := BufferChannelAssignPolicy(s.mockStore, testNodeID)
		s.NotNil(opSet)
		s.Equal(2, opSet.Len())
		s.Equal(3, opSet.GetChannelNumber())

		for _, op := range opSet.Collect() {
			s.ElementsMatch([]string{"ch1", "ch2", "ch3"}, op.GetChannelNames())
			s.True(op.Type == Delete || op.Type == Watch)
			if op.Type == Delete {
				s.EqualValues(bufferID, op.NodeID)
			}

			if op.Type == Watch {
				s.EqualValues(testNodeID, op.NodeID)
			}
		}
	})
}

func (s *PolicySuite) TestAvarageAssignPolicy() {
	ch2Coll := map[string]int64{
		"ch1": 100,
		"ch2": 100,
	}
	var testNodeID int64 = 9

	s.Run("no balance after register", func() {
		s.mockStore.EXPECT().GetBufferChannelInfo().Return(nil)
		s.mockStore.EXPECT().GetNodesChannels().Return([]*NodeChannelInfo{
			{NodeID: testNodeID},
		})

		bufferOp, balanceOp := AvgAssignRegisterPolicy(s.mockStore, testNodeID)
		s.Nil(bufferOp)
		s.Nil(balanceOp)
	})
	s.Run("balance bufferID channels after register", func() {
		s.mockStore.EXPECT().GetBufferChannelInfo().Return(
			&NodeChannelInfo{NodeID: bufferID, Channels: getChannels(ch2Coll)},
		)

		bufferOp, balanceOp := AvgAssignRegisterPolicy(s.mockStore, testNodeID)
		s.Nil(balanceOp)
		s.NotNil(bufferOp)
		s.Equal(2, bufferOp.Len())
		s.Equal(2, bufferOp.GetChannelNumber())

		for _, op := range bufferOp.Collect() {
			s.ElementsMatch(lo.Keys(ch2Coll), op.GetChannelNames())
			s.True(op.Type == Delete || op.Type == Watch)
			if op.Type == Delete {
				s.EqualValues(bufferID, op.NodeID)
			}

			if op.Type == Watch {
				s.EqualValues(testNodeID, op.NodeID)
			}
		}
		log.Info("got bufferOp", zap.Any("op", bufferOp))
	})

	s.Run("balance after register", func() {
		s.mockStore.EXPECT().GetBufferChannelInfo().Return(nil)
		s.mockStore.EXPECT().GetNodesChannels().Return([]*NodeChannelInfo{
			{NodeID: 1, Channels: getChannels(ch2Coll)},
			{NodeID: testNodeID},
		})

		bufferOp, balanceOp := AvgAssignRegisterPolicy(s.mockStore, testNodeID)
		s.Nil(bufferOp)
		s.NotNil(balanceOp)
		s.Equal(1, balanceOp.Len())
		s.Equal(1, balanceOp.GetChannelNumber())

		for _, op := range balanceOp.Collect() {
			s.Equal(Release, op.Type)
			s.EqualValues(1, op.NodeID)
		}
		log.Info("got balanceOp", zap.Any("op", balanceOp))
	})
}

func (s *PolicySuite) TestAverageAssignPolicy() {
	ch2Coll := map[string]int64{
		"ch1": 1,
		"ch2": 1,
		"ch3": 2,
	}
	channels := getChannels(ch2Coll)

	s.Run("no new channels", func() {
		s.mockStore.EXPECT().HasChannel(mock.Anything).Return(true)

		opSet := AverageAssignPolicy(s.mockStore, lo.Values(channels))
		s.Nil(opSet)
	})

	s.Run("no datanodes", func() {
		s.mockStore.EXPECT().HasChannel(mock.Anything).Return(false)
		s.mockStore.EXPECT().GetNodesChannels().Return(nil)
		channels := getChannels(ch2Coll)

		opSet := AverageAssignPolicy(s.mockStore, lo.Values(channels))
		s.NotNil(opSet)
		s.Equal(1, opSet.Len())

		op := opSet.Collect()[0]
		s.EqualValues(bufferID, op.NodeID)
		s.Equal(Watch, op.Type)
		s.ElementsMatch(lo.Keys(ch2Coll), op.GetChannelNames())
	})

	s.Run("one datanode", func() {
		// Test three channels assigned one datanode
		s.mockStore.EXPECT().HasChannel(mock.Anything).Return(false)
		s.mockStore.EXPECT().GetNodesChannels().Return([]*NodeChannelInfo{
			{NodeID: 1, Channels: getChannels(map[string]int64{"channel": 1})},
		})
		channels := getChannels(ch2Coll)

		opSet := AverageAssignPolicy(s.mockStore, lo.Values(channels))
		s.NotNil(opSet)
		s.Equal(1, opSet.Len())
		s.Equal(3, opSet.GetChannelNumber())

		for _, op := range opSet.Collect() {
			s.Equal(Watch, op.Type)

			s.EqualValues(1, op.NodeID)
			s.Equal(3, len(op.GetChannelNames()))
			s.ElementsMatch(lo.Keys(ch2Coll), op.GetChannelNames())
		}
		log.Info("test OpSet", zap.Any("opset", opSet))
	})

	s.Run("three datanode", func() {
		// Test three channels assigned evenly to three datanodes
		s.mockStore.EXPECT().HasChannel(mock.Anything).Return(false)
		s.mockStore.EXPECT().GetNodesChannels().Return([]*NodeChannelInfo{
			{NodeID: 1},
			{NodeID: 2},
			{NodeID: 3},
		})

		opSet := AverageAssignPolicy(s.mockStore, lo.Values(channels))
		s.NotNil(opSet)
		s.Equal(3, opSet.Len())
		s.Equal(3, opSet.GetChannelNumber())

		s.ElementsMatch([]int64{1, 2, 3}, lo.Map(opSet.Collect(), func(op *ChannelOp, _ int) int64 {
			return op.NodeID
		}))
		for _, op := range opSet.Collect() {
			s.True(lo.Contains([]int64{1, 2, 3}, op.NodeID))
			s.Equal(1, len(op.GetChannelNames()))
			s.Equal(Watch, op.Type)
			s.True(lo.Contains(lo.Keys(ch2Coll), op.GetChannelNames()[0]))
		}
		log.Info("test OpSet", zap.Any("opset", opSet))
	})
}

func (s *PolicySuite) TestAvgAssignUnregisteredChannels() {
	ch2Coll := map[string]int64{
		"ch1": 1,
		"ch2": 1,
		"ch3": 2,
	}
	info := &NodeChannelInfo{
		NodeID:   1,
		Channels: getChannels(ch2Coll),
	}

	s.Run("deregistering last node", func() {
		s.mockStore.EXPECT().GetNode(mock.Anything).Return(info)
		s.mockStore.EXPECT().GetNodesChannels().Return(nil)

		opSet := AvgAssignUnregisteredChannels(s.mockStore, info.NodeID)
		s.NotNil(opSet)
		s.Equal(2, opSet.Len())

		for _, op := range opSet.Collect() {
			s.ElementsMatch(lo.Keys(ch2Coll), op.GetChannelNames())
			if op.Type == Delete {
				s.EqualValues(info.NodeID, op.NodeID)
			}

			if op.Type == Watch {
				s.EqualValues(bufferID, op.NodeID)
			}
		}
		log.Info("test OpSet", zap.Any("opset", opSet))
	})

	s.Run("assign channels after deregistering", func() {
		s.mockStore.EXPECT().GetNode(mock.Anything).Return(info)
		s.mockStore.EXPECT().GetNodesChannels().Return([]*NodeChannelInfo{
			{NodeID: 100},
		})

		opSet := AvgAssignUnregisteredChannels(s.mockStore, info.NodeID)
		s.NotNil(opSet)
		s.Equal(2, opSet.Len())
		for _, op := range opSet.Collect() {
			s.ElementsMatch(lo.Keys(ch2Coll), op.GetChannelNames())
			s.True(op.Type == Delete || op.Type == Watch)
			if op.Type == Delete {
				s.EqualValues(info.NodeID, op.NodeID)
			}

			if op.Type == Watch {
				s.EqualValues(100, op.NodeID)
			}
		}
		log.Info("test OpSet", zap.Any("opset", opSet))
	})

	s.Run("test average", func() {
		s.mockStore.EXPECT().GetNode(mock.Anything).Return(info)
		s.mockStore.EXPECT().GetNodesChannels().Return([]*NodeChannelInfo{
			{NodeID: 100},
			{NodeID: 101},
			{NodeID: 102},
		})

		opSet := AvgAssignUnregisteredChannels(s.mockStore, info.NodeID)
		s.NotNil(opSet)
		s.Equal(4, opSet.Len())

		nodeCh := make(map[int64]string)
		for _, op := range opSet.Collect() {
			s.True(op.Type == Delete || op.Type == Watch)
			if op.Type == Delete {
				s.EqualValues(info.NodeID, op.NodeID)
				s.ElementsMatch(lo.Keys(ch2Coll), op.GetChannelNames())
			}

			if op.Type == Watch {
				s.Equal(1, len(op.GetChannelNames()))
				nodeCh[op.NodeID] = op.GetChannelNames()[0]
			}
		}

		s.ElementsMatch([]int64{100, 101, 102}, lo.Keys(nodeCh))
		s.ElementsMatch(lo.Keys(ch2Coll), lo.Values(nodeCh))
		log.Info("test OpSet", zap.Any("opset", opSet))
	})
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

func (s *PolicySuite) TestAvgReassignPolicy() {
	s.Run("test only one node", func() {
		ch2Coll := map[string]int64{
			"ch1": 1,
			"ch2": 1,
			"ch3": 2,
			"ch4": 2,
			"ch5": 3,
		}
		fiveChannels := getChannels(ch2Coll)
		storedInfo := []*NodeChannelInfo{{100, fiveChannels}}
		s.mockStore.EXPECT().GetNodesChannels().Return(storedInfo)

		opSet := AverageReassignPolicy(s.mockStore, storedInfo)
		s.Nil(opSet)
	})
	s.Run("test zero average", func() {
		// as we use ceil to calculate the wanted average number, there should be one reassign
		// though the average num less than 1
		storedInfo := []*NodeChannelInfo{
			{100, getChannels(map[string]int64{"ch1": 1})},
			{NodeID: 102},
			{NodeID: 103},
			{NodeID: 104},
		}

		s.mockStore.EXPECT().GetNodesChannels().Return(storedInfo)
		s.mockStore.EXPECT().GetNodeChannelCount(mock.Anything).RunAndReturn(func(nodeID int64) int {
			for _, info := range storedInfo {
				if info.NodeID == nodeID {
					return len(info.Channels)
				}
			}
			return 0
		})

		opSet := AverageReassignPolicy(s.mockStore, storedInfo[0:1])
		s.NotNil(opSet)
		s.Equal(2, opSet.Len())

		for _, op := range opSet.Collect() {
			s.Equal(1, len(op.GetChannelNames()))
			s.Equal("ch1", op.GetChannelNames()[0])

			s.True(op.Type == Delete || op.Type == Watch)
		}
		log.Info("test OpSet", zap.Any("opset", opSet))
	})
	s.Run("test reassign one to one", func() {
		storedInfo := []*NodeChannelInfo{
			{100, getChannels(map[string]int64{"ch1": 1, "ch2": 1, "ch3": 1, "ch4": 1})},
			{NodeID: 101},
			{NodeID: 102},
		}

		s.mockStore.EXPECT().GetNodesChannels().Return(storedInfo)
		s.mockStore.EXPECT().GetNodeChannelCount(mock.Anything).RunAndReturn(func(nodeID int64) int {
			for _, info := range storedInfo {
				if info.NodeID == nodeID {
					return len(info.Channels)
				}
			}
			return 0
		})

		opSet := AverageReassignPolicy(s.mockStore, storedInfo[0:1])
		s.NotNil(opSet)
		s.Equal(3, opSet.Len())

		for _, op := range opSet.Collect() {
			s.True(op.Type == Delete || op.Type == Watch)
			if op.Type == Delete {
				s.ElementsMatch([]string{"ch1", "ch2", "ch3", "ch4"}, op.GetChannelNames())
				s.EqualValues(100, op.NodeID)
			}

			if op.Type == Watch {
				s.Equal(2, len(op.GetChannelNames()))
				s.True(lo.Contains([]int64{102, 101}, op.NodeID))
			}
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
