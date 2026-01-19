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

package assign

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// TestRoundRobinAssignPolicy_AssignSegment_EvenDistribution tests that segments are evenly distributed
func TestRoundRobinAssignPolicy_AssignSegment_EvenDistribution(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.Key, "100")
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.Key)
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()

	// Add 3 nodes
	for i := int64(1); i <= 3; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	// Create 9 segments (should be distributed 3 per node)
	segments := make([]*meta.Segment, 9)
	for i := 0; i < 9; i++ {
		segments[i] = &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        int64(i + 1),
				NumOfRows: 1000,
			},
		}
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 9, len(plans))

	// Count assignments per node
	nodeCount := make(map[int64]int)
	for _, plan := range plans {
		nodeCount[plan.To]++
	}

	// Each node should get 3 segments (even distribution)
	assert.Equal(t, 3, nodeCount[1], "Node 1 should get 3 segments")
	assert.Equal(t, 3, nodeCount[2], "Node 2 should get 3 segments")
	assert.Equal(t, 3, nodeCount[3], "Node 3 should get 3 segments")
}

// TestRoundRobinAssignPolicy_AssignSegment_EmptySegments tests with empty segment list
func TestRoundRobinAssignPolicy_AssignSegment_EmptySegments(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "node",
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	segments := []*meta.Segment{}
	nodes := []int64{1}

	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 0, len(plans))
}

// TestRoundRobinAssignPolicy_AssignSegment_EmptyNodes tests with empty node list
func TestRoundRobinAssignPolicy_AssignSegment_EmptyNodes(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()
	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000}},
	}
	nodes := []int64{}

	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.Nil(t, plans)
}

// TestRoundRobinAssignPolicy_AssignSegment_AllNodesNotNormal tests when all nodes are not in normal state
func TestRoundRobinAssignPolicy_AssignSegment_AllNodesNotNormal(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()

	// Add nodes in non-normal state
	for i := int64(1); i <= 2; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateStopping)
	}

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000}},
	}
	nodes := []int64{1, 2}

	// Without force assign, should return nil
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)
	assert.Nil(t, plans)

	// With force assign, should proceed
	plansForced := policy.AssignSegment(context.Background(), 100, segments, nodes, true)
	assert.NotNil(t, plansForced)
	assert.Equal(t, 1, len(plansForced))
}

// TestRoundRobinAssignPolicy_AssignSegment_SingleNode tests with single node
func TestRoundRobinAssignPolicy_AssignSegment_SingleNode(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "node",
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	// Create 5 segments
	segments := make([]*meta.Segment, 5)
	for i := 0; i < 5; i++ {
		segments[i] = &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        int64(i + 1),
				NumOfRows: 1000,
			},
		}
	}

	nodes := []int64{1}
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 5, len(plans))

	// All segments should go to node 1
	for _, plan := range plans {
		assert.Equal(t, int64(1), plan.To)
		assert.Equal(t, int64(-1), plan.From)
	}
}

// TestRoundRobinAssignPolicy_AssignChannel_EvenDistribution tests channel distribution
func TestRoundRobinAssignPolicy_AssignChannel_EvenDistribution(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()

	// Add 3 nodes
	for i := int64(1); i <= 3; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Address:  "localhost",
			Hostname: "node",
			Version:  common.Version,
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	// Create 6 channels
	channels := make([]*meta.DmChannel, 6)
	for i := 0; i < 6; i++ {
		channels[i] = &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  string(rune('a' + i)),
			},
		}
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 6, len(plans))

	// Count assignments per node
	nodeCount := make(map[int64]int)
	for _, plan := range plans {
		nodeCount[plan.To]++
	}

	// Each node should get 2 channels
	assert.Equal(t, 2, nodeCount[1])
	assert.Equal(t, 2, nodeCount[2])
	assert.Equal(t, 2, nodeCount[3])
}

// TestRoundRobinAssignPolicy_AssignChannel_EmptyChannels tests with empty channel list
func TestRoundRobinAssignPolicy_AssignChannel_EmptyChannels(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "node",
		Version:  common.Version,
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	channels := []*meta.DmChannel{}
	nodes := []int64{1}

	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 0, len(plans))
}

// TestRoundRobinAssignPolicy_AssignChannel_VerifyFrom tests that From field is correctly set
func TestRoundRobinAssignPolicy_AssignChannel_VerifyFrom(t *testing.T) {
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "node",
		Version:  common.Version,
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newRoundRobinAssignPolicy(nodeManager, mockScheduler, nil)

	channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel1",
			},
		},
	}

	nodes := []int64{1}
	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))
	assert.Equal(t, int64(-1), plans[0].From, "From should be -1 for new assignments")
	assert.Equal(t, int64(1), plans[0].To)
	assert.Equal(t, "channel1", plans[0].Channel.GetChannelName())
}
