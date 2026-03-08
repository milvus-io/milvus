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
)

// TestRowCountBasedAssignPolicy_AssignSegment_PriorityToLeastLoaded tests segments go to least loaded node
func TestRowCountBasedAssignPolicy_AssignSegment_PriorityToLeastLoaded(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	// Add 3 nodes
	for i := int64(1); i <= 3; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Version:  common.Version,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	// Setup existing distribution: node 1 has most rows, node 3 has least
	dist.SegmentDistManager.Update(1, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 100, NumOfRows: 10000}},
		{SegmentInfo: &datapb.SegmentInfo{ID: 101, NumOfRows: 10000}},
	}...)

	dist.SegmentDistManager.Update(2, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 102, NumOfRows: 5000}},
	}...)

	// Node 3 has no segments (0 rows)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	// Assign one new segment
	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000}},
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))
	// Should assign to node 3 (least loaded)
	assert.Equal(t, int64(3), plans[0].To, "Segment should go to least loaded node (node 3)")
}

// TestRowCountBasedAssignPolicy_AssignSegment_MultipleSegments tests multiple segments distribution
func TestRowCountBasedAssignPolicy_AssignSegment_MultipleSegments(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	// Add 2 nodes with equal initial load
	for i := int64(1); i <= 2; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Version:  common.Version,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	// Assign 4 segments of equal size
	segments := make([]*meta.Segment, 4)
	for i := 0; i < 4; i++ {
		segments[i] = &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        int64(i + 1),
				NumOfRows: 1000,
			},
		}
	}

	nodes := []int64{1, 2}
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 4, len(plans))

	// Count assignments - should be balanced (2 per node)
	nodeCount := make(map[int64]int)
	for _, plan := range plans {
		nodeCount[plan.To]++
	}

	assert.Equal(t, 2, nodeCount[1], "Node 1 should get 2 segments")
	assert.Equal(t, 2, nodeCount[2], "Node 2 should get 2 segments")
}

// TestRowCountBasedAssignPolicy_AssignSegment_EmptyNodes tests with no nodes
func TestRowCountBasedAssignPolicy_AssignSegment_EmptyNodes(t *testing.T) {
	nodeManager := session.NewNodeManager()
	dist := meta.NewDistributionManager(nodeManager)

	policy := newRowCountBasedAssignPolicy(nodeManager, nil, dist)

	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000}},
	}
	nodes := []int64{}

	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.Nil(t, plans)
}

// TestRowCountBasedAssignPolicy_AssignSegment_ConvertToNodeItems tests node items creation
func TestRowCountBasedAssignPolicy_AssignSegment_ConvertToNodeItems(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	// Add 2 nodes
	for i := int64(1); i <= 2; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Version:  common.Version,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	// Setup distribution
	dist.SegmentDistManager.Update(1, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 100, NumOfRows: 5000}},
	}...)

	dist.SegmentDistManager.Update(2, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 101, NumOfRows: 3000}},
	}...)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	nodes := []int64{1, 2}
	nodeItems := policy.convertToNodeItemsBySegment(nodes)

	assert.NotNil(t, nodeItems)
	assert.Equal(t, 2, len(nodeItems))

	// Node 1 should have score 5000
	assert.Equal(t, float64(5000), nodeItems[1].GetCurrentScore())

	// Node 2 should have score 3000
	assert.Equal(t, float64(3000), nodeItems[2].GetCurrentScore())
}

// TestRowCountBasedAssignPolicy_AssignChannel_PriorityToLeastLoaded tests channels go to least loaded node
func TestRowCountBasedAssignPolicy_AssignChannel_PriorityToLeastLoaded(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	// Add 3 nodes
	for i := int64(1); i <= 3; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Version:  common.Version,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	// Setup existing distribution: node 1 has 2 channels, node 2 has 1, node 3 has 0
	dist.ChannelDistManager.Update(1,
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel1",
			},
		},
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel2",
			},
		},
	)

	dist.ChannelDistManager.Update(2,
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel3",
			},
		},
	)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	// Assign one new channel
	channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "new_channel",
			},
		},
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))
	// Should assign to node 3 (no channels)
	assert.Equal(t, int64(3), plans[0].To, "Channel should go to node with least channels")
}

// TestRowCountBasedAssignPolicy_AssignChannel_MultipleChannels tests multiple channels distribution
func TestRowCountBasedAssignPolicy_AssignChannel_MultipleChannels(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	// Add 2 nodes
	for i := int64(1); i <= 2; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Version:  common.Version,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	// Assign 4 channels
	channels := make([]*meta.DmChannel, 4)
	for i := 0; i < 4; i++ {
		channels[i] = &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  string(rune('a' + i)),
			},
		}
	}

	nodes := []int64{1, 2}
	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 4, len(plans))

	// Count assignments - should be balanced
	nodeCount := make(map[int64]int)
	for _, plan := range plans {
		nodeCount[plan.To]++
	}

	assert.Equal(t, 2, nodeCount[1], "Node 1 should get 2 channels")
	assert.Equal(t, 2, nodeCount[2], "Node 2 should get 2 channels")
}

// TestRowCountBasedAssignPolicy_AssignChannel_ConvertToNodeItems tests node items creation for channels
func TestRowCountBasedAssignPolicy_AssignChannel_ConvertToNodeItems(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	// Add 2 nodes
	for i := int64(1); i <= 2; i++ {
		nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Version:  common.Version,
			Address:  "localhost",
			Hostname: "node",
		}))
		nodeManager.Get(i).SetState(session.NodeStateNormal)
	}

	// Setup channel distribution
	dist.ChannelDistManager.Update(1,
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel1",
			},
		},
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel2",
			},
		},
	)

	dist.ChannelDistManager.Update(2,
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel3",
			},
		},
	)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	nodes := []int64{1, 2}
	nodeItems := policy.convertToNodeItemsByChannel(nodes)

	assert.NotNil(t, nodeItems)
	assert.Equal(t, 2, len(nodeItems))

	// Node 1 should have 2 channels
	assert.Equal(t, float64(2), nodeItems[1].GetCurrentScore())

	// Node 2 should have 1 channel
	assert.Equal(t, float64(1), nodeItems[2].GetCurrentScore())
}

// TestRowCountBasedAssignPolicy_AssignSegment_EmptySegments tests with empty segments
func TestRowCountBasedAssignPolicy_AssignSegment_EmptySegments(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "node",
		Version:  common.Version,
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	segments := []*meta.Segment{}
	nodes := []int64{1}

	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 0, len(plans))
}

// TestRowCountBasedAssignPolicy_AssignChannel_EmptyChannels tests with empty channels
func TestRowCountBasedAssignPolicy_AssignChannel_EmptyChannels(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "node",
		Version:  common.Version,
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	channels := []*meta.DmChannel{}
	nodes := []int64{1}

	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 0, len(plans))
}

// TestRowCountBasedAssignPolicy_WorkloadStatusOnDemandUpdate tests the on-demand workload status update mechanism
func TestRowCountBasedAssignPolicy_WorkloadStatusOnDemandUpdate(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)

	policy := newRowCountBasedAssignPolicy(nodeManager, mockScheduler, dist)

	// 1. Init status
	firstStatus := policy.getWorkloadStatus()
	firstVersion := policy.version
	assert.NotNil(t, firstStatus)

	// 2. Update without meta change
	secondStatus := policy.getWorkloadStatus()
	// Should be identical pointer
	assert.Equal(t, firstStatus, secondStatus, "Status pointer should be identical when version hasn't changed")
	assert.Equal(t, firstVersion, policy.version)

	// 3. Update with segment meta change
	dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 100}, Node: 1})

	// 4. Update again
	thirdStatus := policy.getWorkloadStatus()
	// Should be new pointer
	assert.NotEqual(t, firstStatus, thirdStatus, "Status should be refreshed when segment version changed")
	assert.Greater(t, policy.version, firstVersion)

	secondVersion := policy.version

	// 5. Update with channel meta change
	dist.ChannelDistManager.Update(1, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "v1"}, Node: 1, View: &meta.LeaderView{ID: 1}})

	// 6. Update again
	fourthStatus := policy.getWorkloadStatus()
	// Should be new pointer
	assert.NotEqual(t, thirdStatus, fourthStatus, "Status should be refreshed when channel version changed")
	assert.Greater(t, policy.version, secondVersion)
}
