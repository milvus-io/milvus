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

// TestScoreBasedAssignPolicy_AssignSegment_BasicFunctionality tests basic segment assignment
func TestScoreBasedAssignPolicy_AssignSegment_BasicFunctionality(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

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

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	// Create test segments
	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000, CollectionID: 100}},
		{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 2000, CollectionID: 100}},
		{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1500, CollectionID: 100}},
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 3, len(plans))

	// Verify all segments are assigned
	for i, plan := range plans {
		assert.Equal(t, segments[i].ID, plan.Segment.ID)
		assert.Contains(t, nodes, plan.To)
		assert.Equal(t, int64(-1), plan.From)
	}
}

// TestScoreBasedAssignPolicy_AssignSegment_PrefersLowScoreNode tests assignment to low score nodes
func TestScoreBasedAssignPolicy_AssignSegment_PrefersLowScoreNode(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

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

	// Setup: node 1 has heavy load, node 3 has no load
	dist.SegmentDistManager.Update(1, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 100, NumOfRows: 50000, CollectionID: 100}},
		{SegmentInfo: &datapb.SegmentInfo{ID: 101, NumOfRows: 50000, CollectionID: 100}},
	}...)

	dist.SegmentDistManager.Update(2, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 102, NumOfRows: 10000, CollectionID: 100}},
	}...)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	// Assign one segment
	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000, CollectionID: 100}},
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 1, len(plans))
	// Should prefer node 3 (lowest load)
	assert.Equal(t, int64(3), plans[0].To)
}

// TestScoreBasedAssignPolicy_AssignSegment_EmptyNodes tests with no nodes
func TestScoreBasedAssignPolicy_AssignSegment_EmptyNodes(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000}},
	}
	nodes := []int64{}

	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.Nil(t, plans)
}

// TestScoreBasedAssignPolicy_AssignSegment_EmptySegments tests with no segments
func TestScoreBasedAssignPolicy_AssignSegment_EmptySegments(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Version:  common.Version,
		Address:  "localhost",
		Hostname: "node",
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	segments := []*meta.Segment{}
	nodes := []int64{1}

	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 0, len(plans))
}

// TestScoreBasedAssignPolicy_AssignSegment_ForceAssign tests force assignment behavior
func TestScoreBasedAssignPolicy_AssignSegment_ForceAssign(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	// Add node in non-normal state
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Version:  common.Version,
		Address:  "localhost",
		Hostname: "node",
	}))
	nodeManager.Get(1).SetState(session.NodeStateStopping)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	segments := []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000}},
	}
	nodes := []int64{1}

	// Without force assign - should return nil (no normal nodes)
	plans := policy.AssignSegment(context.Background(), 100, segments, nodes, false)
	assert.Nil(t, plans)

	// With force assign - should proceed
	plansForced := policy.AssignSegment(context.Background(), 100, segments, nodes, true)
	assert.NotNil(t, plansForced)
	assert.Equal(t, 1, len(plansForced))
	assert.Equal(t, int64(1), plansForced[0].To)
}

// TestScoreBasedAssignPolicy_CalculateSegmentScore tests segment score calculation
func TestScoreBasedAssignPolicy_CalculateSegmentScore(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	segment := &meta.Segment{
		SegmentInfo: &datapb.SegmentInfo{
			ID:        1,
			NumOfRows: 1000,
		},
	}

	score := policy.CalculateSegmentScore(segment)

	// Score should be greater than just the row count
	// (includes GlobalRowCountFactor)
	assert.Greater(t, score, float64(1000))
}

// TestScoreBasedAssignPolicy_ConvertToNodeItemsBySegment tests node item creation
func TestScoreBasedAssignPolicy_ConvertToNodeItemsBySegment(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

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
		{SegmentInfo: &datapb.SegmentInfo{ID: 100, NumOfRows: 5000, CollectionID: 100}},
	}...)

	dist.SegmentDistManager.Update(2, []*meta.Segment{
		{SegmentInfo: &datapb.SegmentInfo{ID: 101, NumOfRows: 3000, CollectionID: 100}},
	}...)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	nodes := []int64{1, 2}
	nodeItems := policy.ConvertToNodeItemsBySegment(100, nodes)

	assert.NotNil(t, nodeItems)
	assert.Equal(t, 2, len(nodeItems))

	// Both nodes should have assigned scores set
	assert.Greater(t, nodeItems[1].GetAssignedScore(), float64(0))
	assert.Greater(t, nodeItems[2].GetAssignedScore(), float64(0))
}

// TestScoreBasedAssignPolicy_AssignChannel_BasicFunctionality tests basic channel assignment
func TestScoreBasedAssignPolicy_AssignChannel_BasicFunctionality(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

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

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	// Create test channels
	channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel1",
			},
		},
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel2",
			},
		},
	}

	nodes := []int64{1, 2, 3}
	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 2, len(plans))

	// Verify all channels are assigned
	for i, plan := range plans {
		assert.Equal(t, channels[i].GetChannelName(), plan.Channel.GetChannelName())
		assert.Contains(t, nodes, plan.To)
	}
}

// TestScoreBasedAssignPolicy_AssignChannel_PrefersLowScoreNode tests channel assignment to low score nodes
func TestScoreBasedAssignPolicy_AssignChannel_PrefersLowScoreNode(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

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

	// Setup: node 1 has many channels, node 3 has none
	dist.ChannelDistManager.Update(1,
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "existing1",
			},
		},
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "existing2",
			},
		},
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "existing3",
			},
		},
	)

	dist.ChannelDistManager.Update(2,
		&meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "existing4",
			},
		},
	)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	// Assign one channel
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
	// Should prefer node 3 (no channels)
	assert.Equal(t, int64(3), plans[0].To)
}

// TestScoreBasedAssignPolicy_CalculateChannelScore tests channel score calculation
func TestScoreBasedAssignPolicy_CalculateChannelScore(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	channel := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "channel1",
		},
	}

	// Score for current collection should be >= 1.0
	scoreCurrentCollection := policy.CalculateChannelScore(channel, 100)
	assert.GreaterOrEqual(t, scoreCurrentCollection, float64(1.0))

	// Score for other collection should be 1.0
	scoreOtherCollection := policy.CalculateChannelScore(channel, 200)
	assert.Equal(t, float64(1.0), scoreOtherCollection)
}

// TestScoreBasedAssignPolicy_ConvertToNodeItemsByChannel tests node item creation for channels
func TestScoreBasedAssignPolicy_ConvertToNodeItemsByChannel(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

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

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	nodes := []int64{1, 2}
	nodeItems := policy.ConvertToNodeItemsByChannel(100, nodes)

	assert.NotNil(t, nodeItems)
	assert.Equal(t, 2, len(nodeItems))

	// Both nodes should have assigned scores set
	assert.Greater(t, nodeItems[1].GetAssignedScore(), float64(0))
	assert.Greater(t, nodeItems[2].GetAssignedScore(), float64(0))
}

// TestScoreBasedAssignPolicy_AssignChannel_EmptyChannels tests with no channels
func TestScoreBasedAssignPolicy_AssignChannel_EmptyChannels(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Version:  common.Version,
		Address:  "localhost",
		Hostname: "node",
	}))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	channels := []*meta.DmChannel{}
	nodes := []int64{1}

	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.NotNil(t, plans)
	assert.Equal(t, 0, len(plans))
}

// TestScoreBasedAssignPolicy_AssignChannel_EmptyNodes tests with no nodes
func TestScoreBasedAssignPolicy_AssignChannel_EmptyNodes(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

	channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "channel1",
			},
		},
	}
	nodes := []int64{}

	plans := policy.AssignChannel(context.Background(), 100, channels, nodes, false)

	assert.Nil(t, plans)
}
