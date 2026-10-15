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
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestScoreBasedAssignPolicy_AssignSegment_BasicFunctionality tests basic segment assignment
func TestScoreBasedAssignPolicy_AssignSegment_BasicFunctionality(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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

func TestScoreBasedAssignPolicy_WorkloadStatusWithCollectionRows(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Version:  common.Version,
		Address:  "localhost",
		Hostname: "node",
	}))
	nodeManager.Get(1).UpdateStats(session.WithMemCapacity(100))
	nodeManager.Get(1).SetState(session.NodeStateNormal)

	dist.SegmentDistManager.Update(1,
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 100, NumOfRows: 10},
			Node:        1,
		},
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 200, NumOfRows: 30},
			Node:        1,
		},
	)
	dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "ch-100"},
		Node:         1,
		View: &meta.LeaderView{
			ID:               1,
			CollectionID:     100,
			Channel:          "ch-100",
			NumOfGrowingRows: 5,
		},
	})

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)
	status := policy.getWorkloadStatus()

	assert.Equal(t, 40, status.nodeGlobalRowCount[1])
	assert.Equal(t, 5, status.nodeGlobalChannelRowCount[1])
	assert.Equal(t, 15, status.nodeCollectionRowCount[1][100])
	assert.Equal(t, 30, status.nodeCollectionRowCount[1][200])
	assert.Equal(t, 1, status.nodeDelegatorCount[1][100])
}

func TestScoreBasedAssignPolicy_ChannelOnlyCollectionRowsAndDelegatorOverhead(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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

	dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 100, ChannelName: "ch-100"},
		Node:         1,
		View: &meta.LeaderView{
			ID:               1,
			CollectionID:     100,
			Channel:          "ch-100",
			NumOfGrowingRows: 10,
		},
	})

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)
	status := policy.getWorkloadStatus()
	assert.Equal(t, 10, status.nodeCollectionRowCount[1][100])
	assert.Equal(t, 1, status.nodeDelegatorCount[1][100])

	nodeItems := policy.ConvertToNodeItemsBySegment(100, []int64{1})
	assert.Greater(t, nodeItems[1].GetCurrentScore(), float64(11))
	ctx := WithPlanningLedger(context.Background(), NewPlanningLedger(nodeManager, dist, mockScheduler, 8))
	projected := ConvertSegmentNodeItems(ctx, policy, 100, []int64{1})
	assert.Equal(t, nodeItems[1].GetCurrentScore(), projected[1].GetCurrentScore(), "ledger must preserve growing-row and delegator score semantics")
	assert.Equal(t, nodeItems[1].GetAssignedScore(), projected[1].GetAssignedScore(), "ledger must preserve node quota semantics")
}

// TestScoreBasedAssignPolicy_AssignChannel_BasicFunctionality tests basic channel assignment
func TestScoreBasedAssignPolicy_AssignChannel_BasicFunctionality(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
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

// TestScoreBasedAssignPolicy_WorkloadStatusOnDemandUpdate tests the on-demand workload status update mechanism
func TestScoreBasedAssignPolicy_WorkloadStatusOnDemandUpdate(t *testing.T) {
	nodeManager := session.NewNodeManager()
	mockScheduler := task.NewMockScheduler(t)
	mockScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	mockScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	dist := meta.NewDistributionManager(nodeManager)
	metaMgr := meta.NewMeta(nil, nil, nodeManager)

	policy := newScoreBasedAssignPolicy(nodeManager, mockScheduler, dist, metaMgr)

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

func TestScoreBasedAssignPolicy_AssignSegment_RepairsSourcePriority(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryCoordCfg
	for key, value := range map[string]string{
		cfg.GlobalRowCountFactor.Key:             "0.1",
		cfg.ScoreUnbalanceTolerationFactor.Key:   "0.05",
		cfg.ReverseUnbalanceTolerationFactor.Key: "1.3",
		cfg.BalanceSegmentBatchSize.Key:          "2",
	} {
		original := paramtable.Get().GetWithDefault(key, "")
		paramtable.Get().Save(key, value)
		t.Cleanup(func() { paramtable.Get().Save(key, original) })
	}
	nodeManager, scheduler, dist := newHeapRegressionAssignmentFixture(t, 4)
	policy := newScoreBasedAssignPolicy(nodeManager, scheduler, dist, meta.NewMeta(nil, nil, nodeManager))
	collectionRows := []int64{0, 0, 70, 10}
	otherRows := []int64{0, 400, 230, 490}
	var segments []*meta.Segment
	for i := range collectionRows {
		node := int64(i + 1)
		main := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: node, CollectionID: 100, NumOfRows: collectionRows[i]}, Node: node}
		other := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: node + 10, CollectionID: 200, NumOfRows: otherRows[i]}, Node: node}
		dist.SegmentDistManager.Update(node, main, other)
		if collectionRows[i] > 0 {
			segments = append(segments, main)
		}
	}
	// Scores are [0,40,100,60], with quota 50. Moving 70 rows from 3
	// to 1 lowers the still-queued source below node 2; the next target is 3.
	plans := policy.AssignSegment(context.Background(), 100, segments, []int64{1, 2, 3, 4}, false)
	require.Len(t, plans, 2)
	assert.Equal(t, int64(1), plans[0].To)
	assert.Equal(t, int64(3), plans[1].To)
	assert.Equal(t, int64(3), plans[0].From)
	assert.Equal(t, int64(4), plans[1].From)
}

func newHeapRegressionAssignmentFixture(t *testing.T, nodeCount int) (*session.NodeManager, *task.MockScheduler, *meta.DistributionManager) {
	t.Helper()
	nodeManager := session.NewNodeManager()
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	scheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	for node := int64(1); node <= int64(nodeCount); node++ {
		info := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: node, Version: common.Version})
		info.SetState(session.NodeStateNormal)
		nodeManager.Add(info)
	}
	return nodeManager, scheduler, meta.NewDistributionManager(nodeManager)
}

func mockHeapRegressionWALLocations(t *testing.T) {
	t.Helper()
	streaming := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	t.Cleanup(func() { streaming.UnPatch() })
	sqns := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDs).Return(typeutil.NewUniqueSet(1, 2, 3)).Build()
	t.Cleanup(func() { sqns.UnPatch() })
	wal := mockey.Mock((*snmanager.StreamingNodeManager).GetLatestWALLocated).To(func(_ *snmanager.StreamingNodeManager, _ context.Context, channel string) (int64, error) {
		if channel == "wal_v1" || channel == "wal2_v1" {
			return 1, nil
		}
		return 4, nil // Another replica owns the fallback channel's WAL node.
	}).Build()
	t.Cleanup(func() { wal.UnPatch() })
}

func TestScoreBasedAssignPolicy_AssignChannel_WALThenFallback(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryCoordCfg
	original := cfg.CollectionChannelCountFactor.GetValue()
	paramtable.Get().Save(cfg.CollectionChannelCountFactor.Key, "10")
	t.Cleanup(func() { paramtable.Get().Save(cfg.CollectionChannelCountFactor.Key, original) })
	mockHeapRegressionWALLocations(t)
	nodeManager, scheduler, dist := newHeapRegressionAssignmentFixture(t, 3)
	policy := newScoreBasedAssignPolicy(nodeManager, scheduler, dist, meta.NewMeta(nil, nil, nodeManager))
	channels := []*meta.DmChannel{
		{VchannelInfo: &datapb.VchannelInfo{ChannelName: "wal_v1", CollectionID: 100}, Node: 3},
		{VchannelInfo: &datapb.VchannelInfo{ChannelName: "fallback_v1", CollectionID: 100}, Node: 3},
	}
	node3Channels := append([]*meta.DmChannel{}, channels...)
	for i := 0; i < 5; i++ {
		node3Channels = append(node3Channels, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: fmt.Sprintf("other%d_v1", i), CollectionID: 200}, Node: 3})
	}
	dist.ChannelDistManager.Update(3, node3Channels...)
	dist.ChannelDistManager.Update(2, &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: "other_v1", CollectionID: 200}, Node: 2})
	// Scores [0,1,25]: after WAL moves 10 from 3 to 1, fallback must
	// choose 2 rather than the previously-minimal WAL target still in the heap.
	plans := policy.AssignChannel(context.Background(), 100, channels, []int64{1, 2, 3}, false)
	require.Len(t, plans, 2)
	assert.Equal(t, int64(1), plans[0].To)
	assert.Equal(t, int64(2), plans[1].To)
}
