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
	"math"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// ScoreBasedAssignPolicy implements score-based assignment strategy for both segments and channels
// It uses comprehensive scoring mechanism considering collection row count, global row count,
// delegator overhead, and benefit evaluation
type ScoreBasedAssignPolicy struct {
	nodeManager *session.NodeManager
	scheduler   task.Scheduler
	dist        *meta.DistributionManager
	meta        *meta.Meta

	mu      sync.Mutex
	status  *workloadStatus
	version int64
}

type workloadStatus struct {
	nodeGlobalRowCount        map[int64]int
	nodeGlobalChannelRowCount map[int64]int
	nodeGlobalChannels        map[int64][]*meta.DmChannel
}

// getWorkloadStatus refreshes and returns the workload status if the underlying distribution version has changed.
func (p *ScoreBasedAssignPolicy) getWorkloadStatus() *workloadStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	currVer := p.dist.SegmentDistManager.GetVersion() + p.dist.ChannelDistManager.GetVersion()
	if currVer == p.version && p.status != nil {
		return p.status
	}

	status := &workloadStatus{
		nodeGlobalRowCount:        make(map[int64]int),
		nodeGlobalChannelRowCount: make(map[int64]int),
		nodeGlobalChannels:        make(map[int64][]*meta.DmChannel),
	}

	allSegments := p.dist.SegmentDistManager.GetByFilter()
	for _, s := range allSegments {
		status.nodeGlobalRowCount[s.Node] += int(s.GetNumOfRows())
	}

	allChannels := p.dist.ChannelDistManager.GetByFilter()
	for _, ch := range allChannels {
		status.nodeGlobalChannels[ch.Node] = append(status.nodeGlobalChannels[ch.Node], ch)
		if ch.View != nil {
			status.nodeGlobalChannelRowCount[ch.Node] += int(ch.View.NumOfGrowingRows)
		}
	}

	p.status = status
	p.version = currVer
	return p.status
}

// newScoreBasedAssignPolicy creates a new ScoreBasedAssignPolicy
// This is a private constructor. Use GetGlobalAssignPolicyFactory().GetPolicy() to create instances.
func newScoreBasedAssignPolicy(
	nodeManager *session.NodeManager,
	scheduler task.Scheduler,
	dist *meta.DistributionManager,
	meta *meta.Meta,
) *ScoreBasedAssignPolicy {
	return &ScoreBasedAssignPolicy{
		nodeManager: nodeManager,
		scheduler:   scheduler,
		dist:        dist,
		meta:        meta,
		version:     -1,
	}
}

// AssignSegment assigns segments to nodes using score-based strategy with benefit evaluation
func (p *ScoreBasedAssignPolicy) AssignSegment(
	ctx context.Context,
	collectionID int64,
	segments []*meta.Segment,
	nodes []int64,
	forceAssign bool,
) []SegmentAssignPlan {
	balanceBatchSize := math.MaxInt64

	// Filter nodes
	if !forceAssign {
		nodeFilter := newCommonSegmentNodeFilter(p.nodeManager)
		filteredNodes := nodeFilter.FilterNodes(ctx, nodes, forceAssign)
		nodes = filteredNodes
		balanceBatchSize = paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.GetAsInt()
	}

	if len(nodes) == 0 {
		return nil
	}

	// Calculate each node's score
	nodeItemsMap := p.ConvertToNodeItemsBySegment(collectionID, nodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}

	// Create priority queue
	queue := NewPriorityQueue()
	for _, item := range nodeItemsMap {
		queue.Push(item)
	}

	// Sort segments by row count (descending), with secondary sort by node's score
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].GetNumOfRows() == segments[j].GetNumOfRows() {
			node1 := nodeItemsMap[segments[i].Node]
			node2 := nodeItemsMap[segments[j].Node]
			if node1 != nil && node2 != nil {
				return node1.getPriority() > node2.getPriority()
			}
		}
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	plans := make([]SegmentAssignPlan, 0, len(segments))
	for _, s := range segments {
		// For each segment, pick the node with the least score
		targetNode := queue.Pop().(*NodeItem)

		scoreChanges := p.CalculateSegmentScore(s)
		sourceNode := nodeItemsMap[s.Node]

		// If segment's node exists, check if there's enough benefit
		if !forceAssign && sourceNode != nil {
			evaluator := &commonScoreBasedBenefitEvaluator{}
			if !evaluator.HasEnoughBenefitForNodes(sourceNode, targetNode, scoreChanges) {
				// Push back without changes if no enough benefit
				queue.Push(targetNode)
				continue
			}
		}

		from := int64(-1)
		fromScore := int64(0)
		if sourceNode != nil {
			from = sourceNode.NodeID
			fromScore = int64(sourceNode.getPriority())
		}

		plan := SegmentAssignPlan{
			From:         from,
			To:           targetNode.NodeID,
			Segment:      s,
			FromScore:    fromScore,
			ToScore:      int64(targetNode.getPriority()),
			SegmentScore: int64(scoreChanges),
		}
		plans = append(plans, plan)

		// Update the sourceNode and targetNode's score
		if sourceNode != nil {
			sourceNode.AddCurrentScoreDelta(-scoreChanges)
		}
		targetNode.AddCurrentScoreDelta(scoreChanges)

		// Push back the updated node
		queue.Push(targetNode)

		if len(plans) >= balanceBatchSize {
			break
		}
	}

	return plans
}

// ConvertToNodeItemsBySegment creates node items with comprehensive scores
func (p *ScoreBasedAssignPolicy) ConvertToNodeItemsBySegment(collectionID int64, nodeIDs []int64) map[int64]*NodeItem {
	status := p.getWorkloadStatus()

	totalScore := 0
	nodeScoreMap := make(map[int64]*NodeItem)
	nodeMemMap := make(map[int64]float64)
	totalMemCapacity := float64(0)
	allNodeHasMemInfo := true

	for _, node := range nodeIDs {
		score := p.calculateScoreBySegment(collectionID, node, status)
		NodeItem := NewNodeItem(score, node)
		nodeScoreMap[node] = &NodeItem
		totalScore += score

		// Get memory capacity
		nodeInfo := p.nodeManager.Get(node)
		if nodeInfo != nil {
			totalMemCapacity += nodeInfo.MemCapacity()
			nodeMemMap[node] = nodeInfo.MemCapacity()
		}
		allNodeHasMemInfo = allNodeHasMemInfo && nodeInfo != nil && nodeInfo.MemCapacity() > 0
	}

	if totalScore == 0 {
		return nodeScoreMap
	}

	// Calculate average score
	average := float64(0)
	if allNodeHasMemInfo {
		// Use memory-weighted average
		average = float64(totalScore) / totalMemCapacity
	} else {
		// Use simple average
		average = float64(totalScore) / float64(len(nodeIDs))
	}

	// Set assigned score and add delegator overhead
	delegatorOverloadFactor := params.Params.QueryCoordCfg.DelegatorMemoryOverloadFactor.GetAsFloat()
	for _, node := range nodeIDs {
		if allNodeHasMemInfo {
			nodeScoreMap[node].SetAssignedScore(nodeMemMap[node] * average)
		} else {
			nodeScoreMap[node].SetAssignedScore(average)
		}

		// Add delegator overhead
		collDelegator := p.dist.ChannelDistManager.GetByFilter(
			meta.WithCollectionID2Channel(collectionID),
			meta.WithNodeID2Channel(node),
		)
		if len(collDelegator) > 0 {
			delegatorDelta := nodeScoreMap[node].GetAssignedScore() * delegatorOverloadFactor * float64(len(collDelegator))
			nodeScoreMap[node].AddCurrentScoreDelta(delegatorDelta)
		}
	}

	return nodeScoreMap
}

// calculateScoreBySegment calculates comprehensive score for a node
func (p *ScoreBasedAssignPolicy) calculateScoreBySegment(collectionID, nodeID int64, status *workloadStatus) int {
	nodeRowCount := status.nodeGlobalRowCount[nodeID] + status.nodeGlobalChannelRowCount[nodeID]

	// Calculate executing task cost in scheduler
	nodeRowCount += p.scheduler.GetSegmentTaskDelta(nodeID, -1)

	// Calculate collection sealed segment row count
	collectionSegments := p.dist.SegmentDistManager.GetByFilter(
		meta.WithCollectionID(collectionID),
		meta.WithNodeID(nodeID),
	)
	collectionRowCount := 0
	for _, s := range collectionSegments {
		collectionRowCount += int(s.GetNumOfRows())
	}

	// Calculate collection growing segment row count
	collDelegatorList := p.dist.ChannelDistManager.GetByFilter(
		meta.WithCollectionID2Channel(collectionID),
		meta.WithNodeID2Channel(nodeID),
	)
	for _, d := range collDelegatorList {
		collectionRowCount += int(d.View.NumOfGrowingRows)
	}

	// Calculate executing task cost for collection
	collectionRowCount += p.scheduler.GetSegmentTaskDelta(nodeID, collectionID)

	// Final score: collection row count + global row count * factor
	return collectionRowCount + int(float64(nodeRowCount)*
		params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

// calculateSegmentScore calculates the score of a segment
func (p *ScoreBasedAssignPolicy) CalculateSegmentScore(s *meta.Segment) float64 {
	return float64(s.GetNumOfRows()) * (1 + params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

// AssignChannel assigns channels to nodes using score-based strategy
func (p *ScoreBasedAssignPolicy) AssignChannel(
	ctx context.Context,
	collectionID int64,
	channels []*meta.DmChannel,
	nodes []int64,
	forceAssign bool,
) []ChannelAssignPlan {
	// Filter nodes
	if !forceAssign {
		nodeFilter := newCommonChannelNodeFilter(p.nodeManager)
		filteredNodes := nodeFilter.FilterNodes(ctx, nodes, forceAssign)
		nodes = filteredNodes
	}

	if len(nodes) == 0 {
		return nil
	}

	// Calculate each node's score
	nodeItemsMap := p.ConvertToNodeItemsByChannel(collectionID, nodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}

	// Create priority queue
	queue := NewPriorityQueue()
	for _, item := range nodeItemsMap {
		queue.Push(item)
	}

	plans := make([]ChannelAssignPlan, 0)
	for _, c := range channels {
		var targetNode *NodeItem
		needPushBack := false

		// Balance scenario: prefer WAL location but can fall back to score-based
		// Handle WAL location for streaming service
		targetNode = nil
		if streamingutil.IsStreamingServiceEnabled() {
			nodeID := snmanager.StaticStreamingNodeManager.GetWALLocated(c.GetChannelName())
			if item, ok := nodeItemsMap[nodeID]; ok {
				targetNode = item
			}
		}

		if targetNode == nil {
			// Fall back to priority queue if WAL node not found
			targetNode = queue.Pop().(*NodeItem)
			needPushBack = true
		}

		scoreChanges := p.CalculateChannelScore(c, collectionID)
		sourceNode := nodeItemsMap[c.Node]

		// Check benefit if source node exists
		if !forceAssign && sourceNode != nil {
			evaluator := &commonScoreBasedBenefitEvaluator{}
			if !evaluator.HasEnoughBenefitForNodes(sourceNode, targetNode, scoreChanges) {
				// Push back without changes if no enough benefit
				if needPushBack {
					queue.Push(targetNode)
				}
				continue
			}
		}

		from := int64(-1)
		fromScore := int64(0)
		if sourceNode != nil {
			from = sourceNode.NodeID
			fromScore = int64(sourceNode.getPriority())
		}

		plan := ChannelAssignPlan{
			From:         from,
			To:           targetNode.NodeID,
			Channel:      c,
			FromScore:    fromScore,
			ToScore:      int64(targetNode.getPriority()),
			ChannelScore: int64(scoreChanges),
		}
		plans = append(plans, plan)

		// Update scores
		if sourceNode != nil {
			sourceNode.AddCurrentScoreDelta(-scoreChanges)
		}
		targetNode.AddCurrentScoreDelta(scoreChanges)

		// Push back the updated node if it was popped from queue
		if needPushBack {
			queue.Push(targetNode)
		}
	}

	return plans
}

// ConvertToNodeItemsByChannel creates node items with channel scores
func (p *ScoreBasedAssignPolicy) ConvertToNodeItemsByChannel(collectionID int64, nodeIDs []int64) map[int64]*NodeItem {
	status := p.getWorkloadStatus()

	totalScore := 0
	nodeScoreMap := make(map[int64]*NodeItem)
	nodeMemMap := make(map[int64]float64)
	totalMemCapacity := float64(0)
	allNodeHasMemInfo := true

	for _, node := range nodeIDs {
		score := p.calculateScoreByChannel(collectionID, node, status)
		NodeItem := NewNodeItem(score, node)
		nodeScoreMap[node] = &NodeItem
		totalScore += score

		nodeInfo := p.nodeManager.Get(node)
		if nodeInfo != nil {
			totalMemCapacity += nodeInfo.MemCapacity()
			nodeMemMap[node] = nodeInfo.MemCapacity()
		}
		allNodeHasMemInfo = allNodeHasMemInfo && nodeInfo != nil && nodeInfo.MemCapacity() > 0
	}

	if totalScore == 0 {
		return nodeScoreMap
	}

	average := float64(0)
	if allNodeHasMemInfo {
		average = float64(totalScore) / totalMemCapacity
	} else {
		average = float64(totalScore) / float64(len(nodeIDs))
	}

	for _, node := range nodeIDs {
		if allNodeHasMemInfo {
			nodeScoreMap[node].SetAssignedScore(nodeMemMap[node] * average)
		} else {
			nodeScoreMap[node].SetAssignedScore(average)
		}
	}

	return nodeScoreMap
}

// calculateScoreByChannel calculates score based on channel count
func (p *ScoreBasedAssignPolicy) calculateScoreByChannel(collectionID, nodeID int64, status *workloadStatus) int {
	channels := status.nodeGlobalChannels[nodeID]

	totalScore := 0.0
	for _, ch := range channels {
		// Use CalculateChannelScore for all channels to maintain consistency
		totalScore += p.CalculateChannelScore(ch, collectionID)
	}

	// Add executing task cost
	taskDelta := p.scheduler.GetChannelTaskDelta(nodeID, -1) + p.scheduler.GetChannelTaskDelta(nodeID, collectionID)

	return int(totalScore) + taskDelta
}

// calculateChannelScore calculates the score of a channel
func (p *ScoreBasedAssignPolicy) CalculateChannelScore(ch *meta.DmChannel, currentCollection int64) float64 {
	if ch.GetCollectionID() == currentCollection {
		// Give higher weight to current collection's channels
		channelWeight := paramtable.Get().QueryCoordCfg.CollectionChannelCountFactor.GetAsFloat()
		return math.Max(1.0, channelWeight)
	}
	return 1.0
}
