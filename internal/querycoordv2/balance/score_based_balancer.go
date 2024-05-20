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

package balance

import (
	"math"
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// score based segment use (collection_row_count + global_row_count * factor) as node' score
// and try to make each node has almost same score through balance segment.
type ScoreBasedBalancer struct {
	*RowCountBasedBalancer
}

func NewScoreBasedBalancer(scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *ScoreBasedBalancer {
	return &ScoreBasedBalancer{
		RowCountBasedBalancer: NewRowCountBasedBalancer(scheduler, nodeManager, dist, meta, targetMgr),
	}
}

// AssignSegment got a segment list, and try to assign each segment to node's with lowest score
func (b *ScoreBasedBalancer) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64, manualBalance bool) []SegmentAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !manualBalance {
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			return info != nil && info.GetState() == session.NodeStateNormal
		})
	}

	// calculate each node's score
	nodeItems := b.convertToNodeItems(collectionID, nodes)
	if len(nodeItems) == 0 {
		return nil
	}

	nodeItemsMap := lo.SliceToMap(nodeItems, func(item *nodeItem) (int64, *nodeItem) { return item.nodeID, item })
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}

	// sort segments by segment row count, if segment has same row count, sort by node's score
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
		func(s *meta.Segment) {
			// for each segment, pick the node with the least score
			targetNode := queue.pop().(*nodeItem)
			// make sure candidate is always push back
			defer queue.push(targetNode)
			priorityChange := b.calculateSegmentScore(s)

			sourceNode := nodeItemsMap[s.Node]
			// if segment's node exist, which means this segment comes from balancer. we should consider the benefit
			// if the segment reassignment doesn't got enough benefit, we should skip this reassignment
			// notice: we should skip benefit check for manual balance
			if !manualBalance && sourceNode != nil && !b.hasEnoughBenefit(sourceNode, targetNode, priorityChange) {
				return
			}

			plan := SegmentAssignPlan{
				From:    -1,
				To:      targetNode.nodeID,
				Segment: s,
			}
			plans = append(plans, plan)

			// update the targetNode's score
			if sourceNode != nil {
				sourceNode.setPriority(sourceNode.getPriority() - priorityChange)
			}
			targetNode.setPriority(targetNode.getPriority() + priorityChange)
		}(s)
	}
	return plans
}

func (b *ScoreBasedBalancer) hasEnoughBenefit(sourceNode *nodeItem, targetNode *nodeItem, priorityChange int) bool {
	// if the score diff between sourceNode and targetNode is lower than the unbalance toleration factor, there is no need to assign it targetNode
	oldScoreDiff := math.Abs(float64(sourceNode.getPriority()) - float64(targetNode.getPriority()))
	if oldScoreDiff < float64(targetNode.getPriority())*params.Params.QueryCoordCfg.ScoreUnbalanceTolerationFactor.GetAsFloat() {
		return false
	}

	newSourceScore := sourceNode.getPriority() - priorityChange
	newTargetScore := targetNode.getPriority() + priorityChange
	if newTargetScore > newSourceScore {
		// if score diff reverted after segment reassignment, we will consider the benefit
		// only trigger following segment reassignment when the generated reverted score diff
		// is far smaller than the original score diff
		newScoreDiff := math.Abs(float64(newSourceScore) - float64(newTargetScore))
		if newScoreDiff*params.Params.QueryCoordCfg.ReverseUnbalanceTolerationFactor.GetAsFloat() >= oldScoreDiff {
			return false
		}
	}

	return true
}

func (b *ScoreBasedBalancer) convertToNodeItems(collectionID int64, nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, node := range nodeIDs {
		priority := b.calculateScore(collectionID, node)
		nodeItem := newNodeItem(priority, node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *ScoreBasedBalancer) calculateScore(collectionID, nodeID int64) int {
	rowCount := 0
	// calculate global sealed segment row count
	globalSegments := b.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(nodeID))
	for _, s := range globalSegments {
		rowCount += int(s.GetNumOfRows())
	}

	// calculate global growing segment row count
	views := b.dist.LeaderViewManager.GetByFilter(meta.WithNodeID2LeaderView(nodeID))
	for _, view := range views {
		rowCount += int(float64(view.NumOfGrowingRows) * params.Params.QueryCoordCfg.GrowingRowCountWeight.GetAsFloat())
	}

	collectionRowCount := 0
	// calculate collection sealed segment row count
	collectionSegments := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID), meta.WithNodeID(nodeID))
	for _, s := range collectionSegments {
		collectionRowCount += int(s.GetNumOfRows())
	}

	// calculate collection growing segment row count
	collectionViews := b.dist.LeaderViewManager.GetByFilter(meta.WithCollectionID2LeaderView(collectionID), meta.WithNodeID2LeaderView(nodeID))
	for _, view := range collectionViews {
		collectionRowCount += int(float64(view.NumOfGrowingRows) * params.Params.QueryCoordCfg.GrowingRowCountWeight.GetAsFloat())
	}
	return collectionRowCount + int(float64(rowCount)*
		params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

// calculateSegmentScore calculate the score which the segment represented
func (b *ScoreBasedBalancer) calculateSegmentScore(s *meta.Segment) int {
	return int(float64(s.GetNumOfRows()) * (1 + params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat()))
}

func (b *ScoreBasedBalancer) BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	log := log.With(
		zap.Int64("collection", replica.GetCollectionID()),
		zap.Int64("replica id", replica.GetID()),
		zap.String("replica group", replica.GetResourceGroup()),
	)
	if replica.NodesCount() == 0 {
		return nil, nil
	}

	rwNodes := replica.GetRWNodes()
	roNodes := replica.GetRONodes()

	if len(rwNodes) == 0 {
		// no available nodes to balance
		return nil, nil
	}

	// print current distribution before generating plans
	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	if len(roNodes) != 0 {
		if !paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
			log.RatedInfo(10, "stopping balance is disabled!", zap.Int64s("stoppingNode", roNodes))
			return nil, nil
		}

		log.Info("Handle stopping nodes",
			zap.Any("stopping nodes", roNodes),
			zap.Any("available nodes", rwNodes),
		)
		// handle stopped nodes here, have to assign segments on stopping nodes to nodes with the smallest score
		channelPlans = append(channelPlans, b.genStoppingChannelPlan(replica, rwNodes, roNodes)...)
		if len(channelPlans) == 0 {
			segmentPlans = append(segmentPlans, b.genStoppingSegmentPlan(replica, rwNodes, roNodes)...)
		}
	} else {
		if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
			channelPlans = append(channelPlans, b.genChannelPlan(replica, rwNodes)...)
		}

		if len(channelPlans) == 0 {
			segmentPlans = append(segmentPlans, b.genSegmentPlan(replica, rwNodes)...)
		}
	}

	return segmentPlans, channelPlans
}

func (b *ScoreBasedBalancer) genStoppingSegmentPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(nodeID))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil &&
				b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.NextTarget) != nil &&
				segment.GetLevel() != datapb.SegmentLevel_L0
		})
		plans := b.AssignSegment(replica.GetCollectionID(), segments, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		segmentPlans = append(segmentPlans, plans...)
	}
	return segmentPlans
}

func (b *ScoreBasedBalancer) genSegmentPlan(replica *meta.Replica, onlineNodes []int64) []SegmentAssignPlan {
	segmentDist := make(map[int64][]*meta.Segment)
	nodeScore := make(map[int64]int, 0)
	totalScore := 0

	// list all segment which could be balanced, and calculate node's score
	for _, node := range onlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil &&
				b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.NextTarget) != nil &&
				segment.GetLevel() != datapb.SegmentLevel_L0
		})
		segmentDist[node] = segments

		rowCount := b.calculateScore(replica.GetCollectionID(), node)
		totalScore += rowCount
		nodeScore[node] = rowCount
	}

	if totalScore == 0 {
		return nil
	}

	// find the segment from the node which has more score than the average
	segmentsToMove := make([]*meta.Segment, 0)
	average := totalScore / len(onlineNodes)
	for node, segments := range segmentDist {
		leftScore := nodeScore[node]
		if leftScore <= average {
			continue
		}

		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
		})
		for _, s := range segments {
			segmentsToMove = append(segmentsToMove, s)
			leftScore -= b.calculateSegmentScore(s)
			if leftScore <= average {
				break
			}
		}
	}

	// if the segment are redundant, skip it's balance for now
	segmentsToMove = lo.Filter(segmentsToMove, func(s *meta.Segment, _ int) bool {
		return len(b.dist.SegmentDistManager.GetByFilter(meta.WithReplica(replica), meta.WithSegmentID(s.GetID()))) == 1
	})

	if len(segmentsToMove) == 0 {
		return nil
	}

	segmentPlans := b.AssignSegment(replica.GetCollectionID(), segmentsToMove, onlineNodes, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}
