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
	"fmt"
	"math"
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"

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
	br := NewBalanceReport()
	return b.assignSegment(br, collectionID, segments, nodes, manualBalance)
}

func (b *ScoreBasedBalancer) assignSegment(br *balanceReport, collectionID int64, segments []*meta.Segment, nodes []int64, manualBalance bool) []SegmentAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !manualBalance {
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			normalNode := info != nil && info.GetState() == session.NodeStateNormal
			if !normalNode {
				br.AddRecord(StrRecord(fmt.Sprintf("non-manual balance, skip abnormal node: %d", node)))
			}
			return normalNode
		})
	}

	// calculate each node's score
	nodeItemsMap := b.convertToNodeItemsBySegment(br, collectionID, nodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}
	log.Info("node workload status", zap.Int64("collectionID", collectionID), zap.Stringers("nodes", lo.Values(nodeItemsMap)))

	queue := newPriorityQueue()
	for _, item := range nodeItemsMap {
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

	balanceBatchSize := paramtable.Get().QueryCoordCfg.CollectionBalanceSegmentBatchSize.GetAsInt()
	plans := make([]SegmentAssignPlan, 0, len(segments))
	for _, s := range segments {
		func(s *meta.Segment) {
			// for each segment, pick the node with the least score
			targetNode := queue.pop().(*nodeItem)
			// make sure candidate is always push back
			defer queue.push(targetNode)
			scoreChanges := b.calculateSegmentScore(s)

			sourceNode := nodeItemsMap[s.Node]
			// if segment's node exist, which means this segment comes from balancer. we should consider the benefit
			// if the segment reassignment doesn't got enough benefit, we should skip this reassignment
			// notice: we should skip benefit check for manual balance
			if !manualBalance && sourceNode != nil && !b.hasEnoughBenefit(sourceNode, targetNode, scoreChanges) {
				br.AddRecord(StrRecordf("skip generate balance plan for segment %d since no enough benefit", s.ID))
				return
			}

			from := int64(-1)
			fromScore := int64(0)
			if sourceNode != nil {
				from = sourceNode.nodeID
				fromScore = int64(sourceNode.getPriority())
			}

			plan := SegmentAssignPlan{
				From:         from,
				To:           targetNode.nodeID,
				Segment:      s,
				FromScore:    fromScore,
				ToScore:      int64(targetNode.getPriority()),
				SegmentScore: int64(scoreChanges),
			}
			br.AddRecord(StrRecordf("add segment plan %s", plan))
			plans = append(plans, plan)

			// update the sourceNode and targetNode's score
			if sourceNode != nil {
				sourceNode.AddCurrentScoreDelta(-scoreChanges)
			}
			targetNode.AddCurrentScoreDelta(scoreChanges)
		}(s)

		if len(plans) > balanceBatchSize {
			break
		}
	}
	return plans
}

func (b *ScoreBasedBalancer) AssignChannel(collectionID int64, channels []*meta.DmChannel, nodes []int64, manualBalance bool) []ChannelAssignPlan {
	br := NewBalanceReport()
	return b.assignChannel(br, collectionID, channels, nodes, manualBalance)
}

func (b *ScoreBasedBalancer) assignChannel(br *balanceReport, collectionID int64, channels []*meta.DmChannel, nodes []int64, manualBalance bool) []ChannelAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !manualBalance {
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			normalNode := info != nil && info.GetState() == session.NodeStateNormal
			if !normalNode {
				br.AddRecord(StrRecord(fmt.Sprintf("non-manual balance, skip abnormal node: %d", node)))
			}
			return normalNode
		})
	}

	// calculate each node's score
	nodeItemsMap := b.convertToNodeItemsByChannel(br, collectionID, nodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}

	queue := newPriorityQueue()
	for _, item := range nodeItemsMap {
		queue.push(item)
	}

	balanceBatchSize := paramtable.Get().QueryCoordCfg.CollectionBalanceSegmentBatchSize.GetAsInt()
	plans := make([]ChannelAssignPlan, 0, len(channels))
	for _, ch := range channels {
		func(ch *meta.DmChannel) {
			// for each channel, pick the node with the least score
			targetNode := queue.pop().(*nodeItem)
			// make sure candidate is always push back
			defer queue.push(targetNode)
			scoreChanges := b.calculateChannelScore(ch, collectionID)

			sourceNode := nodeItemsMap[ch.Node]
			// if segment's node exist, which means this segment comes from balancer. we should consider the benefit
			// if the segment reassignment doesn't got enough benefit, we should skip this reassignment
			// notice: we should skip benefit check for manual balance
			if !manualBalance && sourceNode != nil && !b.hasEnoughBenefit(sourceNode, targetNode, scoreChanges) {
				br.AddRecord(StrRecordf("skip generate balance plan for channel %s since no enough benefit", ch.GetChannelName()))
				return
			}

			from := int64(-1)
			fromScore := int64(0)
			if sourceNode != nil {
				from = sourceNode.nodeID
				fromScore = int64(sourceNode.getPriority())
			}

			plan := ChannelAssignPlan{
				From:         from,
				To:           targetNode.nodeID,
				Channel:      ch,
				FromScore:    fromScore,
				ToScore:      int64(targetNode.getPriority()),
				ChannelScore: int64(scoreChanges),
			}
			br.AddRecord(StrRecordf("add segment plan %s", plan))
			plans = append(plans, plan)

			// update the sourceNode and targetNode's score
			if sourceNode != nil {
				sourceNode.AddCurrentScoreDelta(-scoreChanges)
			}
			targetNode.AddCurrentScoreDelta(scoreChanges)
		}(ch)

		if len(plans) > balanceBatchSize {
			break
		}
	}

	return plans
}

func (b *ScoreBasedBalancer) hasEnoughBenefit(sourceNode *nodeItem, targetNode *nodeItem, scoreChanges float64) bool {
	// if the score diff between sourceNode and targetNode is lower than the unbalance toleration factor, there is no need to assign it targetNode
	oldPriorityDiff := math.Abs(float64(sourceNode.getPriority()) - float64(targetNode.getPriority()))
	if oldPriorityDiff < float64(targetNode.getPriority())*params.Params.QueryCoordCfg.ScoreUnbalanceTolerationFactor.GetAsFloat() {
		return false
	}

	newSourcePriority := sourceNode.getPriorityWithCurrentScoreDelta(-scoreChanges)
	newTargetPriority := targetNode.getPriorityWithCurrentScoreDelta(scoreChanges)
	if newTargetPriority > newSourcePriority {
		// if score diff reverted after segment reassignment, we will consider the benefit
		// only trigger following segment reassignment when the generated reverted score diff
		// is far smaller than the original score diff
		newScoreDiff := math.Abs(float64(newSourcePriority) - float64(newTargetPriority))
		if newScoreDiff*params.Params.QueryCoordCfg.ReverseUnbalanceTolerationFactor.GetAsFloat() >= oldPriorityDiff {
			return false
		}
	}

	return true
}

func (b *ScoreBasedBalancer) convertToNodeItemsBySegment(br *balanceReport, collectionID int64, nodeIDs []int64) map[int64]*nodeItem {
	totalScore := 0
	nodeScoreMap := make(map[int64]*nodeItem)
	nodeMemMap := make(map[int64]float64)
	totalMemCapacity := float64(0)
	allNodeHasMemInfo := true
	for _, node := range nodeIDs {
		score := b.calculateScoreBySegment(br, collectionID, node)
		nodeItem := newNodeItem(score, node)
		nodeScoreMap[node] = &nodeItem
		totalScore += score
		br.AddNodeItem(nodeScoreMap[node])

		// set memory default to 1.0, will multiply average value to compute assigned score
		nodeInfo := b.nodeManager.Get(node)
		if nodeInfo != nil {
			totalMemCapacity += nodeInfo.MemCapacity()
			nodeMemMap[node] = nodeInfo.MemCapacity()
		}
		allNodeHasMemInfo = allNodeHasMemInfo && nodeInfo != nil && nodeInfo.MemCapacity() > 0
	}

	if totalScore == 0 {
		return nodeScoreMap
	}

	// if all node has memory info, we will use totalScore / totalMemCapacity to calculate the score, then average means average score on memory unit
	// otherwise, we will use totalScore / len(nodeItemsMap) to calculate the score, then average means average score on node unit
	average := float64(0)
	if allNodeHasMemInfo {
		average = float64(totalScore) / totalMemCapacity
	} else {
		average = float64(totalScore) / float64(len(nodeIDs))
	}

	delegatorOverloadFactor := params.Params.QueryCoordCfg.DelegatorMemoryOverloadFactor.GetAsFloat()
	for _, node := range nodeIDs {
		if allNodeHasMemInfo {
			nodeScoreMap[node].setAssignedScore(nodeMemMap[node] * average)
			br.SetMemoryFactor(node, nodeMemMap[node])
		} else {
			nodeScoreMap[node].setAssignedScore(average)
		}
		// use assignedScore * delegatorOverloadFactor * delegator_num, to preserve fixed memory size for delegator
		collectionViews := b.dist.LeaderViewManager.GetByFilter(meta.WithCollectionID2LeaderView(collectionID), meta.WithNodeID2LeaderView(node))
		if len(collectionViews) > 0 {
			delegatorDelta := nodeScoreMap[node].getAssignedScore() * delegatorOverloadFactor * float64(len(collectionViews))
			nodeScoreMap[node].AddCurrentScoreDelta(delegatorDelta)
			br.SetDeletagorScore(node, delegatorDelta)
		}
	}
	return nodeScoreMap
}

func (b *ScoreBasedBalancer) convertToNodeItemsByChannel(br *balanceReport, collectionID int64, nodeIDs []int64) map[int64]*nodeItem {
	totalScore := 0
	nodeScoreMap := make(map[int64]*nodeItem)
	nodeMemMap := make(map[int64]float64)
	totalMemCapacity := float64(0)
	allNodeHasMemInfo := true
	for _, node := range nodeIDs {
		score := b.calculateScoreByChannel(br, collectionID, node)
		nodeItem := newNodeItem(score, node)
		nodeScoreMap[node] = &nodeItem
		totalScore += score
		br.AddNodeItem(nodeScoreMap[node])

		// set memory default to 1.0, will multiply average value to compute assigned score
		nodeInfo := b.nodeManager.Get(node)
		if nodeInfo != nil {
			totalMemCapacity += nodeInfo.MemCapacity()
			nodeMemMap[node] = nodeInfo.MemCapacity()
		}
		allNodeHasMemInfo = allNodeHasMemInfo && nodeInfo != nil && nodeInfo.MemCapacity() > 0
	}

	if totalScore == 0 {
		return nodeScoreMap
	}

	// if all node has memory info, we will use totalScore / totalMemCapacity to calculate the score, then average means average score on memory unit
	// otherwise, we will use totalScore / len(nodeItemsMap) to calculate the score, then average means average score on node unit
	average := float64(0)
	if allNodeHasMemInfo {
		average = float64(totalScore) / totalMemCapacity
	} else {
		average = float64(totalScore) / float64(len(nodeIDs))
	}

	for _, node := range nodeIDs {
		if allNodeHasMemInfo {
			nodeScoreMap[node].setAssignedScore(nodeMemMap[node] * average)
			br.SetMemoryFactor(node, nodeMemMap[node])
		} else {
			nodeScoreMap[node].setAssignedScore(average)
		}
	}
	return nodeScoreMap
}

func (b *ScoreBasedBalancer) calculateScoreBySegment(br *balanceReport, collectionID, nodeID int64) int {
	nodeRowCount := 0
	// calculate global sealed segment row count
	globalSegments := b.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(nodeID))
	for _, s := range globalSegments {
		nodeRowCount += int(s.GetNumOfRows())
	}

	// calculate global growing segment row count
	views := b.dist.LeaderViewManager.GetByFilter(meta.WithNodeID2LeaderView(nodeID))
	for _, view := range views {
		nodeRowCount += int(float64(view.NumOfGrowingRows))
	}

	// calculate executing task cost in scheduler
	nodeRowCount += b.scheduler.GetSegmentTaskDelta(nodeID, -1)

	collectionRowCount := 0
	// calculate collection sealed segment row count
	collectionSegments := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID), meta.WithNodeID(nodeID))
	for _, s := range collectionSegments {
		collectionRowCount += int(s.GetNumOfRows())
	}

	// calculate collection growing segment row count
	collectionViews := b.dist.LeaderViewManager.GetByFilter(meta.WithCollectionID2LeaderView(collectionID), meta.WithNodeID2LeaderView(nodeID))
	for _, view := range collectionViews {
		collectionRowCount += int(float64(view.NumOfGrowingRows))
	}

	// calculate executing task cost in scheduler
	collectionRowCount += b.scheduler.GetSegmentTaskDelta(nodeID, collectionID)

	br.AddDetailRecord(StrRecordf("Calcalute score for collection %d on node %d, global row count: %d, collection row count: %d",
		collectionID, nodeID, nodeRowCount, collectionRowCount))

	return collectionRowCount + int(float64(nodeRowCount)*
		params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

func (b *ScoreBasedBalancer) calculateScoreByChannel(br *balanceReport, collectionID, nodeID int64) int {
	// calculate global sealed segment row count
	channels := b.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))

	nodeChannelNum, collectionChannelNum := 0, 0
	for _, ch := range channels {
		if ch.GetCollectionID() == collectionID {
			collectionChannelNum += 1
		} else {
			nodeChannelNum += int(b.calculateChannelScore(ch, -1))
		}
	}

	// calculate executing task cost in scheduler
	nodeChannelNum += b.scheduler.GetChannelTaskDelta(nodeID, -1)
	collectionChannelNum += b.scheduler.GetChannelTaskDelta(nodeID, collectionID)

	br.AddDetailRecord(StrRecordf("Calcalute score for collection %d on node %d, global row count: %d, collection row count: %d",
		collectionID, nodeID, nodeChannelNum, collectionChannelNum))

	// give a higher weight to distribute collection's channels evenly across multiple nodes.
	channelWeight := paramtable.Get().QueryCoordCfg.CollectionChannelCountFactor.GetAsFloat()
	return nodeChannelNum + int(float64(collectionChannelNum)*math.Max(1.0, channelWeight))
}

// calculateSegmentScore calculate the score which the segment represented
func (b *ScoreBasedBalancer) calculateSegmentScore(s *meta.Segment) float64 {
	return float64(s.GetNumOfRows()) * (1 + params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

func (b *ScoreBasedBalancer) calculateChannelScore(ch *meta.DmChannel, currentCollection int64) float64 {
	if ch.GetCollectionID() == currentCollection {
		// give a higher weight to distribute current collection's channels evenly across multiple nodes.
		channelWeight := paramtable.Get().QueryCoordCfg.CollectionChannelCountFactor.GetAsFloat()
		return math.Max(1.0, channelWeight)
	}
	return 1
}

func (b *ScoreBasedBalancer) BalanceReplica(replica *meta.Replica) (segmentPlans []SegmentAssignPlan, channelPlans []ChannelAssignPlan) {
	log := log.With(
		zap.Int64("collection", replica.GetCollectionID()),
		zap.Int64("replica id", replica.GetID()),
		zap.String("replica group", replica.GetResourceGroup()),
	)
	br := NewBalanceReport()
	defer func() {
		if len(segmentPlans) == 0 && len(channelPlans) == 0 {
			log.WithRateGroup(fmt.Sprintf("scorebasedbalance-noplan-%d", replica.GetID()), 1, 60).
				RatedDebug(60, "no plan generated, balance report", zap.Stringers("nodesInfo", br.NodesInfo()), zap.Stringers("records", br.detailRecords))
		} else {
			log.Info("balance plan generated", zap.Stringers("nodesInfo", br.NodesInfo()), zap.Stringers("report details", br.records))
		}
	}()
	if replica.NodesCount() == 0 {
		br.AddRecord(StrRecord("replica has no querynode"))
		return nil, nil
	}

	rwNodes := replica.GetRWNodes()
	roNodes := replica.GetRONodes()

	if len(rwNodes) == 0 {
		// no available nodes to balance
		br.AddRecord(StrRecord("no rwNodes to balance"))
		return nil, nil
	}

	// print current distribution before generating plans
	segmentPlans, channelPlans = make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	if len(roNodes) != 0 {
		if !paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
			log.RatedInfo(10, "stopping balance is disabled!", zap.Int64s("stoppingNode", roNodes))
			br.AddRecord(StrRecord("stopping balance is disabled"))
			return nil, nil
		}

		log.Info("Handle stopping nodes",
			zap.Any("stopping nodes", roNodes),
			zap.Any("available nodes", rwNodes),
		)
		br.AddRecord(StrRecordf("executing stopping balance: %v", roNodes))
		// handle stopped nodes here, have to assign segments on stopping nodes to nodes with the smallest score
		channelPlans = append(channelPlans, b.genStoppingChannelPlan(replica, rwNodes, roNodes)...)
		if len(channelPlans) == 0 {
			segmentPlans = append(segmentPlans, b.genStoppingSegmentPlan(replica, rwNodes, roNodes)...)
		}
	} else {
		if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
			channelPlans = append(channelPlans, b.genChannelPlan(br, replica, rwNodes)...)
		}

		if len(channelPlans) == 0 {
			segmentPlans = append(segmentPlans, b.genSegmentPlan(br, replica, rwNodes)...)
		}
	}

	return segmentPlans, channelPlans
}

func (b *ScoreBasedBalancer) genStoppingChannelPlan(replica *meta.Replica, rwNodes []int64, roNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	for _, nodeID := range roNodes {
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(nodeID))
		plans := b.AssignChannel(replica.GetCollectionID(), dmChannels, rwNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		channelPlans = append(channelPlans, plans...)
	}
	return channelPlans
}

func (b *ScoreBasedBalancer) genStoppingSegmentPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(nodeID))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(segment.GetCollectionID(), segment.GetID())
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

func (b *ScoreBasedBalancer) genSegmentPlan(br *balanceReport, replica *meta.Replica, onlineNodes []int64) []SegmentAssignPlan {
	segmentDist := make(map[int64][]*meta.Segment)
	nodeItemsMap := b.convertToNodeItemsBySegment(br, replica.GetCollectionID(), onlineNodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}

	log.Info("node segment workload status",
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
		zap.Stringers("nodes", lo.Values(nodeItemsMap)))

	// list all segment which could be balanced, and calculate node's score
	for _, node := range onlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(segment.GetCollectionID(), segment.GetID())
		})
		segmentDist[node] = segments
	}

	balanceBatchSize := paramtable.Get().QueryCoordCfg.CollectionBalanceSegmentBatchSize.GetAsInt()

	// find the segment from the node which has more score than the average
	segmentsToMove := make([]*meta.Segment, 0)
	for node, segments := range segmentDist {
		currentScore := nodeItemsMap[node].getCurrentScore()
		assignedScore := nodeItemsMap[node].getAssignedScore()
		if currentScore <= assignedScore {
			br.AddRecord(StrRecordf("node %d skip balance since current score(%f) lower than assigned one (%f)", node, currentScore, assignedScore))
			continue
		}

		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
		})
		for _, s := range segments {
			segmentScore := b.calculateSegmentScore(s)
			br.AddRecord(StrRecordf("pick segment %d with score %f from node %d", s.ID, segmentScore, node))
			segmentsToMove = append(segmentsToMove, s)
			if len(segmentsToMove) >= balanceBatchSize {
				br.AddRecord(StrRecordf("stop add segment candidate since current plan is equal to batch max(%d)", balanceBatchSize))
				break
			}

			currentScore -= segmentScore
			if currentScore <= assignedScore {
				br.AddRecord(StrRecordf("stop add segment candidate since node[%d] current score(%f) below assigned(%f)", node, currentScore, assignedScore))
				break
			}
		}
	}

	// if the segment are redundant, skip it's balance for now
	segmentsToMove = lo.Filter(segmentsToMove, func(s *meta.Segment, _ int) bool {
		times := len(b.dist.SegmentDistManager.GetByFilter(meta.WithReplica(replica), meta.WithSegmentID(s.GetID())))
		segmentUnique := times == 1
		if !segmentUnique {
			br.AddRecord(StrRecordf("abort balancing segment %d since it appear multiple times(%d) in distribution", s.ID, times))
		}
		return segmentUnique
	})

	if len(segmentsToMove) == 0 {
		return nil
	}

	segmentPlans := b.assignSegment(br, replica.GetCollectionID(), segmentsToMove, onlineNodes, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

func (b *ScoreBasedBalancer) genChannelPlan(br *balanceReport, replica *meta.Replica, onlineNodes []int64) []ChannelAssignPlan {
	nodeItemsMap := b.convertToNodeItemsByChannel(br, replica.GetCollectionID(), onlineNodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}

	log.Info("node channel workload status",
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
		zap.Stringers("nodes", lo.Values(nodeItemsMap)))

	channelDist := make(map[int64][]*meta.DmChannel)
	for _, node := range onlineNodes {
		channelDist[node] = b.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(replica.GetCollectionID()), meta.WithNodeID2Channel(node))
	}

	balanceBatchSize := paramtable.Get().QueryCoordCfg.CollectionBalanceSegmentBatchSize.GetAsInt()
	// find the segment from the node which has more score than the average
	channelsToMove := make([]*meta.DmChannel, 0)
	for node, channels := range channelDist {
		currentScore := nodeItemsMap[node].getCurrentScore()
		assignedScore := nodeItemsMap[node].getAssignedScore()
		if currentScore <= assignedScore {
			br.AddRecord(StrRecordf("node %d skip balance since current score(%f) lower than assigned one (%f)", node, currentScore, assignedScore))
			continue
		}

		for _, ch := range channels {
			channelScore := b.calculateChannelScore(ch, replica.GetCollectionID())
			br.AddRecord(StrRecordf("pick channel %s with score %f from node %d", ch.GetChannelName(), channelScore, node))
			channelsToMove = append(channelsToMove, ch)
			if len(channelsToMove) >= balanceBatchSize {
				br.AddRecord(StrRecordf("stop add channel candidate since current plan is equal to batch max(%d)", balanceBatchSize))
				break
			}

			currentScore -= channelScore
			if currentScore <= assignedScore {
				br.AddRecord(StrRecordf("stop add channel candidate since node[%d] current score(%f) below assigned(%f)", node, currentScore, assignedScore))
				break
			}
		}
	}

	// if the channel are redundant, skip it's balance for now
	channelsToMove = lo.Filter(channelsToMove, func(ch *meta.DmChannel, _ int) bool {
		times := len(b.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica), meta.WithChannelName2Channel(ch.GetChannelName())))
		channelUnique := times == 1
		if !channelUnique {
			br.AddRecord(StrRecordf("abort balancing channel %s since it appear multiple times(%d) in distribution", ch.GetChannelName(), times))
		}
		return channelUnique
	})

	if len(channelsToMove) == 0 {
		return nil
	}

	channelPlans := b.assignChannel(br, replica.GetCollectionID(), channelsToMove, onlineNodes, false)
	for i := range channelPlans {
		channelPlans[i].From = channelPlans[i].Channel.Node
		channelPlans[i].Replica = replica
	}

	return channelPlans
}
