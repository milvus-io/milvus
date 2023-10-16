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
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

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

// TODO assign channel need to think of global channels
func (b *ScoreBasedBalancer) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	nodeItems := b.convertToNodeItems(collectionID, nodes)
	if len(nodeItems) == 0 {
		return nil
	}
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	plans := make([]SegmentAssignPlan, 0, len(segments))
	for _, s := range segments {
		// pick the node with the least row count and allocate to it.
		ni := queue.pop().(*nodeItem)
		plan := SegmentAssignPlan{
			From:    -1,
			To:      ni.nodeID,
			Segment: s,
		}
		plans = append(plans, plan)
		// change node's priority and push back, should count for both collection factor and local factor
		p := ni.getPriority()
		ni.setPriority(p + int(s.GetNumOfRows()) +
			int(float64(s.GetNumOfRows())*params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat()))
		queue.push(ni)
	}
	return plans
}

func (b *ScoreBasedBalancer) convertToNodeItems(collectionID int64, nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, nodeInfo := range b.getNodes(nodeIDs) {
		node := nodeInfo.ID()
		priority := b.calculatePriority(collectionID, node)
		nodeItem := newNodeItem(priority, node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *ScoreBasedBalancer) calculatePriority(collectionID, nodeID int64) int {
	globalSegments := b.dist.SegmentDistManager.GetByNode(nodeID)
	rowCount := 0
	for _, s := range globalSegments {
		rowCount += int(s.GetNumOfRows())
	}

	collectionSegments := b.dist.SegmentDistManager.GetByCollectionAndNode(collectionID, nodeID)
	collectionRowCount := 0
	for _, s := range collectionSegments {
		collectionRowCount += int(s.GetNumOfRows())
	}
	return collectionRowCount + int(float64(rowCount)*
		params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

func (b *ScoreBasedBalancer) BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodes := replica.GetNodes()
	if len(nodes) == 0 {
		return nil, nil
	}
	nodesSegments := make(map[int64][]*meta.Segment)
	stoppingNodesSegments := make(map[int64][]*meta.Segment)

	outboundNodes := b.meta.ResourceManager.CheckOutboundNodes(replica)

	// calculate stopping nodes and available nodes.
	for _, nid := range nodes {
		segments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nid)
		// Only balance segments in targets
		segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil
		})

		if isStopping, err := b.nodeManager.IsStoppingNode(nid); err != nil {
			log.Info("not existed node", zap.Int64("nid", nid), zap.Any("segments", segments), zap.Error(err))
			continue
		} else if isStopping {
			stoppingNodesSegments[nid] = segments
		} else if outboundNodes.Contain(nid) {
			// if node is stop or transfer to other rg
			log.RatedInfo(10, "meet outbound node, try to move out all segment/channel",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetCollectionID()),
				zap.Int64("node", nid),
			)
			stoppingNodesSegments[nid] = segments
		} else {
			nodesSegments[nid] = segments
		}
	}

	if len(nodes) == len(stoppingNodesSegments) {
		// no available nodes to balance
		log.Warn("All nodes is under stopping mode or outbound, skip balance replica",
			zap.Int64("collection", replica.CollectionID),
			zap.Int64("replica id", replica.Replica.GetID()),
			zap.String("replica group", replica.Replica.GetResourceGroup()),
			zap.Int64s("nodes", replica.Replica.GetNodes()),
		)
		return nil, nil
	}

	if len(nodesSegments) <= 0 {
		log.Warn("No nodes is available in resource group, skip balance replica",
			zap.Int64("collection", replica.CollectionID),
			zap.Int64("replica id", replica.Replica.GetID()),
			zap.String("replica group", replica.Replica.GetResourceGroup()),
			zap.Int64s("nodes", replica.Replica.GetNodes()),
		)
		return nil, nil
	}
	// print current distribution before generating plans
	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	if len(stoppingNodesSegments) != 0 {
		log.Info("Handle stopping nodes",
			zap.Int64("collection", replica.CollectionID),
			zap.Int64("replica id", replica.Replica.GetID()),
			zap.String("replica group", replica.Replica.GetResourceGroup()),
			zap.Any("stopping nodes", maps.Keys(stoppingNodesSegments)),
			zap.Any("available nodes", maps.Keys(nodesSegments)),
		)
		// handle stopped nodes here, have to assign segments on stopping nodes to nodes with the smallest score
		segmentPlans = append(segmentPlans, b.getStoppedSegmentPlan(replica, nodesSegments, stoppingNodesSegments)...)
		channelPlans = append(channelPlans, b.genChannelPlan(replica, lo.Keys(nodesSegments), lo.Keys(stoppingNodesSegments))...)
	} else {
		// normal balance, find segments from largest score nodes and transfer to smallest score nodes.
		segmentPlans = append(segmentPlans, b.getNormalSegmentPlan(replica, nodesSegments)...)
		channelPlans = append(channelPlans, b.genChannelPlan(replica, lo.Keys(nodesSegments), nil)...)
	}
	if len(segmentPlans) != 0 || len(channelPlans) != 0 {
		PrintCurrentReplicaDist(replica, stoppingNodesSegments, nodesSegments, b.dist.ChannelDistManager, b.dist.SegmentDistManager)
	}

	return segmentPlans, channelPlans
}

func (b *ScoreBasedBalancer) getStoppedSegmentPlan(replica *meta.Replica, nodesSegments map[int64][]*meta.Segment, stoppingNodesSegments map[int64][]*meta.Segment) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)
	// generate candidates
	nodeItems := b.convertToNodeItems(replica.GetCollectionID(), lo.Keys(nodesSegments))
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}

	// collect segment segments to assign
	var segments []*meta.Segment
	nodeIndex := make(map[int64]int64)
	for nodeID, stoppingSegments := range stoppingNodesSegments {
		for _, segment := range stoppingSegments {
			segments = append(segments, segment)
			nodeIndex[segment.GetID()] = nodeID
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	for _, s := range segments {
		// pick the node with the least row count and allocate to it.
		ni := queue.pop().(*nodeItem)
		plan := SegmentAssignPlan{
			ReplicaID: replica.GetID(),
			From:      nodeIndex[s.GetID()],
			To:        ni.nodeID,
			Segment:   s,
		}
		segmentPlans = append(segmentPlans, plan)
		// change node's priority and push back, should count for both collection factor and local factor
		p := ni.getPriority()
		ni.setPriority(p + int(s.GetNumOfRows()) + int(float64(s.GetNumOfRows())*
			params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat()))
		queue.push(ni)
	}

	return segmentPlans
}

func (b *ScoreBasedBalancer) getNormalSegmentPlan(replica *meta.Replica, nodesSegments map[int64][]*meta.Segment) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)

	// generate candidates
	nodeItems := b.convertToNodeItems(replica.GetCollectionID(), lo.Keys(nodesSegments))
	lastIdx := len(nodeItems) - 1
	havingMovedSegments := typeutil.NewUniqueSet()

	for {
		sort.Slice(nodeItems, func(i, j int) bool {
			return nodeItems[i].priority <= nodeItems[j].priority
		})
		toNode := nodeItems[0]
		fromNode := nodeItems[lastIdx]

		fromPriority := fromNode.priority
		toPriority := toNode.priority
		unbalance := float64(fromPriority - toPriority)
		if unbalance < float64(toPriority)*params.Params.QueryCoordCfg.ScoreUnbalanceTolerationFactor.GetAsFloat() {
			break
		}

		// sort the segments in asc order, try to mitigate to-from-unbalance
		// TODO: segment infos inside dist manager may change in the process of making balance plan
		fromSegments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, fromNode.nodeID)
		sort.Slice(fromSegments, func(i, j int) bool {
			return fromSegments[i].GetNumOfRows() < fromSegments[j].GetNumOfRows()
		})
		var targetSegmentToMove *meta.Segment
		for _, segment := range fromSegments {
			targetSegmentToMove = segment
			if havingMovedSegments.Contain(targetSegmentToMove.GetID()) {
				targetSegmentToMove = nil
				continue
			}
			break
		}
		if targetSegmentToMove == nil {
			// the node with the highest score doesn't have any segments suitable for balancing, stop balancing this round
			break
		}

		nextFromPriority := fromPriority - int(targetSegmentToMove.GetNumOfRows()) - int(float64(targetSegmentToMove.GetNumOfRows())*
			params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
		nextToPriority := toPriority + int(targetSegmentToMove.GetNumOfRows()) + int(float64(targetSegmentToMove.GetNumOfRows())*
			params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())

		// still unbalanced after this balance plan is executed
		if nextToPriority <= nextFromPriority {
			plan := SegmentAssignPlan{
				ReplicaID: replica.GetID(),
				From:      fromNode.nodeID,
				To:        toNode.nodeID,
				Segment:   targetSegmentToMove,
			}
			segmentPlans = append(segmentPlans, plan)
		} else {
			// if unbalance reverted after balance action, we will consider the benefit
			// only trigger following balance when the generated reverted balance
			// is far smaller than the original unbalance
			nextUnbalance := nextToPriority - nextFromPriority
			if float64(nextUnbalance)*params.Params.QueryCoordCfg.ReverseUnbalanceTolerationFactor.GetAsFloat() < unbalance {
				plan := SegmentAssignPlan{
					ReplicaID: replica.GetID(),
					From:      fromNode.nodeID,
					To:        toNode.nodeID,
					Segment:   targetSegmentToMove,
				}
				segmentPlans = append(segmentPlans, plan)
			} else {
				// if the tiniest segment movement between the highest scored node and lowest scored node will
				// not provide sufficient balance benefit, we will seize balancing in this round
				break
			}
		}
		havingMovedSegments.Insert(targetSegmentToMove.GetID())

		// update node priority
		toNode.setPriority(nextToPriority)
		fromNode.setPriority(nextFromPriority)
		// if toNode and fromNode can not find segment to balance, break, else try to balance the next round
		// TODO swap segment between toNode and fromNode, see if the cluster becomes more balance
	}
	return segmentPlans
}
