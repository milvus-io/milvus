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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
)

type RowCountBasedBalancer struct {
	*RoundRobinBalancer
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
}

func (b *RowCountBasedBalancer) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	nodeItems := b.convertToNodeItems(nodes)
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
		// change node's priority and push back
		p := ni.getPriority()
		ni.setPriority(p + int(s.GetNumOfRows()))
		queue.push(ni)
	}
	return plans
}

func (b *RowCountBasedBalancer) convertToNodeItems(nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, nodeInfo := range b.getNodes(nodeIDs) {
		node := nodeInfo.ID()
		segments := b.dist.SegmentDistManager.GetByNode(node)
		rowcnt := 0
		for _, s := range segments {
			rowcnt += int(s.GetNumOfRows())
		}
		// more row count, less priority
		nodeItem := newNodeItem(rowcnt, node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *RowCountBasedBalancer) BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodes := replica.GetNodes()
	if len(nodes) < 2 {
		return nil, nil
	}
	onlineNodesSegments := make(map[int64][]*meta.Segment)
	stoppingNodesSegments := make(map[int64][]*meta.Segment)
	outboundNodes := b.meta.ResourceManager.CheckOutboundNodes(replica)

	totalCnt := 0
	for _, nid := range nodes {
		segments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nid)
		// Only balance segments in targets
		segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil &&
				b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.NextTarget) != nil
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
			onlineNodesSegments[nid] = segments
		}

		for _, s := range segments {
			totalCnt += int(s.GetNumOfRows())
		}
	}

	if len(nodes) == len(stoppingNodesSegments) || len(onlineNodesSegments) == 0 {
		// no available nodes to balance
		return nil, nil
	}

	segmentsToMove := make([]*meta.Segment, 0)
	for _, stopSegments := range stoppingNodesSegments {
		segmentsToMove = append(segmentsToMove, stopSegments...)
	}

	// find nodes with less row count than average
	nodesWithLessRow := newPriorityQueue()
	average := totalCnt / len(onlineNodesSegments)
	for node, segments := range onlineNodesSegments {
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
		})

		rowCount := 0
		for _, s := range segments {
			rowCount += int(s.GetNumOfRows())
			if rowCount <= average {
				continue
			}

			segmentsToMove = append(segmentsToMove, s)
		}
		if rowCount < average {
			item := newNodeItem(rowCount, node)
			nodesWithLessRow.push(&item)
		}
	}

	segmentsToMove = lo.Filter(segmentsToMove, func(s *meta.Segment, _ int) bool {
		// if the segment are redundant, skip it's balance for now
		return len(b.dist.SegmentDistManager.Get(s.GetID())) == 1
	})

	return b.genSegmentPlan(replica, nodesWithLessRow, segmentsToMove, average), b.genChannelPlan(replica, lo.Keys(onlineNodesSegments), lo.Keys(stoppingNodesSegments))
}

func (b *RowCountBasedBalancer) genSegmentPlan(replica *meta.Replica, nodesWithLessRowCount priorityQueue, segmentsToMove []*meta.Segment, average int) []SegmentAssignPlan {
	if nodesWithLessRowCount.Len() == 0 || len(segmentsToMove) == 0 {
		return nil
	}

	sort.Slice(segmentsToMove, func(i, j int) bool {
		return segmentsToMove[i].GetNumOfRows() < segmentsToMove[j].GetNumOfRows()
	})

	// allocate segments to those nodes with row cnt less than average
	plans := make([]SegmentAssignPlan, 0)
	for _, s := range segmentsToMove {
		if nodesWithLessRowCount.Len() <= 0 {
			break
		}

		node := nodesWithLessRowCount.pop().(*nodeItem)
		newPriority := node.getPriority() + int(s.GetNumOfRows())
		if newPriority > average {
			nodesWithLessRowCount.push(node)
			continue
		}

		plan := SegmentAssignPlan{
			ReplicaID: replica.GetID(),
			From:      s.Node,
			To:        node.nodeID,
			Segment:   s,
		}
		plans = append(plans, plan)
		node.setPriority(newPriority)
		nodesWithLessRowCount.push(node)
	}
	return plans
}

func (b *RowCountBasedBalancer) genChannelPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []ChannelAssignPlan {
	log.Info("balance channel",
		zap.Int64s("online nodes", onlineNodes),
		zap.Int64s("offline nodes", offlineNodes))
	channelPlans := make([]ChannelAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nodeID)
		plans := b.AssignChannel(dmChannels, onlineNodes)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].ReplicaID = replica.ID
		}
		channelPlans = append(channelPlans, plans...)
	}

	// if len(channelPlans) == 0 && len(onlineNodes) > 1 {
	// 	// start to balance channels on all available nodes
	// 	channels := b.dist.ChannelDistManager.GetByCollection(replica.CollectionID)
	// 	channelsOnNode := lo.GroupBy(channels, func(channel *meta.DmChannel) int64 { return channel.Node })

	// 	nodes := replica.GetNodes()
	// 	getChannelNum := func(node int64) int {
	// 		if channelsOnNode[node] == nil {
	// 			return 0
	// 		}
	// 		return len(channelsOnNode[node])
	// 	}
	// 	sort.Slice(nodes, func(i, j int) bool { return getChannelNum(nodes[i]) < getChannelNum(nodes[j]) })

	// 	start := int64(0)
	// 	end := int64(len(nodes) - 1)

	// 	averageChannel := int(math.Ceil(float64(len(channels)) / float64(len(onlineNodes))))
	// 	if averageChannel == 0 || getChannelNum(nodes[start]) >= getChannelNum(nodes[end]) {
	// 		return channelPlans
	// 	}

	// 	for start < end {
	// 		// segment to move in
	// 		targetNode := nodes[start]
	// 		// segment to move out
	// 		sourceNode := nodes[end]

	// 		if len(channelsOnNode[sourceNode])-1 < averageChannel {
	// 			break
	// 		}

	// 		// remove channel from end node
	// 		selectChannel := channelsOnNode[sourceNode][0]
	// 		channelsOnNode[sourceNode] = channelsOnNode[sourceNode][1:]

	// 		// add channel to start node
	// 		if channelsOnNode[targetNode] == nil {
	// 			channelsOnNode[targetNode] = make([]*meta.DmChannel, 0)
	// 		}
	// 		channelsOnNode[targetNode] = append(channelsOnNode[targetNode], selectChannel)

	// 		// generate channel plan
	// 		plan := ChannelAssignPlan{
	// 			Channel:   selectChannel,
	// 			From:      sourceNode,
	// 			To:        targetNode,
	// 			ReplicaID: replica.ID,
	// 		}
	// 		channelPlans = append(channelPlans, plan)
	// 		for end > 0 && getChannelNum(nodes[end]) <= averageChannel {
	// 			end--
	// 		}

	// 		for start < end && getChannelNum(nodes[start]) >= averageChannel {
	// 			start++
	// 		}
	// 	}

	// }
	return channelPlans
}

func NewRowCountBasedBalancer(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager,
) *RowCountBasedBalancer {
	return &RowCountBasedBalancer{
		RoundRobinBalancer: NewRoundRobinBalancer(scheduler, nodeManager),
		dist:               dist,
		meta:               meta,
		targetMgr:          targetMgr,
	}
}

type nodeItem struct {
	baseItem
	nodeID int64
}

func newNodeItem(priority int, nodeID int64) nodeItem {
	return nodeItem{
		baseItem: baseItem{
			priority: priority,
		},
		nodeID: nodeID,
	}
}
