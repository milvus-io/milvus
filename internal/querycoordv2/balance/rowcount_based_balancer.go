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
	"context"
	"math"
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type RowCountBasedBalancer struct {
	*RoundRobinBalancer
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
}

// AssignSegment, when row count based balancer assign segments, it will assign segment to node with least global row count.
// try to make every query node has same row count.
func (b *RowCountBasedBalancer) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64, manualBalance bool) []SegmentAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !manualBalance {
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			return info != nil && info.GetState() == session.NodeStateNormal
		})
	}

	nodeItems := b.convertToNodeItemsBySegment(nodes)
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

// AssignSegment, when row count based balancer assign segments, it will assign channel to node with least global channel count.
// try to make every query node has channel count
func (b *RowCountBasedBalancer) AssignChannel(channels []*meta.DmChannel, nodes []int64, manualBalance bool) []ChannelAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !manualBalance {
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			return info != nil && info.GetState() == session.NodeStateNormal
		})
	}

	nodeItems := b.convertToNodeItemsByChannel(nodes)
	nodeItems = lo.Shuffle(nodeItems)
	if len(nodeItems) == 0 {
		return nil
	}
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}

	plans := make([]ChannelAssignPlan, 0, len(channels))
	for _, c := range channels {
		// pick the node with the least channel num and allocate to it.
		ni := queue.pop().(*nodeItem)
		plan := ChannelAssignPlan{
			From:    -1,
			To:      ni.nodeID,
			Channel: c,
		}
		plans = append(plans, plan)
		// change node's priority and push back
		p := ni.getPriority()
		ni.setPriority(p + 1)
		queue.push(ni)
	}
	return plans
}

func (b *RowCountBasedBalancer) convertToNodeItemsBySegment(nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, nodeInfo := range b.getNodes(nodeIDs) {
		node := nodeInfo.ID()

		// calculate sealed segment row count on node
		segments := b.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(node))
		rowcnt := 0
		for _, s := range segments {
			rowcnt += int(s.GetNumOfRows())
		}

		// calculate growing segment row count on node
		views := b.dist.GetLeaderView(node)
		for _, view := range views {
			rowcnt += int(view.NumOfGrowingRows)
		}

		// more row count, less priority
		nodeItem := newNodeItem(rowcnt, node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *RowCountBasedBalancer) convertToNodeItemsByChannel(nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, nodeInfo := range b.getNodes(nodeIDs) {
		node := nodeInfo.ID()
		channels := b.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(node))

		// more channel num, less priority
		nodeItem := newNodeItem(len(channels), node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *RowCountBasedBalancer) BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.RowCountBasedBalancer", 1, 60).With(
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetCollectionID()),
		zap.String("resourceGroup", replica.Replica.GetResourceGroup()),
	)
	nodes := replica.GetNodes()
	if len(nodes) < 2 {
		return nil, nil
	}
	outboundNodes := b.meta.ResourceManager.CheckOutboundNodes(replica)

	onlineNodes := make([]int64, 0)
	offlineNodes := make([]int64, 0)

	for _, nid := range nodes {
		if isStopping, err := b.nodeManager.IsStoppingNode(nid); err != nil {
			log.Info("not existed node", zap.Int64("nid", nid), zap.Error(err))
			continue
		} else if isStopping {
			offlineNodes = append(offlineNodes, nid)
		} else if outboundNodes.Contain(nid) {
			// if node is stop or transfer to other rg
			log.RatedInfo(10, "meet outbound node, try to move out all segment/channel", zap.Int64("node", nid))
			offlineNodes = append(offlineNodes, nid)
		} else {
			onlineNodes = append(onlineNodes, nid)
		}
	}

	if len(nodes) == len(offlineNodes) || len(onlineNodes) == 0 {
		// no available nodes to balance
		return nil, nil
	}

	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	if len(offlineNodes) != 0 {
		if !paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
			log.RatedInfo(10, "stopping balance is disabled!", zap.Int64s("stoppingNode", offlineNodes))
			return nil, nil
		}

		log.Info("Handle stopping nodes",
			zap.Any("stopping nodes", offlineNodes),
			zap.Any("available nodes", onlineNodes),
		)
		// handle stopped nodes here, have to assign segments on stopping nodes to nodes with the smallest score
		channelPlans = append(channelPlans, b.genStoppingChannelPlan(replica, onlineNodes, offlineNodes)...)
		segmentPlans = append(segmentPlans, b.genStoppingSegmentPlan(replica, onlineNodes, offlineNodes)...)
	} else {
		if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
			channelPlans = append(channelPlans, b.genChannelPlan(replica, onlineNodes)...)
		}

		segmentPlans = append(segmentPlans, b.genSegmentPlan(replica, onlineNodes)...)
	}

	return segmentPlans, channelPlans
}

func (b *RowCountBasedBalancer) genStoppingSegmentPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(nodeID))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil &&
				b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.NextTarget) != nil &&
				segment.GetLevel() != datapb.SegmentLevel_L0
		})
		plans := b.AssignSegment(replica.CollectionID, segments, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		segmentPlans = append(segmentPlans, plans...)
	}
	return segmentPlans
}

func (b *RowCountBasedBalancer) genSegmentPlan(replica *meta.Replica, onlineNodes []int64) []SegmentAssignPlan {
	segmentsToMove := make([]*meta.Segment, 0)

	nodeRowCount := make(map[int64]int, 0)
	segmentDist := make(map[int64][]*meta.Segment)
	totalRowCount := 0
	for _, node := range onlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil &&
				b.targetMgr.GetSealedSegment(segment.GetCollectionID(), segment.GetID(), meta.NextTarget) != nil &&
				segment.GetLevel() != datapb.SegmentLevel_L0
		})
		rowCount := 0
		for _, s := range segments {
			rowCount += int(s.GetNumOfRows())
		}
		totalRowCount += rowCount
		segmentDist[node] = segments
		nodeRowCount[node] = rowCount
	}

	if totalRowCount == 0 {
		return nil
	}

	// find nodes with less row count than average
	average := totalRowCount / len(onlineNodes)
	nodesWithLessRow := make([]int64, 0)
	for node, segments := range segmentDist {
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
		})

		leftRowCount := nodeRowCount[node]
		if leftRowCount < average {
			nodesWithLessRow = append(nodesWithLessRow, node)
			continue
		}

		for _, s := range segments {
			leftRowCount -= int(s.GetNumOfRows())
			if leftRowCount < average {
				break
			}
			segmentsToMove = append(segmentsToMove, s)
		}
	}

	segmentsToMove = lo.Filter(segmentsToMove, func(s *meta.Segment, _ int) bool {
		// if the segment are redundant, skip it's balance for now
		return len(b.dist.SegmentDistManager.GetByFilter(meta.WithReplica(replica), meta.WithSegmentID(s.GetID()))) == 1
	})

	if len(nodesWithLessRow) == 0 || len(segmentsToMove) == 0 {
		return nil
	}

	segmentPlans := b.AssignSegment(replica.CollectionID, segmentsToMove, nodesWithLessRow, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

func (b *RowCountBasedBalancer) genStoppingChannelPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dmChannels := b.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(replica.GetCollectionID()), meta.WithNodeID2Channel(nodeID))
		plans := b.AssignChannel(dmChannels, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		channelPlans = append(channelPlans, plans...)
	}
	return channelPlans
}

func (b *RowCountBasedBalancer) genChannelPlan(replica *meta.Replica, onlineNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	if len(onlineNodes) > 1 {
		// start to balance channels on all available nodes
		channelDist := b.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica))
		if len(channelDist) == 0 {
			return nil
		}
		average := int(math.Ceil(float64(len(channelDist)) / float64(len(onlineNodes))))

		// find nodes with less channel count than average
		nodeWithLessChannel := make([]int64, 0)
		channelsToMove := make([]*meta.DmChannel, 0)
		for _, node := range onlineNodes {
			channels := b.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(replica.GetCollectionID()), meta.WithNodeID2Channel(node))

			if len(channels) <= average {
				nodeWithLessChannel = append(nodeWithLessChannel, node)
				continue
			}

			channelsToMove = append(channelsToMove, channels[average:]...)
		}

		if len(nodeWithLessChannel) == 0 || len(channelsToMove) == 0 {
			return nil
		}

		channelPlans := b.AssignChannel(channelsToMove, nodeWithLessChannel, false)
		for i := range channelPlans {
			channelPlans[i].From = channelPlans[i].Channel.Node
			channelPlans[i].Replica = replica
		}

		return channelPlans
	}
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
