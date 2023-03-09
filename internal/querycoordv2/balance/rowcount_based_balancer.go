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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type RowCountBasedBalancer struct {
	*RoundRobinBalancer
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager
}

func (b *RowCountBasedBalancer) AssignSegment(segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
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

func (b *RowCountBasedBalancer) Balance() ([]SegmentAssignPlan, []ChannelAssignPlan) {
	ids := b.meta.CollectionManager.GetAll()

	// loading collection should skip balance
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		return b.meta.GetStatus(cid) == querypb.LoadStatus_Loaded
	})

	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	for _, cid := range loadedCollections {
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
		for _, replica := range replicas {
			splans, cplans := b.balanceReplica(replica)
			segmentPlans = append(segmentPlans, splans...)
			channelPlans = append(channelPlans, cplans...)
		}
	}
	return segmentPlans, channelPlans
}

func (b *RowCountBasedBalancer) balanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodes := replica.GetNodes()
	if len(nodes) == 0 {
		return nil, nil
	}
	nodesRowCnt := make(map[int64]int)
	nodesSegments := make(map[int64][]*meta.Segment)
	stoppingNodesSegments := make(map[int64][]*meta.Segment)

	outboundNodes := b.meta.ResourceManager.CheckOutboundNodes(replica)

	totalCnt := 0
	for _, nid := range nodes {
		segments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nid)
		// Only balance segments in targets
		segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.GetHistoricalSegment(segment.GetCollectionID(), segment.GetID(), meta.CurrentTarget) != nil
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

		cnt := 0
		for _, s := range segments {
			cnt += int(s.GetNumOfRows())
		}
		nodesRowCnt[nid] = cnt
		totalCnt += cnt
	}

	if len(nodes) == len(stoppingNodesSegments) {
		// no available nodes to balance
		return nil, nil
	}

	if len(nodesSegments) == 0 {
		return nil, nil
	}

	average := totalCnt / len(nodesSegments)
	neededRowCnt := 0
	for nodeID := range nodesSegments {
		rowcnt := nodesRowCnt[nodeID]
		if rowcnt < average {
			neededRowCnt += average - rowcnt
		}
	}

	if neededRowCnt == 0 {
		return nil, nil
	}

	segmentsToMove := make([]*meta.Segment, 0)

	stopSegments, cnt := b.collectionStoppingSegments(stoppingNodesSegments)
	segmentsToMove = append(segmentsToMove, stopSegments...)
	neededRowCnt -= cnt

	// select segments to be moved
outer:
	for nodeID, segments := range nodesSegments {
		rowcnt := nodesRowCnt[nodeID]
		if rowcnt <= average {
			continue
		}
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
		})

		for _, s := range segments {
			if rowcnt-int(s.GetNumOfRows()) < average {
				continue
			}
			rowcnt -= int(s.GetNumOfRows())
			segmentsToMove = append(segmentsToMove, s)
			neededRowCnt -= int(s.GetNumOfRows())
			if neededRowCnt <= 0 {
				break outer
			}
		}
	}

	sort.Slice(segmentsToMove, func(i, j int) bool {
		return segmentsToMove[i].GetNumOfRows() < segmentsToMove[j].GetNumOfRows()
	})

	// allocate segments to those nodes with row cnt less than average
	queue := newPriorityQueue()
	for nodeID := range nodesSegments {
		rowcnt := nodesRowCnt[nodeID]
		if rowcnt >= average {
			continue
		}
		item := newNodeItem(rowcnt, nodeID)
		queue.push(&item)
	}

	plans := make([]SegmentAssignPlan, 0)
	getPlanWeight := func(nodeID int64) Weight {
		if _, ok := stoppingNodesSegments[nodeID]; ok {
			return GetWeight(1)
		}
		return GetWeight(0)
	}
	for _, s := range segmentsToMove {
		node := queue.pop().(*nodeItem)

		plan := SegmentAssignPlan{
			ReplicaID: replica.GetID(),
			From:      s.Node,
			To:        node.nodeID,
			Segment:   s,
			Weight:    getPlanWeight(s.Node),
		}
		plans = append(plans, plan)
		node.setPriority(node.getPriority() + int(s.GetNumOfRows()))
		queue.push(node)
	}
	return plans, b.getChannelPlan(replica, lo.Keys(nodesSegments), lo.Keys(stoppingNodesSegments))
}

func (b *RowCountBasedBalancer) collectionStoppingSegments(stoppingNodesSegments map[int64][]*meta.Segment) ([]*meta.Segment, int) {
	var (
		segments     []*meta.Segment
		removeRowCnt int
	)

	for _, stoppingSegments := range stoppingNodesSegments {
		for _, segment := range stoppingSegments {
			segments = append(segments, segment)
			removeRowCnt += int(segment.GetNumOfRows())
		}
	}
	return segments, removeRowCnt
}

func (b *RowCountBasedBalancer) getChannelPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndNode(replica.GetCollectionID(), nodeID)
		plans := b.AssignChannel(dmChannels, onlineNodes)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].ReplicaID = replica.ID
			plans[i].Weight = GetWeight(1)
		}
		channelPlans = append(channelPlans, plans...)
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
