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
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"sort"

	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/samber/lo"
)

type ScoreBasedBalancer struct {
	*RowCountBasedBalancer
	balancedCollectionsCurrentRound typeutil.UniqueSet
}

func NewScoreBasedBalancer(scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr *meta.TargetManager) *ScoreBasedBalancer {
	return &ScoreBasedBalancer{
		RowCountBasedBalancer:           NewRowCountBasedBalancer(scheduler, nodeManager, dist, meta, targetMgr),
		balancedCollectionsCurrentRound: typeutil.NewUniqueSet(),
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
			Weight:  GetWeight(1),
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

func (b *ScoreBasedBalancer) calculatePriority(collectionID, nodeId int64) int {
	globalSegments := b.dist.SegmentDistManager.GetByNode(nodeId)
	rowcnt := 0
	for _, s := range globalSegments {
		rowcnt += int(s.GetNumOfRows())
	}

	collectionSegments := b.dist.SegmentDistManager.GetByCollectionAndNode(collectionID, nodeId)
	collectionRowCount := 0
	for _, s := range collectionSegments {
		collectionRowCount += int(s.GetNumOfRows())
	}
	return collectionRowCount + int(float64(rowcnt)*
		params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

func (b *ScoreBasedBalancer) Balance() ([]SegmentAssignPlan, []ChannelAssignPlan) {
	ids := b.meta.CollectionManager.GetAll()

	// loading collection should skip balance
	loadedCollections := lo.Filter(ids, func(cid int64, _ int) bool {
		return b.meta.GetStatus(cid) == querypb.LoadStatus_Loaded
	})

	sort.Slice(loadedCollections, func(i, j int) bool {
		return loadedCollections[i] < loadedCollections[j]
	})

	segmentPlans, channelPlans := make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	hasUnBalancedCollections := false
	for _, cid := range loadedCollections {
		if b.balancedCollectionsCurrentRound.Contain(cid) {
			log.Debug("ScoreBasedBalancer has balanced collection, skip balancing in this round",
				zap.Int64("collectionID", cid))
			continue
		}
		hasUnBalancedCollections = true
		replicas := b.meta.ReplicaManager.GetByCollection(cid)
		for _, replica := range replicas {
			sPlans, cPlans := b.balanceReplica(replica)
			b.PrintNewBalancePlans(cid, replica.GetID(), sPlans, cPlans)
			segmentPlans = append(segmentPlans, sPlans...)
			channelPlans = append(channelPlans, cPlans...)
		}
		b.balancedCollectionsCurrentRound.Insert(cid)
		if len(segmentPlans) != 0 || len(channelPlans) != 0 {
			log.Debug("ScoreBasedBalancer has generated balance plans for", zap.Int64("collectionID", cid))
			break
		}
	}
	if !hasUnBalancedCollections {
		b.balancedCollectionsCurrentRound.Clear()
		log.Debug("ScoreBasedBalancer has balanced all " +
			"collections in one round, clear collectionIDs for this round")
	}

	return segmentPlans, channelPlans
}

func (b *ScoreBasedBalancer) balanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
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
	//print current distribution before generating plans
	b.PrintCurrentReplicaDist(replica, stoppingNodesSegments, nodesSegments, b.dist.ChannelDistManager)
	if len(stoppingNodesSegments) != 0 {
		log.Info("Handle stopping nodes",
			zap.Int64("collection", replica.CollectionID),
			zap.Int64("replica id", replica.Replica.GetID()),
			zap.String("replica group", replica.Replica.GetResourceGroup()),
			zap.Any("stopping nodes", maps.Keys(stoppingNodesSegments)),
			zap.Any("available nodes", maps.Keys(nodesSegments)),
		)
		// handle stopped nodes here, have to assign segments on stopping nodes to nodes with the smallest score
		return b.getStoppedSegmentPlan(replica, nodesSegments, stoppingNodesSegments), b.getStoppedChannelPlan(replica, lo.Keys(nodesSegments), lo.Keys(stoppingNodesSegments))
	}

	// normal balance, find segments from largest score nodes and transfer to smallest score nodes.
	return b.getNormalSegmentPlan(replica, nodesSegments), b.getNormalChannelPlan(replica, lo.Keys(nodesSegments))
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
	for nodeId, stoppingSegments := range stoppingNodesSegments {
		for _, segment := range stoppingSegments {
			segments = append(segments, segment)
			nodeIndex[segment.GetID()] = nodeId
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
			Weight:    GetWeight(1),
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

func (b *ScoreBasedBalancer) getStoppedChannelPlan(replica *meta.Replica, onlineNodes []int64, offlineNodes []int64) []ChannelAssignPlan {
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

func (b *ScoreBasedBalancer) getNormalSegmentPlan(replica *meta.Replica, nodesSegments map[int64][]*meta.Segment) []SegmentAssignPlan {
	if b.scheduler.GetSegmentTaskNum() != 0 {
		// scheduler is handling segment task, skip
		return nil
	}
	segmentPlans := make([]SegmentAssignPlan, 0)
	// generate candidates
	nodeItems := b.convertToNodeItems(replica.GetCollectionID(), lo.Keys(nodesSegments))

	minQueue := newPriorityQueue()
	for _, item := range nodeItems {
		minQueue.push(item)
	}

	maxQueue := newPriorityQueue()
	for _, item := range nodeItems {
		nodeItem := newNodeItem(-item.priority, item.nodeID)
		maxQueue.push(&nodeItem)
	}

	for {
		toNode := minQueue.pop()
		fromNode := maxQueue.pop()

		// pick the largest segment from fromNode, try to assign to toNode see if the cluster becomes more balance.
		fromSegments := b.dist.SegmentDistManager.GetByCollectionAndNode(replica.CollectionID, fromNode.(*nodeItem).nodeID)
		// sort the segments in asc order, try to mitigate to-from-unbalance
		sort.Slice(fromSegments, func(i, j int) bool {
			return fromSegments[i].GetNumOfRows() < fromSegments[j].GetNumOfRows()
		})

		// TODO we shouldn't use calculatePriority, because it it's balanced by replica, then global segment count will not be stable
		// Better way is to calculate priority with a segment distribution map
		// should not use the same snapshot from distribution throughout the process
		fromPriority := -fromNode.(*nodeItem).priority
		toPriority := toNode.(*nodeItem).priority

		inbalance := fromPriority - toPriority
		havingBalanced := false

		updatePriority := func(nextToPriority int, toNode item, nextFromPriority int, fromNode item) {
			havingBalanced = true
			toNode.setPriority(nextToPriority)
			minQueue.push(toNode)
			fromNode.setPriority(-nextFromPriority)
			maxQueue.push(fromNode)
			fromPriority = nextFromPriority
			toPriority = nextToPriority
		}

		for _, s := range fromSegments {
			nextFromPriority := fromPriority - int(s.GetNumOfRows()) - int(float64(s.GetNumOfRows())*
				params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
			nextToPriority := toPriority + int(s.GetNumOfRows()) + int(float64(s.GetNumOfRows())*
				params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
			if nextToPriority <= nextFromPriority {
				plan := SegmentAssignPlan{
					ReplicaID: replica.GetID(),
					From:      fromNode.(*nodeItem).nodeID,
					To:        toNode.(*nodeItem).nodeID,
					Segment:   s,
					Weight:    GetWeight(0),
				}
				segmentPlans = append(segmentPlans, plan)
				updatePriority(nextToPriority, toNode, nextFromPriority, fromNode)
			} else {
				nextInbalance := nextToPriority - nextFromPriority
				if int(float64(nextInbalance)*params.Params.QueryCoordCfg.ScoreUnbalanceTolerationFactor.GetAsFloat()) < inbalance {
					plan := SegmentAssignPlan{
						ReplicaID: replica.GetID(),
						From:      fromNode.(*nodeItem).nodeID,
						To:        toNode.(*nodeItem).nodeID,
						Segment:   s,
						Weight:    GetWeight(0),
					}
					segmentPlans = append(segmentPlans, plan)
					updatePriority(nextToPriority, toNode, nextFromPriority, fromNode)
					break
				} else {
					havingBalanced = false
					break
				}
			}
		}

		// if toNode and fromNode can not find segment to balance, break, else try to balance the next round
		if !havingBalanced {
			// nothing to balance
			break
		}
		// TODO swap segment between toNode and fromNode, see if the cluster becomes more balance
	}
	return segmentPlans
}

func (b *ScoreBasedBalancer) getNormalChannelPlan(replica *meta.Replica, onlineNodes []int64) []ChannelAssignPlan {
	// TODO
	return make([]ChannelAssignPlan, 0)
}
