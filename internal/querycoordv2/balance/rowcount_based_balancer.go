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
	"fmt"
	"math"
	"sort"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"go.uber.org/zap"

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
	targetMgr meta.TargetManagerInterface
}

// AssignSegment, when row count based balancer assign segments, it will assign segment to node with least global row count.
// try to make every query node has same row count.
func (b *RowCountBasedBalancer) AssignSegment(ctx context.Context, collectionID int64, segments []*meta.Segment, nodes []int64, forceAssign bool) []SegmentAssignPlan {
	if !forceAssign {
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

	balanceBatchSize := paramtable.Get().QueryCoordCfg.CollectionBalanceSegmentBatchSize.GetAsInt()
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
		if len(plans) > balanceBatchSize {
			break
		}
		// change node's score and push back
		ni.AddCurrentScoreDelta(float64(s.GetNumOfRows()))
		queue.push(ni)
	}
	return plans
}

// AssignSegment, when row count based balancer assign segments, it will assign channel to node with least global channel count.
// try to make every query node has channel count
func (b *RowCountBasedBalancer) AssignChannel(ctx context.Context, collectionID int64, channels []*meta.DmChannel, nodes []int64, forceAssign bool) []ChannelAssignPlan {
	// skip out suspend node and stopping node during assignment, but skip this check for manual balance
	if !forceAssign {
		versionRangeFilter := semver.MustParseRange(">2.3.x")
		nodes = lo.Filter(nodes, func(node int64, _ int) bool {
			info := b.nodeManager.Get(node)
			// balance channel to qn with version < 2.4 is not allowed since l0 segment supported
			// if watch channel on qn with version < 2.4, it may cause delete data loss
			return info != nil && info.GetState() == session.NodeStateNormal && versionRangeFilter(info.Version())
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
		// change node's score and push back
		ni.AddCurrentScoreDelta(1)
		queue.push(ni)
	}
	return plans
}

func (b *RowCountBasedBalancer) convertToNodeItemsBySegment(nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, node := range nodeIDs {
		// calculate sealed segment row count on node
		segments := b.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(node))
		rowcnt := 0
		for _, s := range segments {
			rowcnt += int(s.GetNumOfRows())
		}

		// calculate growing segment row count on node
		views := b.dist.LeaderViewManager.GetByFilter(meta.WithNodeID2LeaderView(node))
		for _, view := range views {
			rowcnt += int(view.NumOfGrowingRows)
		}

		// calculate executing task cost in scheduler
		rowcnt += b.scheduler.GetSegmentTaskDelta(node, -1)

		// more row count, less priority
		nodeItem := newNodeItem(rowcnt, node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *RowCountBasedBalancer) convertToNodeItemsByChannel(nodeIDs []int64) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, node := range nodeIDs {
		channels := b.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(node))

		channelCount := len(channels)
		// calculate executing task cost in scheduler
		channelCount += b.scheduler.GetChannelTaskDelta(node, -1)
		// more channel num, less priority
		nodeItem := newNodeItem(channelCount, node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func (b *RowCountBasedBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []SegmentAssignPlan, channelPlans []ChannelAssignPlan) {
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.RowCountBasedBalancer", 1, 60).With(
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetCollectionID()),
		zap.String("resourceGroup", replica.GetResourceGroup()),
	)
	br := NewBalanceReport()
	defer func() {
		if len(segmentPlans) == 0 && len(channelPlans) == 0 {
			log.WithRateGroup(fmt.Sprintf("scorebasedbalance-noplan-%d", replica.GetID()), 1, 60).
				RatedDebug(60, "no plan generated, balance report", zap.Stringers("records", br.detailRecords))
		} else {
			log.Info("balance plan generated", zap.Stringers("report details", br.records))
		}
	}()
	if replica.NodesCount() == 0 {
		return nil, nil
	}

	rwNodes := replica.GetRWNodes()
	roNodes := replica.GetRONodes()
	if len(rwNodes) == 0 {
		// no available nodes to balance
		return nil, nil
	}

	segmentPlans, channelPlans = make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
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
		if b.permitBalanceChannel(replica.GetCollectionID()) {
			channelPlans = append(channelPlans, b.genStoppingChannelPlan(ctx, replica, rwNodes, roNodes)...)
		}

		if len(channelPlans) == 0 && b.permitBalanceSegment(replica.GetCollectionID()) {
			segmentPlans = append(segmentPlans, b.genStoppingSegmentPlan(ctx, replica, rwNodes, roNodes)...)
		}
	} else {
		if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() && b.permitBalanceChannel(replica.GetCollectionID()) {
			channelPlans = append(channelPlans, b.genChannelPlan(ctx, br, replica, rwNodes)...)
		}

		if len(channelPlans) == 0 && b.permitBalanceSegment(replica.GetCollectionID()) {
			segmentPlans = append(segmentPlans, b.genSegmentPlan(ctx, replica, rwNodes)...)
		}
	}

	return segmentPlans, channelPlans
}

func (b *RowCountBasedBalancer) genStoppingSegmentPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64, roNodes []int64) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)
	for _, nodeID := range roNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(nodeID))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})
		plans := b.AssignSegment(ctx, replica.GetCollectionID(), segments, rwNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		segmentPlans = append(segmentPlans, plans...)
	}
	return segmentPlans
}

func (b *RowCountBasedBalancer) genSegmentPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64) []SegmentAssignPlan {
	segmentsToMove := make([]*meta.Segment, 0)

	nodeRowCount := make(map[int64]int, 0)
	segmentDist := make(map[int64][]*meta.Segment)
	totalRowCount := 0
	for _, node := range rwNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
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
	average := totalRowCount / len(rwNodes)
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

	segmentPlans := b.AssignSegment(ctx, replica.GetCollectionID(), segmentsToMove, nodesWithLessRow, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

func (b *RowCountBasedBalancer) genStoppingChannelPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64, roNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	for _, nodeID := range roNodes {
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(nodeID))
		plans := b.AssignChannel(ctx, replica.GetCollectionID(), dmChannels, rwNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		channelPlans = append(channelPlans, plans...)
	}
	return channelPlans
}

func (b *RowCountBasedBalancer) genChannelPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, rwNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	if len(rwNodes) > 1 {
		// start to balance channels on all available nodes
		channelDist := b.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica))
		if len(channelDist) == 0 {
			return nil
		}
		average := int(math.Ceil(float64(len(channelDist)) / float64(len(rwNodes))))

		// find nodes with less channel count than average
		nodeWithLessChannel := make([]int64, 0)
		channelsToMove := make([]*meta.DmChannel, 0)
		for _, node := range rwNodes {
			channels := b.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(node))

			if len(channels) <= average {
				nodeWithLessChannel = append(nodeWithLessChannel, node)
				continue
			}

			channelsToMove = append(channelsToMove, channels[average:]...)
		}

		if len(nodeWithLessChannel) == 0 || len(channelsToMove) == 0 {
			return nil
		}

		channelPlans := b.AssignChannel(ctx, replica.GetCollectionID(), channelsToMove, nodeWithLessChannel, false)
		for i := range channelPlans {
			channelPlans[i].From = channelPlans[i].Channel.Node
			channelPlans[i].Replica = replica
			br.AddRecord(StrRecordf("add channel plan %s", channelPlans[i]))
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
	targetMgr meta.TargetManagerInterface,
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
	fmt.Stringer
	nodeID        int64
	assignedScore float64
	currentScore  float64
}

func newNodeItem(currentScore int, nodeID int64) nodeItem {
	return nodeItem{
		baseItem:     baseItem{},
		nodeID:       nodeID,
		currentScore: float64(currentScore),
	}
}

func (b *nodeItem) getPriority() int {
	// if node lacks more score between assignedScore and currentScore, then higher priority
	return int(math.Ceil(b.currentScore - b.assignedScore))
}

func (b *nodeItem) setPriority(priority int) {
	panic("not supported, use updatePriority instead")
}

func (b *nodeItem) getPriorityWithCurrentScoreDelta(delta float64) int {
	return int((b.currentScore + delta) - b.assignedScore)
}

func (b *nodeItem) getCurrentScore() float64 {
	return b.currentScore
}

func (b *nodeItem) AddCurrentScoreDelta(delta float64) {
	b.currentScore += delta
	b.priority = b.getPriority()
}

func (b *nodeItem) getAssignedScore() float64 {
	return b.assignedScore
}

func (b *nodeItem) setAssignedScore(delta float64) {
	b.assignedScore += delta
	b.priority = b.getPriority()
}

func (b *nodeItem) String() string {
	return fmt.Sprintf("{NodeID: %d, AssignedScore: %f, CurrentScore: %f, Priority: %d}", b.nodeID, b.assignedScore, b.currentScore, b.priority)
}
