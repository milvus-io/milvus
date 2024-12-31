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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// score based segment use (collection_row_count + global_row_count * factor) as node' score
// and try to make each node has almost same score through balance segment.
type ChannelLevelScoreBalancer struct {
	*ScoreBasedBalancer
}

func NewChannelLevelScoreBalancer(scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *ChannelLevelScoreBalancer {
	return &ChannelLevelScoreBalancer{
		ScoreBasedBalancer: NewScoreBasedBalancer(scheduler, nodeManager, dist, meta, targetMgr),
	}
}

func (b *ChannelLevelScoreBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []SegmentAssignPlan, channelPlans []ChannelAssignPlan) {
	log := log.With(
		zap.Int64("collection", replica.GetCollectionID()),
		zap.Int64("replica id", replica.GetID()),
		zap.String("replica group", replica.GetResourceGroup()),
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

	exclusiveMode := true
	channels := b.targetMgr.GetDmChannelsByCollection(ctx, replica.GetCollectionID(), meta.CurrentTarget)
	for channelName := range channels {
		if len(replica.GetChannelRWNodes(channelName)) == 0 {
			exclusiveMode = false
			break
		}
	}

	// if some channel doesn't own nodes, exit exclusive mode
	if !exclusiveMode {
		return b.ScoreBasedBalancer.BalanceReplica(ctx, replica)
	}

	channelPlans = make([]ChannelAssignPlan, 0)
	segmentPlans = make([]SegmentAssignPlan, 0)
	for channelName := range channels {
		if replica.NodesCount() == 0 {
			return nil, nil
		}

		rwNodes := replica.GetChannelRWNodes(channelName)
		roNodes := replica.GetRONodes()

		// mark channel's outbound access node as offline
		channelRWNode := typeutil.NewUniqueSet(rwNodes...)
		channelDist := b.dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(channelName), meta.WithReplica2Channel(replica))
		for _, channel := range channelDist {
			if !channelRWNode.Contain(channel.Node) {
				roNodes = append(roNodes, channel.Node)
			}
		}
		segmentDist := b.dist.SegmentDistManager.GetByFilter(meta.WithChannel(channelName), meta.WithReplica(replica))
		for _, segment := range segmentDist {
			if !channelRWNode.Contain(segment.Node) {
				roNodes = append(roNodes, segment.Node)
			}
		}

		if len(rwNodes) == 0 {
			// no available nodes to balance
			return nil, nil
		}

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
				channelPlans = append(channelPlans, b.genStoppingChannelPlan(ctx, replica, channelName, rwNodes, roNodes)...)
			}

			if len(channelPlans) == 0 && b.permitBalanceSegment(replica.GetCollectionID()) {
				segmentPlans = append(segmentPlans, b.genStoppingSegmentPlan(ctx, replica, channelName, rwNodes, roNodes)...)
			}
		} else {
			if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() && b.permitBalanceChannel(replica.GetCollectionID()) {
				channelPlans = append(channelPlans, b.genChannelPlan(ctx, replica, channelName, rwNodes)...)
			}

			if len(channelPlans) == 0 && b.permitBalanceSegment(replica.GetCollectionID()) {
				segmentPlans = append(segmentPlans, b.genSegmentPlan(ctx, br, replica, channelName, rwNodes)...)
			}
		}
	}

	return segmentPlans, channelPlans
}

func (b *ChannelLevelScoreBalancer) genStoppingChannelPlan(ctx context.Context, replica *meta.Replica, channelName string, onlineNodes []int64, offlineNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(nodeID), meta.WithChannelName2Channel(channelName))
		plans := b.AssignChannel(ctx, replica.GetCollectionID(), dmChannels, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		channelPlans = append(channelPlans, plans...)
	}
	return channelPlans
}

func (b *ChannelLevelScoreBalancer) genStoppingSegmentPlan(ctx context.Context, replica *meta.Replica, channelName string, onlineNodes []int64, offlineNodes []int64) []SegmentAssignPlan {
	segmentPlans := make([]SegmentAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(nodeID), meta.WithChannel(channelName))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})
		plans := b.AssignSegment(ctx, replica.GetCollectionID(), segments, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		segmentPlans = append(segmentPlans, plans...)
	}
	return segmentPlans
}

func (b *ChannelLevelScoreBalancer) genSegmentPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, channelName string, onlineNodes []int64) []SegmentAssignPlan {
	segmentDist := make(map[int64][]*meta.Segment)
	nodeItemsMap := b.convertToNodeItemsBySegment(br, replica.GetCollectionID(), onlineNodes)
	if len(nodeItemsMap) == 0 {
		return nil
	}
	log.Info("node workload status",
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
		zap.String("channelName", channelName),
		zap.Stringers("nodes", lo.Values(nodeItemsMap)))

	// list all segment which could be balanced, and calculate node's score
	for _, node := range onlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node), meta.WithChannel(channelName))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})
		segmentDist[node] = segments
	}

	// find the segment from the node which has more score than the average
	segmentsToMove := make([]*meta.Segment, 0)
	for node, segments := range segmentDist {
		currentScore := nodeItemsMap[node].getCurrentScore()
		assignedScore := nodeItemsMap[node].getAssignedScore()
		if currentScore <= assignedScore {
			continue
		}

		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
		})
		for _, s := range segments {
			segmentsToMove = append(segmentsToMove, s)
			currentScore -= b.calculateSegmentScore(s)
			if currentScore <= assignedScore {
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

	segmentPlans := b.AssignSegment(ctx, replica.GetCollectionID(), segmentsToMove, onlineNodes, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

func (b *ChannelLevelScoreBalancer) genChannelPlan(ctx context.Context, replica *meta.Replica, channelName string, onlineNodes []int64) []ChannelAssignPlan {
	channelPlans := make([]ChannelAssignPlan, 0)
	if len(onlineNodes) > 1 {
		// start to balance channels on all available nodes
		channelDist := b.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica), meta.WithChannelName2Channel(channelName))
		if len(channelDist) == 0 {
			return nil
		}
		average := int(math.Ceil(float64(len(channelDist)) / float64(len(onlineNodes))))

		// find nodes with less channel count than average
		nodeWithLessChannel := make([]int64, 0)
		channelsToMove := make([]*meta.DmChannel, 0)
		for _, node := range onlineNodes {
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
		}

		return channelPlans
	}
	return channelPlans
}
