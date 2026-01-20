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

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ChannelLevelScoreBalancer extends ScoreBasedBalancer to provide channel-level balance awareness.
// It balances segments and channels at the channel level, ensuring that segments belonging to
// the same channel are balanced among the nodes assigned to that channel (exclusive mode).
// When exclusive mode cannot be achieved (some channels have no assigned nodes), it falls back
// to the ScoreBasedBalancer behavior.
//
// The score calculation uses (collection_row_count + global_row_count * factor) to determine
// node scores, and attempts to equalize scores across nodes through segment redistribution.
type ChannelLevelScoreBalancer struct {
	*ScoreBasedBalancer
	targetMgr meta.TargetManagerInterface
}

// NewChannelLevelScoreBalancer creates a new ChannelLevelScoreBalancer instance.
// It embeds a ScoreBasedBalancer and adds channel-level balance awareness.
func NewChannelLevelScoreBalancer(scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *ChannelLevelScoreBalancer {
	return &ChannelLevelScoreBalancer{
		ScoreBasedBalancer: NewScoreBasedBalancer(scheduler, nodeManager, dist, meta, targetMgr),
		targetMgr:          targetMgr,
	}
}

// BalanceReplica balances segments and channels at the channel level within a replica.
// It first checks if exclusive mode can be enabled (all channels have assigned RW nodes).
// In exclusive mode, it balances segments and channels per channel among the channel's assigned nodes.
// If exclusive mode cannot be achieved, it delegates to ScoreBasedBalancer.
func (b *ChannelLevelScoreBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []assign.SegmentAssignPlan, channelPlans []assign.ChannelAssignPlan) {
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

	if streamingutil.IsStreamingServiceEnabled() {
		// Make a plan to rebalance the channel first.
		// The Streaming QueryNode doesn't make the channel level score, so just fallback to the ScoreBasedBalancer.
		channelPlan := b.ScoreBasedBalancer.balanceChannels(ctx, br, replica)
		// If the channelPlan is not empty, do it directly, don't do the segment balance.
		if len(channelPlan) > 0 {
			return nil, channelPlan
		}
	}

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

	// TODO: assign by channel
	channelPlans = make([]assign.ChannelAssignPlan, 0)
	segmentPlans = make([]assign.SegmentAssignPlan, 0)
	for channelName := range channels {
		if replica.NodesCount() == 0 {
			return nil, nil
		}

		channelRwNodes := replica.GetChannelRWNodes(channelName)
		channelRONodes := make([]int64, 0)
		// mark channel's outbound access node as offline
		channelRWNode := typeutil.NewUniqueSet(channelRwNodes...)
		channelDist := b.dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(channelName), meta.WithReplica2Channel(replica))
		for _, channel := range channelDist {
			if !channelRWNode.Contain(channel.Node) {
				channelRONodes = append(channelRONodes, channel.Node)
			}
		}
		segmentDist := b.dist.SegmentDistManager.GetByFilter(meta.WithChannel(channelName), meta.WithReplica(replica))
		for _, segment := range segmentDist {
			if !channelRWNode.Contain(segment.Node) {
				channelRONodes = append(channelRONodes, segment.Node)
			}
		}

		if len(channelRwNodes) == 0 {
			// no available nodes to balance
			return nil, nil
		}

		if len(channelRONodes) != 0 {
			if !streamingutil.IsStreamingServiceEnabled() {
				channelPlans = append(channelPlans, b.genChannelPlanForOutboundNodes(ctx, replica, channelName, channelRwNodes, channelRONodes)...)
			}

			if len(channelPlans) == 0 {
				segmentPlans = append(segmentPlans, b.genSegmentPlanForOutboundNodes(ctx, replica, channelName, channelRwNodes, channelRONodes)...)
			}
		} else {
			if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() && !streamingutil.IsStreamingServiceEnabled() {
				channelPlans = append(channelPlans, b.genChannelPlan(ctx, replica, channelName, channelRwNodes)...)
			}

			if len(channelPlans) == 0 {
				segmentPlans = append(segmentPlans, b.genSegmentPlan(ctx, br, replica, channelName, channelRwNodes)...)
			}
		}
	}

	return segmentPlans, channelPlans
}

// genChannelPlanForOutboundNodes generates channel plans to move channels from outbound nodes
// (nodes that are no longer in the channel's RW node list) to online nodes.
func (b *ChannelLevelScoreBalancer) genChannelPlanForOutboundNodes(ctx context.Context, replica *meta.Replica, channelName string, onlineNodes []int64, offlineNodes []int64) []assign.ChannelAssignPlan {
	channelPlans := make([]assign.ChannelAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(nodeID), meta.WithChannelName2Channel(channelName))
		plans := b.GetAssignPolicy().AssignChannel(ctx, replica.GetCollectionID(), dmChannels, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		channelPlans = append(channelPlans, plans...)
	}
	return channelPlans
}

// genSegmentPlanForOutboundNodes generates segment plans to move segments from outbound nodes
// (nodes that are no longer in the channel's RW node list) to online nodes for a specific channel.
func (b *ChannelLevelScoreBalancer) genSegmentPlanForOutboundNodes(ctx context.Context, replica *meta.Replica, channelName string, onlineNodes []int64, offlineNodes []int64) []assign.SegmentAssignPlan {
	segmentPlans := make([]assign.SegmentAssignPlan, 0)
	for _, nodeID := range offlineNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(nodeID), meta.WithChannel(channelName))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})
		plans := b.GetAssignPolicy().AssignSegment(ctx, replica.GetCollectionID(), segments, onlineNodes, false)
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
		}
		segmentPlans = append(segmentPlans, plans...)
	}
	return segmentPlans
}

// genSegmentPlan generates segment balance plans for a specific channel within a replica.
// It identifies segments on nodes with higher-than-average scores and moves them to nodes
// with lower scores, using the score-based assign policy.
func (b *ChannelLevelScoreBalancer) genSegmentPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, channelName string, onlineNodes []int64) []assign.SegmentAssignPlan {
	// Delegate to the assign policy's implementation with safe type assertion
	policy, ok := b.assignPolicy.(*assign.ScoreBasedAssignPolicy)
	if !ok {
		log.Error("invalid policy type for ScoreBasedBalancer",
			zap.String("expected", "*assign.ScoreBasedAssignPolicy"),
			zap.String("actual", fmt.Sprintf("%T", b.assignPolicy)))
		return nil
	}
	nodeItemsMap := policy.ConvertToNodeItemsBySegment(replica.GetCollectionID(), onlineNodes)
	for _, item := range nodeItemsMap {
		br.AddNodeItem(item)
	}
	if len(nodeItemsMap) == 0 {
		return nil
	}

	log.Ctx(ctx).WithRateGroup(fmt.Sprintf("genSegmentPlan-%d-%d", replica.GetCollectionID(), replica.GetID()), 1, 60).
		RatedInfo(30, "node segment workload status",
			zap.Int64("collectionID", replica.GetCollectionID()),
			zap.Int64("replicaID", replica.GetID()),
			zap.Stringers("nodes", lo.Values(nodeItemsMap)))

	segmentDist := make(map[int64][]*meta.Segment)
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
		currentScore := nodeItemsMap[node].GetCurrentScore()
		assignedScore := nodeItemsMap[node].GetAssignedScore()
		if currentScore <= assignedScore {
			continue
		}

		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
		})
		for _, s := range segments {
			segmentsToMove = append(segmentsToMove, s)
			currentScore -= policy.CalculateSegmentScore(s)
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

	segmentPlans := b.GetAssignPolicy().AssignSegment(ctx, replica.GetCollectionID(), segmentsToMove, onlineNodes, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

// genChannelPlan generates channel balance plans for a specific channel within a replica.
// It distributes channels evenly across online nodes based on channel count.
func (b *ChannelLevelScoreBalancer) genChannelPlan(ctx context.Context, replica *meta.Replica, channelName string, onlineNodes []int64) []assign.ChannelAssignPlan {
	channelPlans := make([]assign.ChannelAssignPlan, 0)
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
			channels = sortIfChannelAtWALLocated(channels)

			if len(channels) <= average {
				nodeWithLessChannel = append(nodeWithLessChannel, node)
				continue
			}

			channelsToMove = append(channelsToMove, channels[average:]...)
		}

		if len(nodeWithLessChannel) == 0 || len(channelsToMove) == 0 {
			return nil
		}

		channelPlans := b.GetAssignPolicy().AssignChannel(ctx, replica.GetCollectionID(), channelsToMove, nodeWithLessChannel, false)
		for i := range channelPlans {
			channelPlans[i].From = channelPlans[i].Channel.Node
			channelPlans[i].Replica = replica
		}

		return channelPlans
	}
	return channelPlans
}
