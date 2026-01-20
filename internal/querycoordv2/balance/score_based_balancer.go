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
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// ScoreBasedBalancer implements a score-based load balancing strategy.
// It calculates node scores using the formula: (collection_row_count + global_row_count * factor)
// and attempts to equalize scores across nodes by redistributing segments.
// This approach considers both collection-specific and global workload to achieve
// comprehensive load balancing.
type ScoreBasedBalancer struct {
	scheduler    task.Scheduler
	nodeManager  *session.NodeManager
	dist         *meta.DistributionManager
	meta         *meta.Meta
	targetMgr    meta.TargetManagerInterface
	assignPolicy assign.AssignPolicy
}

// NewScoreBasedBalancer creates a new ScoreBasedBalancer instance.
// It uses the ScoreBased assign policy from the global factory.
func NewScoreBasedBalancer(scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *ScoreBasedBalancer {
	policy := assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeScoreBased)
	return &ScoreBasedBalancer{
		scheduler:    scheduler,
		nodeManager:  nodeManager,
		dist:         dist,
		meta:         meta,
		targetMgr:    targetMgr,
		assignPolicy: policy,
	}
}

// GetAssignPolicy returns the assign policy used by this balancer.
func (b *ScoreBasedBalancer) GetAssignPolicy() assign.AssignPolicy {
	return b.assignPolicy
}

// BalanceReplica balances segments and channels across nodes within a replica based on node scores.
// It first attempts to balance channels if AutoBalanceChannel is enabled, then balances segments
// if no channel plans were generated. The balancing considers both collection-specific and global
// workload through score calculation.
func (b *ScoreBasedBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []assign.SegmentAssignPlan, channelPlans []assign.ChannelAssignPlan) {
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

	if replica.NodesCount() < 2 {
		br.AddRecord(StrRecord("replica has less than 2 querynodes"))
		return nil, nil
	}

	if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
		channelPlans = b.balanceChannels(ctx, br, replica)
	}
	if len(channelPlans) == 0 {
		segmentPlans = b.balanceSegments(ctx, br, replica)
	}
	return
}

// balanceChannels generates channel balance plans for a replica.
// It requires at least 2 RW nodes to perform balancing.
func (b *ScoreBasedBalancer) balanceChannels(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.ChannelAssignPlan {
	var rwNodes []int64
	if streamingutil.IsStreamingServiceEnabled() {
		rwNodes, _ = utils.GetChannelRWAndRONodesFor260(replica, b.nodeManager)
	} else {
		rwNodes = replica.GetRWNodes()
	}
	if len(rwNodes) < 2 {
		br.AddRecord(StrRecord("no enough rwNodes to balance channels"))
		return nil
	}

	return b.genChannelPlan(ctx, br, replica, rwNodes)
}

// balanceSegments generates segment balance plans for a replica.
// It requires at least 2 RW nodes to perform balancing.
func (b *ScoreBasedBalancer) balanceSegments(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.SegmentAssignPlan {
	rwNodes := replica.GetRWNodes()
	if len(rwNodes) < 2 {
		br.AddRecord(StrRecord("no enough rwNodes to balance segments"))
		return nil
	}

	return b.genSegmentPlan(ctx, br, replica, rwNodes)
}

// genSegmentPlan generates segment balance plans based on node scores.
// It identifies segments on nodes with scores above their assigned quota and moves them
// to nodes with lower scores. Redundant segments (appearing multiple times in distribution)
// are skipped to avoid conflicts.
func (b *ScoreBasedBalancer) genSegmentPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, onlineNodes []int64) []assign.SegmentAssignPlan {
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
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
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
			br.AddRecord(StrRecordf("node %d skip balance since current score(%f) lower than assigned one (%f)", node, currentScore, assignedScore))
			continue
		}

		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
		})
		for _, s := range segments {
			segmentScore := policy.CalculateSegmentScore(s)
			br.AddRecord(StrRecordf("pick segment %d with score %f from node %d", s.ID, segmentScore, node))
			segmentsToMove = append(segmentsToMove, s)
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

	segmentPlans := b.GetAssignPolicy().AssignSegment(ctx, replica.GetCollectionID(), segmentsToMove, onlineNodes, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

// genChannelPlan generates channel balance plans based on node scores.
// It identifies channels on nodes with scores above their assigned quota and moves them
// to nodes with lower scores. Redundant channels are skipped to avoid conflicts.
func (b *ScoreBasedBalancer) genChannelPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, onlineNodes []int64) []assign.ChannelAssignPlan {
	// Delegate to the assign policy's implementation with safe type assertion
	policy, ok := b.assignPolicy.(*assign.ScoreBasedAssignPolicy)
	if !ok {
		log.Error("invalid policy type for ScoreBasedBalancer",
			zap.String("expected", "*assign.ScoreBasedAssignPolicy"),
			zap.String("actual", fmt.Sprintf("%T", b.assignPolicy)))
		return nil
	}
	nodeItemsMap := policy.ConvertToNodeItemsByChannel(replica.GetCollectionID(), onlineNodes)
	// Add nodes to balance report for logging
	for _, item := range nodeItemsMap {
		br.AddNodeItem(item)
	}

	if len(nodeItemsMap) == 0 {
		return nil
	}

	log.Ctx(ctx).WithRateGroup(fmt.Sprintf("genChannelPlan-%d-%d", replica.GetCollectionID(), replica.GetID()), 1, 60).
		RatedInfo(30, "node channel workload status",
			zap.Int64("collectionID", replica.GetCollectionID()),
			zap.Int64("replicaID", replica.GetID()),
			zap.Stringers("nodes", lo.Values(nodeItemsMap)))

	channelDist := make(map[int64][]*meta.DmChannel)
	for _, node := range onlineNodes {
		channelDist[node] = b.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(replica.GetCollectionID()), meta.WithNodeID2Channel(node))
	}

	// find the segment from the node which has more score than the average
	channelsToMove := make([]*meta.DmChannel, 0)
	for node, channels := range channelDist {
		currentScore := nodeItemsMap[node].GetCurrentScore()
		assignedScore := nodeItemsMap[node].GetAssignedScore()
		if currentScore <= assignedScore {
			br.AddRecord(StrRecordf("node %d skip balance since current score(%f) lower than assigned one (%f)", node, currentScore, assignedScore))
			continue
		}
		channels = sortIfChannelAtWALLocated(channels)

		for _, ch := range channels {
			channelScore := policy.CalculateChannelScore(ch, replica.GetCollectionID())
			br.AddRecord(StrRecordf("pick channel %s with score %f from node %d", ch.GetChannelName(), channelScore, node))
			channelsToMove = append(channelsToMove, ch)

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

	channelPlans := b.GetAssignPolicy().AssignChannel(ctx, replica.GetCollectionID(), channelsToMove, onlineNodes, false)
	for i := range channelPlans {
		channelPlans[i].From = channelPlans[i].Channel.Node
		channelPlans[i].Replica = replica
	}

	return channelPlans
}
