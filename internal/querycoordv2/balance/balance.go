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

// Package balance provides load balancing functionality for QueryCoord.
// It contains different balancer implementations that redistribute segments and channels
// across query nodes to achieve balanced resource utilization.
package balance

import (
	"context"
	"math"

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

// Balance is the interface that all balancers must implement.
// It defines the contract for balancing segments and channels across query nodes within a replica.
type Balance interface {
	// BalanceReplica balances segments and channels across nodes in a replica.
	// It returns segment assignment plans and channel assignment plans that describe
	// the movements needed to achieve better balance.
	BalanceReplica(ctx context.Context, replica *meta.Replica) ([]assign.SegmentAssignPlan, []assign.ChannelAssignPlan)

	// GetAssignPolicy returns the underlying AssignPolicy used for resource assignment.
	// This allows callers to access the policy for custom assignment operations.
	GetAssignPolicy() assign.AssignPolicy
}

// RoundRobinBalancer implements a simple round-robin based load balancing strategy.
// It balances segments and channels by distributing them evenly across nodes based on count,
// without considering the actual resource consumption (like row count or memory usage).
// This balancer is useful for scenarios where uniform distribution is more important than
// weighted distribution based on actual load.
type RoundRobinBalancer struct {
	scheduler    task.Scheduler
	nodeManager  *session.NodeManager
	dist         *meta.DistributionManager
	meta         *meta.Meta
	targetMgr    meta.TargetManagerInterface
	assignPolicy assign.AssignPolicy
}

// GetAssignPolicy returns the underlying AssignPolicy used by this balancer.
func (b *RoundRobinBalancer) GetAssignPolicy() assign.AssignPolicy {
	return b.assignPolicy
}

// BalanceReplica balances segments and channels across nodes within a replica using round-robin strategy.
// It first attempts to balance channels if AutoBalanceChannel is enabled, then balances segments
// if no channel plans were generated.
func (b *RoundRobinBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []assign.SegmentAssignPlan, channelPlans []assign.ChannelAssignPlan) {
	log := log.Ctx(ctx).WithRateGroup("qcv2.RoundRobinBalancer", 1, 60).With(
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
		zap.String("resourceGroup", replica.GetResourceGroup()),
	)

	if replica.NodesCount() < 2 {
		log.RatedDebug(60, "replica has less than 2 querynodes, skip balance")
		return nil, nil
	}

	if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
		channelPlans = b.balanceChannels(ctx, replica)
	}
	if len(channelPlans) == 0 {
		segmentPlans = b.balanceSegments(ctx, replica)
	}

	if len(segmentPlans) > 0 || len(channelPlans) > 0 {
		log.Info("balance plan generated",
			zap.Int("segmentPlans", len(segmentPlans)),
			zap.Int("channelPlans", len(channelPlans)))
	}
	return
}

// balanceChannels generates channel balance plans for a replica.
// It requires at least 2 RW nodes to perform balancing.
func (b *RoundRobinBalancer) balanceChannels(ctx context.Context, replica *meta.Replica) []assign.ChannelAssignPlan {
	var rwNodes []int64
	if streamingutil.IsStreamingServiceEnabled() {
		rwNodes, _ = utils.GetChannelRWAndRONodesFor260(replica, b.nodeManager)
	} else {
		rwNodes = replica.GetRWNodes()
	}

	if len(rwNodes) < 2 {
		return nil
	}

	return b.genChannelPlan(ctx, replica, rwNodes)
}

// balanceSegments generates segment balance plans for a replica.
// It requires at least 2 RW nodes to perform balancing.
func (b *RoundRobinBalancer) balanceSegments(ctx context.Context, replica *meta.Replica) []assign.SegmentAssignPlan {
	rwNodes := replica.GetRWNodes()
	if len(rwNodes) < 2 {
		return nil
	}
	return b.genSegmentPlan(ctx, replica, rwNodes)
}

// genSegmentPlan generates segment balance plans based on segment count per node.
// It moves segments from nodes with more segments to nodes with fewer segments.
func (b *RoundRobinBalancer) genSegmentPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64) []assign.SegmentAssignPlan {
	segmentDist := make(map[int64][]*meta.Segment)
	totalSegments := 0

	for _, node := range rwNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})
		segmentDist[node] = segments
		totalSegments += len(segments)
	}

	if totalSegments == 0 {
		return nil
	}

	// Calculate average segment count per node
	average := int(math.Ceil(float64(totalSegments) / float64(len(rwNodes))))

	// Find nodes with fewer segments than average
	nodesWithLessSegment := make([]int64, 0)
	segmentsToMove := make([]*meta.Segment, 0)

	for node, segments := range segmentDist {
		if len(segments) <= average {
			nodesWithLessSegment = append(nodesWithLessSegment, node)
			continue
		}

		// Pick segments to move from nodes with more segments than average
		segmentsToMove = append(segmentsToMove, segments[average:]...)
	}

	// Filter out redundant segments (segments that appear multiple times in distribution)
	segmentsToMove = lo.Filter(segmentsToMove, func(s *meta.Segment, _ int) bool {
		return len(b.dist.SegmentDistManager.GetByFilter(meta.WithReplica(replica), meta.WithSegmentID(s.GetID()))) == 1
	})

	if len(nodesWithLessSegment) == 0 || len(segmentsToMove) == 0 {
		return nil
	}

	segmentPlans := b.assignPolicy.AssignSegment(ctx, replica.GetCollectionID(), segmentsToMove, nodesWithLessSegment, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

// genChannelPlan generates channel balance plans based on channel count per node.
// It moves channels from nodes with more channels to nodes with fewer channels.
func (b *RoundRobinBalancer) genChannelPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64) []assign.ChannelAssignPlan {
	channelDist := b.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica))
	if len(channelDist) == 0 {
		return nil
	}

	// Calculate average channel count per node
	average := int(math.Ceil(float64(len(channelDist)) / float64(len(rwNodes))))

	// Find nodes with fewer channels than average
	nodesWithLessChannel := make([]int64, 0)
	channelsToMove := make([]*meta.DmChannel, 0)

	for _, node := range rwNodes {
		channels := b.dist.ChannelDistManager.GetByCollectionAndFilter(replica.GetCollectionID(), meta.WithNodeID2Channel(node))
		channels = sortIfChannelAtWALLocated(channels)

		if len(channels) <= average {
			nodesWithLessChannel = append(nodesWithLessChannel, node)
			continue
		}

		// Pick channels to move from nodes with more channels than average
		channelsToMove = append(channelsToMove, channels[average:]...)
	}

	if len(nodesWithLessChannel) == 0 || len(channelsToMove) == 0 {
		return nil
	}

	channelPlans := b.assignPolicy.AssignChannel(ctx, replica.GetCollectionID(), channelsToMove, nodesWithLessChannel, false)
	for i := range channelPlans {
		channelPlans[i].From = channelPlans[i].Channel.Node
		channelPlans[i].Replica = replica
	}

	return channelPlans
}

// NewRoundRobinBalancer creates a new RoundRobinBalancer instance.
// It uses the RoundRobin assign policy from the global factory.
func NewRoundRobinBalancer(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *RoundRobinBalancer {
	policy := assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeRoundRobin)
	return &RoundRobinBalancer{
		scheduler:    scheduler,
		nodeManager:  nodeManager,
		dist:         dist,
		meta:         meta,
		targetMgr:    targetMgr,
		assignPolicy: policy,
	}
}
