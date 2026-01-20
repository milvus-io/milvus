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
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// RowCountBasedBalancer implements a row count based load balancing strategy.
// It balances segments across nodes by considering the total row count on each node,
// attempting to equalize the row count distribution. This is more accurate than
// round-robin balancing as it accounts for actual data volume rather than just segment count.
type RowCountBasedBalancer struct {
	scheduler    task.Scheduler
	nodeManager  *session.NodeManager
	dist         *meta.DistributionManager
	meta         *meta.Meta
	targetMgr    meta.TargetManagerInterface
	assignPolicy assign.AssignPolicy
}

// GetAssignPolicy returns the assign policy used by this balancer.
func (b *RowCountBasedBalancer) GetAssignPolicy() assign.AssignPolicy {
	return b.assignPolicy
}

// BalanceReplica balances segments and channels across nodes within a replica based on row count.
// It first attempts to balance channels if AutoBalanceChannel is enabled, then balances segments
// if no channel plans were generated.
func (b *RowCountBasedBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []assign.SegmentAssignPlan, channelPlans []assign.ChannelAssignPlan) {
	log := log.Ctx(context.TODO()).WithRateGroup("qcv2.RowCountBasedBalancer", 1, 60).With(
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
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
func (b *RowCountBasedBalancer) balanceChannels(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.ChannelAssignPlan {
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
func (b *RowCountBasedBalancer) balanceSegments(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.SegmentAssignPlan {
	rwNodes := replica.GetRWNodes()
	if len(rwNodes) < 2 {
		br.AddRecord(StrRecord("no enough rwNodes to balance segments"))
		return nil
	}
	return b.genSegmentPlan(ctx, replica, rwNodes)
}

// genSegmentPlan generates segment balance plans based on row count per node.
// It identifies nodes with row count above average and moves segments from them
// to nodes with row count below average.
func (b *RowCountBasedBalancer) genSegmentPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64) []assign.SegmentAssignPlan {
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

	segmentPlans := b.assignPolicy.AssignSegment(ctx, replica.GetCollectionID(), segmentsToMove, nodesWithLessRow, false)
	for i := range segmentPlans {
		segmentPlans[i].From = segmentPlans[i].Segment.Node
		segmentPlans[i].Replica = replica
	}

	return segmentPlans
}

// genChannelPlan generates channel balance plans based on channel count per node.
// It distributes channels evenly across RW nodes.
func (b *RowCountBasedBalancer) genChannelPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, rwNodes []int64) []assign.ChannelAssignPlan {
	channelPlans := make([]assign.ChannelAssignPlan, 0)
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

		channelPlans := b.assignPolicy.AssignChannel(ctx, replica.GetCollectionID(), channelsToMove, nodeWithLessChannel, false)
		for i := range channelPlans {
			channelPlans[i].From = channelPlans[i].Channel.Node
			channelPlans[i].Replica = replica
			br.AddRecord(StrRecordf("add channel plan %s", channelPlans[i]))
		}

		return channelPlans
	}
	return channelPlans
}

// NewRowCountBasedBalancer creates a new RowCountBasedBalancer instance.
// It uses the RowCount assign policy from the global factory.
func NewRowCountBasedBalancer(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *RowCountBasedBalancer {
	policy := assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeRowCount)
	return &RowCountBasedBalancer{
		scheduler:    scheduler,
		nodeManager:  nodeManager,
		dist:         dist,
		meta:         meta,
		targetMgr:    targetMgr,
		assignPolicy: policy,
	}
}
