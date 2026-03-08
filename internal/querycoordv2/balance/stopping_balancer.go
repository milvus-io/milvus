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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// StoppingBalancer is responsible for balancing segments and channels from stopping nodes (RO nodes)
// to active nodes (RW nodes). It provides a centralized implementation of stopping balance logic
// that was previously duplicated across multiple balancer implementations.
type StoppingBalancer struct {
	dist         *meta.DistributionManager
	targetMgr    meta.TargetManagerInterface
	assignPolicy assign.AssignPolicy
	nodeManager  *session.NodeManager
}

// NewStoppingBalancer creates a new StoppingBalancer instance
func NewStoppingBalancer(
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	assignPolicy assign.AssignPolicy,
	nodeManager *session.NodeManager,
) *StoppingBalancer {
	return &StoppingBalancer{
		dist:         dist,
		targetMgr:    targetMgr,
		assignPolicy: assignPolicy,
		nodeManager:  nodeManager,
	}
}

// GetAssignPolicy returns the assign policy used by this balancer
func (b *StoppingBalancer) GetAssignPolicy() assign.AssignPolicy {
	return b.assignPolicy
}

// BalanceReplica generates balance plans for segments and channels on stopping nodes (RO nodes)
// and reassigns them to active nodes (RW nodes).
func (b *StoppingBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []assign.SegmentAssignPlan, channelPlans []assign.ChannelAssignPlan) {
	log := log.Ctx(ctx).WithRateGroup("qcv2.StoppingBalancer", 1, 60).With(
		zap.Int64("collectionID", replica.GetCollectionID()),
		zap.Int64("replicaID", replica.GetID()),
		zap.String("resourceGroup", replica.GetResourceGroup()),
	)

	br := NewBalanceReport()
	defer func() {
		if len(segmentPlans) == 0 && len(channelPlans) == 0 {
			log.WithRateGroup(fmt.Sprintf("stoppingbalance-noplan-%d", replica.GetID()), 1, 60).
				RatedDebug(60, "no stopping balance plan generated", zap.Stringers("records", br.detailRecords))
		} else {
			log.Info("stopping balance plan generated", zap.Stringers("report details", br.records))
		}
	}()

	if !paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		br.AddRecord(StrRecord("stopping balance is disabled"))
		log.RatedInfo(10, "stopping balance is disabled")
		return nil, nil
	}

	// Generate plans to move segments and channels from stopping nodes to active nodes
	channelPlans = b.balanceChannels(ctx, br, replica)
	if len(channelPlans) == 0 {
		segmentPlans = b.balanceSegments(ctx, br, replica)
	}
	return
}

func (b *StoppingBalancer) balanceChannels(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.ChannelAssignPlan {
	var rwNodes, roNodes []int64
	if streamingutil.IsStreamingServiceEnabled() {
		rwNodes, roNodes = utils.GetChannelRWAndRONodesFor260(replica, b.nodeManager)
	} else {
		rwNodes, roNodes = replica.GetRWNodes(), replica.GetRONodes()
	}

	// If there are no RW nodes or no RO nodes, no stopping balance is needed
	if len(rwNodes) == 0 || len(roNodes) == 0 {
		br.AddRecord(StrRecordf("no stopping balance needed: rwNodes=%d, roNodes=%d", len(rwNodes), len(roNodes)))
		return nil
	}

	return b.genChannelPlan(ctx, br, replica, rwNodes, roNodes)
}

func (b *StoppingBalancer) balanceSegments(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.SegmentAssignPlan {
	rwNodes, roNodes := replica.GetRWNodes(), replica.GetRONodes()
	// If there are no RW nodes or no RO nodes, no stopping balance is needed
	if len(rwNodes) == 0 || len(roNodes) == 0 {
		br.AddRecord(StrRecordf("no stopping balance needed: rwNodes=%d, roNodes=%d", len(rwNodes), len(roNodes)))
		return nil
	}
	return b.genSegmentPlan(ctx, br, replica, rwNodes, roNodes)
}

// genChannelPlan generates channel assignment plans to move channels from stopping nodes (roNodes)
// to active nodes (rwNodes).
func (b *StoppingBalancer) genChannelPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, rwNodes []int64, roNodes []int64) []assign.ChannelAssignPlan {
	channelPlans := make([]assign.ChannelAssignPlan, 0)

	for _, nodeID := range roNodes {
		// Get all channels on this stopping node
		dmChannels := b.dist.ChannelDistManager.GetByCollectionAndFilter(
			replica.GetCollectionID(),
			meta.WithNodeID2Channel(nodeID),
		)

		if len(dmChannels) == 0 {
			br.AddDetailRecord(StrRecordf("no channels found on stopping node %d", nodeID))
			continue
		}

		br.AddRecord(StrRecordf("found %d channels on stopping node %d to be reassigned", len(dmChannels), nodeID))

		// Assign these channels to active nodes
		plans := b.assignPolicy.AssignChannel(ctx, replica.GetCollectionID(), dmChannels, rwNodes, false)

		// Set the source node and replica for each plan
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
			br.AddDetailRecord(StrRecordf("channel %s: node %d -> node %d", plans[i].Channel.GetChannelName(), nodeID, plans[i].To))
		}

		channelPlans = append(channelPlans, plans...)
	}

	if len(channelPlans) > 0 {
		br.AddRecord(StrRecordf("generated %d channel plans for stopping balance", len(channelPlans)))
	}

	return channelPlans
}

// genSegmentPlan generates segment assignment plans to move segments from stopping nodes (roNodes)
// to active nodes (rwNodes). Only segments that can be moved are included in the plans.
func (b *StoppingBalancer) genSegmentPlan(ctx context.Context, br *balanceReport, replica *meta.Replica, rwNodes []int64, roNodes []int64) []assign.SegmentAssignPlan {
	segmentPlans := make([]assign.SegmentAssignPlan, 0)

	for _, nodeID := range roNodes {
		// Get all segments on this stopping node
		dist := b.dist.SegmentDistManager.GetByFilter(
			meta.WithCollectionID(replica.GetCollectionID()),
			meta.WithNodeID(nodeID),
		)

		// Filter out segments that cannot be moved
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})

		if len(dist) > 0 {
			immovableCount := len(dist) - len(segments)
			if immovableCount > 0 {
				br.AddDetailRecord(StrRecordf("node %d has %d segments, %d are immovable", nodeID, len(dist), immovableCount))
			}
		}

		if len(segments) == 0 {
			br.AddDetailRecord(StrRecordf("no movable segments found on stopping node %d", nodeID))
			continue
		}

		br.AddRecord(StrRecordf("found %d movable segments on stopping node %d to be reassigned", len(segments), nodeID))

		// Assign these segments to active nodes
		plans := b.assignPolicy.AssignSegment(ctx, replica.GetCollectionID(), segments, rwNodes, false)

		// Set the source node and replica for each plan
		for i := range plans {
			plans[i].From = nodeID
			plans[i].Replica = replica
			br.AddDetailRecord(StrRecordf("segment %d: node %d -> node %d", plans[i].Segment.GetID(), nodeID, plans[i].To))
		}

		segmentPlans = append(segmentPlans, plans...)
	}

	if len(segmentPlans) > 0 {
		br.AddRecord(StrRecordf("generated %d segment plans for stopping balance", len(segmentPlans)))
	}

	return segmentPlans
}
