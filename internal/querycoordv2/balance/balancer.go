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
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Balance defines two things about balance impl details.
// 1. what to balance.
// for now, if node becomes stopping, or node has been transfer between resource groups.
// segments/channels on it should be moved to other nodes, which and be achieved by balance;
// also if segments/channels distribution on query node, we should trigger balance to do reassign.
//
// 2. how to balance.
// balance should be triggered in order. the stopping node's balance should be triggered first,
// which will recover the collection's load percents; then the transfer node operation's balance should be triggered,
// to complete the transfer operation in async; the distribution balance should be triggered in last if auto balance is enabled.
type Balance interface {
	GetPolicy() BalancePolicy
	BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan)
}
type Balancer struct {
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
	targetMgr   *meta.TargetManager

	policy BalancePolicy
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

type SegmentSkipBalanceFilter func(s *meta.Segment) bool

func (b *Balancer) segmentNotRedundant(s *meta.Segment) bool {
	return len(b.dist.SegmentDistManager.Get(s.GetID())) == 1
}

func (b *Balancer) segmentInTarget(s *meta.Segment) bool {
	return b.targetMgr.GetSealedSegment(s.GetCollectionID(), s.GetID(), meta.CurrentTarget) != nil
}

type NodeChangeFilter func(replica *meta.Replica, nodeID int64) bool

// for query node which becomes stopping
func (b *Balancer) StoppingNodeFilter(replica *meta.Replica, nodeID int64) bool {
	isStopping, err := b.nodeManager.IsStoppingNode(nodeID)
	if err != nil {
		log.Info("node not found",
			zap.Int64("nodeID", nodeID),
			zap.Error(err))
		return false
	}

	return isStopping
}

// for query node which is available but not visible for replica
func (b *Balancer) OutboundNodeFilter(replica *meta.Replica, nodeID int64) bool {
	outboundNodes := b.meta.ResourceManager.CheckOutboundNodes(replica)

	return outboundNodes.Contain(nodeID)
}

func (b *Balancer) GetPolicy() BalancePolicy {
	return b.policy
}

func (b *Balancer) BalanceReplica(replica *meta.Replica) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	nodes := replica.GetNodes()
	if len(nodes) < 2 {
		return nil, nil
	}

	// 1. balance for stopping nodes
	sPlans, cPlans := b.balanceNodeChanges(replica, b.StoppingNodeFilter)

	// 2. balance for query node resource change, such as transfer nodes between resource group
	if len(sPlans) == 0 && len(cPlans) == 0 {
		sPlans, cPlans = b.balanceNodeChanges(replica, b.OutboundNodeFilter)
	}

	// 3. balance for all available node, if auto balance is enabled
	if len(sPlans) == 0 && len(cPlans) == 0 && paramtable.Get().QueryCoordCfg.AutoBalance.GetAsBool() {
		nodes = lo.Filter(replica.GetNodes(), func(node int64, _ int) bool {
			return !b.StoppingNodeFilter(replica, node) && !b.OutboundNodeFilter(replica, node)
		})
		sPlans = b.balanceSegment(replica.GetCollectionID(), replica.GetID(), nodes)
		cPlans = b.balanceChannel(replica.GetCollectionID(), replica.GetID(), nodes)
	}

	for i := range cPlans {
		cPlans[i].From = cPlans[i].Channel.Node
		cPlans[i].ReplicaID = replica.GetID()
	}

	sPlans = lo.Filter(sPlans, func(plan SegmentAssignPlan, _ int) bool {
		return b.segmentInTarget(plan.Segment) && b.segmentNotRedundant(plan.Segment)
	})
	for i := range sPlans {
		sPlans[i].From = sPlans[i].Segment.Node
		sPlans[i].ReplicaID = replica.GetID()
	}

	return sPlans, cPlans
}

func (b *Balancer) balanceNodeChanges(replica *meta.Replica, filter NodeChangeFilter) ([]SegmentAssignPlan, []ChannelAssignPlan) {
	segmentToMove := make([]*meta.Segment, 0)
	channelToMove := make([]*meta.DmChannel, 0)

	targetNodes := make([]int64, 0)
	for _, node := range replica.GetNodes() {
		if filter(replica, node) {
			log.RatedInfo(10, "meet node changes, try to move out all segment/channel",
				zap.Int64("collectionID", replica.GetCollectionID()),
				zap.Int64("replicaID", replica.GetCollectionID()),
				zap.Int64("node", node),
			)
			segmentToMove = append(segmentToMove, b.dist.SegmentDistManager.GetByCollectionAndNode(replica.GetCollectionID(), node)...)
			channelToMove = append(channelToMove, b.dist.ChannelDistManager.GetByCollectionAndNode(replica.GetCollectionID(), node)...)
			continue
		}

		targetNodes = append(targetNodes, node)
	}

	return b.policy.AssignSegment(replica.GetCollectionID(), segmentToMove, targetNodes),
		b.policy.AssignChannel(channelToMove, targetNodes)
}

func (b *Balancer) balanceChannel(collectionID int64, replicaID int64, nodes []int64) []ChannelAssignPlan {
	averageChannelCount := b.policy.getAverageWithChannel(collectionID, nodes)
	if averageChannelCount == 0 {
		return nil
	}

	// for nodes in same replica, move channels from nodes which beyond average count to others
	targetNode := make([]int64, 0)
	toMove := make([]*meta.DmChannel, 0)
	for _, node := range nodes {
		channels := b.policy.getChannelsToMove(collectionID, node, averageChannelCount)

		if len(channels) > 0 {
			toMove = append(toMove, channels...)
		} else {
			targetNode = append(targetNode, node)
		}
	}

	cPlans := b.policy.AssignChannel(toMove, targetNode)

	// if assign plan to target node cause target node beyond the average count, skip it
	cPlans = lo.Filter(cPlans, func(plan ChannelAssignPlan, _ int) bool {
		targetNode := plan.To
		channels := b.dist.ChannelDistManager.GetByCollectionAndNode(collectionID, targetNode)
		return len(channels)+1 <= averageChannelCount
	})
	return cPlans
}

func (b *Balancer) balanceSegment(collectionID int64, replicaID int64, nodes []int64) []SegmentAssignPlan {
	averageRowCount := b.policy.getAverageWithSegment(collectionID, nodes)
	if averageRowCount == 0 {
		return nil
	}

	// for nodes in same replica, move segments from nodes which beyond average count to others
	segmentTargetNode := make([]int64, 0)
	segmentToMove := make([]*meta.Segment, 0)
	for _, node := range nodes {
		segments := b.policy.getSegmentsToMove(collectionID, node, averageRowCount)
		if len(segments) > 0 {
			segmentToMove = append(segmentToMove, segments...)
		} else {
			segmentTargetNode = append(segmentTargetNode, node)
		}
	}

	sPlans := b.policy.AssignSegment(collectionID, segmentToMove, segmentTargetNode)

	// if assign plan to target node cause target node beyond the average count, skip it
	sPlans = lo.Filter(sPlans, func(plan SegmentAssignPlan, _ int) bool {
		target := b.policy.getPriorityWithSegment(collectionID, plan.To)
		change := b.policy.getPriorityChange(plan.Segment)

		return target+change <= averageRowCount
	})

	return sPlans
}

func NewBalancer(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	nodeManager *session.NodeManager,
	policy BalancePolicy,
) *Balancer {
	return &Balancer{
		dist:        dist,
		meta:        meta,
		targetMgr:   targetMgr,
		nodeManager: nodeManager,
		policy:      policy,
	}
}
