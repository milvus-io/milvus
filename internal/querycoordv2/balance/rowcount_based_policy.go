// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package balance

import (
	"math"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// RowCountBasedPolicy try to achieve that each nodes have same num of rows and channels in collection level.
// it will use row count as node's priority to move segment from the highest node to lowest node.
type RowCountBasedPolicy struct {
	dist      *meta.DistributionManager
	scheduler task.Scheduler
}

// getPriorityWithSegment use row count to compute node's priority for balance
func (b *RowCountBasedPolicy) getPriorityWithSegment(collectionID int64, nodeID int64) int {
	segments := b.dist.SegmentDistManager.GetByCollectionAndNode(collectionID, nodeID)
	rowCount := lo.SumBy(segments, func(s *meta.Segment) int { return int(s.GetNumOfRows()) })

	// segment distribution is a bit delayed, so plus the row count in executing task
	return rowCount + b.scheduler.GetNodeSegmentDelta(nodeID)
}

// getPriorityWithChannel use channel count to compute nodes's priority for balance
func (b *RowCountBasedPolicy) getPriorityWithChannel(collectionID int64, nodeID int64) int {
	channels := b.dist.ChannelDistManager.GetByCollectionAndNode(collectionID, nodeID)

	// channel distribution is a bit delayed, so plus the channel count in executing task
	return len(channels) + b.scheduler.GetNodeChannelDelta(nodeID)
}

// getPriorityChange defines the priority change when assign a segment to node
func (b *RowCountBasedPolicy) getPriorityChange(s *meta.Segment) int {
	return int(s.GetNumOfRows())
}

func (b *RowCountBasedPolicy) getAverageWithChannel(collectionID int64, nodes []int64) int {
	totalChannelCount := 0
	for _, node := range nodes {
		totalChannelCount += len(b.dist.ChannelDistManager.GetByCollectionAndNode(collectionID, node))
	}
	return int(math.Ceil(float64(totalChannelCount) / float64(len(nodes))))
}

func (b *RowCountBasedPolicy) getAverageWithSegment(collectionID int64, nodes []int64) int {
	totalScore := 0
	for _, node := range nodes {
		totalScore += b.getPriorityWithSegment(collectionID, node)
	}
	return int(math.Ceil(float64(totalScore) / float64(len(nodes))))
}

func (b *RowCountBasedPolicy) getSegmentsToMove(collectionID int64, nodeID int64, averageCount int) []*meta.Segment {
	segments := b.dist.SegmentDistManager.GetByCollectionAndNode(collectionID, nodeID)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	rowCount := 0
	segmentToMove := make([]*meta.Segment, 0)
	for _, s := range segments {
		rowCount += int(s.GetNumOfRows())
		if rowCount <= averageCount {
			continue
		}
		segmentToMove = append(segmentToMove, s)
	}

	return segmentToMove
}

func (b *RowCountBasedPolicy) getChannelsToMove(collectionID int64, nodeID int64, averageCount int) []*meta.DmChannel {
	channels := b.dist.ChannelDistManager.GetByCollectionAndNode(collectionID, nodeID)

	if len(channels) > averageCount {
		return channels[averageCount:]
	}
	return nil
}

func (b *RowCountBasedPolicy) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	if len(nodes) == 0 || len(segments) == 0 {
		return nil
	}

	// compute node's priority by row count
	nodeItems := b.convertToNodeItems(collectionID, nodes, b.getPriorityWithSegment)
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

	plans := make([]SegmentAssignPlan, 0)
	for _, s := range segments {
		node := queue.pop().(*nodeItem)
		plan := SegmentAssignPlan{
			From:    -1,
			To:      node.nodeID,
			Segment: s,
		}
		plans = append(plans, plan)
		node.setPriority(node.getPriority() + b.getPriorityChange(s))
		queue.push(node)
	}
	return plans
}

func (b *RowCountBasedPolicy) AssignChannel(channels []*meta.DmChannel, nodes []int64) []ChannelAssignPlan {
	if len(nodes) == 0 || len(channels) == 0 {
		return nil
	}

	nodeItems := b.convertToNodeItems(channels[0].CollectionID, nodes, b.getPriorityWithChannel)
	queue := newPriorityQueue()
	for _, item := range nodeItems {
		queue.push(item)
	}

	plans := make([]ChannelAssignPlan, 0)
	for _, c := range channels {
		node := queue.pop().(*nodeItem)
		plan := ChannelAssignPlan{
			From:    -1,
			To:      node.nodeID,
			Channel: c,
		}
		plans = append(plans, plan)
		node.setPriority(node.getPriority() + 1)
		queue.push(node)
	}

	return plans
}

func (b *RowCountBasedPolicy) convertToNodeItems(collectionID int64, nodeIDs []int64, nodePriority NodePriority) []*nodeItem {
	ret := make([]*nodeItem, 0, len(nodeIDs))
	for _, node := range nodeIDs {
		// more row count, less priority
		nodeItem := newNodeItem(nodePriority(collectionID, node), node)
		ret = append(ret, &nodeItem)
	}
	return ret
}

func NewRowCountBasedPolicy(
	scheduler task.Scheduler,
	dist *meta.DistributionManager,
) *RowCountBasedPolicy {
	return &RowCountBasedPolicy{
		scheduler: scheduler,
		dist:      dist,
	}
}
