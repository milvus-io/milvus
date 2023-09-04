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
	"math"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// ScoreBasedPolicy also try to achieve that each nodes have same num of rows and channels.
// but it make a tradeoff between collection level and global level.
// for segment it introduced a global row count factor, and use row_count + global_row_count * factor
// as node's priority during balance, and global_row_count means total row count of all collections in same node
type ScoreBasedPolicy struct {
	*RowCountBasedPolicy
}

func NewScoreBasedPolicy(scheduler task.Scheduler, dist *meta.DistributionManager) *ScoreBasedPolicy {
	return &ScoreBasedPolicy{
		RowCountBasedPolicy: NewRowCountBasedPolicy(scheduler, dist),
	}
}

func (b *ScoreBasedPolicy) AssignSegment(collectionID int64, segments []*meta.Segment, nodes []int64) []SegmentAssignPlan {
	if len(nodes) == 0 || len(segments) == 0 {
		return nil
	}

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

// getPriorityChange get priority change when assign segment to node
func (b *ScoreBasedPolicy) getPriorityChange(s *meta.Segment) int {
	// use rowCount + rowCount*factor as the priority change
	return int(s.GetNumOfRows()) + int(float64(s.GetNumOfRows())*params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat())
}

// getPriorityWithSegment calculate node's priority by collection_row_count + global_row_count * factor
func (b *ScoreBasedPolicy) getPriorityWithSegment(collectionID, nodeID int64) int {
	globalSegments := b.dist.SegmentDistManager.GetByNode(nodeID)
	rowCount := lo.SumBy(globalSegments, func(s *meta.Segment) int { return int(s.GetNumOfRows()) })

	collectionSegments := b.dist.SegmentDistManager.GetByCollectionAndNode(collectionID, nodeID)
	collectionRowCount := lo.SumBy(collectionSegments, func(s *meta.Segment) int { return int(s.GetNumOfRows()) })

	// segment distribution is a bit delayed, so plus the row count in executing task
	return collectionRowCount + int(float64(rowCount)*params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat()) + b.scheduler.GetNodeSegmentDelta(nodeID)
}

func (b *ScoreBasedPolicy) getAverageWithSegment(collectionID int64, nodes []int64) int {
	totalScore := 0
	for _, node := range nodes {
		totalScore += b.getPriorityWithSegment(collectionID, node)
	}
	return int(math.Ceil(float64(totalScore) / float64(len(nodes))))
}

func (b *ScoreBasedPolicy) getSegmentsToMove(collectionID int64, nodeID int64, average int) []*meta.Segment {
	segments := b.dist.SegmentDistManager.GetByCollectionAndNode(collectionID, nodeID)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() < segments[j].GetNumOfRows()
	})

	nodeP := b.getPriorityWithSegment(collectionID, nodeID)
	segmentToMove := make([]*meta.Segment, 0)
	for _, s := range segments {
		change := b.getPriorityChange(s)
		if nodeP-change < average {
			break
		}
		segmentToMove = append(segmentToMove, s)
		nodeP -= change
	}

	return segmentToMove
}
