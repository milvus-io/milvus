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

package assign

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// RowCountBasedAssignPolicy implements priority queue-based assignment strategy for both segments and channels
// It assigns segments to nodes with the least row count, and channels to nodes with the least channel count
type RowCountBasedAssignPolicy struct {
	nodeManager *session.NodeManager
	scheduler   task.Scheduler
	dist        *meta.DistributionManager
}

// newRowCountBasedAssignPolicy creates a new RowCountBasedAssignPolicy
// This is a private constructor. Use GetGlobalAssignPolicyFactory().GetPolicy() to create instances.
func newRowCountBasedAssignPolicy(
	nodeManager *session.NodeManager,
	scheduler task.Scheduler,
	dist *meta.DistributionManager,
) *RowCountBasedAssignPolicy {
	return &RowCountBasedAssignPolicy{
		nodeManager: nodeManager,
		scheduler:   scheduler,
		dist:        dist,
	}
}

// AssignSegment assigns segments to nodes using row count-based priority queue strategy
func (p *RowCountBasedAssignPolicy) AssignSegment(
	ctx context.Context,
	collectionID int64,
	segments []*meta.Segment,
	nodes []int64,
	forceAssign bool,
) []SegmentAssignPlan {
	// Filter nodes
	nodeFilter := newCommonSegmentNodeFilter(p.nodeManager)
	filteredNodes := nodeFilter.FilterNodes(ctx, nodes, forceAssign)
	if len(filteredNodes) == 0 {
		return nil
	}

	// Convert nodes to node items with row count scores
	nodeItems := p.convertToNodeItemsBySegment(filteredNodes)
	if len(nodeItems) == 0 {
		return nil
	}

	// Create priority queue and push all node items
	queue := NewPriorityQueue()
	for _, item := range nodeItems {
		queue.Push(item)
	}

	// Sort segments by row count (descending)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetNumOfRows() > segments[j].GetNumOfRows()
	})

	// Apply batch size limit
	balanceBatchSize := paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.GetAsInt()
	plans := make([]SegmentAssignPlan, 0, len(segments))

	// Assign segments using priority queue
	for _, s := range segments {
		// Pick the node with the least row count
		ni := queue.Pop().(*NodeItem)
		plan := SegmentAssignPlan{
			From:    -1,
			To:      ni.NodeID,
			Segment: s,
		}
		plans = append(plans, plan)
		if len(plans) >= balanceBatchSize {
			break
		}

		// Update node's score and push back to queue
		ni.AddCurrentScoreDelta(float64(s.GetNumOfRows()))
		queue.Push(ni)
	}

	return plans
}

// AssignChannel assigns channels to nodes using channel count-based priority queue strategy
func (p *RowCountBasedAssignPolicy) AssignChannel(
	ctx context.Context,
	collectionID int64,
	channels []*meta.DmChannel,
	nodes []int64,
	forceAssign bool,
) []ChannelAssignPlan {
	// Filter nodes
	nodeFilter := newCommonChannelNodeFilter(p.nodeManager)
	filteredNodes := nodeFilter.FilterNodes(ctx, nodes, forceAssign)
	if len(filteredNodes) == 0 {
		return nil
	}

	// Convert nodes to node items with channel count scores
	nodeItems := p.convertToNodeItemsByChannel(filteredNodes)
	if len(nodeItems) == 0 {
		return nil
	}

	// Create priority queue and push all node items
	queue := NewPriorityQueue()
	for _, item := range nodeItems {
		queue.Push(item)
	}

	plans := make([]ChannelAssignPlan, 0)
	for _, c := range channels {
		var ni *NodeItem

		// If streaming service is enabled, assign channel to the node where WAL is located
		if streamingutil.IsStreamingServiceEnabled() {
			nodeID := snmanager.StaticStreamingNodeManager.GetWALLocated(c.GetChannelName())
			if item, ok := nodeItems[nodeID]; ok {
				ni = item
			}
		}

		if ni == nil {
			// Pick the node with the least channel num
			ni = queue.Pop().(*NodeItem)
		}

		plan := ChannelAssignPlan{
			From:    -1,
			To:      ni.NodeID,
			Channel: c,
		}
		plans = append(plans, plan)

		// Update node's score and push back to queue
		ni.AddCurrentScoreDelta(1)
		queue.Push(ni)
	}

	return plans
}

// convertToNodeItemsBySegment creates node items with row count scores
func (p *RowCountBasedAssignPolicy) convertToNodeItemsBySegment(nodeIDs []int64) map[int64]*NodeItem {
	ret := make(map[int64]*NodeItem, len(nodeIDs))
	for _, node := range nodeIDs {
		// Calculate sealed segment row count on node
		segments := p.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(node))
		rowcnt := 0
		for _, s := range segments {
			rowcnt += int(s.GetNumOfRows())
		}

		// Calculate growing segment row count on node
		channels := p.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(node))
		for _, channel := range channels {
			rowcnt += int(channel.View.NumOfGrowingRows)
		}

		// Calculate executing task cost in scheduler
		rowcnt += p.scheduler.GetSegmentTaskDelta(node, -1)

		// More row count means less priority
		NodeItem := NewNodeItem(rowcnt, node)
		ret[node] = &NodeItem
	}
	return ret
}

// convertToNodeItemsByChannel creates node items with channel count scores
func (p *RowCountBasedAssignPolicy) convertToNodeItemsByChannel(nodeIDs []int64) map[int64]*NodeItem {
	ret := make(map[int64]*NodeItem, len(nodeIDs))
	for _, node := range nodeIDs {
		channels := p.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(node))

		channelCount := len(channels)
		// Calculate executing task cost in scheduler
		channelCount += p.scheduler.GetChannelTaskDelta(node, -1)

		// More channel num means less priority
		NodeItem := NewNodeItem(channelCount, node)
		ret[node] = &NodeItem
	}
	return ret
}
