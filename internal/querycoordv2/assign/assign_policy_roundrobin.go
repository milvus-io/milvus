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
	"math"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// RoundRobinAssignPolicy implements a simple round-robin assignment strategy
// for both segments and channels
type RoundRobinAssignPolicy struct {
	nodeManager *session.NodeManager
	scheduler   task.Scheduler
	targetMgr   meta.TargetManagerInterface
}

// newRoundRobinAssignPolicy creates a new RoundRobinAssignPolicy
// This is a private constructor. Use GetGlobalAssignPolicyFactory().GetPolicy() to create instances.
func newRoundRobinAssignPolicy(
	nodeManager *session.NodeManager,
	scheduler task.Scheduler,
	targetMgr meta.TargetManagerInterface,
) *RoundRobinAssignPolicy {
	return &RoundRobinAssignPolicy{
		nodeManager: nodeManager,
		scheduler:   scheduler,
		targetMgr:   targetMgr,
	}
}

// AssignSegment assigns segments to nodes using round-robin strategy
func (p *RoundRobinAssignPolicy) AssignSegment(
	ctx context.Context,
	collectionID int64,
	segments []*meta.Segment,
	nodes []int64,
	forceAssign bool,
) []SegmentAssignPlan {
	balanceBatchSize := math.MaxInt64

	// Filter nodes
	if !forceAssign {
		filter := newCommonSegmentNodeFilter(p.nodeManager)
		nodes = filter.FilterNodes(ctx, nodes, forceAssign)
		balanceBatchSize = paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.GetAsInt()
	}
	if len(nodes) == 0 {
		return nil
	}

	// Create a copy of nodes to avoid race condition when sorting
	nodesCopy := make([]int64, len(nodes))
	copy(nodesCopy, nodes)
	nodes = nodesCopy

	// Sort nodes by current segment load (ascending)
	// Consider: segment count only.
	sort.Slice(nodes, func(i, j int) bool {
		load1 := p.calculateSegmentLoad(nodes[i])
		load2 := p.calculateSegmentLoad(nodes[j])
		if load1 != load2 {
			return load1 < load2
		}
		// If loads are equal, use node ID as tie-breaker for stability
		return nodes[i] < nodes[j]
	})

	ret := make([]SegmentAssignPlan, 0, len(segments))

	// Assign segments in round-robin fashion
	for i, s := range segments {
		plan := SegmentAssignPlan{
			Segment: s,
			From:    -1,
			To:      nodes[i%len(nodes)],
		}
		ret = append(ret, plan)
		if len(ret) >= balanceBatchSize {
			break
		}
	}

	return ret
}

// AssignChannel assigns channels to nodes using round-robin strategy
func (p *RoundRobinAssignPolicy) AssignChannel(
	ctx context.Context,
	collectionID int64,
	channels []*meta.DmChannel,
	nodes []int64,
	forceAssign bool,
) []ChannelAssignPlan {
	return p.AssignChannelWithProjectedLoad(ctx, collectionID, channels, nodes, forceAssign, nil)
}

// AssignChannelWithProjectedLoad is AssignChannel plus a round-scoped projected
// channel count. projectedLoad holds the number of channels already planned for
// each node earlier in the same Check() round; it is added to the load before
// sorting and updated after every plan, so later collections see the channels
// planned by earlier ones. This prevents the initial-allocation skew where each
// per-collection call observes an identical scheduler delta (tasks are only
// submitted after the whole round) and repeatedly hits the smallest node ID.
// A nil projectedLoad keeps the legacy behavior.
func (p *RoundRobinAssignPolicy) AssignChannelWithProjectedLoad(
	ctx context.Context,
	collectionID int64,
	channels []*meta.DmChannel,
	nodes []int64,
	forceAssign bool,
	projectedLoad map[int64]int,
) []ChannelAssignPlan {
	if projectedLoad == nil {
		projectedLoad = make(map[int64]int)
	}

	// Filter nodes
	nodeFilter := newCommonChannelNodeFilter(p.nodeManager)
	nodes = nodeFilter.FilterNodes(ctx, nodes, forceAssign)
	if len(nodes) == 0 {
		return nil
	}

	// Handle WAL-based assignment if streaming service is enabled
	plans := make([]ChannelAssignPlan, 0)
	scoreDelta := make(map[int64]int)
	if streamingutil.IsStreamingServiceEnabled() {
		channels, plans, scoreDelta = assignChannelToWALLocatedFirstForNodeInfo(channels, nodes)
		// WAL-owner placements are also planned for this round.
		for node, count := range scoreDelta {
			projectedLoad[node] += count
		}
	}

	// Create a copy of nodes to avoid race condition when sorting
	nodesCopy := make([]int64, len(nodes))
	copy(nodesCopy, nodes)
	nodes = nodesCopy

	// Sort nodes by current channel load (ascending)
	// Consider: current channel count + WAL assignment delta + scheduler task delta + round projected load
	sort.Slice(nodes, func(i, j int) bool {
		// Base load: current channels + scheduler task delta
		load1 := p.calculateChannelLoad(nodes[i])
		load2 := p.calculateChannelLoad(nodes[j])

		// Add WAL-based assignment delta
		load1 += scoreDelta[nodes[i]]
		load2 += scoreDelta[nodes[j]]

		// Add the round-scoped projected load planned earlier in this round
		load1 += projectedLoad[nodes[i]]
		load2 += projectedLoad[nodes[j]]

		if load1 != load2 {
			return load1 < load2
		}
		// If loads are equal, use node ID as tie-breaker for stability
		return nodes[i] < nodes[j]
	})

	// Assign remaining channels in round-robin fashion
	for i, c := range channels {
		to := nodes[i%len(nodes)]
		projectedLoad[to]++
		plans = append(plans, ChannelAssignPlan{
			Channel: c,
			From:    -1,
			To:      to,
		})
	}

	return plans
}

// calculateSegmentLoad calculates the total segment load for a node
// Load = segment count
func (p *RoundRobinAssignPolicy) calculateSegmentLoad(nodeID int64) int {
	load := 0

	nodeInfo := p.nodeManager.Get(nodeID)
	if nodeInfo != nil {
		load += nodeInfo.SegmentCnt()
	}

	return load
}

// calculateChannelLoad calculates the total channel load for a node
// Load = channel count + scheduler task delta
func (p *RoundRobinAssignPolicy) calculateChannelLoad(nodeID int64) int {
	load := 0

	nodeInfo := p.nodeManager.Get(nodeID)
	if nodeInfo != nil {
		load += nodeInfo.ChannelCnt()
	}

	// Add scheduler task delta (pending channel tasks)
	load += p.scheduler.GetChannelTaskDelta(nodeID, -1)

	return load
}
