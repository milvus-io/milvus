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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

// nodeFilter filters nodes based on various criteria
type nodeFilter interface {
	// FilterNodes filters the input nodes and returns the valid ones
	FilterNodes(ctx context.Context, nodes []int64, forceAssign bool) []int64
}

// segmentScoreCalculator calculates the score of a segment
type segmentScoreCalculator interface {
	// CalculateScore returns the score of a segment (e.g., row count)
	CalculateScore(segment *meta.Segment) int64
}

// channelScoreCalculator calculates the score of a channel
type channelScoreCalculator interface {
	// CalculateScore returns the score of a channel
	CalculateScore(channel *meta.DmChannel) int64
}

// nodeScoreCalculator calculates the load score of a node
type nodeScoreCalculator interface {
	// CalculateScore returns the current load score of a node for the given collection
	CalculateScore(ctx context.Context, nodeID int64, collectionID int64) float64
}

// assignmentStrategy defines how to select the best node for resource assignment
type assignmentStrategy interface {
	// SelectNode selects the best node from candidates
	// Returns the selected node ID and updated node info
	SelectNode(candidates []*NodeItem, resourceScore int64) (selectedNode int64, updatedNodeInfo []*NodeItem)
}

// benefitEvaluator evaluates whether an assignment provides enough benefit
type benefitEvaluator interface {
	// HasEnoughBenefit checks if the assignment from source to target is beneficial enough
	// sourceScore: current score of source node
	// targetScore: current score of target node
	// resourceScore: score of the resource to be assigned
	HasEnoughBenefit(sourceScore float64, targetScore float64, resourceScore int64) bool
}

// ============================================================================
// Common Component Implementations
// These are reusable implementations that can be shared across different policies
// ============================================================================

// commonSegmentNodeFilter is a reusable node filter for segment assignment
// It filters out nodes that are not in normal state
type commonSegmentNodeFilter struct {
	nodeManager *session.NodeManager
}

// newCommonSegmentNodeFilter creates a new common segment node filter
func newCommonSegmentNodeFilter(nodeManager *session.NodeManager) nodeFilter {
	return &commonSegmentNodeFilter{nodeManager: nodeManager}
}

// FilterNodes filters the input nodes and returns nodes in normal state
func (f *commonSegmentNodeFilter) FilterNodes(ctx context.Context, nodes []int64, forceAssign bool) []int64 {
	if forceAssign {
		return nodes
	}
	return lo.Filter(nodes, func(node int64, _ int) bool {
		info := f.nodeManager.Get(node)
		return info != nil && info.GetState() == session.NodeStateNormal && !f.nodeManager.IsResourceExhausted(node)
	})
}

// commonChannelNodeFilter is a reusable node filter for channel assignment
// It filters out SQN nodes (if enabled), nodes with version < 2.4, and non-normal nodes
type commonChannelNodeFilter struct {
	nodeManager *session.NodeManager
}

// newCommonChannelNodeFilter creates a new common channel node filter
func newCommonChannelNodeFilter(nodeManager *session.NodeManager) nodeFilter {
	return &commonChannelNodeFilter{nodeManager: nodeManager}
}

// FilterNodes filters nodes for channel assignment considering SQN, version, and state
func (f *commonChannelNodeFilter) FilterNodes(ctx context.Context, nodes []int64, forceAssign bool) []int64 {
	// Filter SQN if streaming service is enabled
	nodes = filterSQNIfStreamingServiceEnabled(nodes)

	if forceAssign {
		return nodes
	}

	return lo.Filter(nodes, func(node int64, _ int) bool {
		info := f.nodeManager.Get(node)
		return info != nil && info.GetState() == session.NodeStateNormal && !f.nodeManager.IsResourceExhausted(node)
	})
}

// commonScoreBasedBenefitEvaluator is a reusable benefit evaluator
// for score-based policies
type commonScoreBasedBenefitEvaluator struct{}

// HasEnoughBenefit checks if the assignment provides enough benefit
// It considers:
// 1. Score unbalance toleration factor
// 2. Reverse unbalance toleration factor (if assignment would reverse the balance)
func (e *commonScoreBasedBenefitEvaluator) HasEnoughBenefit(sourceScore float64, targetScore float64, resourceScore int64) bool {
	// Check if the score diff between source and target is below tolerance
	oldPriorityDiff := math.Abs(sourceScore - targetScore)
	if oldPriorityDiff < targetScore*params.Params.QueryCoordCfg.ScoreUnbalanceTolerationFactor.GetAsFloat() {
		return false
	}

	// Check if assignment would reverse the balance
	newSourceScore := sourceScore - float64(resourceScore)
	newTargetScore := targetScore + float64(resourceScore)
	if newTargetScore > newSourceScore {
		// If score diff is reversed, check if the new diff is acceptable
		newScoreDiff := math.Abs(newSourceScore - newTargetScore)
		if newScoreDiff*params.Params.QueryCoordCfg.ReverseUnbalanceTolerationFactor.GetAsFloat() >= oldPriorityDiff {
			return false
		}
	}

	return true
}

// HasEnoughBenefitForNodes is a helper method for NodeItem-based evaluation
func (e *commonScoreBasedBenefitEvaluator) HasEnoughBenefitForNodes(sourceNode *NodeItem, targetNode *NodeItem, scoreChanges float64) bool {
	return e.HasEnoughBenefit(
		float64(sourceNode.getPriority()),
		float64(targetNode.getPriority()),
		int64(scoreChanges),
	)
}
