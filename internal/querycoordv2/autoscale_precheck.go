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

package querycoordv2

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// AutoscalePreChecker performs precheck validations for autoscaling load operations
type AutoscalePreChecker struct {
	dist    *meta.DistributionManager
	meta    *meta.Meta
	nodeMgr *session.NodeManager
}

// NewAutoscalePreChecker creates a new AutoscalePreChecker instance
func NewAutoscalePreChecker(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	nodeMgr *session.NodeManager,
) *AutoscalePreChecker {
	return &AutoscalePreChecker{
		dist:    dist,
		meta:    meta,
		nodeMgr: nodeMgr,
	}
}

// PrecheckLoadResource validates that sufficient resources exist across QueryNodes
// before attempting to load segments. It checks memory capacity against
// estimated segment resource requirements.
//
// Returns an error if:
// - No QueryNodes available
// - Insufficient memory capacity on available QueryNodes
// - Unable to estimate segment resource usage
//
// The error message includes a suggested scale-out percentage when capacity is insufficient.
func (apc *AutoscalePreChecker) PrecheckLoadResource(
	ctx context.Context,
	collectionID int64,
	segmentEstimations map[int64]*SegmentResourceEstimate, // segmentID -> resource estimate
) error {
	if len(segmentEstimations) == 0 {
		return nil
	}

	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
	)

	// Aggregate total estimated resources needed
	totalEstimatedMemory := uint64(0)
	maxSegmentMemory := uint64(0)

	for segmentID, estimate := range segmentEstimations {
		if estimate == nil {
			logger.Warn(ctx, "nil segment resource estimate",
				mlog.Int64("segmentID", segmentID))
			continue
		}
		totalEstimatedMemory += estimate.MemoryBytes
		if estimate.MemoryBytes > maxSegmentMemory {
			maxSegmentMemory = estimate.MemoryBytes
		}
	}

	// Get all available QueryNodes
	allNodes := apc.nodeMgr.GetAll()
	if len(allNodes) == 0 {
		return merr.WrapErrInsufficientMemory(
			fmt.Sprintf("no available QueryNodes for loading segments in collection %d", collectionID),
		)
	}

	// Calculate total available memory capacity across all nodes
	totalAvailableMemory := float64(0)
	var nodeMemoryStats []struct {
		nodeID    int64
		capacity  float64
		available float64
	}

	for _, nodeInfo := range allNodes {
		if nodeInfo == nil {
			continue
		}

		nodeID := nodeInfo.ID()
		nodeCapacity := nodeInfo.MemCapacity() // returns MB

		// Estimate per-node available memory (simplified: assume even distribution)
		// In a real implementation, this could account for current load distribution
		estimatedPerNodeUsage := float64(totalEstimatedMemory) / float64(len(allNodes)) / (1024 * 1024) // convert to MB
		availableMemory := nodeCapacity - estimatedPerNodeUsage

		// Get in-flight resources if available
		inflight := apc.getNodeInflightResources(ctx, nodeID)
		availableMemory -= float64(inflight.MemoryBytes) / (1024 * 1024) // convert to MB

		totalAvailableMemory += availableMemory

		nodeMemoryStats = append(nodeMemoryStats, struct {
			nodeID    int64
			capacity  float64
			available float64
		}{nodeID, nodeCapacity, availableMemory})

		logger.Debug(ctx, "node resource status",
			mlog.Int64("nodeID", nodeID),
			mlog.Float64("capacityMB", nodeCapacity),
			mlog.Float64("estimatedUsageMB", estimatedPerNodeUsage),
			mlog.Float64("availableMemoryMB", availableMemory),
		)
	}

	// Check memory capacity (convert totalEstimatedMemory to MB for comparison)
	estimatedMemoryMB := float64(totalEstimatedMemory) / (1024 * 1024)

	if estimatedMemoryMB > totalAvailableMemory {
		memoryShortfall := estimatedMemoryMB - totalAvailableMemory
		scaleOutPercentage := calculateScaleOutPercentage(estimatedMemoryMB, totalAvailableMemory)

		logger.Warn(ctx, "insufficient memory capacity for loading segments",
			mlog.Float64("totalEstimatedMemoryMB", estimatedMemoryMB),
			mlog.Float64("totalAvailableMemoryMB", totalAvailableMemory),
			mlog.Float64("memoryShortfallMB", memoryShortfall),
			mlog.Float64("suggestedScaleOutPercentage", scaleOutPercentage),
			mlog.Int("currentNodeCount", len(allNodes)),
		)

		return merr.WrapErrInsufficientMemory(
			fmt.Sprintf(
				"insufficient memory capacity for collection %d: need %.2f MB, available %.2f MB, suggested scale-out: %.2f%% more nodes",
				collectionID,
				estimatedMemoryMB,
				totalAvailableMemory,
				scaleOutPercentage,
			),
		)
	}

	logger.Info(ctx, "autoscale precheck passed",
		mlog.Float64("totalEstimatedMemoryMB", estimatedMemoryMB),
		mlog.Float64("totalAvailableMemoryMB", totalAvailableMemory),
		mlog.Int("nodeCount", len(allNodes)),
	)

	return nil
}

// getNodeInflightResources returns scheduled-but-not-yet-committed resources for a node
// This allows external autoscalers to see pending load operations
func (apc *AutoscalePreChecker) getNodeInflightResources(ctx context.Context, nodeID int64) *ResourceUsage {
	// TODO: Implement tracking of in-flight resources from target distribution
	// This would come from pending target assignments that have not yet been
	// reflected in the node's actual resource usage
	// For now, return zero; this can be enhanced to scan pending targets
	return &ResourceUsage{
		MemoryBytes: 0,
		DiskBytes:   0,
	}
}

// GetNodeResourceMetrics returns current and in-flight resource metrics for a node
// Used for external autoscalers to make informed scaling decisions
func (apc *AutoscalePreChecker) GetNodeResourceMetrics(ctx context.Context, nodeID int64) (*NodeResourceMetrics, error) {
	info := apc.nodeMgr.Get(nodeID)
	if info == nil {
		return nil, merr.WrapErrNodeNotFound(fmt.Sprintf("node %d not found", nodeID))
	}

	inflight := apc.getNodeInflightResources(ctx, nodeID)
	memCapacityMB := info.MemCapacity() // in MB
	memCapacityBytes := uint64(memCapacityMB * 1024 * 1024)

	return &NodeResourceMetrics{
		NodeID:         nodeID,
		MemoryLimit:    memCapacityBytes,
		MemoryInflight: inflight.MemoryBytes,
		SegmentCount:   int64(info.SegmentCnt()),
		ChannelCount:   int64(info.ChannelCnt()),
		LastUpdateTime: info.LastHeartbeat().UnixNano(),
	}, nil
}

// calculateScaleOutPercentage computes the percentage of additional capacity needed
func calculateScaleOutPercentage(needed, available uint64) float64 {
	if available == 0 {
		// If we have zero available capacity, we need 100% more than what we have
		return 100.0
	}
	// percentage = (shortfall / available) * 100
	shortfall := float64(needed - available)
	percentNeeded := (shortfall / float64(available)) * 100.0
	return percentNeeded
}

// SegmentResourceEstimate holds estimated resource requirements for a segment
type SegmentResourceEstimate struct {
	SegmentID   int64
	MemoryBytes uint64
	DiskBytes   uint64
}

// ResourceUsage holds resource usage information
type ResourceUsage struct {
	MemoryBytes uint64
	DiskBytes   uint64
}

// NodeResourceMetrics holds comprehensive resource metrics for a QueryNode
type NodeResourceMetrics struct {
	NodeID         int64
	MemoryLimit    uint64
	MemoryInflight uint64
	SegmentCount   int64
	ChannelCount   int64
	LastUpdateTime int64
}
