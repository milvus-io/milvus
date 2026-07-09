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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func TestPrecheckLoadResource_NoSegments(t *testing.T) {
	// Test with empty segment estimations
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	err := checker.PrecheckLoadResource(context.Background(), 1, map[int64]*SegmentResourceEstimate{})
	assert.NoError(t, err, "should pass with no segments")
}

func TestPrecheckLoadResource_NoNodes(t *testing.T) {
	// Test with no available nodes
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	estimations := map[int64]*SegmentResourceEstimate{
		1: {
			SegmentID:   1,
			MemoryBytes: 1024 * 1024 * 100, // 100 MB
			DiskBytes:   1024 * 1024 * 500,
		},
	}

	err := checker.PrecheckLoadResource(context.Background(), 1, estimations)
	assert.Error(t, err, "should fail with no nodes")
	assert.Contains(t, err.Error(), "no available QueryNodes", "error message should indicate no nodes")
}

func TestPrecheckLoadResource_SufficientCapacity(t *testing.T) {
	// Test with sufficient memory capacity
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	// Create a mock node with sufficient capacity
	nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:  1,
		Address: "localhost:21124",
		Labels:  map[string]string{sessionutil.LabelResourceGroup: ""},
	})
	nodeInfo.UpdateStats(session.WithMemCapacity(1024)) // 1GB capacity
	nodeMgr.Add(nodeInfo)

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	estimations := map[int64]*SegmentResourceEstimate{
		1: {
			SegmentID:   1,
			MemoryBytes: 1024 * 1024 * 100, // 100 MB (well within 1GB)
			DiskBytes:   1024 * 1024 * 500,
		},
	}

	err := checker.PrecheckLoadResource(context.Background(), 1, estimations)
	assert.NoError(t, err, "should pass with sufficient capacity")
}

func TestPrecheckLoadResource_InsufficientCapacity(t *testing.T) {
	// Test with insufficient memory capacity
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	// Create a node with limited capacity
	nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:  1,
		Address: "localhost:21124",
		Labels:  map[string]string{sessionutil.LabelResourceGroup: ""},
	})
	nodeInfo.UpdateStats(session.WithMemCapacity(100)) // 100 MB capacity
	nodeMgr.Add(nodeInfo)

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	estimations := map[int64]*SegmentResourceEstimate{
		1: {
			SegmentID:   1,
			MemoryBytes: 1024 * 1024 * 500, // 500 MB (exceeds 100 MB capacity)
			DiskBytes:   1024 * 1024 * 500,
		},
	}

	err := checker.PrecheckLoadResource(context.Background(), 1, estimations)
	assert.Error(t, err, "should fail with insufficient capacity")
	assert.Contains(t, err.Error(), "insufficient memory capacity", "error message should indicate memory issue")
	assert.Contains(t, err.Error(), "scale-out", "error message should suggest scale-out percentage")
}

func TestPrecheckLoadResource_MultipleNodes(t *testing.T) {
	// Test with multiple nodes distributing load
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	// Create 3 nodes, each with 512 MB capacity
	for i := 0; i < 3; i++ {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:  int64(i + 1),
			Address: "localhost:2112" + string(rune('0'+i)),
			Labels:  map[string]string{sessionutil.LabelResourceGroup: ""},
		})
		nodeInfo.UpdateStats(session.WithMemCapacity(512)) // 512 MB per node
		nodeMgr.Add(nodeInfo)
	}

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	// Total need: 500 MB (should fit across 3 nodes with 512 MB each = 1536 MB total)
	estimations := map[int64]*SegmentResourceEstimate{
		1: {
			SegmentID:   1,
			MemoryBytes: 1024 * 1024 * 500,
			DiskBytes:   1024 * 1024 * 500,
		},
	}

	err := checker.PrecheckLoadResource(context.Background(), 1, estimations)
	assert.NoError(t, err, "should pass with sufficient distributed capacity")
}

func TestGetNodeResourceMetrics(t *testing.T) {
	// Test retrieving node resource metrics
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:  1,
		Address: "localhost:21124",
		Labels:  map[string]string{sessionutil.LabelResourceGroup: ""},
	})
	nodeInfo.UpdateStats(
		session.WithMemCapacity(1024),
		session.WithSegmentCnt(10),
	)
	nodeMgr.Add(nodeInfo)

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	metrics, err := checker.GetNodeResourceMetrics(context.Background(), 1)
	require.NoError(t, err)
	assert.NotNil(t, metrics)
	assert.Equal(t, int64(1), metrics.NodeID)
	assert.Equal(t, int64(10), metrics.SegmentCount)
}

func TestGetNodeResourceMetrics_NodeNotFound(t *testing.T) {
	// Test error when node not found
	dist := meta.NewDistributionManager()
	m := meta.NewMeta(nil, nil, nil, nil)
	nodeMgr := session.NewNodeManager()

	checker := NewAutoscalePreChecker(dist, m, nodeMgr)
	_, err := checker.GetNodeResourceMetrics(context.Background(), 999)
	assert.Error(t, err, "should error for non-existent node")
	assert.Contains(t, err.Error(), "not found")
}

func TestCalculateScaleOutPercentage(t *testing.T) {
	testCases := []struct {
		needed   float64
		available float64
		expected float64
		desc     string
	}{
		{100, 50, 100, "need 2x what we have"},
		{150, 100, 50, "need 1.5x what we have"},
		{200, 100, 100, "need 2x what we have"},
		{105, 100, 5, "need 5% more"},
		{100, 100, 0, "exact match"},
		{50, 100, -50, "over-provisioned"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			result := calculateScaleOutPercentage(tc.needed, tc.available)
			assert.InDelta(t, tc.expected, result, 0.01, "percentage mismatch")
		})
	}
}

func TestCalculateScaleOutPercentage_ZeroAvailable(t *testing.T) {
	// Test edge case: zero available capacity
	result := calculateScaleOutPercentage(100, 0)
	assert.Equal(t, 100.0, result, "should return 100% for zero available")
}
