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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// testNamespaceEncoder is an order-preserving stand-in for the routing
// package's encoder: the namespace bytes themselves are byte-comparable,
// deterministic and unique, which is all the planner logic relies on.
type testNamespaceEncoder struct{}

func (testNamespaceEncoder) EncodeNamespace(namespace string) []byte {
	return []byte(namespace)
}

func TestPlanBalancedSplitIndex(t *testing.T) {
	mk := func(sizes ...int64) []namespaceWeight {
		weights := make([]namespaceWeight, len(sizes))
		for i, s := range sizes {
			weights[i] = namespaceWeight{partitionID: int64(i), key: []byte{byte('a' + i)}, size: s}
		}
		return weights
	}

	t.Run("even split", func(t *testing.T) {
		index, err := planBalancedSplitIndex(mk(10, 10, 10, 10))
		assert.NoError(t, err)
		assert.Equal(t, 2, index)
	})
	t.Run("two equal namespaces", func(t *testing.T) {
		index, err := planBalancedSplitIndex(mk(5, 5))
		assert.NoError(t, err)
		assert.Equal(t, 1, index)
	})
	t.Run("isolate an oversized namespace on the left", func(t *testing.T) {
		index, err := planBalancedSplitIndex(mk(100, 1, 1, 1))
		assert.NoError(t, err)
		assert.Equal(t, 1, index)
	})
	t.Run("isolate an oversized namespace on the right", func(t *testing.T) {
		index, err := planBalancedSplitIndex(mk(1, 1, 1, 100))
		assert.NoError(t, err)
		assert.Equal(t, 3, index)
	})
	t.Run("a single namespace cannot be split", func(t *testing.T) {
		_, err := planBalancedSplitIndex(mk(10))
		assert.ErrorIs(t, err, ErrShardNotSplittable)
	})
}

func TestRoutingKeyRangeContains(t *testing.T) {
	full := routingKeyRange{}
	assert.True(t, full.contains([]byte("anything")))

	lowerBounded := routingKeyRange{lower: []byte("m")}
	assert.True(t, lowerBounded.contains([]byte("m")))
	assert.True(t, lowerBounded.contains([]byte("z")))
	assert.False(t, lowerBounded.contains([]byte("a")))

	upperBounded := routingKeyRange{upper: []byte("m")}
	assert.True(t, upperBounded.contains([]byte("a")))
	assert.False(t, upperBounded.contains([]byte("m")))

	bounded := routingKeyRange{lower: []byte("d"), upper: []byte("m")}
	assert.True(t, bounded.contains([]byte("d")))
	assert.True(t, bounded.contains([]byte("f")))
	assert.False(t, bounded.contains([]byte("c")))
	assert.False(t, bounded.contains([]byte("m")))
}

// countingResolver returns a fixed name map and records how many times it ran,
// so a test can assert the planner memoizes it.
type countingResolver struct {
	names map[int64]string
	calls int
}

func (r *countingResolver) resolve(_ context.Context, _ int64) (map[int64]string, error) {
	r.calls++
	return r.names, nil
}

func TestRangeSplitPlannerPlanTargets(t *testing.T) {
	// four namespaces of equal size: a, b, c, d on partitions 10..13.
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 100, 11: 100, 12: 100, 13: 100})
	resolver := &countingResolver{names: map[int64]string{10: "a", 11: "b", 12: "c", 13: "d"}}
	planner := newRangeSplitPlanner(m, testNamespaceEncoder{}, resolver.resolve)

	targets, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	assert.NoError(t, err)
	assert.Len(t, targets, 2)

	// the balanced split point falls on namespace "c": [nil,"c") and ["c",nil).
	assert.Equal(t, "v1", targets[0].GetVchannel())
	assert.Nil(t, targets[0].GetRoutingKeyLower())
	assert.Equal(t, []byte("c"), targets[0].GetRoutingKeyUpper())
	assert.Equal(t, "v2", targets[1].GetVchannel())
	assert.Equal(t, []byte("c"), targets[1].GetRoutingKeyLower())
	assert.Nil(t, targets[1].GetRoutingKeyUpper())

	// the ranges are disjoint and gap-free: the upper of the first equals the
	// lower of the second.
	assert.Equal(t, targets[0].GetRoutingKeyUpper(), targets[1].GetRoutingKeyLower())

	// a segment of namespace "a" lands on the left target, "c" on the right.
	left := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 10}}
	idx, err := planner.AssignSegment(context.Background(), left, targets)
	assert.NoError(t, err)
	assert.Equal(t, 0, idx)

	right := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, PartitionID: 12}}
	idx, err = planner.AssignSegment(context.Background(), right, targets)
	assert.NoError(t, err)
	assert.Equal(t, 1, idx)

	// the namespace resolution is memoized across the plan and the assigns.
	assert.Equal(t, 1, resolver.calls)
}

func TestRangeSplitPlannerSkewedSplit(t *testing.T) {
	// one oversized namespace "a" dominates; it is isolated on the left.
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 1000, 11: 1, 12: 1, 13: 1})
	resolver := &countingResolver{names: map[int64]string{10: "a", 11: "b", 12: "c", 13: "d"}}
	planner := newRangeSplitPlanner(m, testNamespaceEncoder{}, resolver.resolve)

	targets, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	assert.NoError(t, err)
	assert.Equal(t, []byte("b"), targets[0].GetRoutingKeyUpper(), "split key isolates namespace a")
}

func TestRangeSplitPlannerSingleNamespace(t *testing.T) {
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 100})
	resolver := &countingResolver{names: map[int64]string{10: "a"}}
	planner := newRangeSplitPlanner(m, testNamespaceEncoder{}, resolver.resolve)

	_, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	assert.ErrorIs(t, err, ErrShardNotSplittable)
}

func TestRangeSplitPlannerWrongTargetCount(t *testing.T) {
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 100, 11: 100})
	resolver := &countingResolver{names: map[int64]string{10: "a", 11: "b"}}
	planner := newRangeSplitPlanner(m, testNamespaceEncoder{}, resolver.resolve)

	_, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1"})
	assert.Error(t, err)
}

func TestBrokerNamespaceResolver(t *testing.T) {
	b := broker.NewMockBroker(t)
	b.EXPECT().ShowPartitions(mock.Anything, int64(1)).Return(&milvuspb.ShowPartitionsResponse{
		PartitionIDs:   []int64{10, 11},
		PartitionNames: []string{"a", "b"},
	}, nil).Once()
	resolve := brokerNamespaceResolver(b)
	names, err := resolve(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, map[int64]string{10: "a", 11: "b"}, names)

	// a mismatched ids/names length is reported instead of mis-zipping.
	b.EXPECT().ShowPartitions(mock.Anything, int64(2)).Return(&milvuspb.ShowPartitionsResponse{
		PartitionIDs:   []int64{10, 11},
		PartitionNames: []string{"a"},
	}, nil).Once()
	_, err = resolve(context.Background(), 2)
	assert.Error(t, err)
}

func TestRangeSplitPlannerAssignUnknownPartition(t *testing.T) {
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 100, 11: 100})
	resolver := &countingResolver{names: map[int64]string{10: "a", 11: "b"}}
	planner := newRangeSplitPlanner(m, testNamespaceEncoder{}, resolver.resolve)

	targets := []*datapb.SplitShardTaskTarget{
		{Vchannel: "v1", RoutingKeyUpper: []byte("b")},
		{Vchannel: "v2", RoutingKeyLower: []byte("b")},
	}
	// partition 99 has no namespace key.
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 99}}
	_, err := planner.AssignSegment(context.Background(), seg, targets)
	assert.ErrorIs(t, err, ErrSegmentNamespaceUnrouted)
}

func TestRangeSplitPlannerAssignRefreshesCacheOnMiss(t *testing.T) {
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 100, 11: 100})
	resolver := &countingResolver{names: map[int64]string{10: "a", 11: "b"}}
	planner := newRangeSplitPlanner(m, testNamespaceEncoder{}, resolver.resolve)
	targets := []*datapb.SplitShardTaskTarget{
		{Vchannel: "v1", RoutingKeyUpper: []byte("b")},
		{Vchannel: "v2", RoutingKeyLower: []byte("b")},
	}

	// the first assign builds and memoizes the partition-key cache.
	idx, err := planner.AssignSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 10}}, targets)
	assert.NoError(t, err)
	assert.Equal(t, 0, idx)
	assert.Equal(t, 1, resolver.calls)

	// a tenant arrives mid-redistribution: a new partition absent from the cache.
	// the assign must refresh the cache and route it instead of failing.
	resolver.names[12] = "c"
	idx, err = planner.AssignSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, CollectionID: 1, PartitionID: 12}}, targets)
	assert.NoError(t, err)
	assert.Equal(t, 1, idx) // namespace "c" >= "b" routes to the right target
	assert.Equal(t, 2, resolver.calls, "exactly one refresh on the miss")

	// the refreshed cache is reused: a known partition does not re-resolve.
	idx, err = planner.AssignSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 12}}, targets)
	assert.NoError(t, err)
	assert.Equal(t, 1, idx)
	assert.Equal(t, 2, resolver.calls)

	// a genuinely-unknown partition still fails after the refresh (no fallback
	// to a wrong shard).
	_, err = planner.AssignSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 1, PartitionID: 99}}, targets)
	assert.ErrorIs(t, err, ErrSegmentNamespaceUnrouted)
}
