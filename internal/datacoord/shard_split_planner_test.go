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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPlanBalancedSplitIndex(t *testing.T) {
	mk := func(sizes ...int64) []residueWeight {
		weights := make([]residueWeight, len(sizes))
		for i, s := range sizes {
			weights[i] = residueWeight{residue: uint64(i), size: s}
		}
		return weights
	}

	// Equal weights: the cut falls in the middle.
	index, err := planBalancedSplitIndex(mk(100, 100, 100, 100))
	require.NoError(t, err)
	assert.Equal(t, 2, index)

	// One residue holds most of the data, so the cut moves to isolate it.
	index, err = planBalancedSplitIndex(mk(900, 30, 30, 40))
	require.NoError(t, err)
	assert.Equal(t, 1, index)

	// And symmetrically at the other end.
	index, err = planBalancedSplitIndex(mk(40, 30, 30, 900))
	require.NoError(t, err)
	assert.Equal(t, 3, index)

	// Both halves must be non-empty, so two residues always cut between them,
	// however lopsided.
	index, err = planBalancedSplitIndex(mk(1, 1_000_000))
	require.NoError(t, err)
	assert.Equal(t, 1, index)

	// Nothing to divide.
	_, err = planBalancedSplitIndex(mk(100))
	assert.ErrorIs(t, err, ErrShardNotSplittable)
	_, err = planBalancedSplitIndex(nil)
	assert.ErrorIs(t, err, ErrShardNotSplittable)
}

func TestNamespaceResidueMatchesTheWritePathHash(t *testing.T) {
	// The planner must hash a namespace exactly as the proxy write path does, or
	// a segment is relabeled onto a shard its future writes will not go to.
	for _, ns := range []string{"a", "tenant-1", "", "zzzz"} {
		assert.Equal(t,
			uint64(typeutil.HashString2Uint32(ns))%8,
			namespaceResidue(ns, 8), "namespace %q", ns)
	}
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

// residuePlannerMeta builds a namespace collection on one shard that owns the
// given residues at the given modulus, with one segment per partition.
func residuePlannerMeta(modulus uint64, own []uint64, partitionRows map[int64]int64) *meta {
	m := newSplitTestMeta(true, "v0", partitionRows)
	collection, _ := m.collections.Get(1)
	collection.RoutingModulus = modulus
	collection.ShardInfos = map[string]*schemapb.CollectionShardInfo{
		"v0": {
			VchannelName: "v0",
			State:        schemapb.ShardState_ShardNormal,
			Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: own},
			},
		},
	}
	return m
}

// A source down to its last residue has nothing to divide: the modulus doubles
// and the residue is cut on one more hash bit.
func TestResidueSplitPlannerDoublesASingleResidue(t *testing.T) {
	m := residuePlannerMeta(2, []uint64{1}, map[int64]int64{10: 100, 11: 100})
	resolver := &countingResolver{names: map[int64]string{10: "a", 11: "b"}}
	planner := newResidueSplitPlanner(m, resolver.resolve)

	targets, modulus, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	require.NoError(t, err)
	require.Len(t, targets, 2)
	assert.EqualValues(t, 4, modulus)
	assert.Equal(t, "v1", targets[0].GetVchannel())
	assert.Equal(t, []uint64{1}, targets[0].GetBuckets())
	assert.Equal(t, "v2", targets[1].GetVchannel())
	assert.Equal(t, []uint64{3}, targets[1].GetBuckets())

	// The two halves cover exactly what the source covered.
	for hash := uint64(0); hash < 40; hash++ {
		wasSource := hash%2 == 1
		isTarget := hash%4 == 1 || hash%4 == 3
		assert.Equal(t, wasSource, isTarget, "hash %d", hash)
	}
}

// A source still owning several residues is halved by dividing that set,
// weighted by the data on each, and the modulus does not move.
func TestResidueSplitPlannerDividesAWeightedResidueSet(t *testing.T) {
	// Put every partition's data on the residue its namespace hashes to, then
	// assert the cut isolates the heavy side.
	names := map[int64]string{10: "a", 11: "b", 12: "c", 13: "d"}
	rows := map[int64]int64{10: 100, 11: 100, 12: 100, 13: 100}

	own := make([]uint64, 0, 4)
	seen := typeutil.NewSet[uint64]()
	for _, ns := range names {
		r := namespaceResidue(ns, 8)
		if !seen.Contain(r) {
			seen.Insert(r)
			own = append(own, r)
		}
	}
	require.GreaterOrEqual(t, len(own), 2, "the fixture needs at least two distinct residues")

	m := residuePlannerMeta(8, own, rows)
	resolver := &countingResolver{names: names}
	planner := newResidueSplitPlanner(m, resolver.resolve)

	targets, modulus, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	require.NoError(t, err)
	require.Len(t, targets, 2)
	// The modulus stays put -- this is what keeps a deep split from growing it
	// without bound.
	assert.EqualValues(t, 8, modulus)

	// Together the targets cover exactly the source's residues, with no overlap.
	union := append(append([]uint64{}, targets[0].GetBuckets()...), targets[1].GetBuckets()...)
	assert.ElementsMatch(t, own, union)
	assert.NotEmpty(t, targets[0].GetBuckets())
	assert.NotEmpty(t, targets[1].GetBuckets())

	// The namespace resolution is memoized across the plan and later assigns.
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, CollectionID: 1, PartitionID: 10}}
	_, err = planner.AssignSegment(context.Background(), seg, targets)
	require.NoError(t, err)
	assert.Equal(t, 1, resolver.calls)
}

// Every segment must land on the target owning its namespace's residue, and the
// two rules -- planning and assignment -- must agree.
func TestResidueSplitPlannerAssignsSegmentsToTheOwningTarget(t *testing.T) {
	names := map[int64]string{10: "a", 11: "b", 12: "c", 13: "d"}
	own := []uint64{0, 1, 2, 3}
	m := residuePlannerMeta(4, own, map[int64]int64{10: 10, 11: 10, 12: 10, 13: 10})
	planner := newResidueSplitPlanner(m, (&countingResolver{names: names}).resolve)

	targets, modulus, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	require.NoError(t, err)
	assert.EqualValues(t, 4, modulus)

	for partitionID, ns := range names {
		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: partitionID, CollectionID: 1, PartitionID: partitionID}}
		idx, err := planner.AssignSegment(context.Background(), seg, targets)
		require.NoError(t, err, "namespace %q", ns)
		assert.Contains(t, targets[idx].GetBuckets(), namespaceResidue(ns, modulus),
			"namespace %q must land on the target owning its residue", ns)
	}
}

func TestResidueSplitPlannerWrongTargetCount(t *testing.T) {
	m := residuePlannerMeta(2, []uint64{0}, map[int64]int64{10: 10})
	planner := newResidueSplitPlanner(m, (&countingResolver{names: map[int64]string{10: "a"}}).resolve)

	_, _, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly two target vchannels")
}

func TestResidueSplitPlannerRejectsAnUnknownSource(t *testing.T) {
	m := residuePlannerMeta(2, []uint64{0}, map[int64]int64{10: 10})
	planner := newResidueSplitPlanner(m, (&countingResolver{names: map[int64]string{10: "a"}}).resolve)

	_, _, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "missing", []string{"v1", "v2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a routable shard")
}

// A partition created after the cache was built must become routable rather than
// wedge the task: a namespace collection adds partitions as tenants arrive, and
// the redistribution window can be long.
func TestResidueSplitPlannerAssignRefreshesCacheOnMiss(t *testing.T) {
	m := residuePlannerMeta(2, []uint64{0, 1}, map[int64]int64{10: 10})
	resolver := &countingResolver{names: map[int64]string{10: "a"}}
	planner := newResidueSplitPlanner(m, resolver.resolve)

	targets, _, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	require.NoError(t, err)
	require.Equal(t, 1, resolver.calls)

	// A segment of a partition the cache does not know: the resolver is consulted
	// again, and once it knows the partition the segment routes.
	resolver.names = map[int64]string{10: "a", 99: "late-tenant"}
	late := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, PartitionID: 99}}
	idx, err := planner.AssignSegment(context.Background(), late, targets)
	require.NoError(t, err)
	assert.Equal(t, 2, resolver.calls)
	assert.Contains(t, targets[idx].GetBuckets(), namespaceResidue("late-tenant", 2))
}

func TestResidueSplitPlannerAssignUnknownPartition(t *testing.T) {
	m := residuePlannerMeta(2, []uint64{0, 1}, map[int64]int64{10: 10})
	resolver := &countingResolver{names: map[int64]string{10: "a"}}
	planner := newResidueSplitPlanner(m, resolver.resolve)

	targets, _, err := planner.PlanTargets(context.Background(), m.GetCollection(1), "v0", []string{"v1", "v2"})
	require.NoError(t, err)

	orphan := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1, PartitionID: 404}}
	_, err = planner.AssignSegment(context.Background(), orphan, targets)
	require.ErrorIs(t, err, ErrSegmentNamespaceUnrouted)
}

func TestTargetsModulus(t *testing.T) {
	// A doubling gives {r} and {r+M/2} at 2M, whose maximum is at least M, so
	// max+1 agrees with the real modulus on every plan the planner produces.
	assert.EqualValues(t, 4, targetsModulus([]*datapb.SplitShardTaskTarget{
		{Buckets: []uint64{1}}, {Buckets: []uint64{3}},
	}))
	assert.EqualValues(t, 8, targetsModulus([]*datapb.SplitShardTaskTarget{
		{Buckets: []uint64{0, 2}}, {Buckets: []uint64{4, 7}},
	}))
	// No residues at all: the caller must refuse rather than divide by zero.
	assert.EqualValues(t, 0, targetsModulus(nil))
	assert.EqualValues(t, 0, targetsModulus([]*datapb.SplitShardTaskTarget{{}}))
}

func TestUnimplementedSplitPlanner(t *testing.T) {
	var planner splitPlanner = unimplementedSplitPlanner{}
	_, _, err := planner.PlanTargets(context.Background(), nil, "v0", nil)
	assert.ErrorIs(t, err, ErrSplitPlannerNotReady)
	_, err = planner.AssignSegment(context.Background(), nil, nil)
	assert.ErrorIs(t, err, ErrSplitPlannerNotReady)
}

func TestBrokerNamespaceResolver(t *testing.T) {
	b := broker.NewMockBroker(t)
	b.EXPECT().ShowPartitions(mock.Anything, int64(1)).Return(&milvuspb.ShowPartitionsResponse{
		PartitionIDs:   []int64{10, 11},
		PartitionNames: []string{"a", "b"},
	}, nil).Once()
	names, err := brokerNamespaceResolver(b)(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, map[int64]string{10: "a", 11: "b"}, names)

	// A response whose arrays disagree is a coordinator bug; zipping it would
	// silently mis-name a tenant.
	b.EXPECT().ShowPartitions(mock.Anything, int64(2)).Return(&milvuspb.ShowPartitionsResponse{
		PartitionIDs:   []int64{10, 11},
		PartitionNames: []string{"a"},
	}, nil).Once()
	_, err = brokerNamespaceResolver(b)(context.Background(), 2)
	require.Error(t, err)
}
