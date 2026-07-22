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
	"fmt"
	"math"
	"testing"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/stretchr/testify/require"
)

func TestScoreEpochPolicyUsesProjectedPlacement(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 80, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 20, 1, "alpha", true)
	policy := newScoreEpochPolicy(false)
	config := scorePolicyTestConfig(false)
	projected := NewProjectedPlacement(snapshot)

	before := policy.Evaluate(&snapshot, projected, PlanKindSegment, config)
	require.NoError(t, projected.Apply(epochSegmentPlan(snapshot, 1000, 102, 1, 2)))
	after := policy.Evaluate(&snapshot, projected, PlanKindSegment, config)

	require.True(t, after.Improves(before), "before=%v after=%v", before, after)
}

func TestScoreEpochPolicyRejectsWholeWaveOverCorrection(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 100, 1, "alpha", true)

	wave := newScoreEpochPolicy(false).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)

	require.Empty(t, wave.Plans)
	require.Equal(t, wave.Before, wave.After)
}

func TestScoreEpochPolicyConvergedUsesSamePotential(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 50, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 50, 2, "alpha", true)
	policy := newScoreEpochPolicy(false)
	config := scorePolicyTestConfig(false)

	wave := policy.Plan(&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, config)
	expected := policy.Evaluate(&snapshot, NewProjectedPlacement(snapshot), wave.Kind, config)

	require.True(t, wave.Converged)
	require.Empty(t, wave.Plans)
	require.Equal(t, expected, wave.Before)
	require.Equal(t, expected, wave.After)
}

func TestScoreEpochPolicyDeterministicForShuffledSnapshotInput(t *testing.T) {
	left := scorePolicyTestSnapshot()
	for _, spec := range []struct {
		id   int64
		rows int64
	}{{101, 40}, {102, 30}, {103, 20}, {104, 10}} {
		addHistoricalSegment(&left, spec.id, spec.rows, 1, "alpha", true)
	}
	right := scorePolicyTestSnapshot()
	for _, spec := range []struct {
		id   int64
		rows int64
	}{{104, 10}, {103, 20}, {102, 30}, {101, 40}} {
		addHistoricalSegment(&right, spec.id, spec.rows, 1, "alpha", true)
	}
	right.Replicas[1000] = ReplicaSnapshot{
		ID: 1000, CollectionID: 10, ResourceGroup: "rg1",
		RWNodes: []int64{2, 1}, ChannelRWNodes: map[string][]int64{"alpha": {2, 1}},
	}

	policy := newScoreEpochPolicy(false)
	config := scorePolicyTestConfig(false)
	leftWave := policy.Plan(&left, scorePolicyTestBudget(), EpochPlanningConstraints{}, config)
	rightWave := policy.Plan(&right, scorePolicyTestBudget(), EpochPlanningConstraints{}, config)

	require.Equal(t, leftWave.Plans, rightWave.Plans)
	require.Equal(t, leftWave.PrefixAfter, rightWave.PrefixAfter)
}

func TestScoreEpochPolicyDeterministicFractionalPotential(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	snapshot.Nodes = make(map[int64]NodeSnapshot)
	nodes := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	capacities := []float64{1, 2, 3, 5, 7, 11, 13, 17}
	rows := []int64{1, 3, 7, 11, 100000001, 100000003, 100000019, 900000001}
	for index, nodeID := range nodes {
		snapshot.Nodes[nodeID] = NodeSnapshot{
			ID: nodeID, Exists: true, Eligible: true, ResourceGroup: "rg1",
			MemoryCapacity: capacities[index],
		}
		addHistoricalSegment(&snapshot, int64(101+index), rows[index], nodeID, "alpha", true)
	}
	replica := snapshot.Replicas[1000]
	replica.RWNodes = append([]int64(nil), nodes...)
	snapshot.Replicas[1000] = replica
	policy := newScoreEpochPolicy(false)
	config := scorePolicyTestConfig(false)
	first := math.Float64bits(policy.Evaluate(
		&snapshot, NewProjectedPlacement(snapshot), PlanKindSegment, config,
	).Value)

	for iteration := 0; iteration < 1000; iteration++ {
		got := math.Float64bits(policy.Evaluate(
			&snapshot, NewProjectedPlacement(snapshot), PlanKindSegment, config,
		).Value)
		require.Equal(t, first, got)
	}
}

func TestScoreEpochPolicyReturnsImmutableAlignedPrefixPotentials(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	for segmentID := int64(101); segmentID <= 104; segmentID++ {
		addHistoricalSegment(&snapshot, segmentID, 10, 1, "alpha", true)
	}

	wave := newScoreEpochPolicy(false).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)

	require.Len(t, wave.PrefixAfter, len(wave.Plans))
	require.NotEmpty(t, wave.Plans)
	previous := wave.Before
	for index, plan := range wave.Plans {
		require.True(t, validEpochPlan(plan))
		require.True(t, wave.PrefixAfter[index].Improves(previous))
		require.True(t, plan.Token.Snapshot.Equal(snapshot.Token))
		previous = wave.PrefixAfter[index]
	}
	require.Equal(t, wave.After, wave.PrefixAfter[len(wave.PrefixAfter)-1])

	snapshot.Token.CurrentTargetVersion[10] = 99
	require.Equal(t, int64(1), wave.Plans[0].Token.Snapshot.CurrentTargetVersion[10])
	if len(wave.Plans) > 1 {
		wave.Plans[0].Token.Snapshot.CurrentTargetVersion[10] = 88
		wave.Plans[0].Token.Segment.SegmentID = 999
		require.Equal(t, int64(1), wave.Plans[1].Token.Snapshot.CurrentTargetVersion[10])
		require.NotEqual(t, int64(999), wave.Plans[1].Token.Segment.SegmentID)
	}
}

func TestChannelLevelEpochPolicyUsesExclusiveShardDomain(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	snapshot.Replicas[1000] = ReplicaSnapshot{
		ID: 1000, CollectionID: 10, ResourceGroup: "rg1", RWNodes: []int64{1, 2, 3},
		RONodes: []int64{3}, ChannelRWNodes: map[string][]int64{"alpha": {1, 2}},
	}
	snapshot.CollectionTargets[10] = CollectionTargetSnapshot{
		Current: TargetScopeSnapshot{Channels: map[string]TargetChannelSnapshot{
			"alpha": {CollectionID: 10, Channel: "alpha"},
		}},
	}
	addHistoricalSegment(&snapshot, 101, 80, 3, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 20, 3, "alpha", true)

	wave := newScoreEpochPolicy(true).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)

	require.NotEmpty(t, wave.Plans)
	for _, plan := range wave.Plans {
		require.Equal(t, "alpha", plan.Shard)
		require.Contains(t, []int64{1, 2}, plan.To)
		require.Equal(t, int64(3), plan.From)
	}
}

func TestChannelLevelEpochPolicyPreservesChannelFirst(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	snapshot.Replicas[1000] = ReplicaSnapshot{
		ID: 1000, CollectionID: 10, ResourceGroup: "rg1", RWNodes: []int64{1, 2},
		ChannelRWNodes: map[string][]int64{"alpha": {1, 2}, "beta": {1, 2}},
	}
	snapshot.CollectionTargets[10] = CollectionTargetSnapshot{
		Current: TargetScopeSnapshot{Channels: map[string]TargetChannelSnapshot{
			"alpha": {CollectionID: 10, Channel: "alpha"},
			"beta":  {CollectionID: 10, Channel: "beta"},
		}},
	}
	addChannelPlacement(&snapshot, "alpha", 1, 0)
	addChannelPlacement(&snapshot, "beta", 1, 0)
	addHistoricalSegment(&snapshot, 101, 80, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 20, 1, "alpha", true)
	config := scorePolicyTestConfig(true)

	wave := newScoreEpochPolicy(true).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, config,
	)

	require.Equal(t, PlanKindChannel, wave.Kind)
	require.NotEmpty(t, wave.Plans)
	for _, plan := range wave.Plans {
		require.Equal(t, PlanKindChannel, plan.Kind)
	}
}

func TestChannelLevelEpochPolicyFallsBackWhenMappingsIncomplete(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	snapshot.Replicas[1000] = ReplicaSnapshot{
		ID: 1000, CollectionID: 10, ResourceGroup: "rg1", RWNodes: []int64{1, 2},
		ChannelRWNodes: map[string][]int64{"alpha": {1}},
	}
	snapshot.CollectionTargets[10] = CollectionTargetSnapshot{
		Current: TargetScopeSnapshot{Channels: map[string]TargetChannelSnapshot{
			"alpha": {CollectionID: 10, Channel: "alpha"},
			"beta":  {CollectionID: 10, Channel: "beta"},
		}},
	}
	addHistoricalSegment(&snapshot, 101, 60, 1, "beta", true)
	addHistoricalSegment(&snapshot, 102, 40, 1, "beta", true)

	wave := newScoreEpochPolicy(true).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)

	require.NotEmpty(t, wave.Plans)
	require.Equal(t, int64(2), wave.Plans[0].To)
}

func TestBalancerFactoryEpochPolicyRejectsChannelLevelWithStreamingService(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	streamingutil.SetStreamingServiceEnabled()
	t.Cleanup(func() {
		streamingutil.UnsetStreamingServiceEnabled()
		params.Reset(params.QueryCoordCfg.Balancer.Key)
		params.Reset(params.QueryCoordCfg.AutoBalanceChannel.Key)
	})
	factory := NewBalancerFactory(nil, nil, nil, nil)

	policy, supported := factory.GetEpochPolicy()
	require.False(t, supported)
	require.Nil(t, policy)

	params.Save(params.QueryCoordCfg.Balancer.Key, meta.ScoreBasedBalancerName)
	params.Save(params.QueryCoordCfg.AutoBalanceChannel.Key, "true")
	policy, supported = factory.GetEpochPolicy()
	require.False(t, supported)
	require.Nil(t, policy)

	params.Save(params.QueryCoordCfg.AutoBalanceChannel.Key, "false")
	policy, supported = factory.GetEpochPolicy()
	require.True(t, supported)
	require.NotNil(t, policy)
}

func TestBalancerFactoryEpochPolicySupportsOnlyScorePolicies(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	streamingutil.UnsetStreamingServiceEnabled()
	t.Cleanup(func() {
		params.Reset(params.QueryCoordCfg.Balancer.Key)
		streamingutil.UnsetStreamingServiceEnabled()
	})
	factory := NewBalancerFactory(nil, nil, nil, nil)

	for _, balancer := range []string{
		meta.ScoreBasedBalancerName,
		meta.ChannelLevelScoreBalancerName,
	} {
		params.Save(params.QueryCoordCfg.Balancer.Key, balancer)
		policy, supported := factory.GetEpochPolicy()
		require.True(t, supported, balancer)
		require.NotNil(t, policy, balancer)
	}
	for _, balancer := range []string{
		meta.RoundRobinBalancerName,
		meta.RowCountBasedBalancerName,
		meta.MultiTargetBalancerName,
		"UnknownBalancerType",
	} {
		params.Save(params.QueryCoordCfg.Balancer.Key, balancer)
		policy, supported := factory.GetEpochPolicy()
		require.False(t, supported, balancer)
		require.Nil(t, policy, balancer)
	}
}

func TestEpochPolicyConfigIsFrozenForOnePlan(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	t.Cleanup(func() {
		params.Reset(params.QueryCoordCfg.AutoBalanceChannel.Key)
		params.Reset(params.QueryCoordCfg.CollectionChannelCountFactor.Key)
	})
	snapshot := scorePolicyTestSnapshot()
	addChannelPlacement(&snapshot, "alpha", 1, 0)
	addChannelPlacement(&snapshot, "beta", 1, 0)
	config := scorePolicyTestConfig(true)
	config.CollectionChannelCountFactor = 2
	policy := newScoreEpochPolicy(false)

	first := policy.Plan(&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, config)
	params.Save(params.QueryCoordCfg.AutoBalanceChannel.Key, "false")
	params.Save(params.QueryCoordCfg.CollectionChannelCountFactor.Key, "100")
	second := policy.Plan(&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, config)

	require.Equal(t, first, second)
}

func TestEpochPolicyRespectsPersistentPlanningConstraints(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 60, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 40, 1, "alpha", true)
	policy := newScoreEpochPolicy(false)
	withoutConstraint := policy.Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)
	require.NotEmpty(t, withoutConstraint.Plans)
	constraints := EpochPlanningConstraints{Objects: make(map[BalanceObjectKey]EpochObjectConstraint)}
	for _, segmentID := range []int64{101, 102} {
		locked := BalanceObjectKey{
			Kind: BalanceObjectSegment, ReplicaID: 1000,
			SegmentID: segmentID, Scope: querypb.DataScope_Historical,
		}
		constraints.Objects[locked] = EpochObjectConstraint{
			Object: locked, CollectionID: 10, From: 1, To: 2, Class: ReservationQuarantineOnly,
		}
	}

	wave := policy.Plan(&snapshot, scorePolicyTestBudget(), constraints, scorePolicyTestConfig(false))

	require.Empty(t, wave.Plans)
}

func TestScoreEpochPolicyDeduplicatesPhysicalWorkloadAcrossReplicaKeys(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 80, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 20, 2, "alpha", true)
	policy := newScoreEpochPolicy(false)
	config := scorePolicyTestConfig(false)
	want := policy.Evaluate(&snapshot, NewProjectedPlacement(snapshot), PlanKindSegment, config)

	duplicated := clonePlacementSnapshot(snapshot)
	for key, placements := range snapshot.Segments {
		key.ReplicaID = 2000
		duplicated.Segments[key] = append([]SegmentPlacement(nil), placements...)
	}
	got := policy.Evaluate(&duplicated, NewProjectedPlacement(duplicated), PlanKindSegment, config)

	require.Equal(t, want, got)
}

func TestScoreEpochPolicySkipsAliasedPhysicalMovementCandidates(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 80, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 20, 1, "alpha", true)
	snapshot.Replicas[2000] = ReplicaSnapshot{
		ID: 2000, CollectionID: 10, ResourceGroup: "rg1", RWNodes: []int64{1, 2},
		ChannelRWNodes: map[string][]int64{"alpha": {1, 2}},
	}
	snapshot.EligibleReplicas[2000] = struct{}{}
	for key, placements := range cloneSegments(snapshot.Segments) {
		key.ReplicaID = 2000
		snapshot.Segments[key] = placements
	}

	wave := newScoreEpochPolicy(false).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)

	require.Empty(t, wave.Plans)
	require.Empty(t, wave.PrefixAfter)
}

func TestScoreEpochPolicyCountsGrowingRowsOnce(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addChannelPlacement(&snapshot, "alpha", 1, 100)
	config := scorePolicyTestConfig(false)
	config.GlobalRowCountFactor = 1
	policy := newScoreEpochPolicy(false)
	want := policy.Evaluate(&snapshot, NewProjectedPlacement(snapshot), PlanKindSegment, config)

	withStreamingPlacement := clonePlacementSnapshot(snapshot)
	withStreamingPlacement.Segments[SegmentObjectKey{
		ReplicaID: 1000, SegmentID: 9001, Scope: querypb.DataScope_Streaming,
	}] = []SegmentPlacement{{
		NodeID: 1, CollectionID: 10, Channel: "alpha", RowCount: 100, Present: true,
	}}
	got := policy.Evaluate(
		&withStreamingPlacement, NewProjectedPlacement(withStreamingPlacement), PlanKindSegment, config,
	)

	require.Equal(t, want, got)
}

func TestScoreEpochPolicyPreservesPendingChannelDoubleDelta(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	snapshot.PendingWork.ChannelWorkloadByNode[1] = 1
	snapshot.PendingWork.Tasks = []task.PendingBalanceTaskSnapshot{{
		TaskID: 1, CollectionID: 10, ReplicaID: 1000,
		Actions: []task.PendingBalanceActionSnapshot{{
			NodeID: 1, Type: task.ActionTypeGrow, Channel: "alpha", Workload: 1,
		}},
	}}
	config := scorePolicyTestConfig(false)
	config.CollectionChannelCountFactor = 10

	potential := newScoreEpochPolicy(false).Evaluate(
		&snapshot, NewProjectedPlacement(snapshot), PlanKindChannel, config,
	)

	require.Equal(t, float64(2), potential.Value)
}

func TestScoreEpochPolicyPreservesPerChannelFloatAccumulationBeforeTruncation(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	for index := 0; index < 10; index++ {
		addChannelPlacement(&snapshot, fmt.Sprintf("channel-%02d", index), 1, 0)
	}
	config := scorePolicyTestConfig(false)
	config.CollectionChannelCountFactor = 1.1

	potential := newScoreEpochPolicy(false).Evaluate(
		&snapshot, NewProjectedPlacement(snapshot), PlanKindChannel, config,
	)

	require.Equal(t, float64(50), potential.Value)
}

func TestScoreEpochPolicyMovesOnlyTargetedUniqueHistoricalSegments(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	addHistoricalSegment(&snapshot, 101, 30, 1, "alpha", true)
	addHistoricalSegment(&snapshot, 102, 100, 1, "alpha", false)
	snapshot.Segments[SegmentObjectKey{
		ReplicaID: 1000, SegmentID: 103, Scope: querypb.DataScope_Streaming,
	}] = []SegmentPlacement{{
		NodeID: 1, CollectionID: 10, Channel: "alpha", RowCount: 100, Present: true,
	}}
	addHistoricalSegment(&snapshot, 104, 100, 1, "alpha", true)
	snapshot.Segments[SegmentObjectKey{
		ReplicaID: 1000, SegmentID: 104, Scope: querypb.DataScope_Historical,
	}] = append(snapshot.Segments[SegmentObjectKey{
		ReplicaID: 1000, SegmentID: 104, Scope: querypb.DataScope_Historical,
	}], SegmentPlacement{
		NodeID: 2, CollectionID: 10, Channel: "alpha", RowCount: 100, Present: true,
	})

	wave := newScoreEpochPolicy(false).Plan(
		&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, scorePolicyTestConfig(false),
	)

	require.Len(t, wave.Plans, 1)
	require.Equal(t, int64(101), wave.Plans[0].SegmentID)
	require.Equal(t, querypb.DataScope_Historical, wave.Plans[0].Scope)
}

func TestScoreEpochPolicyStaticTargetMoveCountDecaysToZero(t *testing.T) {
	snapshot := scorePolicyTestSnapshot()
	for segmentID := int64(101); segmentID <= 104; segmentID++ {
		addHistoricalSegment(&snapshot, segmentID, 10, 1, "alpha", true)
	}
	policy := newScoreEpochPolicy(false)
	config := scorePolicyTestConfig(false)
	previousCount := int(^uint(0) >> 1)

	for iteration := 0; iteration < 10; iteration++ {
		wave := policy.Plan(&snapshot, scorePolicyTestBudget(), EpochPlanningConstraints{}, config)
		require.Less(t, len(wave.Plans), previousCount)
		if len(wave.Plans) == 0 {
			require.True(t, wave.Converged)
			return
		}
		previousCount = len(wave.Plans)
		projected := NewProjectedPlacement(snapshot)
		for _, plan := range wave.Plans {
			require.NoError(t, projected.Apply(plan))
		}
		snapshot = projected.Snapshot()
	}
	t.Fatal("static target did not converge")
}

func scorePolicyTestSnapshot() PlacementSnapshot {
	return PlacementSnapshot{
		Token: SnapshotToken{
			ResourceGroup:        "rg1",
			CurrentTargetVersion: map[int64]int64{10: 1},
			NextTargetVersion:    map[int64]int64{10: 2},
		},
		Nodes: map[int64]NodeSnapshot{
			1: {ID: 1, Exists: true, Eligible: true, ResourceGroup: "rg1"},
			2: {ID: 2, Exists: true, Eligible: true, ResourceGroup: "rg1"},
			3: {ID: 3, Exists: true, Eligible: true, ResourceGroup: "rg1"},
		},
		Replicas: map[int64]ReplicaSnapshot{
			1000: {
				ID: 1000, CollectionID: 10, ResourceGroup: "rg1", RWNodes: []int64{1, 2},
				ChannelRWNodes: map[string][]int64{"alpha": {1, 2}},
			},
		},
		Segments: map[SegmentObjectKey][]SegmentPlacement{},
		Channels: map[ChannelObjectKey][]ChannelPlacement{},
		CollectionTargets: map[int64]CollectionTargetSnapshot{
			10: {
				Current: TargetScopeSnapshot{
					Version:  1,
					Segments: map[int64]TargetSegmentSnapshot{},
					Channels: map[string]TargetChannelSnapshot{
						"alpha": {CollectionID: 10, Channel: "alpha"},
					},
				},
				Next: TargetScopeSnapshot{
					Version: 2, Segments: map[int64]TargetSegmentSnapshot{},
					Channels: map[string]TargetChannelSnapshot{},
				},
			},
		},
		PendingWork: PendingWorkSnapshot{
			SegmentWorkloadByNode: map[int64]int{},
			ChannelWorkloadByNode: map[int64]int{},
		},
		EligibleReplicas: map[int64]struct{}{1000: {}},
	}
}

func addHistoricalSegment(
	snapshot *PlacementSnapshot,
	segmentID, rows, nodeID int64,
	channel string,
	targeted bool,
) {
	key := SegmentObjectKey{ReplicaID: 1000, SegmentID: segmentID, Scope: querypb.DataScope_Historical}
	snapshot.Segments[key] = []SegmentPlacement{{
		NodeID: nodeID, CollectionID: 10, Channel: channel, RowCount: rows, Present: true,
	}}
	if targeted {
		target := snapshot.CollectionTargets[10]
		if target.Current.Segments == nil {
			target.Current.Segments = make(map[int64]TargetSegmentSnapshot)
		}
		target.Current.Segments[segmentID] = TargetSegmentSnapshot{
			ID: segmentID, CollectionID: 10, Channel: channel, RowCount: rows,
		}
		snapshot.CollectionTargets[10] = target
	}
}

func addChannelPlacement(snapshot *PlacementSnapshot, channel string, nodeID, growingRows int64) {
	snapshot.Channels[ChannelObjectKey{ReplicaID: 1000, Channel: channel}] = []ChannelPlacement{{
		NodeID: nodeID, CollectionID: 10, Present: true, NumOfGrowingRows: growingRows,
	}}
	target := snapshot.CollectionTargets[10]
	if target.Current.Channels == nil {
		target.Current.Channels = make(map[string]TargetChannelSnapshot)
	}
	target.Current.Channels[channel] = TargetChannelSnapshot{CollectionID: 10, Channel: channel}
	snapshot.CollectionTargets[10] = target
}

func epochSegmentPlan(
	snapshot PlacementSnapshot,
	replicaID, segmentID, from, to int64,
) EpochPlan {
	key := SegmentObjectKey{ReplicaID: replicaID, SegmentID: segmentID, Scope: querypb.DataScope_Historical}
	placement := snapshot.Segments[key][0]
	return EpochPlan{
		Kind: PlanKindSegment, CollectionID: placement.CollectionID, ReplicaID: replicaID,
		Shard: placement.Channel, SegmentID: segmentID, Scope: querypb.DataScope_Historical,
		RowCount: placement.RowCount, From: from, To: to,
		Token: AdmissionToken{
			Snapshot: cloneSnapshotToken(snapshot.Token), CollectionID: placement.CollectionID,
			ReplicaID: replicaID, ExpectedSourceNode: from, Segment: &key,
		},
	}
}

func scorePolicyTestConfig(autoBalanceChannel bool) EpochPolicyConfig {
	return EpochPolicyConfig{
		GlobalRowCountFactor:          0,
		DelegatorMemoryOverloadFactor: 0,
		CollectionChannelCountFactor:  10,
		AutoBalanceChannel:            autoBalanceChannel,
	}
}

func scorePolicyTestBudget() BalanceWaveBudget {
	return BalanceWaveBudget{
		MaxSegmentTasks: 10, MaxChannelTasks: 10,
		MaxTasksPerNode: 10, MaxTasksPerCollection: 10,
	}
}
