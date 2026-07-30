package balancer

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
)

func TestBalanceConfigDoesNotExposeLegacyScoringFields(t *testing.T) {
	typ := reflect.TypeOf(BalanceConfig{})
	for _, field := range []string{
		"StickinessBaseWeight",
		"RowCountWeight",
		"SegmentCountWeight",
		"BaselineSegmentRows",
		"BalanceThreshold",
		"CostEfficiencyThreshold",
	} {
		_, ok := typ.FieldByName(field)
		assert.False(t, ok, "legacy field %s must be removed", field)
	}
}

func TestBalanceNodeDoesNotExposeSegmentCount(t *testing.T) {
	_, ok := reflect.TypeOf(BalanceNode{}).FieldByName("SegmentCount")
	assert.False(t, ok)
}

func normalizedTestConfig() *BalanceConfig {
	return &BalanceConfig{
		StickinessWeight:       1,
		NodeLoadWeight:         1,
		FanoutWeight:           1,
		StickyRowsScale:        1_000_000,
		TargetRowsPerShardNode: 100_000,
	}
}

func TestDefaultBalanceConfigUsesNormalizedRowScores(t *testing.T) {
	cfg := DefaultBalanceConfig()

	assert.Positive(t, cfg.StickinessWeight)
	assert.Positive(t, cfg.NodeLoadWeight)
	assert.Positive(t, cfg.FanoutWeight)
	assert.Positive(t, cfg.StickyRowsScale)
	assert.Positive(t, cfg.TargetRowsPerShardNode)
}

func TestHardConstraintsRejectUnavailableNodes(t *testing.T) {
	seg := &SegmentInfo{RowNum: 100}

	assert.False(t, passHardConstraints(&BalanceNode{NodeID: 1}, seg))
	assert.False(t, passHardConstraints(&BalanceNode{NodeID: 2, Alive: true, Stopping: true}, seg))
	assert.True(t, passHardConstraints(&BalanceNode{NodeID: 3, Alive: true, UpRowCount: 1 << 40}, seg))
}

func TestSegmentRowsUsesRowNumInsteadOfMemSize(t *testing.T) {
	seg := &SegmentInfo{MemSize: 1_000_000, RowNum: 10}
	assert.Equal(t, int64(10), segmentRows(seg))
}

func TestStickinessScoreIsSegmentLocal(t *testing.T) {
	cfg := normalizedTestConfig()
	states := map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}
	eligible := map[int64]struct{}{1: {}, 2: {}}

	assert.InDelta(t, 1.0, stickinessScore(1, 100_000, states, eligible, cfg), 1e-12)
	assert.InDelta(t, 0.9, stickinessScore(2, 100_000, states, eligible, cfg), 1e-12)
	assert.InDelta(t, 0.9, stickinessScore(2, 100_000, states, eligible, cfg), 1e-12,
		"the score must not depend on how many earlier segments moved")
}

func TestStickinessScoreSaturatesAndHandlesMandatoryMovement(t *testing.T) {
	cfg := normalizedTestConfig()
	states := map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}

	assert.InDelta(t, 0.0, stickinessScore(2, 2_000_000, states, map[int64]struct{}{1: {}, 2: {}}, cfg), 1e-12)
	assert.InDelta(t, 1.0, stickinessScore(2, 2_000_000, states, map[int64]struct{}{2: {}}, cfg), 1e-12,
		"no eligible reusable copy makes the movement unavoidable")
	assert.InDelta(t, 1.0, stickinessScore(2, 100_000, nil, map[int64]struct{}{2: {}}, cfg), 1e-12,
		"a new segment has no movement cost")
}

func TestStickinessScoreTreatsReusableStatesEqually(t *testing.T) {
	cfg := normalizedTestConfig()
	eligible := map[int64]struct{}{1: {}, 2: {}}

	for _, state := range []coordview.SegmentState{
		coordview.SegmentStateUp,
		coordview.SegmentStateReady,
		coordview.SegmentStatePreparing,
	} {
		states := map[int64]coordview.SegmentState{1: state}
		assert.InDelta(t, 1.0, stickinessScore(1, 100_000, states, eligible, cfg), 1e-12)
		assert.InDelta(t, 0.9, stickinessScore(2, 100_000, states, eligible, cfg), 1e-12)
	}
}

func TestNodeLoadScoreUsesFixedReference(t *testing.T) {
	assert.InDelta(t, 1.0, nodeLoadScore(100, 0), 1e-12)
	assert.InDelta(t, 2.0/3.0, nodeLoadScore(100, 50), 1e-12)
	assert.InDelta(t, 0.5, nodeLoadScore(100, 100), 1e-12)
	assert.InDelta(t, 1.0/3.0, nodeLoadScore(100, 200), 1e-12)
	assert.InDelta(t, 1.0, nodeLoadScore(0, 200), 1e-12)
}

func TestFanoutScoreChargesOnlyOpeningBeyondBudget(t *testing.T) {
	opened := map[int64]struct{}{1: {}}

	assert.Equal(t, 1.0, fanoutScore(1, opened, 1))
	assert.Equal(t, 0.0, fanoutScore(2, opened, 1))

	opened[2] = struct{}{}
	assert.Equal(t, 1.0, fanoutScore(2, opened, 1), "reuse must not pay the opening cost twice")
}

func TestFanoutScoreDoesNotForceBudgetUsage(t *testing.T) {
	opened := map[int64]struct{}{1: {}}

	assert.Equal(t, 1.0, fanoutScore(1, opened, 3))
	assert.Equal(t, 1.0, fanoutScore(2, opened, 3))
}

func TestCalculateFanoutBudgetDoesNotOverflow(t *testing.T) {
	assert.Equal(t, 10, calculateFanoutBudget(10, 10, math.MaxInt64, 100_000))
	assert.Zero(t, calculateFanoutBudget(0, 10, 100_000, 100_000))
	assert.Equal(t, 1, calculateFanoutBudget(10, 10, 100_000, 0))
}

func TestPlacementIntentIsBoundedWeightedAverage(t *testing.T) {
	cfg := normalizedTestConfig()

	assert.InDelta(t, 0.5, placementIntent(1, 0.5, 0, cfg), 1e-12)
	assert.InDelta(t, 1.0, placementIntent(1, 1, 1, cfg), 1e-12)
	assert.InDelta(t, 0.0, placementIntent(0, 0, 0, cfg), 1e-12)
	assert.Zero(t, placementIntent(1, 1, 1, nil))
	assert.Zero(t, placementIntent(1, 1, 1, &BalanceConfig{}))
}

func TestNodeCandidateBetterThanUsesDeterministicPrecedence(t *testing.T) {
	base := nodeCandidate{
		nodeID:        10,
		intent:        0.5,
		projectedRows: 100,
	}

	tests := []struct {
		name      string
		candidate nodeCandidate
	}{
		{
			name:      "higher intent",
			candidate: nodeCandidate{nodeID: 20, intent: 0.6, projectedRows: 200},
		},
		{
			name:      "reusable copy",
			candidate: nodeCandidate{nodeID: 20, intent: 0.5, reusable: true, projectedRows: 200},
		},
		{
			name:      "already open",
			candidate: nodeCandidate{nodeID: 20, intent: 0.5, opened: true, projectedRows: 200},
		},
		{
			name:      "lower projected rows",
			candidate: nodeCandidate{nodeID: 20, intent: 0.5, projectedRows: 99},
		},
		{
			name:      "lower node id",
			candidate: nodeCandidate{nodeID: 9, intent: 0.5, projectedRows: 100},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.True(t, test.candidate.betterThan(base))
			assert.False(t, base.betterThan(test.candidate))
		})
	}

	withinEpsilon := base
	withinEpsilon.nodeID = 9
	withinEpsilon.intent -= scoreEpsilon / 2
	assert.True(t, withinEpsilon.betterThan(base), "epsilon equality must continue to deterministic tie-breaking")
}

func TestAllocationContextTreatsNegativeRowsAsZero(t *testing.T) {
	ctx := &allocationContext{
		baseRows:     map[int64]int64{1: 10},
		assignedRows: make(map[int64]int64),
		openedNodes:  make(map[int64]struct{}),
	}

	assert.Equal(t, int64(10), ctx.projectedRows(1, -1))
	ctx.assign(1, -1)
	assert.Zero(t, ctx.assignedRows[1])
	assert.Contains(t, ctx.openedNodes, int64(1))
	assert.Zero(t, segmentRows(nil))
	assert.False(t, reusableState(coordview.SegmentStateUnrecoverable))
}
