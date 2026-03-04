package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
)

// --- hard constraints ---

func TestHardConstraints_DeadNodeRejected(t *testing.T) {
	node := &BalanceNode{NodeID: 1, Alive: false, MemoryCapacity: 1000}
	seg := &SegmentInfo{MemSize: 100}
	assert.False(t, passHardConstraints(node, seg))
}

func TestHardConstraints_StoppingNodeRejected(t *testing.T) {
	node := &BalanceNode{NodeID: 1, Alive: true, Stopping: true, MemoryCapacity: 1000}
	seg := &SegmentInfo{MemSize: 100}
	assert.False(t, passHardConstraints(node, seg))
}

func TestHardConstraints_MemCapacityExceededRejected(t *testing.T) {
	node := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 100, UpMemLoad: 90}
	// existing 90 + new 50 = 140 > 100.
	seg := &SegmentInfo{MemSize: 50}
	assert.False(t, passHardConstraints(node, seg))
}

func TestHardConstraints_ZeroCapacityTreatedAsUnlimited(t *testing.T) {
	node := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 0, UpMemLoad: 1 << 40}
	seg := &SegmentInfo{MemSize: 1 << 40}
	assert.True(t, passHardConstraints(node, seg))
}

func TestHardConstraints_PendingLoadIncluded(t *testing.T) {
	node := &BalanceNode{
		NodeID:         1,
		Alive:          true,
		MemoryCapacity: 100,
		UpMemLoad:      40,
		PendingMemLoad: 50,
	}
	// 40 + 50 + 20 = 110 > 100 → rejected even though UpMemLoad alone fits.
	seg := &SegmentInfo{MemSize: 20}
	assert.False(t, passHardConstraints(node, seg))
}

func TestHardConstraints_RowNumFallbackUsedWhenMemSizeMissing(t *testing.T) {
	node := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 100, UpMemLoad: 50}
	seg := &SegmentInfo{MemSize: 0, RowNum: 60}
	assert.False(t, passHardConstraints(node, seg))
}

// --- scoring: relative comparisons, not absolute values ---

func testConfig() *BalanceConfig {
	return &BalanceConfig{
		StickinessBaseWeight: 1_000,
		MemoryWeight:         100,
		SegmentCountWeight:   10,
		BaselineSegmentSize:  100,
	}
}

func TestScore_StickinessDominatesSmallImbalance(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}

	// Node 1 currently holds this segment. Node 2 is marginally less loaded.
	n1 := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 300}
	n2 := &BalanceNode{NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 200}

	current := map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}
	s1 := score(n1, seg, current, cfg)
	s2 := score(n2, seg, current, cfg)
	assert.Greater(t, s1, s2, "stickiness bonus should dominate a minor memory imbalance")
}

func TestScore_StickinessProportionalToSize(t *testing.T) {
	cfg := testConfig()
	smallSeg := &SegmentInfo{MemSize: 10}
	bigSeg := &SegmentInfo{MemSize: 1000}
	n := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 1000}

	current := map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}
	smallBonus := score(n, smallSeg, current, cfg) - score(n, smallSeg, nil, cfg)
	bigBonus := score(n, bigSeg, current, cfg) - score(n, bigSeg, nil, cfg)
	assert.Greater(t, bigBonus, smallBonus*50,
		"large segment should get a much larger stickiness bonus (proportional to MemSize)")
}

func TestScore_MemoryBalancePrefersEmptier(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}

	emptier := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 100}
	loaded := &BalanceNode{NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 800}

	s1 := score(emptier, seg, nil, cfg)
	s2 := score(loaded, seg, nil, cfg)
	assert.Greater(t, s1, s2)
}

func TestScore_SegmentCountTiebreak(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}

	// Two nodes with identical memory usage; pick the one with fewer segments.
	fewerSegs := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 200, SegmentCount: 2}
	moreSegs := &BalanceNode{NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 200, SegmentCount: 10}

	s1 := score(fewerSegs, seg, nil, cfg)
	s2 := score(moreSegs, seg, nil, cfg)
	assert.Greater(t, s1, s2)
}

func TestScore_StateAffinityDecreasesBySegmentState(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	node := &BalanceNode{NodeID: 1, Alive: true, MemoryCapacity: 1000}

	up := score(node, seg, map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}, cfg)
	ready := score(node, seg, map[int64]coordview.SegmentState{1: coordview.SegmentStateReady}, cfg)
	preparing := score(node, seg, map[int64]coordview.SegmentState{1: coordview.SegmentStatePreparing}, cfg)
	none := score(node, seg, nil, cfg)
	unrecoverable := score(node, seg, map[int64]coordview.SegmentState{1: coordview.SegmentStateUnrecoverable}, cfg)

	assert.Greater(t, up, ready)
	assert.Greater(t, ready, preparing)
	assert.Greater(t, preparing, none)
	assert.Greater(t, none, unrecoverable)
}

// --- pickNode ---

func TestPickNode_PicksStickyHost(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 400}, // sticky
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 200}, // less loaded
	}

	current := map[int64]coordview.SegmentState{1: coordview.SegmentStateUp}
	id, ok := pickNode(predicted, seg, current, []int64{1, 2}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), id)
}

func TestPickNode_PicksEmptierWhenNoStickiness(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 800},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 200},
	}

	id, ok := pickNode(predicted, seg, nil /* no current */, []int64{1, 2}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(2), id)
}

func TestPickNode_SkipsHardConstraintFailures(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: false, MemoryCapacity: 1000},                // dead
		2: {NodeID: 2, Alive: true, Stopping: true, MemoryCapacity: 1000}, // stopping
		3: {NodeID: 3, Alive: true, MemoryCapacity: 50},                   // too small
		4: {NodeID: 4, Alive: true, MemoryCapacity: 1000},                 // good
	}

	id, ok := pickNode(predicted, seg, nil, []int64{1, 2, 3, 4}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(4), id)
}

func TestPickNode_NoEligibleNodeReturnsFalse(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: false, MemoryCapacity: 1000},
		2: {NodeID: 2, Alive: true, Stopping: true, MemoryCapacity: 1000},
	}

	_, ok := pickNode(predicted, seg, nil, []int64{1, 2}, cfg)
	assert.False(t, ok)
}

func TestPickNode_IgnoresNodesOutsideReplicaScope(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 900}, // in scope, heavily loaded
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 0},   // out of replica scope
	}

	// Only node 1 is in the replica.
	id, ok := pickNode(predicted, seg, nil, []int64{1}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), id, "node 2 must not be chosen despite being empty — it's outside the replica")
}

func TestPickNode_UnknownNodeInReplicaScopeSkipped(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000},
	}

	// Replica lists node 99 but it's missing from predicted (e.g., just removed).
	id, ok := pickNode(predicted, seg, nil, []int64{99, 1}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), id)
}

func TestPickNode_AvoidsUnrecoverableWhenAlternativeExists(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000},
	}
	current := map[int64]coordview.SegmentState{1: coordview.SegmentStateUnrecoverable}

	id, ok := pickNode(predicted, seg, current, []int64{1, 2}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(2), id)
}

func TestPickNode_CanUseUnrecoverableNodeWhenOnlyEligible(t *testing.T) {
	cfg := testConfig()
	seg := &SegmentInfo{MemSize: 100}
	predicted := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000},
	}
	current := map[int64]coordview.SegmentState{1: coordview.SegmentStateUnrecoverable}

	id, ok := pickNode(predicted, seg, current, []int64{1}, cfg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), id)
}
