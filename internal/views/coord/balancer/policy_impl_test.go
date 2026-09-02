package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func policyTestConfig() *BalanceConfig {
	return &BalanceConfig{
		StickinessWeight:       1,
		NodeLoadWeight:         1,
		FanoutWeight:           1,
		StickyRowsScale:        1_000_000,
		TargetRowsPerShardNode: 100_000,
	}
}

func distinctAssignmentNodes(assignments map[int64]int64) map[int64]struct{} {
	nodes := make(map[int64]struct{})
	for _, nodeID := range assignments {
		nodes[nodeID] = struct{}{}
	}
	return nodes
}

func shardDataView(vchannel string, partitionID int64, segmentIDs ...int64) *viewpb.DataViewOfShard {
	return &viewpb.DataViewOfShard{
		Vchannel: vchannel,
		Partitions: []*viewpb.DataViewOfPartition{
			{PartitionId: partitionID, SegmentIds: segmentIDs},
		},
	}
}

func assignmentsFromBuilder(builder *qviews.QueryViewAtCoordBuilder) map[int64]int64 {
	return flattenAssignments(builder.Build())
}

func setTestShardRows(snap *BalancerSnapshot, shardID qviews.ShardID, rowsByNode map[int64]int64) {
	if snap.ShardRowStatsSnapshot == nil {
		snap.ShardRowStatsSnapshot = make(map[qviews.ShardID]ShardRowStats)
	}
	rowStats := make(ShardRowStats, len(rowsByNode))
	for nodeID, rows := range rowsByNode {
		rowStats[nodeID] = NodeRowStats{UpRowCount: rows}
	}
	snap.ShardRowStatsSnapshot[shardID] = rowStats
}

func upStats(version qviews.DataVersion, partitions []int64, fields []int64, placements ...testSegmentPlacement) *coordview.ShardStats {
	return testShardStats(
		&qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
		1,
		placements...,
	)
}

func TestDefaultBalancePolicy_ReleaseResidualShard(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "v0"}
	snap := &BalancerSnapshot{
		ShardViewSnapshot: coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			shardID: testShardStats(nil, 0, placement(101, 1, 1, coordview.SegmentStateUp)),
		}),
		LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{}),
		Nodes:              map[int64]*BalanceNode{1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"}},
		Config:             policyTestConfig(),
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID, shardID})

	require.Empty(t, plan.Prepares)
	assert.Equal(t, []qviews.ShardID{shardID}, plan.Releases)
}

func TestDefaultBalancePolicy_MandatoryInitialLoadAllocatesLargestRowCountFirst(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, MemSize: 800, RowNum: 100_000},
		102: {SegmentID: 102, PartitionID: 1, MemSize: 100, RowNum: 800_000},
	}), shardDataView("v0", 1, 101, 102))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(1), assignments[102], "segment with more rows claims the first empty node")
	assert.Equal(t, int64(2), assignments[101], "segment with fewer rows fills the less loaded node")
}

func TestDefaultBalancePolicy_SmallShardStaysWithinOneNodeFanout(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 40_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 30_000},
		103: {SegmentID: 103, PartitionID: 1, RowNum: 20_000},
		104: {SegmentID: 104, PartitionID: 1, RowNum: 10_000},
	}), shardDataView("v0", 1, 101, 102, 103, 104))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Len(t, distinctAssignmentNodes(assignments), 1)
}

func TestDefaultBalancePolicy_TenSmallSegmentsConsolidate(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	segments := make(map[int64]*SegmentInfo, 10)
	segmentIDs := make([]int64, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		segments[segmentID] = &SegmentInfo{SegmentID: segmentID, PartitionID: 1, RowNum: 10_000}
		segmentIDs = append(segmentIDs, segmentID)
	}
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(segments), shardDataView("v0", 1, segmentIDs...))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Len(t, distinctAssignmentNodes(assignments), 1)
}

func TestDefaultBalancePolicy_EqualRowsUseSegmentIDOrder(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 100_000},
	}), shardDataView("v0", 1, 102, 101))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(1), assignments[101])
	assert.Equal(t, int64(2), assignments[102])
}

func TestDefaultBalancePolicy_PredictedLoadCoordinatesAcrossShards(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardA := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	shardB := qviews.ShardID{ReplicaID: replicaID, VChannel: "v1"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardA)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 600},
		201: {SegmentID: 201, PartitionID: 1, RowNum: 600},
	}), shardDataView("v0", 1, 101), shardDataView("v1", 1, 201))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardB, shardA})

	require.Contains(t, plan.Prepares, shardA)
	require.Contains(t, plan.Prepares, shardB)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardA])[101])
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardB])[201])
}

func TestDefaultBalancePolicy_ReusedShardRowsAreNotDoubleCounted(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	desiredVersion := qviews.DataVersion{StreamingVersion: 2}
	shardA := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	shardB := qviews.ShardID{ReplicaID: replicaID, VChannel: "v1"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardA)
	snap.Config = policyTestConfig()
	snap.Config.StickinessWeight = 10
	setTestDataSnapshot(snap, collectionID, desiredVersion, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		201: {SegmentID: 201, PartitionID: 1, RowNum: 50_000},
	}), shardDataView("v0", 1, 101), shardDataView("v1", 1, 201))
	snap.ShardStatsMap()[shardA] = upStats(
		qviews.DataVersion{StreamingVersion: 1},
		[]int64{1},
		nil,
		placement(101, 1, 1, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 100_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 150_000},
	}
	setTestShardRows(snap, shardA, map[int64]int64{1: 100_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardB, shardA})

	require.Contains(t, plan.Prepares, shardA)
	require.Contains(t, plan.Prepares, shardB)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardA])[101])
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardB])[201],
		"shard A must contribute 100k rows once, not 200k rows after reuse")
}

func TestDefaultBalancePolicy_ReleasedShardRowsAreRemovedBeforeAllocation(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	releaseShard := qviews.ShardID{ReplicaID: 99, VChannel: "old"}
	loadShard := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, loadShard)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		201: {SegmentID: 201, PartitionID: 1, RowNum: 50_000},
	}), shardDataView("v0", 1, 201))
	snap.ShardStatsMap()[releaseShard] = upStats(
		qviews.DataVersion{StreamingVersion: 1},
		[]int64{1},
		nil,
		placement(101, 1, 1, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 200_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 150_000},
	}
	setTestShardRows(snap, releaseShard, map[int64]int64{1: 100_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{loadShard, releaseShard})

	assert.Equal(t, []qviews.ShardID{releaseShard}, plan.Releases)
	require.Contains(t, plan.Prepares, loadShard)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[loadShard])[201],
		"the released shard removes 100k rows from node 1 before new allocation")
}

func TestDefaultBalancePolicy_OptionalOptimizationRequiresMovement(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placement(101, 1, 1, coordview.SegmentStateUp))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 100},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 100},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 100})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID)
}

func TestDefaultBalancePolicy_OptionalOptimizationAcceptedWhenWorthCost(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 10},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placement(101, 1, 1, coordview.SegmentStateUp))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 900},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 10})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}

func TestDefaultBalancePolicy_OptionalChangedAssignmentEmitsWithoutPlanLevelThreshold(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 10},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placement(101, 1, 1, coordview.SegmentStateUp))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 900},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 10})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}

func TestDefaultBalancePolicy_LowBenefitScaleOutDoesNotOpenBeyondFanoutBudget(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 80_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 40_000},
		103: {SegmentID: 103, PartitionID: 1, RowNum: 20_000},
		104: {SegmentID: 104, PartitionID: 1, RowNum: 10_000},
	}), shardDataView("v0", 1, 101, 102, 103, 104))
	snap.ShardStatsMap()[shardID] = upStats(
		version, []int64{1}, nil,
		placement(101, 1, 1, coordview.SegmentStateUp),
		placement(102, 1, 2, coordview.SegmentStateUp),
		placement(103, 1, 2, coordview.SegmentStateUp),
		placement(104, 1, 2, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 80_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 70_000},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 80_000, 2: 70_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID)
}

func TestDefaultBalancePolicy_HighBenefitScaleOutUsesNewNodeWithinFanoutBudget(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	segments := make(map[int64]*SegmentInfo, 10)
	segmentIDs := make([]int64, 0, 10)
	placements := make([]testSegmentPlacement, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		segments[segmentID] = &SegmentInfo{SegmentID: segmentID, PartitionID: 1, RowNum: 100_000}
		segmentIDs = append(segmentIDs, segmentID)
		nodeID := int64(1)
		if i >= 5 {
			nodeID = 2
		}
		placements = append(placements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(segments), shardDataView("v0", 1, segmentIDs...))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placements...)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 500_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 500_000},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 500_000, 2: 500_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Contains(t, distinctAssignmentNodes(assignments), int64(3))
}

func TestDefaultBalancePolicy_SaturatedStickinessIsMaximumOptionalMoveCost(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = DefaultBalanceConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 1_000_000},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(
		version,
		[]int64{1},
		nil,
		placement(101, 1, 1, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 101_000_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 1_000_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID,
		"a segment at StickyRowsScale pays the full default movement cost")
}

func TestDefaultBalancePolicy_DefaultFanoutBudgetRejectsPureLoadOnlyOverflow(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = DefaultBalanceConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 99_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 1_000},
	}), shardDataView("v0", 1, 101, 102))
	snap.ShardStatsMap()[shardID] = upStats(
		version,
		[]int64{1},
		nil,
		placement(101, 1, 1, coordview.SegmentStateUp),
		placement(102, 1, 1, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 100_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 100_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID,
		"a shard fitting one target must not open another node only for a tiny load-score gain")
}

func TestDefaultBalancePolicy_SmallSpreadShardConsolidates(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	segments := make(map[int64]*SegmentInfo, 10)
	segmentIDs := make([]int64, 0, 10)
	placements := make([]testSegmentPlacement, 0, 10)
	rowsByNode := map[int64]int64{}
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		nodeID := 1 + i%3
		segments[segmentID] = &SegmentInfo{SegmentID: segmentID, PartitionID: 1, RowNum: 10_000}
		segmentIDs = append(segmentIDs, segmentID)
		placements = append(placements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
		rowsByNode[nodeID] += 10_000
	}
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(segments), shardDataView("v0", 1, segmentIDs...))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placements...)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: rowsByNode[1]},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: rowsByNode[2]},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1", UpRowCount: rowsByNode[3]},
	}
	setTestShardRows(snap, shardID, rowsByNode)

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Len(t, distinctAssignmentNodes(assignmentsFromBuilder(plan.Prepares[shardID])), 1)
}

func TestDefaultBalancePolicy_AppliedOptionalCandidateIsStable(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = DefaultBalanceConfig()
	segments := make(map[int64]*SegmentInfo, 10)
	segmentIDs := make([]int64, 0, 10)
	placements := make([]testSegmentPlacement, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		nodeID := 1 + i%3
		segments[segmentID] = &SegmentInfo{SegmentID: segmentID, PartitionID: 1, RowNum: 10_000}
		segmentIDs = append(segmentIDs, segmentID)
		placements = append(placements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(segments), shardDataView("v0", 1, segmentIDs...))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placements...)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 40_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 30_000},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1", UpRowCount: 30_000},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 40_000, 2: 30_000, 3: 30_000})

	first := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})
	require.Contains(t, first.Prepares, shardID)
	assignments := assignmentsFromBuilder(first.Prepares[shardID])
	require.Len(t, distinctAssignmentNodes(assignments), 1)

	appliedPlacements := make([]testSegmentPlacement, 0, len(assignments))
	for segmentID, nodeID := range assignments {
		appliedPlacements = append(appliedPlacements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, appliedPlacements...)
	for _, node := range snap.Nodes {
		node.UpRowCount = 0
	}
	appliedRows := make(map[int64]int64)
	for _, nodeID := range assignments {
		snap.Nodes[nodeID].UpRowCount += 10_000
		appliedRows[nodeID] += 10_000
	}
	setTestShardRows(snap, shardID, appliedRows)

	second := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, second.Prepares, shardID)
}

func TestDefaultBalancePolicy_NodeLossPreservesSurvivingReusableSegments(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = DefaultBalanceConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 100_000},
	}), shardDataView("v0", 1, 101, 102))
	snap.ShardStatsMap()[shardID] = upStats(
		version,
		[]int64{1},
		nil,
		placement(101, 1, 2, coordview.SegmentStateUp),
		placement(102, 1, 1, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 100_000},
		3: {NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 100_000, 2: 100_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(2), assignments[101], "the copy on the surviving node remains reusable")
	assert.Equal(t, int64(3), assignments[102], "only the segment on the failed node is redistributed")
}

func TestDefaultBalancePolicy_MandatorySameAssignmentStillEmits(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 2}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(
		qviews.DataVersion{StreamingVersion: 1},
		[]int64{1},
		nil,
		placement(101, 1, 1, coordview.SegmentStateUp),
	)
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1", UpRowCount: 100_000},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1", UpRowCount: 200_000},
	}
	setTestShardRows(snap, shardID, map[int64]int64{1: 100_000})

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}
