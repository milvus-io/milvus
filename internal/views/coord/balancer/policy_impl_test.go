package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func policyTestConfig() *BalanceConfig {
	return DefaultBalanceConfig()
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

func upStats(version qviews.DataVersion, placements ...testSegmentPlacement) *coordview.ShardStats {
	return testShardStats(
		&qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
		1,
		placements...,
	)
}

func TestDefaultBalancePolicy_ReleaseResidualShard(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	c := balancercache.New(policyTestConfig())
	c.PublishShard(shardID, testShardStats(nil, 0, placement(101, 1, 1, coordview.SegmentStateUp)))

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID, shardID})

	require.Empty(t, plan.Prepares)
	assert.Equal(t, []qviews.ShardID{shardID}, plan.Releases)
}

func TestDefaultBalancePolicy_MandatoryInitialLoadAllocatesLargestRowCountFirst(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 800_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101, 102))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(1), assignments[102], "segment with more rows claims the first empty node")
	assert.Equal(t, int64(2), assignments[101], "segment with fewer rows fills the less loaded node")
}

func TestDefaultBalancePolicy_SmallShardStaysWithinOneNodeFanout(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 40_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 30_000},
		103: {SegmentID: 103, PartitionID: 1, RowNum: 20_000},
		104: {SegmentID: 104, PartitionID: 1, RowNum: 10_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101, 102, 103, 104))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Len(t, distinctAssignmentNodes(assignments), 1)
}

func TestDefaultBalancePolicy_TenSmallSegmentsConsolidate(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	segments := make(map[int64]*SegmentDataView, 10)
	segmentIDs := make([]int64, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		segments[segmentID] = &SegmentDataView{SegmentID: segmentID, PartitionID: 1, RowNum: 10_000}
		segmentIDs = append(segmentIDs, segmentID)
	}
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 1}, segments, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, segmentIDs...))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Len(t, distinctAssignmentNodes(assignments), 1)
}

func TestDefaultBalancePolicy_EqualRowsUseSegmentIDOrder(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 100_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 102, 101))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(1), assignments[101])
	assert.Equal(t, int64(2), assignments[102])
}

func TestDefaultBalancePolicy_PredictedLoadCoordinatesAcrossShards(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardA := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	shardB := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v1"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 600},
		201: {SegmentID: 201, PartitionID: 1, RowNum: 600},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101), shardDataView("by-dev-rootcoord-dml_0_1v1", 1, 201))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardB, shardA})

	require.Contains(t, plan.Prepares, shardA)
	require.Contains(t, plan.Prepares, shardB)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardA])[101])
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardB])[201])
}

func TestDefaultBalancePolicy_ReusedShardRowsAreNotDoubleCounted(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	desiredVersion := qviews.DataVersion{StreamingVersion: 2}
	shardA := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	shardB := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v1"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	config := policyTestConfig()
	config.StickinessWeight = 10
	c.UpdateBalanceConfig(config)
	publishTestData(c, collectionID, desiredVersion, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		201: {SegmentID: 201, PartitionID: 1, RowNum: 50_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101), shardDataView("by-dev-rootcoord-dml_0_1v1", 1, 201))
	c.PublishShard(shardA, upStats(
		qviews.DataVersion{StreamingVersion: 1},
		placement(101, 1, 1, coordview.SegmentStateUp),
	))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{2: 150_000})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardB, shardA})

	require.Contains(t, plan.Prepares, shardA)
	require.Contains(t, plan.Prepares, shardB)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardA])[101])
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardB])[201],
		"shard A must contribute 100k rows once, not 200k rows after reuse")
}

func TestDefaultBalancePolicy_ReleasedShardRowsAreRemovedBeforeAllocation(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	releaseShard := qviews.ShardID{ReplicaID: 99, VChannel: "by-dev-rootcoord-dml_0_1v9"}
	loadShard := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{
		201: {SegmentID: 201, PartitionID: 1, RowNum: 50_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 201))
	c.PublishShard(releaseShard, withSegmentRows(upStats(
		qviews.DataVersion{StreamingVersion: 1},
		placement(101, 1, 1, coordview.SegmentStateUp),
	), map[int64]int64{101: 100_000}))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{1: 100_000, 2: 150_000})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{loadShard, releaseShard})

	assert.Equal(t, []qviews.ShardID{releaseShard}, plan.Releases)
	require.Contains(t, plan.Prepares, loadShard)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[loadShard])[201],
		"the released shard removes 100k rows from node 1 before new allocation")
}

func TestDefaultBalancePolicy_OptionalOptimizationRequiresMovement(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101))
	c.PublishShard(shardID, upStats(version, placement(101, 1, 1, coordview.SegmentStateUp)))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{2: 100})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID)
}

func TestDefaultBalancePolicy_OptionalOptimizationAcceptedWhenWorthCost(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 10},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101))
	c.PublishShard(shardID, upStats(version, placement(101, 1, 1, coordview.SegmentStateUp)))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{1: 890})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}

func TestDefaultBalancePolicy_OptionalChangedAssignmentEmitsWithoutPlanLevelThreshold(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 10},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101))
	c.PublishShard(shardID, upStats(version, placement(101, 1, 1, coordview.SegmentStateUp)))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{1: 890})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}

func TestDefaultBalancePolicy_LowBenefitScaleOutDoesNotOpenBeyondFanoutBudget(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 80_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 40_000},
		103: {SegmentID: 103, PartitionID: 1, RowNum: 20_000},
		104: {SegmentID: 104, PartitionID: 1, RowNum: 10_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101, 102, 103, 104))
	c.PublishShard(shardID, upStats(
		version,
		placement(101, 1, 1, coordview.SegmentStateUp),
		placement(102, 1, 2, coordview.SegmentStateUp),
		placement(103, 1, 2, coordview.SegmentStateUp),
		placement(104, 1, 2, coordview.SegmentStateUp),
	))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID)
}

func TestDefaultBalancePolicy_HighBenefitScaleOutUsesNewNodeWithinFanoutBudget(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	segments := make(map[int64]*SegmentDataView, 10)
	segmentIDs := make([]int64, 0, 10)
	placements := make([]testSegmentPlacement, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		segments[segmentID] = &SegmentDataView{SegmentID: segmentID, PartitionID: 1, RowNum: 100_000}
		segmentIDs = append(segmentIDs, segmentID)
		nodeID := int64(1)
		if i >= 5 {
			nodeID = 2
		}
		placements = append(placements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	publishTestData(c, collectionID, version, segments, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, segmentIDs...))
	c.PublishShard(shardID, upStats(version, placements...))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Contains(t, distinctAssignmentNodes(assignments), int64(3))
}

func TestDefaultBalancePolicy_SaturatedStickinessIsMaximumOptionalMoveCost(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	c.UpdateBalanceConfig(DefaultBalanceConfig())
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 1_000_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101))
	c.PublishShard(shardID, upStats(
		version,
		placement(101, 1, 1, coordview.SegmentStateUp),
	))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{1: 100_000_000})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID,
		"a segment at StickyRowsScale pays the full default movement cost")
}

func TestDefaultBalancePolicy_DefaultFanoutBudgetRejectsPureLoadOnlyOverflow(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	c.UpdateBalanceConfig(DefaultBalanceConfig())
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 99_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 1_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101, 102))
	c.PublishShard(shardID, upStats(
		version,
		placement(101, 1, 1, coordview.SegmentStateUp),
		placement(102, 1, 1, coordview.SegmentStateUp),
	))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID,
		"a shard fitting one target must not open another node only for a tiny load-score gain")
}

func TestDefaultBalancePolicy_SmallSpreadShardConsolidates(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	segments := make(map[int64]*SegmentDataView, 10)
	segmentIDs := make([]int64, 0, 10)
	placements := make([]testSegmentPlacement, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		nodeID := 1 + i%3
		segments[segmentID] = &SegmentDataView{SegmentID: segmentID, PartitionID: 1, RowNum: 10_000}
		segmentIDs = append(segmentIDs, segmentID)
		placements = append(placements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	publishTestData(c, collectionID, version, segments, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, segmentIDs...))
	c.PublishShard(shardID, upStats(version, placements...))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Len(t, distinctAssignmentNodes(assignmentsFromBuilder(plan.Prepares[shardID])), 1)
}

func TestDefaultBalancePolicy_AppliedOptionalCandidateIsStable(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	c.UpdateBalanceConfig(DefaultBalanceConfig())
	segments := make(map[int64]*SegmentDataView, 10)
	segmentIDs := make([]int64, 0, 10)
	placements := make([]testSegmentPlacement, 0, 10)
	for i := int64(0); i < 10; i++ {
		segmentID := int64(101) + i
		nodeID := 1 + i%3
		segments[segmentID] = &SegmentDataView{SegmentID: segmentID, PartitionID: 1, RowNum: 10_000}
		segmentIDs = append(segmentIDs, segmentID)
		placements = append(placements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	publishTestData(c, collectionID, version, segments, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, segmentIDs...))
	c.PublishShard(shardID, upStats(version, placements...))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	first := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})
	require.Contains(t, first.Prepares, shardID)
	assignments := assignmentsFromBuilder(first.Prepares[shardID])
	require.Len(t, distinctAssignmentNodes(assignments), 1)

	appliedPlacements := make([]testSegmentPlacement, 0, len(assignments))
	for segmentID, nodeID := range assignments {
		appliedPlacements = append(appliedPlacements, placement(segmentID, 1, nodeID, coordview.SegmentStateUp))
	}
	c.PublishShard(shardID, upStats(version, appliedPlacements...))

	second := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	assert.NotContains(t, second.Prepares, shardID)
}

func TestDefaultBalancePolicy_NodeLossPreservesSurvivingReusableSegments(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	c.UpdateBalanceConfig(DefaultBalanceConfig())
	publishTestData(c, collectionID, version, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
		102: {SegmentID: 102, PartitionID: 1, RowNum: 100_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101, 102))
	c.PublishShard(shardID, upStats(
		version,
		placement(101, 1, 2, coordview.SegmentStateUp),
		placement(102, 1, 1, coordview.SegmentStateUp),
	))
	publishTestNodes(c,
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"},
	)

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(2), assignments[101], "the copy on the surviving node remains reusable")
	assert.Equal(t, int64(3), assignments[102], "only the segment on the failed node is redistributed")
}

func TestDefaultBalancePolicy_MandatorySameAssignmentStillEmits(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	c := newTestCache(cfg)
	publishTestData(c, collectionID, qviews.DataVersion{StreamingVersion: 2}, map[int64]*SegmentDataView{
		101: {SegmentID: 101, PartitionID: 1, RowNum: 100_000},
	}, shardDataView("by-dev-rootcoord-dml_0_1v0", 1, 101))
	c.PublishShard(shardID, upStats(
		qviews.DataVersion{StreamingVersion: 1},
		placement(101, 1, 1, coordview.SegmentStateUp),
	))
	publishTestNodes(c,
		&NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		&NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	)

	publishBackgroundRows(c, map[int64]int64{2: 200_000})

	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}

func TestDefaultBalancePolicy_MandatoryPrecedesLargerOptionalShard(t *testing.T) {
	config := DefaultBalanceConfig()
	config.StickinessWeight, config.FanoutWeight = 0, 0
	c := balancercache.New(config)
	mandatory, optional := cacheShard(1, 10), cacheShard(2, 20)
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	c.PublishLoadConfig(2, cfgFor(2, 20, nil, nil), 1)
	c.PublishDataView(1, cacheData(1, mandatory.VChannel, 100))
	c.PublishDataView(2, cacheData(2, optional.VChannel, 200))
	c.PublishShard(optional, withSegmentRows(testShardStats(ver(1, 0, 1), 1, placement(2000, 1, 1, coordview.SegmentStateUp)), map[int64]int64{2000: 200}))
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	publishBackgroundRows(c, map[int64]int64{1: 60})

	for _, dirty := range [][]qviews.ShardID{{optional, mandatory}, {mandatory, optional, mandatory}} {
		plan := NewDefaultBalancePolicy().Plan(c, dirty)
		require.Empty(t, plan.Retries)
		require.Len(t, plan.Prepares, 1)
		require.Contains(t, plan.Prepares, mandatory)
		require.Equal(t, map[int64]int64{1000: 2}, assignmentsFromBuilder(plan.Prepares[mandatory]))
		require.NotContains(t, plan.Prepares, optional, "mandatory adds 100 rows on node 2 before optional evaluates its 60-vs-100 baseline")
		require.Equal(t, int64(260), c.GetNode(1).Info().UpRowCount)
		require.Zero(t, c.GetNode(2).Info().UpRowCount, "predictions must not be published as actual load")
	}
}

func TestDefaultBalancePolicy_LargerShardPrecedesSmallerShard(t *testing.T) {
	config := DefaultBalanceConfig()
	config.StickinessWeight, config.FanoutWeight = 0, 0
	c := balancercache.New(config)
	small, large := cacheShard(1, 10), cacheShard(2, 20)
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	c.PublishLoadConfig(2, cfgFor(2, 20, nil, nil), 1)
	c.PublishDataView(1, cacheData(1, small.VChannel, 100))
	c.PublishDataView(2, cacheData(2, large.VChannel, 200))
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	for _, dirty := range [][]qviews.ShardID{{small, large}, {large, small}} {
		plan := NewDefaultBalancePolicy().Plan(c, dirty)
		require.Empty(t, plan.Retries)
		require.Len(t, plan.Prepares, 2)
		require.Equal(t, map[int64]int64{2000: 1}, assignmentsFromBuilder(plan.Prepares[large]))
		require.Equal(t, map[int64]int64{1000: 2}, assignmentsFromBuilder(plan.Prepares[small]))
	}
}
