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
		StickinessBaseWeight:    1,
		MemoryWeight:            100,
		SegmentCountWeight:      1,
		BaselineSegmentSize:     100,
		BalanceThreshold:        1,
		CostEfficiencyThreshold: 0.01,
	}
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

func upStats(version qviews.DataVersion, partitions []int64, fields []int64, placements ...testSegmentPlacement) *coordview.ShardStats {
	return testShardStats(
		&qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
		&viewpb.QueryViewSettings{
			RequiredPartitions: partitions,
			RequiredFields:     fields,
		},
		placements...,
	)
}

func TestDefaultBalancePolicy_ReleaseResidualShard(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "v0"}
	snap := &BalancerSnapshot{
		ShardViewSnapshot: coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			shardID: testShardStats(nil, nil, placement(101, 1, 1, coordview.SegmentStateUp)),
		}),
		LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{}),
		Nodes:              map[int64]*BalanceNode{1: {NodeID: 1, Alive: true}},
		Config:             policyTestConfig(),
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID, shardID})

	require.Empty(t, plan.Prepares)
	assert.Equal(t, []qviews.ShardID{shardID}, plan.Releases)
}

func TestDefaultBalancePolicy_MandatoryInitialLoadAllocatesLargestFirst(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	cfg.Replicas[0].Nodes = []int64{1, 2}
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, MemSize: 800},
		102: {SegmentID: 102, PartitionID: 1, MemSize: 100},
	}), shardDataView("v0", 1, 101, 102))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assignments := assignmentsFromBuilder(plan.Prepares[shardID])
	assert.Equal(t, int64(1), assignments[101], "largest segment claims the first empty node")
	assert.Equal(t, int64(2), assignments[102], "smaller segment fills the less loaded node")
}

func TestDefaultBalancePolicy_PredictedLoadCoordinatesAcrossShards(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shardA := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	shardB := qviews.ShardID{ReplicaID: replicaID, VChannel: "v1"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	cfg.Replicas[0].Nodes = []int64{1, 2}
	snap := baseSnap(cfg, shardA)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, qviews.DataVersion{StreamingVersion: 1}, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, MemSize: 600},
		201: {SegmentID: 201, PartitionID: 1, MemSize: 600},
	}), shardDataView("v0", 1, 101), shardDataView("v1", 1, 201))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardB, shardA})

	require.Contains(t, plan.Prepares, shardA)
	require.Contains(t, plan.Prepares, shardB)
	assert.Equal(t, int64(1), assignmentsFromBuilder(plan.Prepares[shardA])[101])
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardB])[201])
}

func TestDefaultBalancePolicy_OptionalOptimizationRequiresMovement(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	cfg.Replicas[0].Nodes = []int64{1, 2}
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, MemSize: 100},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placement(101, 1, 1, coordview.SegmentStateUp))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 100},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000, UpMemLoad: 100},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID)
}

func TestDefaultBalancePolicy_OptionalOptimizationAcceptedWhenWorthCost(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	cfg.Replicas[0].Nodes = []int64{1, 2}
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, MemSize: 10},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placement(101, 1, 1, coordview.SegmentStateUp))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 900},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	require.Contains(t, plan.Prepares, shardID)
	assert.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[shardID])[101])
}

func TestDefaultBalancePolicy_OptionalOptimizationRejectedByThreshold(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	version := qviews.DataVersion{StreamingVersion: 1}
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}
	cfg := cfgFor(collectionID, replicaID, []int64{1}, nil)
	cfg.Replicas[0].Nodes = []int64{1, 2}
	snap := baseSnap(cfg, shardID)
	snap.Config = policyTestConfig()
	snap.Config.BalanceThreshold = 1_000
	setTestDataSnapshot(snap, collectionID, version, newMapSegmentSnapshot(map[int64]*SegmentInfo{
		101: {SegmentID: 101, PartitionID: 1, MemSize: 10},
	}), shardDataView("v0", 1, 101))
	snap.ShardStatsMap()[shardID] = upStats(version, []int64{1}, nil, placement(101, 1, 1, coordview.SegmentStateUp))
	snap.Nodes = map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, MemoryCapacity: 1000, UpMemLoad: 900},
		2: {NodeID: 2, Alive: true, MemoryCapacity: 1000},
	}

	plan := NewDefaultBalancePolicy().Plan(snap, []qviews.ShardID{shardID})

	assert.NotContains(t, plan.Prepares, shardID)
}
