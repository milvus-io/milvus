package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// --- helpers ---

func cfgFor(collectionID, replicaID int64, partitions []int64, fields []int64) *loadmgr.LoadConfig {
	lf := make([]*messagespb.LoadFieldConfig, len(fields))
	for i, fid := range fields {
		lf[i] = &messagespb.LoadFieldConfig{FieldId: fid}
	}
	return &loadmgr.LoadConfig{
		CollectionID: collectionID,
		PartitionIDs: append([]int64{}, partitions...),
		LoadFields:   lf,
		Replicas:     []*loadmgr.ReplicaAssignment{{ReplicaID: replicaID, ResourceGroup: "rg1"}},
	}
}

// baseSnap returns a snapshot with one collection + replica + shard loaded.
// The caller tweaks ShardStatsMap / Nodes / DataViewSnapshot to produce each test case.
func baseSnap(cfg *loadmgr.LoadConfig, shardID qviews.ShardID) *BalancerSnapshot {
	return &BalancerSnapshot{
		LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{cfg.CollectionID: cfg}),
		ShardViewSnapshot:  coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{}),
		DataViewSnapshot:   NewDataViewSnapshot(1, nil, nil),
		Nodes:              map[int64]*BalanceNode{},
	}
}

func setTestDataSnapshot(
	snap *BalancerSnapshot,
	collectionID int64,
	version qviews.DataVersion,
	segments SegmentSnapshot,
	shards ...*viewpb.DataViewOfShard,
) {
	snap.DataViewSnapshot = NewDataViewSnapshot(1, []*viewpb.DataViewOfCollection{
		{
			CollectionId: collectionID,
			DataVersion:  version.IntoProto(),
			Shards:       shards,
		},
	}, segments)
}

func ver(sv, cv, qv int64) *qviews.QueryViewVersion {
	return &qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: sv, CompactVersion: cv},
		QueryVersion: qv,
	}
}

type testSegmentPlacement struct {
	segmentID   int64
	partitionID int64
	nodeID      int64
	state       coordview.SegmentState
}

func placement(segmentID, partitionID, nodeID int64, state coordview.SegmentState) testSegmentPlacement {
	return testSegmentPlacement{
		segmentID:   segmentID,
		partitionID: partitionID,
		nodeID:      nodeID,
		state:       state,
	}
}

func testShardStats(
	upVersion *qviews.QueryViewVersion,
	settings *viewpb.QueryViewSettings,
	placements ...testSegmentPlacement,
) *coordview.ShardStats {
	stats := &coordview.ShardStats{
		UpVersion:  upVersion,
		UpSettings: settings,
		Segments:   make(map[int64]*coordview.SegmentStats),
	}
	for _, p := range placements {
		segment := stats.Segments[p.segmentID]
		if segment == nil {
			segment = &coordview.SegmentStats{
				SegmentID:   p.segmentID,
				PartitionID: p.partitionID,
				Nodes:       make(map[int64]coordview.SegmentState),
			}
			stats.Segments[p.segmentID] = segment
		}
		segment.Nodes[p.nodeID] = p.state
	}
	return stats
}

func withPreparingVersion(stats *coordview.ShardStats, version *qviews.QueryViewVersion) *coordview.ShardStats {
	stats.PreparingVersion = version
	return stats
}

// --- tests ---

func TestClassify_DesiredAbsentWithResidualViews_Release(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	snap := &BalancerSnapshot{
		LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{}),
		ShardViewSnapshot: coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			shardID: testShardStats(nil, nil, placement(101, 0, 1, coordview.SegmentStateUp)),
		}),
		Nodes: map[int64]*BalanceNode{1: {NodeID: 1, Alive: true}},
	}
	assert.Equal(t, actionRelease, classifyShard(snap, shardID))
}

func TestClassify_DesiredAbsentWithEmptyUpView_Release(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	snap := &BalancerSnapshot{
		LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{}),
		ShardViewSnapshot: coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			shardID: testShardStats(ver(1, 1, 1), nil),
		}),
	}
	assert.Equal(t, actionRelease, classifyShard(snap, shardID))
}

func TestClassify_DesiredAndCurrentBothAbsent_None(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	snap := &BalancerSnapshot{
		LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{}),
		ShardViewSnapshot:  coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{}),
	}
	assert.Equal(t, actionNone, classifyShard(snap, shardID))
}

func TestClassify_DesiredPresentNoCurrentView_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	// No ShardStats entry — classify should treat as "no current view".
	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_PreparingOnly_None(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = &coordview.ShardStats{
		PreparingVersion: ver(1, 1, 1),
		Segments:         map[int64]*coordview.SegmentStats{},
	}

	assert.Equal(t, actionNone, classifyShard(snap, shardID))
}

func TestClassify_DataVersionAdvanced_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 2, CompactVersion: 0}, nil) // advanced

	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_UnavailableNode_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	// Node 1 missing from Nodes map — treat as unavailable.
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_NodeStopping_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true, Stopping: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_PartitionsChanged_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	// Desired wants partitions {10, 20}; current only required {10}.
	cfg := cfgFor(1, 1, []int64{10, 20}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_FieldsChanged_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	// Desired wants field 200; current required {}.
	cfg := cfgFor(1, 1, []int64{10}, []int64{200})
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_HasPreparingView_None(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = withPreparingVersion(testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
		placement(202, 10, 1, coordview.SegmentStatePreparing),
	), ver(1, 1, 2))
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionNone, classifyShard(snap, shardID))
}

func TestClassify_UnrecoverableOnly_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		nil,
		nil,
		placement(202, 10, 1, coordview.SegmentStateUnrecoverable),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(snap, shardID))
}

func TestClassify_SteadyState_MayOptimize(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	setTestDataSnapshot(snap, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMayOptimize, classifyShard(snap, shardID))
}

func TestClassify_MissingDataVersionDoesNotTriggerMust(t *testing.T) {
	// If DataView Manager hasn't yet reported a DataVersion, classifier
	// should fall through to steady-state instead of falsely requiring a
	// new view.
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	snap := baseSnap(cfg, shardID)
	snap.ShardStatsMap()[shardID] = testShardStats(
		ver(1, 1, 1),
		&viewpb.QueryViewSettings{RequiredPartitions: []int64{10}},
		placement(101, 10, 1, coordview.SegmentStateUp),
	)
	snap.Nodes[1] = &BalanceNode{NodeID: 1, Alive: true}
	// DataViewSnapshot intentionally has no collection DataVersion.

	assert.Equal(t, actionMayOptimize, classifyShard(snap, shardID))
}
