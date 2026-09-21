package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
	loadInfoVersion uint64,
	placements ...testSegmentPlacement,
) *coordview.ShardStats {
	stats := &coordview.ShardStats{
		UpVersion:         upVersion,
		UpLoadInfoVersion: loadInfoVersion,
		Segments:          make(map[int64]*coordview.SegmentStats),
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

func TestClassify_DesiredAbsent(t *testing.T) {
	shard := cacheShard(1, 10)
	for _, tc := range []struct {
		name  string
		stats *coordview.ShardStats
		want  actionKind
	}{
		{"residual placements", testShardStats(nil, 0, placement(101, 1, 1, coordview.SegmentStateUp)), actionRelease},
		{"empty Up", testShardStats(ver(1, 1, 1), 0), actionRelease},
		{"absent", nil, actionNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := balancercache.New(nil)
			c.PublishShard(shard, tc.stats)
			assert.Equal(t, tc.want, classifyShard(newPlanningContext(c), shard))
		})
	}
}

func TestClassify_DesiredPresentNoCurrentView_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	// No ShardStats entry — classify should treat as "no current view".
	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_PreparingOnly_None(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, &coordview.ShardStats{
		PreparingVersion: ver(1, 1, 1),
		Segments:         map[int64]*coordview.SegmentStats{},
	})

	assert.Equal(t, actionNone, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_DataVersionAdvanced_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 2, CompactVersion: 0}, nil) // advanced

	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_UnavailableNode_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	// Node 1 has placement but no live topology publication — treat as unavailable.
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_NodeStopping_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, Stopping: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_PartitionsChanged_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	// Desired wants partitions {10, 20}; current only required {10}.
	cfg := cfgFor(1, 1, []int64{10, 20}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		0,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_FieldsChanged_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	// Desired wants field 200; current required {}.
	cfg := cfgFor(1, 1, []int64{10}, []int64{200})
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		0,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_HasPreparingView_None(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, withPreparingVersion(testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
		placement(202, 10, 1, coordview.SegmentStatePreparing),
	), ver(1, 1, 2)))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionNone, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_HasPreparingViewWithAdvancedDataVersion_None(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, withPreparingVersion(testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
		placement(202, 10, 1, coordview.SegmentStatePreparing),
	), ver(2, 1, 1)))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 3, CompactVersion: 1}, nil)

	assert.Equal(t, actionNone, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_UnrecoverableOnly_Must(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		nil,
		0,
		placement(202, 10, 1, coordview.SegmentStateUnrecoverable),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMust, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_SteadyState_MayOptimize(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMayOptimize, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_UnrelatedLoadConfigVersionChangeDoesNotTriggerMust(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishLoadConfig(2, cfgFor(2, 2, []int64{20}, nil), 2)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, nil)

	assert.Equal(t, actionMayOptimize, classifyShard(newPlanningContext(c), shardID))
}

func TestClassify_MissingDataVersionDoesNotTriggerMust(t *testing.T) {
	// If DataView Manager hasn't yet reported a DataVersion, classifier
	// should fall through to steady-state instead of falsely requiring a
	// new view.
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	cfg := cfgFor(1, 1, []int64{10}, nil)
	c := newTestCache(cfg)
	c.PublishShard(shardID, testShardStats(
		ver(1, 1, 1),
		1,
		placement(101, 10, 1, coordview.SegmentStateUp),
	))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true})
	// No DataView has been published for the collection.

	assert.Equal(t, actionMayOptimize, classifyShard(newPlanningContext(c), shardID))
}
