package coordview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	viewsyncer "github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type immediateLostRecoverySyncer struct {
	lostCallbacks atomic.Int64
}

func (s *immediateLostRecoverySyncer) SyncViews(_ context.Context, group viewsyncer.SyncGroup) error {
	for _, views := range group.ViewsByNode {
		for _, view := range views {
			node, ok := view.View.WorkNode().(qviews.QueryNode)
			if !ok || view.OnQueryNodeLost == nil {
				continue
			}
			s.lostCallbacks.Add(1)
			view.OnQueryNodeLost(node)
		}
	}
	return nil
}

func (*immediateLostRecoverySyncer) Close() error { return nil }

// newTestRegistry builds a fresh (empty) registry via the recovery path with
// an empty catalog.
func newTestRegistry(t *testing.T, catalog *mockCatalog, s *mockSyncer) *ShardViewRegistry {
	t.Helper()
	reg, err := RecoverShardViewRegistry(context.Background(), catalog, s)
	require.NoError(t, err)
	t.Cleanup(reg.Close)
	return reg
}

func TestRegistry_FlushesAcrossManagers(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	reg := newTestRegistry(t, catalog, s)

	shard1 := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}
	shard2 := qviews.ShardID{ReplicaID: 1, VChannel: "v2"}
	batch := reg.Begin()
	require.NoError(t, reg.Ensure(shard1).AddPreparing(context.Background(), testBuilderForShard(1, shard1)))
	require.NoError(t, reg.Ensure(shard2).AddPreparing(context.Background(), testBuilderForShard(2, shard2)))
	assert.Zero(t, catalog.numSaveCalls())
	batch.Commit()
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))

	assert.Equal(t, 1, catalog.numSaveCalls())
	assert.Len(t, catalog.saved, 2)
}

func TestRegistry_EmptyRecovery(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	assert.Empty(t, reg.Snapshot().StatsMap())
	assert.Nil(t, reg.Get(testShardID))
}

func TestRegistry_EnsureCreatesOnce(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	before := reg.Snapshot()

	mgr1 := reg.Ensure(testShardID)
	require.NotNil(t, mgr1)
	require.NotSame(t, before, reg.Snapshot())

	mgr2 := reg.Ensure(testShardID)
	assert.Same(t, mgr1, mgr2, "Ensure must return the same instance on repeat")

	assert.Len(t, reg.Snapshot().StatsMap(), 1)
}

func TestRegistry_RequestReleaseRemovesEmptyManager(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	manager := reg.Ensure(testShardID)

	require.NoError(t, manager.RequestRelease(context.Background()))

	assert.Nil(t, reg.Get(testShardID))
	assert.Empty(t, reg.ShardIDs())
	assert.NotContains(t, reg.Snapshot().StatsMap(), testShardID)
}

func TestRegistry_RemoveReleasedManagerDoesNotAccessManagerState(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	manager := reg.Ensure(testShardID)

	manager.mu.Lock()
	done := make(chan struct{})
	go func() {
		reg.removeReleasedManager(testShardID, manager)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		manager.mu.Unlock()
		t.Fatal("registry removal accessed manager state")
	}
	manager.mu.Unlock()
	assert.Nil(t, reg.Get(testShardID))
}

func TestRegistry_RemovesManagerAfterLastViewDurablyDropped(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	reg := newTestRegistry(t, catalog, s)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	version := testVersion(1, 1, 1)
	manager := reg.Ensure(shardID)

	require.NoError(t, manager.AddPreparing(context.Background(), testBuilderForShard(100, shardID)))
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	simulateNodeResponse(t, s, testQN1, version, qviews.QueryViewStateReady)
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	simulateNodeResponse(t, s, qviews.NewStreamingNodeFromVChannel(shardID.VChannel), version, qviews.QueryViewStateReady)
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	simulateNodeResponse(t, s, qviews.NewStreamingNodeFromVChannel(shardID.VChannel), version, qviews.QueryViewStateUp)
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	require.NoError(t, manager.RequestRelease(context.Background()))
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	simulateNodeResponse(t, s, qviews.NewStreamingNodeFromVChannel(shardID.VChannel), version, qviews.QueryViewStateDown)
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	simulateNodeResponse(t, s, qviews.NewStreamingNodeFromVChannel(shardID.VChannel), version, qviews.QueryViewStateDropped)
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))
	simulateNodeResponse(t, s, testQN1, version, qviews.QueryViewStateDropped)
	require.NoError(t, reg.flushScheduler.Flush(context.Background()))

	assert.Nil(t, reg.Get(shardID))
	assert.Empty(t, reg.ShardIDs())
	assert.Empty(t, reg.CollectionShards(100))
	assert.NotContains(t, reg.Snapshot().StatsMap(), shardID)
	assert.NotSame(t, manager, reg.Ensure(shardID))
}

func TestRegistry_DoesNotRemoveReplacementManager(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	oldManager := reg.Ensure(shardID)
	replacement := newShardViewManager(context.Background(), shardID, reg.flushScheduler, nil)
	replacement.SetStatsObserver(reg.onShardStatsChanged)
	replacement.setOnReleasedEmpty(reg.removeReleasedManager)

	reg.mu.Lock()
	reg.shards[shardID] = replacement
	reg.mu.Unlock()
	reg.removeReleasedManager(shardID, oldManager)

	assert.Same(t, replacement, reg.Get(shardID))
}

func TestRegistry_IgnoresStatsFromEvictedManager(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	oldManager := reg.Ensure(shardID)
	replacement := newShardViewManager(context.Background(), shardID, reg.flushScheduler, nil)
	replacement.SetStatsObserver(reg.onShardStatsChanged)
	replacement.setOnReleasedEmpty(reg.removeReleasedManager)

	reg.mu.Lock()
	reg.shards[shardID] = replacement
	reg.mu.Unlock()

	// A late stats publication from the evicted manager must not install its
	// stale placements into the slot now owned by the replacement.
	versionBefore := reg.version
	reg.onShardStatsChanged(shardID, oldManager, shardStatsForNodes(map[int64][]int64{101: {1}}))
	assert.Empty(t, reg.NodeShards(1))
	assert.Equal(t, versionBefore, reg.version)
	require.NotNil(t, reg.stats[shardID])
	assert.Empty(t, reg.stats[shardID].Segments)

	// The current manager's stats are still adopted.
	reg.onShardStatsChanged(shardID, replacement, shardStatsForNodes(map[int64][]int64{101: {1}}))
	assert.ElementsMatch(t, []qviews.ShardID{shardID}, reg.NodeShards(1))
}

func TestRegistry_RecoverWithPersistedViews(t *testing.T) {
	catalog := newMockCatalog()

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "by-dev-rootcoord-dml_200v1"}

	viewA := buildTestViewWithVersion(1, 1, 1, 1)
	viewA.Meta.CollectionId = 100
	viewA.Meta.ReplicaId = shardA.ReplicaID
	viewA.Meta.Vchannel = shardA.VChannel

	viewB := buildTestViewWithVersion(2, 1, 1, 1)
	viewB.Meta.CollectionId = 200
	viewB.Meta.ReplicaId = shardB.ReplicaID
	viewB.Meta.Vchannel = shardB.VChannel

	// Seed the catalog by directly writing the persisted views.
	require.NoError(t, catalog.SaveQueryViews(context.Background(),
		[]*viewpb.QueryViewOfShard{viewA, viewB}))

	// But ListQueryViews is not wired via SaveQueryViews in mockCatalog:
	// we need to populate the listed views manually.
	catalog.listed = []*viewpb.QueryViewOfShard{viewA, viewB}

	reg, err := RecoverShardViewRegistry(context.Background(), catalog, newMockSyncer())
	require.NoError(t, err)
	t.Cleanup(reg.Close)

	assert.Len(t, reg.Snapshot().StatsMap(), 2)

	mgrA := reg.Get(shardA)
	require.NotNil(t, mgrA)

	mgrB := reg.Get(shardB)
	require.NotNil(t, mgrB)

	assert.ElementsMatch(t, []qviews.ShardID{shardA}, reg.CollectionShards(100))
	assert.ElementsMatch(t, []qviews.ShardID{shardB}, reg.CollectionShards(200))
	assert.ElementsMatch(t, []qviews.ShardID{shardA, shardB}, reg.NodeShards(1))
	assert.ElementsMatch(t, []qviews.ShardID{shardB}, reg.NodeShards(2))
}

func TestRegistry_RecoveryPublishesImmediateQueryNodeLoss(t *testing.T) {
	catalog := newMockCatalog()
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	view := buildTestViewWithVersion(1, 1, 1, 1)
	view.Meta.CollectionId = 100
	view.Meta.ReplicaId = shardID.ReplicaID
	view.Meta.Vchannel = shardID.VChannel

	segmentIDs := make([]int64, 50_000)
	for i := range segmentIDs {
		segmentIDs[i] = int64(10_000 + i)
	}
	view.QueryNode[0].Partitions[0].SegmentIds = segmentIDs
	catalog.listed = []*viewpb.QueryViewOfShard{view}
	s := &immediateLostRecoverySyncer{}

	reg, err := RecoverShardViewRegistry(context.Background(), catalog, s)
	require.NoError(t, err)
	t.Cleanup(reg.Close)
	require.Equal(t, int64(1), s.lostCallbacks.Load())

	managerStats := reg.Get(shardID).Stats()
	registryStats := reg.SnapshotForShards([]qviews.ShardID{shardID}).StatsMap()[shardID]
	require.NotNil(t, registryStats)
	require.Nil(t, managerStats.PreparingVersion)
	require.Nil(t, registryStats.PreparingVersion)
	assert.Len(t, registryStats.Segments, len(managerStats.Segments))
	require.Contains(t, registryStats.Segments, segmentIDs[0])
	assert.Equal(t, SegmentStateUnrecoverable, registryStats.Segments[segmentIDs[0]].Nodes[1])
}

func TestRegistry_EnsureIndexesCollection(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "by-dev-rootcoord-dml_100v1"}
	invalid := qviews.ShardID{ReplicaID: 3, VChannel: "invalid-vchannel"}

	reg.Ensure(shardA)
	reg.Ensure(shardB)
	reg.Ensure(invalid)

	assert.ElementsMatch(t, []qviews.ShardID{shardA, shardB}, reg.CollectionShards(100))
	assert.Empty(t, reg.CollectionShards(0))
	assert.ElementsMatch(t, []qviews.ShardID{shardA, shardB, invalid}, reg.ShardIDs())
}

func TestRegistry_NodeIndexTracksStatsReplacement(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	mgr := reg.Ensure(shardID)

	observed := make(chan struct{}, 1)
	reg.RegisterStatsObserver(func(_ qviews.ShardID, _ *ShardStats) {
		reg.NodeShards(1)
		observed <- struct{}{}
	})

	reg.onShardStatsChanged(shardID, mgr, shardStatsForNodes(map[int64][]int64{
		101: {1, 2},
		102: {1},
	}))
	assert.ElementsMatch(t, []qviews.ShardID{shardID}, reg.NodeShards(1))
	assert.ElementsMatch(t, []qviews.ShardID{shardID}, reg.NodeShards(2))
	assert.Len(t, reg.NodeShards(1), 1)
	<-observed

	reg.onShardStatsChanged(shardID, mgr, shardStatsForNodes(map[int64][]int64{103: {3}}))
	assert.Empty(t, reg.NodeShards(1))
	assert.Empty(t, reg.NodeShards(2))
	assert.ElementsMatch(t, []qviews.ShardID{shardID}, reg.NodeShards(3))
	<-observed

	reg.onShardStatsChanged(shardID, mgr, emptyShardStats())
	assert.Empty(t, reg.NodeShards(3))
	<-observed

	assert.NotPanics(t, func() {
		reg.onShardStatsChanged(shardID, mgr, nil)
	})
	assert.Empty(t, reg.NodeShards(3))
	<-observed
}

func TestRegistry_SnapshotForShards(t *testing.T) {
	reg := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "by-dev-rootcoord-dml_200v0"}
	missing := qviews.ShardID{ReplicaID: 3, VChannel: "by-dev-rootcoord-dml_300v0"}
	mgrA := reg.Ensure(shardA)
	mgrB := reg.Ensure(shardB)
	statsA := shardStatsForNodes(map[int64][]int64{101: {1}})
	statsB := shardStatsForNodes(map[int64][]int64{201: {2}})
	reg.onShardStatsChanged(shardA, mgrA, statsA)
	reg.onShardStatsChanged(shardB, mgrB, statsB)

	resident := reg.Snapshot()
	updatedStatsA := shardStatsForNodes(map[int64][]int64{102: {3}})
	reg.onShardStatsChanged(shardA, mgrA, updatedStatsA)
	require.Same(t, resident, reg.snapshot)

	scoped := reg.SnapshotForShards([]qviews.ShardID{shardA, missing, shardA})
	require.Same(t, resident, reg.snapshot)
	assert.Equal(t, reg.version, scoped.Version())
	require.Len(t, scoped.StatsMap(), 1)
	assert.Same(t, updatedStatsA, scoped.StatsMap()[shardA])
	assert.NotContains(t, scoped.StatsMap(), missing)

	scoped.StatsMap()[shardB] = statsB
	next := reg.SnapshotForShards([]qviews.ShardID{shardA})
	assert.NotContains(t, next.StatsMap(), shardB)
	assert.Same(t, updatedStatsA, next.StatsMap()[shardA])
}

func TestRegistry_SnapshotStatsForMultipleShards(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	reg := newTestRegistry(t, catalog, s)

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0_c0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "v0_c0"}

	// Add a Preparing view to shardA (with node 1, seg 1001).
	mgrA := reg.Ensure(shardA)
	bA := testBuilder(1, 1, 1)
	require.NoError(t, mgrA.AddPreparing(context.Background(), bA))

	// Add a Preparing view to shardB via a fresh builder that patches replica id.
	mgrB := reg.Ensure(shardB)
	bB := testBuilder(1, 1, 1)
	// Overwrite assignments and a fresh replicaID+vchannel via a custom builder.
	bB.SetAssignments(map[int64]map[int64][]int64{
		2: {10: {2001}},
	})
	require.NoError(t, mgrB.AddPreparing(context.Background(), bB))

	stats := reg.Snapshot().StatsMap()
	require.Len(t, stats, 2)
	require.NotNil(t, stats[shardA])
	require.NotNil(t, stats[shardB])

	// shardA has segment 1001 on node 1 only.
	assert.Equal(t, map[int64]SegmentState{1: SegmentStatePreparing}, stats[shardA].Segments[1001].Nodes)
	// shardB has segment 2001 on node 2 only.
	assert.Equal(t, map[int64]SegmentState{2: SegmentStatePreparing}, stats[shardB].Segments[2001].Nodes)
}

func TestRegistry_SnapshotLazilyRefreshesResidentSnapshot(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	reg := newTestRegistry(t, catalog, s)

	s1 := reg.Snapshot()
	require.Same(t, s1, reg.Snapshot())

	mgr := reg.Ensure(testShardID)
	require.Same(t, s1, reg.snapshot, "Ensure should only advance version and keep the cached snapshot stale")
	s2 := reg.Snapshot()
	require.NotSame(t, s1, s2)
	require.Same(t, s2, reg.Snapshot())
	assert.Contains(t, s2.StatsMap(), testShardID)

	b := testBuilder(1, 1, 1)
	b.SetAssignments(map[int64]map[int64][]int64{1: {10: {101}}})
	require.NoError(t, mgr.AddPreparing(context.Background(), b))
	require.Same(t, s2, reg.snapshot, "manager updates should not rebuild registry snapshot until Snapshot is requested")

	s3 := reg.Snapshot()
	require.NotSame(t, s2, s3)
	require.Same(t, s3, reg.Snapshot())
	assert.NotNil(t, s3.StatsMap()[testShardID].PreparingVersion)
	assert.Equal(t, map[int64]SegmentState{1: SegmentStatePreparing}, s3.StatsMap()[testShardID].Segments[101].Nodes)
}

func TestRegistry_SnapshotCoalescesMultipleManagerUpdates(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	reg := newTestRegistry(t, catalog, s)
	s1 := reg.Snapshot()

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0_c0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "v0_c0"}

	mgrA := reg.Ensure(shardA)
	bA := testBuilder(1, 1, 1)
	bA.SetAssignments(map[int64]map[int64][]int64{1: {10: {101}}})
	require.NoError(t, mgrA.AddPreparing(context.Background(), bA))

	mgrB := reg.Ensure(shardB)
	bB := testBuilder(1, 1, 1)
	bB.SetAssignments(map[int64]map[int64][]int64{2: {10: {102}}})
	require.NoError(t, mgrB.AddPreparing(context.Background(), bB))

	require.Same(t, s1, reg.snapshot)
	s2 := reg.Snapshot()
	require.NotSame(t, s1, s2)
	assert.Equal(t, uint64(5), s2.Version())
	assert.Equal(t, map[int64]SegmentState{1: SegmentStatePreparing}, s2.StatsMap()[shardA].Segments[101].Nodes)
	assert.Equal(t, map[int64]SegmentState{2: SegmentStatePreparing}, s2.StatsMap()[shardB].Segments[102].Nodes)
	require.Same(t, s2, reg.Snapshot())
}

func TestRegistry_SnapshotSegmentNodeStates(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	reg := newTestRegistry(t, catalog, s)

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0_c0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "v0_c0"}
	shardC := qviews.ShardID{ReplicaID: 3, VChannel: "v0_c0"}

	// shardA on node 1, shardB on node 2, shardC on both 1 and 2.
	mgrA := reg.Ensure(shardA)
	bA := testBuilder(1, 1, 1)
	bA.SetAssignments(map[int64]map[int64][]int64{1: {10: {101}}})
	require.NoError(t, mgrA.AddPreparing(context.Background(), bA))

	mgrB := reg.Ensure(shardB)
	bB := testBuilder(1, 1, 1)
	bB.SetAssignments(map[int64]map[int64][]int64{2: {10: {102}}})
	require.NoError(t, mgrB.AddPreparing(context.Background(), bB))

	mgrC := reg.Ensure(shardC)
	bC := testBuilder(1, 1, 1)
	bC.SetAssignments(map[int64]map[int64][]int64{
		1: {10: {103}},
		2: {10: {104}},
	})
	require.NoError(t, mgrC.AddPreparing(context.Background(), bC))

	snapshot := reg.Snapshot()
	stats := snapshot.StatsMap()
	assert.Equal(t, map[int64]SegmentState{1: SegmentStatePreparing}, stats[shardA].Segments[101].Nodes)
	assert.Equal(t, map[int64]SegmentState{2: SegmentStatePreparing}, stats[shardB].Segments[102].Nodes)
	assert.Equal(t, map[int64]SegmentState{1: SegmentStatePreparing}, stats[shardC].Segments[103].Nodes)
	assert.Equal(t, map[int64]SegmentState{2: SegmentStatePreparing}, stats[shardC].Segments[104].Nodes)
}

func shardStatsForNodes(segmentNodes map[int64][]int64) *ShardStats {
	stats := emptyShardStats()
	for segmentID, nodeIDs := range segmentNodes {
		nodes := make(map[int64]SegmentState, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			nodes[nodeID] = SegmentStatePreparing
		}
		stats.Segments[segmentID] = &SegmentStats{
			SegmentID: segmentID,
			Nodes:     nodes,
		}
	}
	return stats
}
