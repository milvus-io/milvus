package coordview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// newTestRegistry builds a fresh (empty) registry via the recovery path with
// an empty catalog.
func newTestRegistry(t *testing.T, catalog *mockCatalog, s *mockSyncer) *ShardViewRegistry {
	t.Helper()
	reg, err := RecoverShardViewRegistry(context.Background(), catalog, s)
	require.NoError(t, err)
	return reg
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

func TestRegistry_RecoverWithPersistedViews(t *testing.T) {
	catalog := newMockCatalog()

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0_c0"}
	shardB := qviews.ShardID{ReplicaID: 2, VChannel: "v0_c1"}

	viewA := buildTestViewWithVersion(1, 1, 1, 1)
	viewA.Meta.ReplicaId = shardA.ReplicaID
	viewA.Meta.Vchannel = shardA.VChannel

	viewB := buildTestViewWithVersion(2, 1, 1, 1)
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

	assert.Len(t, reg.Snapshot().StatsMap(), 2)

	mgrA := reg.Get(shardA)
	require.NotNil(t, mgrA)

	mgrB := reg.Get(shardB)
	require.NotNil(t, mgrB)
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
