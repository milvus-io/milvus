package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// --- fake providers used throughout the tests ---

type fakeNodeProvider struct {
	infos     map[int64]*NodeInfo
	notifiers []func()
}

func (f *fakeNodeProvider) Snapshot() *NodeSnapshot {
	return NewNodeSnapshot(1, f.infos)
}

func (f *fakeNodeProvider) RegisterNodeChangedNotifier(notifier func()) {
	if notifier != nil {
		f.notifiers = append(f.notifiers, notifier)
	}
}

func (f *fakeNodeProvider) notifyNodeChanged() {
	for _, notifier := range f.notifiers {
		notifier()
	}
}

type fakeDataViewProvider struct {
	collections []*viewpb.DataViewOfCollection
	segments    map[int64]*SegmentInfo
}

func (f *fakeDataViewProvider) DataViewSnapshot(context.Context) *DataViewSnapshot {
	return NewDataViewSnapshot(1, f.collections, newMapSegmentSnapshot(f.segments))
}

// --- fake catalog/syncer for the registry and store ---

type stubCatalog struct{}

func (s *stubCatalog) ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return nil, nil
}

func (s *stubCatalog) SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	return nil
}

type stubSyncer struct{}

func (s *stubSyncer) SyncViews(ctx context.Context, group syncer.SyncGroup) error { return nil }
func (s *stubSyncer) Close() error                                                { return nil }

// --- test helpers ---

// emptyLoadConfigStore returns a fresh store after a clean Recover.
func emptyLoadConfigStore(t *testing.T) *loadmgr.LoadConfigStore {
	t.Helper()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()
	store, err := loadmgr.RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	return store
}

// emptyRegistry returns a fresh ShardViewRegistry backed by stub catalog/syncer.
func emptyRegistry(t *testing.T) *coordview.ShardViewRegistry {
	t.Helper()
	reg, err := coordview.RecoverShardViewRegistry(context.Background(), &stubCatalog{}, &stubSyncer{})
	require.NoError(t, err)
	return reg
}

// storeWithConfig returns a LoadConfigStore seeded via Recover with one
// collection + one replica + given partitions.
func storeWithConfig(t *testing.T, collectionID, replicaID int64, partitions []int64, nodes []int64) *loadmgr.LoadConfigStore {
	t.Helper()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{
		{CollectionID: collectionID},
	}, nil).Once()
	parts := make([]*querypb.PartitionLoadInfo, 0, len(partitions))
	for _, pid := range partitions {
		parts = append(parts, &querypb.PartitionLoadInfo{CollectionID: collectionID, PartitionID: pid})
	}
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{collectionID: parts}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{
		{ID: replicaID, CollectionID: collectionID, Nodes: nodes},
	}, nil).Once()
	store, err := loadmgr.RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	return store
}

// addShardWithPreparingView inserts a shard in the registry and gives it a
// single Preparing view with the specified placements.
func addShardWithPreparingView(
	t *testing.T,
	reg *coordview.ShardViewRegistry,
	shardID qviews.ShardID,
	assignments map[int64]map[int64][]int64, // nodeID -> partitionID -> segIDs
) {
	t.Helper()
	mgr := reg.Ensure(shardID)
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 1,
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: shardID.VChannel}},
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
	}
	b := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, dataView, shardID.VChannel)
	b.SetAssignments(assignments)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))
}

// --- actual tests ---

func TestSnapshotBuilder_EmptyInputs(t *testing.T) {
	store := emptyLoadConfigStore(t)
	reg := emptyRegistry(t)

	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{}},
		&fakeDataViewProvider{},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())
	require.NotNil(t, snap)
	assert.Empty(t, snap.ConfigsMap())
	assert.Empty(t, snap.ShardStatsMap())
	require.NotNil(t, snap.DataViewSnapshot)
	assert.Equal(t, uint64(1), snap.DataViewSnapshot.Version())
	assert.Empty(t, snap.Nodes)
	assert.NotNil(t, snap.Config)
}

func TestSnapshotBuilder_NodeInfosCopied(t *testing.T) {
	store := emptyLoadConfigStore(t)
	reg := emptyRegistry(t)

	nodes := map[int64]*NodeInfo{
		1: {NodeID: 1, Alive: true, ResourceGroup: "default"},
		2: {NodeID: 2, Alive: false, Stopping: true},
	}

	builder := NewSnapshotBuilder(
		store, reg,
		&fakeNodeProvider{infos: nodes},
		&fakeDataViewProvider{},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())
	require.Len(t, snap.Nodes, 2)

	n1 := snap.Nodes[1]
	require.NotNil(t, n1)
	assert.True(t, n1.Alive)
	assert.Equal(t, "default", n1.ResourceGroup)
	assert.Equal(t, int64(0), n1.UpRowCount, "no shards → zero aggregate")

	n2 := snap.Nodes[2]
	require.NotNil(t, n2)
	assert.True(t, n2.Stopping)
}

func TestSnapshotBuilder_AggregatePerNodeRowLoad(t *testing.T) {
	store := emptyLoadConfigStore(t)
	reg := emptyRegistry(t)

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	shardB := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}

	// shardA: seg 101 on node 1, seg 102 on node 2 (both Preparing).
	addShardWithPreparingView(t, reg, shardA, map[int64]map[int64][]int64{
		1: {10: {101}},
		2: {10: {102}},
	})
	// shardB: seg 201 on node 1 (Preparing).
	addShardWithPreparingView(t, reg, shardB, map[int64]map[int64][]int64{
		1: {10: {201}},
	})

	segInfos := map[int64]*SegmentInfo{
		101: {SegmentID: 101, MemSize: 100, RowNum: 10},
		102: {SegmentID: 102, MemSize: 200, RowNum: 20},
		201: {SegmentID: 201, MemSize: 50, RowNum: 5},
	}

	nodes := map[int64]*NodeInfo{
		1: {NodeID: 1, Alive: true},
		2: {NodeID: 2, Alive: true},
	}

	builder := NewSnapshotBuilder(
		store, reg,
		&fakeNodeProvider{infos: nodes},
		&fakeDataViewProvider{segments: segInfos},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())
	require.Len(t, snap.Nodes, 2)

	n1 := snap.Nodes[1]
	// Both placements on node 1 are Preparing (no Up view).
	assert.Equal(t, int64(0), n1.UpRowCount)
	assert.Equal(t, int64(10+5), n1.PendingRowCount)

	n2 := snap.Nodes[2]
	assert.Equal(t, int64(0), n2.UpRowCount)
	assert.Equal(t, int64(20), n2.PendingRowCount)
}

func TestSnapshotBuilder_AggregatePerNodeRowCount(t *testing.T) {
	store := emptyLoadConfigStore(t)
	reg := emptyRegistry(t)

	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	addShardWithPreparingView(t, reg, shardID, map[int64]map[int64][]int64{
		1: {10: {101}},
	})

	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
		}},
		&fakeDataViewProvider{segments: map[int64]*SegmentInfo{
			101: {SegmentID: 101, MemSize: 1_000_000, RowNum: 10},
		}},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())
	assert.Equal(t, int64(10), snap.Nodes[1].PendingRowCount)
}

func TestSnapshotBuilder_CollectsSegmentsFromDataViewsAndPlacements(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardA := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{10}, []int64{1})
	reg := emptyRegistry(t)

	// Shard placement references segments 101, 102.
	addShardWithPreparingView(t, reg, shardA, map[int64]map[int64][]int64{
		1: {10: {101, 102}},
	})

	// Collection DataView references additional segment 103.
	shardDV := &viewpb.DataViewOfShard{
		Vchannel: shardA.VChannel,
		Partitions: []*viewpb.DataViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{101, 102, 103}},
		},
	}
	collDV := &viewpb.DataViewOfCollection{
		CollectionId: collID,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
		Shards:       []*viewpb.DataViewOfShard{shardDV},
	}

	segInfos := map[int64]*SegmentInfo{
		101: {SegmentID: 101, MemSize: 100, RowNum: 10},
		102: {SegmentID: 102, MemSize: 200, RowNum: 20},
		103: {SegmentID: 103, MemSize: 300, RowNum: 30},
	}

	builder := NewSnapshotBuilder(
		store, reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{1: {NodeID: 1, Alive: true}}},
		&fakeDataViewProvider{collections: []*viewpb.DataViewOfCollection{collDV}, segments: segInfos},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())

	// Segment metadata stays behind the DataView snapshot lookup; Build no
	// longer materializes a segment map.
	info, ok := snap.SegmentInfo(103)
	require.True(t, ok)
	assert.Equal(t, int64(300), info.MemSize)
	assert.Equal(t, int64(30), info.RowNum)

	// DataView stays owned by DataViewSnapshot and is exposed through lookup.
	assert.Same(t, shardDV, snap.DataViewForShard(shardA))

	// DataVersion stays owned by DataViewSnapshot and is exposed through lookup.
	dv, ok := snap.DataVersionForCollection(collID)
	require.True(t, ok)
	assert.Equal(t, int64(1), dv.StreamingVersion)
	assert.Equal(t, int64(1), dv.CompactVersion)
}

func TestSnapshotBuilder_UnknownNodeInPlacementIsSkipped(t *testing.T) {
	store := emptyLoadConfigStore(t)
	reg := emptyRegistry(t)

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	// Node 99 is in placement but NOT in NodeProvider's map (simulates a node
	// that was just removed).
	addShardWithPreparingView(t, reg, shardA, map[int64]map[int64][]int64{
		99: {10: {101}},
	})

	builder := NewSnapshotBuilder(
		store, reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{}}, // no node 99
		&fakeDataViewProvider{segments: map[int64]*SegmentInfo{101: {SegmentID: 101, RowNum: 10}}},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())
	// Node 99 does not appear in snap.Nodes; Balancer's Phase 1 will flag
	// current view as referencing an unavailable node.
	_, ok := snap.Nodes[99]
	assert.False(t, ok)
}

func TestSnapshotBuilder_MissingSegmentInfoContributesZero(t *testing.T) {
	store := emptyLoadConfigStore(t)
	reg := emptyRegistry(t)

	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	addShardWithPreparingView(t, reg, shardA, map[int64]map[int64][]int64{
		1: {10: {101, 102}},
	})

	builder := NewSnapshotBuilder(
		store, reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{1: {NodeID: 1, Alive: true}}},
		// Segment 102's info is missing.
		&fakeDataViewProvider{segments: map[int64]*SegmentInfo{101: {SegmentID: 101, RowNum: 10}}},
		&BalanceConfig{},
	)

	snap := builder.Build(context.Background())
	n1 := snap.Nodes[1]
	// Only segment 101 contributes its RowNum; segment 102 contributes 0.
	assert.Equal(t, int64(10), n1.PendingRowCount)
}

func TestAggregateNodeLoad_SkipsUnrecoverableLoad(t *testing.T) {
	nodes := map[int64]*BalanceNode{
		1: {NodeID: 1},
		2: {NodeID: 2},
	}
	snap := &BalancerSnapshot{
		DataViewSnapshot: NewDataViewSnapshot(1, nil, newMapSegmentSnapshot(map[int64]*SegmentInfo{
			101: {SegmentID: 101, RowNum: 100},
			102: {SegmentID: 102, RowNum: 200},
		})),
	}
	stats := map[qviews.ShardID]*coordview.ShardStats{
		{ReplicaID: 1, VChannel: "v0"}: {
			Segments: map[int64]*coordview.SegmentStats{
				101: {
					SegmentID:   101,
					PartitionID: 10,
					Nodes:       map[int64]coordview.SegmentState{1: coordview.SegmentStateUnrecoverable},
				},
				102: {
					SegmentID:   102,
					PartitionID: 10,
					Nodes:       map[int64]coordview.SegmentState{2: coordview.SegmentStateReady},
				},
			},
		},
	}

	aggregateNodeLoad(nodes, stats, snap)

	assert.Zero(t, nodes[1].PendingRowCount)
	assert.Equal(t, int64(200), nodes[2].PendingRowCount)
}
