package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBalancer_ReconcileDirtyShardAppliesPrepare(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1, 2})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)

	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
			2: {NodeID: 2, Alive: true},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards: []*viewpb.DataViewOfShard{
					shardDataView(shardID.VChannel, 100, 101, 102),
				},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 600},
				102: {SegmentID: 102, PartitionID: 100, RowNum: 200},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats)
	assert.NotNil(t, stats.PreparingVersion)
	assert.NotEmpty(t, stats.Segments)
}

func TestBalancer_ReconcileDirtyCollectionExpandsTrackedShards(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger(TriggerScope{DirtyCollections: []int64{collID}})
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	assert.NotNil(t, stats.PreparingVersion)
}

func TestBalancer_NodeChangedNotifierTriggersFullScan(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	nodeProvider := &fakeNodeProvider{infos: map[int64]*NodeInfo{
		1: {NodeID: 1, Alive: true},
	}}
	builder := NewSnapshotBuilder(
		store,
		reg,
		nodeProvider,
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	nodeProvider.notifyNodeChanged()
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	assert.NotNil(t, stats.PreparingVersion)
}

func TestBalancer_ReconcileFullScanDoesNotRestackPreparing(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))
	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats.PreparingVersion)
	before := stats.Segments

	b.Trigger()
	require.NoError(t, b.Reconcile(context.Background()))
	after := reg.Get(shardID).Stats().Segments
	assert.Equal(t, before, after)
}

func TestBalancer_StartStop(t *testing.T) {
	reg := emptyRegistry(t)
	b := NewDefaultBalancer(nil, reg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	b.Trigger()
	time.Sleep(10 * time.Millisecond)
	b.Stop()
	b.Stop()
}
