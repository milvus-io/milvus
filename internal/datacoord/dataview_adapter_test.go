package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestServerSnapshotDelegatesToDataViewManager(t *testing.T) {
	manager := &fakeGCDataViewManager{
		snapshotViews: []*viewpb.DataViewOfCollection{
			{CollectionId: 10, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
		},
	}
	server := &Server{dataViewManager: manager}

	views, err := server.Snapshot(context.Background(), []int64{10})

	require.NoError(t, err)
	require.Equal(t, []int64{10}, manager.snapshotRequested)
	require.Equal(t, manager.snapshotViews, views)
}

func TestServerSnapshotReturnsEmptyWithoutDataViewManager(t *testing.T) {
	server := &Server{}

	views, err := server.Snapshot(context.Background(), []int64{10})

	require.NoError(t, err)
	require.Nil(t, views)
}

func TestDataViewSegmentStoreSelectSegmentsSkipsDroppedPartition(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewSegmentsInfo(),
	}
	m.collections.Insert(1, &collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	m.segments.SetSegment(100, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	}))
	m.segments.SetSegment(101, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	}))

	store := &dataViewSegmentStore{meta: m}
	segments := store.SelectSegments(context.Background(), 1)

	require.Len(t, segments, 1)
	require.Equal(t, int64(100), segments[0].GetID())
}

func TestDataViewRecoveryUsesCollectionPartitions(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.AddCollection(&collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	})))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	})))
	manager := newDataViewManager(m.catalog, m)

	require.NoError(t, manager.RecoverCollection(ctx, 1))
	view, err := manager.LatestVisibleDataView(ctx, 1)

	require.NoError(t, err)
	require.NotNil(t, view)
	require.Len(t, view.GetShards(), 1)
	require.Len(t, view.GetShards()[0].GetPartitions(), 1)
	require.Equal(t, int64(10), view.GetShards()[0].GetPartitions()[0].GetPartitionId())
	require.Equal(t, []int64{100}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestGetCollectionIDsByPartitionUsesSegmentMeta(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewSegmentsInfo(),
	}
	m.collections.Insert(1, &collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	m.segments.SetSegment(100, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	}))

	collectionIDs := m.GetCollectionIDsByPartition(context.Background(), []int64{11})

	require.Equal(t, []int64{1}, collectionIDs)
}
