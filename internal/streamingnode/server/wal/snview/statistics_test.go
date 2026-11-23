package snview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStatisticsGrowingHandlesUseSelectedDataVersion(t *testing.T) {
	paramtable.Init()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: 1, Schema: &schemapb.CollectionSchema{Name: "statistics", Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
	})
	require.NoError(t, err)
	defer collection.Release()
	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: collection, SegmentID: 1000, SegmentType: segcore.SegmentTypeGrowing})
	require.NoError(t, err)
	defer segment.Release()
	mockey.PatchConvey("retained growing handles and frontier belong to the chosen View", t, func() {
		handle := &mockGrowingSegmentHandle{id: 1000, partitionID: 10}
		runtime := &mockQueryRuntime{handles: []GrowingSegmentHandle{handle}}
		version := qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 8}, QueryVersion: 9}
		released := 0
		lease := &QueryViewLease{Meta: &viewpb.QueryViewMeta{LoadInfoVersion: 12}, Release: func() { released++ }}
		mockey.Mock((*SNQueryViewHandler).acquireUpView).Return(lease, nil).Build()
		mockey.Mock((*SNQueryViewHandler).queryRuntime).Return(runtime, nil).Build()
		mockey.Mock((*mockQueryRuntime).ResolvePartitions).To(func(_ *mockQueryRuntime, load uint64, partitions []int64) ([]int64, error) {
			require.EqualValues(t, 12, load)
			require.Equal(t, []int64{10}, partitions)
			return partitions, nil
		}).Build()
		mockey.Mock((*mockGrowingSegmentHandle).Segment).Return(segment).Build()
		mockey.Mock(mockey.GetMethod(segment, "RowNum")).To(func() int64 {
			require.Zero(t, released)
			require.Zero(t, handle.releaseCount)
			return 7
		}).Build()
		h := &SNQueryViewHandler{}
		req := &viewpb.StatisticsOnViewRequest{ShardId: &viewpb.ShardID{Vchannel: "p_1v0", ReplicaId: 3}, Version: version.IntoProto(), Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}, PartitionIds: []int64{10}}
		rows, err := h.Statistics(t.Context(), req)
		require.NoError(t, err)
		require.EqualValues(t, 7, rows)
		require.Equal(t, version.DataVersion, runtime.dataVersion)
		require.EqualValues(t, 100, runtime.growingTimetick)
		require.EqualValues(t, 90, runtime.transformingTimetick)
		require.Equal(t, 1, handle.releaseCount)
		require.Equal(t, 1, released)

		runtime.waitErr = context.DeadlineExceeded
		_, err = h.Statistics(t.Context(), req)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, handle.releaseCount)
		require.Equal(t, 2, released)
	})
}
