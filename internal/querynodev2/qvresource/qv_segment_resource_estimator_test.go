package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResourceEstimatorUsesPinnedSchema(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Version: 17}
	info := &querypb.SegmentLoadInfo{SegmentID: 10}
	usage := loadresource.SegmentResourceUsage{MemoryBytes: 123, DiskBytes: 45}
	patchCollectionLifetime(t, mockey.Mock(loadresource.EstimateSegmentLoadingResource).To(func(ctx context.Context, s *schemapb.CollectionSchema, i *querypb.SegmentLoadInfo, o loadresource.SegmentLoadingEstimateOptions, r loadresource.Runner) (loadresource.SegmentResourceUsage, error) {
		require.Same(t, schema, s)
		require.Same(t, info, i)
		return usage, nil
	}).Build())
	reservation := &segments.LoadResourceReservation{}
	patchCollectionLifetime(t, mockey.Mock((*segments.LoadResourceBudget).Reserve).To(func(b *segments.LoadResourceBudget, ctx context.Context, u loadresource.SegmentResourceUsage) (*segments.LoadResourceReservation, error) {
		require.Equal(t, usage, u)
		return reservation, nil
	}).Build())
	got, err := newQueryViewSegmentResourceEstimator(&segments.LoadResourceBudget{}).Reserve(context.Background(), info, fakeQVCollectionRuntime{schema: schema})
	require.NoError(t, err)
	require.Same(t, reservation, got)
}

func TestResourceEstimatorFailureDoesNotReserve(t *testing.T) {
	paramtable.Init()
	expected := merr.WrapErrServiceInternalMsg("estimate failed")
	patchCollectionLifetime(t, mockey.Mock(loadresource.EstimateSegmentLoadingResource).Return(loadresource.SegmentResourceUsage{}, expected).Build())
	reservation := mockey.Mock((*segments.LoadResourceBudget).Reserve).Return(nil, nil).Build()
	patchCollectionLifetime(t, reservation)
	_, err := newQueryViewSegmentResourceEstimator(&segments.LoadResourceBudget{}).Reserve(context.Background(), &querypb.SegmentLoadInfo{}, fakeQVCollectionRuntime{})
	require.ErrorIs(t, err, expected)
	require.Zero(t, reservation.Times())
}
