package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type queryViewSegmentResourceEstimator struct{ budget *segments.LoadResourceBudget }

func newQueryViewSegmentResourceEstimator(budget *segments.LoadResourceBudget) *queryViewSegmentResourceEstimator {
	return &queryViewSegmentResourceEstimator{budget: budget}
}

func (e *queryViewSegmentResourceEstimator) Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection qnview.CollectionRuntime) (qnview.ResourceReservation, error) {
	usage, err := loadresource.EstimateSegmentLoadingResource(ctx, collection.Schema(), info, loadresource.DefaultSegmentLoadingEstimateOptions(), func(fn func() error) error { return fn() })
	if err != nil {
		return nil, err
	}
	return e.budget.Reserve(ctx, usage)
}
