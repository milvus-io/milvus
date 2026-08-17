package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type queryViewSegmentResourceEstimator struct {
	loader queryViewResourceLoader
}

type queryViewResourceLoader interface {
	ReserveLoadResource(ctx context.Context, infos ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, error)
}

func NewQueryViewSegmentResourceEstimator(loader segments.QueryViewLoader) qnview.SegmentResourceEstimator {
	return newQueryViewSegmentResourceEstimator(loader)
}

func newQueryViewSegmentResourceEstimator(loader queryViewResourceLoader) *queryViewSegmentResourceEstimator {
	return &queryViewSegmentResourceEstimator{loader: loader}
}

func (e *queryViewSegmentResourceEstimator) Reserve(
	ctx context.Context,
	info *querypb.SegmentLoadInfo,
	_ qnview.CollectionRuntime,
) (qnview.ResourceReservation, error) {
	return e.loader.ReserveLoadResource(ctx, info)
}
