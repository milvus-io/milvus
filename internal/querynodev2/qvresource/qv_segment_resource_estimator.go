package qvresource

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type qvSegmentResourceLoader interface {
	ReserveLoadResourceWithSchema(ctx context.Context, schema *schemapb.CollectionSchema, infos ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, error)
}

type queryViewSegmentResourceEstimator struct {
	loader qvSegmentResourceLoader
}

func newQueryViewSegmentResourceEstimator(loader qvSegmentResourceLoader) *queryViewSegmentResourceEstimator {
	return &queryViewSegmentResourceEstimator{loader: loader}
}

func (e *queryViewSegmentResourceEstimator) Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection qnview.CollectionRuntime) (qnview.ResourceReservation, error) {
	return e.loader.ReserveLoadResourceWithSchema(ctx, collection.Schema(), info)
}
