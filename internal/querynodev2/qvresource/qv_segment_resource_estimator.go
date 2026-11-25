package qvresource

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type qvSegmentResourceLoader interface {
	ReserveLoadResourceWithStatus(ctx context.Context, schema *schemapb.CollectionSchema, infos ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, segments.LoadResource, error)
}

type queryViewSegmentResourceEstimator struct {
	loader qvSegmentResourceLoader
}

func newQueryViewSegmentResourceEstimator(loader qvSegmentResourceLoader) *queryViewSegmentResourceEstimator {
	return &queryViewSegmentResourceEstimator{loader: loader}
}

func (e *queryViewSegmentResourceEstimator) Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection qnview.CollectionRuntime) (qnview.ResourceReservation, bool, error) {
	reservation, competing, err := e.loader.ReserveLoadResourceWithStatus(ctx, collection.Schema(), info)
	return reservation, competing.MemorySize > 0 || competing.DiskSize > 0, err
}
