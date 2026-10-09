package provider

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type QueryPlanProvider interface {
	GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlan, error)
	GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}
