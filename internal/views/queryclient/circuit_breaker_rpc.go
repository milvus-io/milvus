package queryclient

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Observe QueryNode RPCs before reduction, optimization or rendering.
// Retained requery also uses this wrapper but never changes its pinned plan.
type circuitBreakingViewClient struct {
	owner *shardViewQueryClient
	inner ViewQueryServiceClient
}

func observeViewRPC[T any](ctx context.Context, c *circuitBreakingViewClient, node qviews.WorkNode, shard *viewpb.ShardID, version *viewpb.QueryViewVersion, call func() (T, error)) (T, error) {
	qn, ok := node.(qviews.QueryNode)
	if !ok {
		// StreamingNode ownership/readiness is handled by its WAL client. Its
		// handoff errors do not invalidate a View on the new owner.
		return call()
	}
	b := c.owner.breaker
	started := time.Now()
	resp, err := call()
	observed := err
	if observed == nil {
		switch result := any(resp).(type) {
		case *viewpb.SearchOnViewResponse:
			if result.GetLegacyResults() != nil {
				observed = merr.Error(result.GetLegacyResults().GetStatus())
			}
		case *viewpb.QueryOnViewResponse:
			if result.GetLegacyResults() != nil {
				observed = merr.Error(result.GetLegacyResults().GetStatus())
			}
		}
	}
	key := qviews.QueryViewKey{ShardID: qviews.FromProtoShardID(shard), QueryViewVersion: qviews.FromProtoQueryViewVersion(version)}
	b.observe(ctx, qn, key, started, observed)
	return resp, err
}

func (c *circuitBreakingViewClient) SearchOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return observeViewRPC(ctx, c, node, req.ShardId, req.Version, func() (*viewpb.SearchOnViewResponse, error) { return c.inner.SearchOnView(ctx, node, req) })
}

func (c *circuitBreakingViewClient) QueryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return observeViewRPC(ctx, c, node, req.ShardId, req.Version, func() (*viewpb.QueryOnViewResponse, error) { return c.inner.QueryOnView(ctx, node, req) })
}

func (c *circuitBreakingViewClient) StatisticsOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
	return observeViewRPC(ctx, c, node, req.ShardId, req.Version, func() (*viewpb.StatisticsOnViewResponse, error) { return c.inner.StatisticsOnView(ctx, node, req) })
}
