package queryclient

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Observe at the RPC boundary, before reduction, optimization or rendering.
// Retained requery also uses this wrapper but never changes its pinned plan.
type circuitBreakingViewClient struct {
	owner *shardViewQueryClient
	inner ViewQueryServiceClient
}

func observeViewRPC[T any](ctx context.Context, c *circuitBreakingViewClient, node qviews.WorkNode, shard *viewpb.ShardID, version *viewpb.QueryViewVersion, call func() (T, error)) (T, error) {
	b := c.owner.breaker
	token := b.observeStart(ctx, node)
	// Retained calls have no planning attempt, but must still fence stale TTL
	// observations by the time execution actually began.
	if token.attempt == nil {
		token.attempt = &routingAttempt{started: time.Now(), failedNodes: make(map[int64]time.Time), failedViews: make(map[qviews.QueryViewKey]time.Time)}
	}
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
	b.observe(ctx, node, key, token, observed)
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
