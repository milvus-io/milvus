package queryclient

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestCircuitBreakingRPCObservesOnlyQueryNodes(t *testing.T) {
	for _, method := range []string{"search", "query", "statistics"} {
		for _, node := range []qviews.WorkNode{qviews.NewQueryNode(11), qviews.NewStreamingNodeFromVChannel("p_1v0")} {
			for _, scenario := range []struct {
				name string
				err  error
			}{
				{"not-found", viewerror.NewViewNotFound("missing")},
				{"invalidated", viewerror.NewViewInvalidated("closed")},
				{"connection", io.EOF},
				{"success", nil},
			} {
				t.Run(method+"/"+node.String()+"/"+scenario.name, func(t *testing.T) {
					b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{NodeFailureThreshold: 1})
					key := breakerTestKey("p_1v0", 1, 1, 1)
					ctx, attempt := beginBreakerTest(t, b, key.ShardID.VChannel)
					client := &circuitBreakingViewClient{owner: &shardViewQueryClient{breaker: b}, inner: &fakeViewQueryServiceClient{}}
					var err error
					switch method {
					case "search":
						response := &viewpb.SearchOnViewResponse{}
						patch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).Return(response, scenario.err).Build()
						defer patch.UnPatch()
						var got *viewpb.SearchOnViewResponse
						got, err = client.SearchOnView(ctx, node, &viewpb.SearchOnViewRequest{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()})
						require.Same(t, response, got)
						require.Equal(t, 1, patch.Times())
					case "query":
						response := &viewpb.QueryOnViewResponse{}
						patch := mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).Return(response, scenario.err).Build()
						defer patch.UnPatch()
						var got *viewpb.QueryOnViewResponse
						got, err = client.QueryOnView(ctx, node, &viewpb.QueryOnViewRequest{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()})
						require.Same(t, response, got)
						require.Equal(t, 1, patch.Times())
					case "statistics":
						response := &viewpb.StatisticsOnViewResponse{}
						patch := mockey.Mock((*fakeViewQueryServiceClient).StatisticsOnView).Return(response, scenario.err).Build()
						defer patch.UnPatch()
						var got *viewpb.StatisticsOnViewResponse
						got, err = client.StatisticsOnView(ctx, node, &viewpb.StatisticsOnViewRequest{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()})
						require.Same(t, response, got)
						require.Equal(t, 1, patch.Times())
					}
					require.ErrorIs(t, err, scenario.err)
					if node.NodeType() == qviews.NodeTypeQueryNode && scenario.err != nil {
						if scenario.err == io.EOF {
							require.Contains(t, b.nodes, int64(11))
							require.Contains(t, attempt.failedNodes, int64(11))
							require.Empty(t, b.views)
						} else {
							require.Contains(t, b.views[key.ShardID.VChannel], key)
							require.Contains(t, attempt.failedViews, key)
							require.Empty(t, b.nodes)
						}
					} else {
						require.Empty(t, b.nodes)
						require.Empty(t, b.views)
						require.Empty(t, attempt.failedNodes)
						require.Empty(t, attempt.failedViews)
					}
				})
			}
		}
	}
}

func TestRetainedStreamingNodeRPCDoesNotCreateViewExclusion(t *testing.T) {
	b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
	client := &circuitBreakingViewClient{owner: &shardViewQueryClient{breaker: b}}
	key := breakerTestKey("p_1v0", 1, 1, 1)
	failure := viewerror.NewViewInvalidated("old WAL owner closed")
	_, err := observeViewRPC(context.Background(), client, qviews.NewStreamingNodeFromVChannel(key.ShardID.VChannel), key.ShardID.IntoProto(), key.QueryViewVersion.IntoProto(), func() (int, error) { return 0, failure })
	require.ErrorIs(t, err, failure)
	_, next := beginBreakerTest(t, b, key.ShardID.VChannel)
	require.Empty(t, next.constraints.ExcludedQueryViews)
	require.Empty(t, next.constraints.ExcludedQueryNodeIds)
}
