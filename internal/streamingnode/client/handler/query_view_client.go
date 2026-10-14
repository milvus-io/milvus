package handler

import (
	"context"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// QueryViewClient is the QueryView domain client under HandlerClient.
type QueryViewClient interface {
	// GetQueryPlan generates a shard-level query plan from the StreamingNode owning the shard pchannel.
	GetQueryPlan(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)

	// GetMVCCTimestamp returns query-plan MVCC frontiers from the StreamingNode owning the shard pchannel.
	GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)

	// SearchOnView executes a QueryView search on the StreamingNode owning the pchannel.
	SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)

	// QueryOnView executes a QueryView retrieve on the StreamingNode owning the pchannel.
	QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)

	// RequeryOnView fetches fields from the StreamingNode owning the pchannel for a previous QueryView plan.
	RequeryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
}

type queryViewClient struct {
	owner            *handlerClientImpl
	queryPlanService lazygrpc.Service[viewpb.QueryPlanServiceClient]
	viewQueryService lazygrpc.Service[viewpb.ViewQueryServiceClient]
}

type viewRPCResult[T any] struct {
	resp T
	err  error
}

func newQueryViewClient(owner *handlerClientImpl, conn lazygrpc.Conn) QueryViewClient {
	return &queryViewClient{
		owner:            owner,
		queryPlanService: lazygrpc.WithServiceCreator(conn, viewpb.NewQueryPlanServiceClient),
		viewQueryService: lazygrpc.WithServiceCreator(conn, viewpb.NewViewQueryServiceClient),
	}
}

func (hc *handlerClientImpl) QueryViewClient() QueryViewClient {
	return hc.queryViewClient
}

func (qvc *queryViewClient) GetQueryPlan(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	result, err := executeQueryPlanRPC(ctx, qvc, shardID.VChannel, "QueryPlanService.GetQueryPlan", func(ctx context.Context, client viewpb.QueryPlanServiceClient) (*viewpb.GetQueryPlanResponse, error) {
		return client.GetQueryPlan(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	result, err := executeQueryPlanRPC(ctx, qvc, shardID.VChannel, "QueryPlanService.GetMVCCTimestamp", func(ctx context.Context, client viewpb.QueryPlanServiceClient) (*viewpb.GetMVCCTimestampResponse, error) {
		return client.GetMVCCTimestamp(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, "ViewQueryService.SearchOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.SearchOnViewResponse, error) {
		return client.SearchOnView(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, "ViewQueryService.QueryOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.QueryOnViewResponse, error) {
		return client.QueryOnView(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) RequeryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, "ViewQueryService.RequeryOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.RequeryOnViewResponse, error) {
		return client.RequeryOnView(ctx, req)
	})
	return result, err
}

func executeQueryPlanRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	vchannel string,
	method string,
	call func(context.Context, viewpb.QueryPlanServiceClient) (T, error),
) (T, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", method))
	return executeViewRPC(ctx, qvc, logger, pchannel, func(ctx context.Context, _ *types.PChannelInfoAssigned) (viewRPCResult[T], error) {
		client, err := qvc.queryPlanService.GetService(ctx)
		if err != nil {
			return viewRPCResult[T]{}, err
		}
		resp, err := call(ctx, client)
		return viewRPCResult[T]{resp: resp, err: viewerror.ConvertViewError(method, err)}, nil
	})
}

func executeViewQueryRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	pchannel types.PChannelInfo,
	method string,
	call func(context.Context, viewpb.ViewQueryServiceClient) (T, error),
) (T, error) {
	logger := mlog.With(mlog.FieldPChannel(pchannel.Name), mlog.String("handler", method))
	return executeViewRPC(ctx, qvc, logger, pchannel.Name, func(ctx context.Context, _ *types.PChannelInfoAssigned) (viewRPCResult[T], error) {
		client, err := qvc.viewQueryService.GetService(ctx)
		if err != nil {
			return viewRPCResult[T]{}, err
		}
		resp, err := call(ctx, client)
		return viewRPCResult[T]{resp: resp, err: viewerror.ConvertViewError(method, err)}, nil
	})
}

func executeViewRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	logger *mlog.Logger,
	pchannel string,
	call func(context.Context, *types.PChannelInfoAssigned) (viewRPCResult[T], error),
) (T, error) {
	var zero T
	if !qvc.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		return zero, ErrClientClosed
	}
	defer qvc.owner.lifetime.Done()
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	// Wait for readiness through the WAL handler's assignment watch and backoff.
	// A successful connection does not imply that its assigned WAL is still ready.
	result, err := qvc.owner.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		rpcCtx := worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, assign.Channel)
		rpcResult, err := call(rpcCtx, assign)
		if err != nil {
			return nil, err
		}
		if queryViewRPCNeedsWaitForReady(rpcResult.err) {
			return nil, rpcResult.err
		}
		// View invalidation and request errors belong to the query orchestrator.
		// In particular, a pinned Phase 2 request must not wait for a retired view.
		return rpcResult, nil
	})
	if err != nil {
		return zero, err
	}
	rpcResult := result.(viewRPCResult[T])
	return rpcResult.resp, rpcResult.err
}

func queryViewRPCNeedsWaitForReady(err error) bool {
	if err == nil {
		return false
	}
	viewErr := viewerror.AsViewError(err)
	return viewErr.IsOnShutdown() || viewErr.IsNotPrimary() || grpcstatus.Code(err) == codes.Unavailable
}
