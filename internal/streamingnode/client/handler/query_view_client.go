package handler

import (
	"context"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/reduce"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
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
	GetQueryPlan(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, *types.PChannelInfoAssigned, error)

	// GetMVCCTimestamp returns query-plan MVCC frontiers from the StreamingNode owning the shard pchannel.
	GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)

	// SearchOnView executes a QueryView search on the StreamingNode owning the pchannel.
	SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	SearchOnViewStream(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error)

	// QueryOnView executes a QueryView retrieve on the StreamingNode owning the pchannel.
	QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	QueryOnViewStream(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error)
}

type queryViewClient struct {
	owner            *handlerClientImpl
	queryPlanService lazygrpc.Service[viewpb.QueryPlanServiceClient]
	viewQueryService lazygrpc.Service[viewpb.ViewQueryServiceClient]
}

type viewRPCResult[T any] struct {
	source *types.PChannelInfoAssigned
	resp   T
	err    error
}

func newQueryViewClient(owner *handlerClientImpl, conn lazygrpc.Conn) QueryViewClient {
	client := &queryViewClient{
		owner:            owner,
		queryPlanService: lazygrpc.WithServiceCreator(conn, viewpb.NewQueryPlanServiceClient),
		viewQueryService: lazygrpc.WithServiceCreator(conn, viewpb.NewViewQueryServiceClient),
	}
	return client
}

func (hc *handlerClientImpl) QueryViewClient() QueryViewClient {
	return hc.queryViewClient
}

func (qvc *queryViewClient) GetQueryPlan(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, *types.PChannelInfoAssigned, error) {
	level, samples := req.GetMvcc().(*viewpb.GetQueryPlanRequest_ConsistencyLevel)
	strong := samples && level.ConsistencyLevel == commonpb.ConsistencyLevel_Strong
	preferReader := samples && !strong
	return executeQueryPlanRPC(ctx, qvc, shardID.VChannel, strong, preferReader, viewpb.QueryPlanService_GetQueryPlan_FullMethodName, func(ctx context.Context, client viewpb.QueryPlanServiceClient) (*viewpb.GetQueryPlanResponse, error) {
		return client.GetQueryPlan(ctx, req)
	})
}

func (qvc *queryViewClient) GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	result, _, err := executeQueryPlanRPC(ctx, qvc, shardID.VChannel, true, false, viewpb.QueryPlanService_GetMVCCTimestamp_FullMethodName, func(ctx context.Context, client viewpb.QueryPlanServiceClient) (*viewpb.GetMVCCTimestampResponse, error) {
		return client.GetMVCCTimestamp(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, viewpb.ViewQueryService_SearchOnView_FullMethodName, func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.SearchOnViewResponse, error) {
		return client.SearchOnView(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) SearchOnViewStream(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, "ViewQueryService.SearchOnViewStream", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (searchreducestream.ReduceStream, error) {
		return reduce.NewSearchGRPCReduceStream(ctx, client, req)
	})
	return result, err
}

func (qvc *queryViewClient) QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, viewpb.ViewQueryService_QueryOnView_FullMethodName, func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.QueryOnViewResponse, error) {
		return client.QueryOnView(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) QueryOnViewStream(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, "ViewQueryService.QueryOnViewStream", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (queryreducestream.ReduceStream, error) {
		return reduce.NewQueryGRPCReduceStream(ctx, client, req)
	})
	return result, err
}

func executeQueryPlanRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	vchannel string,
	strong, preferReader bool,
	method string,
	call func(context.Context, viewpb.QueryPlanServiceClient) (T, error),
) (T, *types.PChannelInfoAssigned, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", method))
	return executeViewRPC(ctx, qvc, logger, pchannel, strong, preferReader, method, func(ctx context.Context, assign *types.PChannelInfoAssigned) (viewRPCResult[T], error) {
		client, err := qvc.queryPlanService.GetService(ctx)
		if err != nil {
			return viewRPCResult[T]{}, err
		}
		resp, err := call(ctx, client)
		return viewRPCResult[T]{resp: resp, source: assign, err: err}, nil
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
	if source := contextutil.QueryWALReplica(ctx); source != nil {
		var zero T
		if source.Channel.Name != pchannel.Name {
			return zero, viewerror.NewViewInvalidated("plan belongs to another WAL channel")
		}
		if !qvc.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
			return zero, ErrClientClosed
		}
		defer qvc.owner.lifetime.Done()
		ctx = contextutil.WithFailFastPick(contextutil.WithPickServerID(ctx, source.Node.ServerID))
		ctx = worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, source.Channel)
		client, err := qvc.viewQueryService.GetService(ctx)
		if err != nil {
			return zero, err
		}
		return call(ctx, client)
	}
	resp, _, err := executeViewRPC(ctx, qvc, logger, pchannel.Name, false, false, method, func(ctx context.Context, _ *types.PChannelInfoAssigned) (viewRPCResult[T], error) {
		client, err := qvc.viewQueryService.GetService(ctx)
		if err != nil {
			return viewRPCResult[T]{}, err
		}
		resp, err := call(ctx, client)
		return viewRPCResult[T]{resp: resp, err: err}, nil
	})
	return resp, err
}

func executeViewRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	logger *mlog.Logger,
	pchannel string,
	strong, preferReader bool,
	method string,
	call func(context.Context, *types.PChannelInfoAssigned) (viewRPCResult[T], error),
) (T, *types.PChannelInfoAssigned, error) {
	var zero T
	if !qvc.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		return zero, nil, ErrClientClosed
	}
	defer qvc.owner.lifetime.Done()
	if err := ctx.Err(); err != nil {
		return zero, nil, err
	}
	// Wait for readiness through the WAL handler's assignment watch and backoff.
	// A successful connection does not imply that its assigned WAL is still ready.
	create := func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		channel := assign.Channel
		if channel.WALReplicaID != 0 && !strong {
			channel.Term = 0
		}
		rpcCtx := worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, channel)
		target := *assign
		target.Channel = channel
		rpcResult, err := call(rpcCtx, &target)
		if err != nil {
			return nil, err
		}
		if queryViewRPCNeedsWaitForReady(rpcResult.err) || (preferReader && rpcResult.err != nil && viewerror.AsViewError(rpcResult.err).IsRetryable()) {
			return nil, rpcResult.err
		}
		// View invalidation and request errors belong to the query orchestrator.
		// In particular, a pinned Phase 2 request must not wait for a retired view.
		return rpcResult, nil
	}
	var result any
	var err error
	if preferReader {
		result, err = qvc.owner.createOnReadable(ctx, pchannel, create)
	} else {
		result, err = qvc.owner.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, create)
	}
	if err != nil {
		return zero, nil, err
	}
	rpcResult := result.(viewRPCResult[T])
	return rpcResult.resp, rpcResult.source, rpcResult.err
}

func queryViewRPCNeedsWaitForReady(err error) bool {
	if err == nil {
		return false
	}
	viewErr := viewerror.AsViewError(err)
	return viewErr.IsOnShutdown() || viewErr.IsNotPrimary() || grpcstatus.Code(err) == codes.Unavailable
}
