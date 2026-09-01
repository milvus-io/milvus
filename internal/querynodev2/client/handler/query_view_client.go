package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// QueryViewClient is the QueryView domain client under the QueryNode handler client.
type QueryViewClient interface {
	SearchOnView(ctx context.Context, nodeID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, nodeID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	RequeryOnView(ctx context.Context, nodeID int64, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
}

type queryViewClient struct {
	owner   *clientImpl
	service lazygrpc.Service[viewpb.ViewQueryServiceClient]
}

func newQueryViewClient(owner *clientImpl, conn lazygrpc.Conn) *queryViewClient {
	return &queryViewClient{
		owner:   owner,
		service: lazygrpc.WithServiceCreator(conn, viewpb.NewViewQueryServiceClient),
	}
}

func (qvc *queryViewClient) SearchOnView(ctx context.Context, nodeID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return executeViewQueryRPC(ctx, qvc, nodeID, "ViewQueryService.SearchOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.SearchOnViewResponse, error) {
		return client.SearchOnView(ctx, req)
	})
}

func (qvc *queryViewClient) QueryOnView(ctx context.Context, nodeID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return executeViewQueryRPC(ctx, qvc, nodeID, "ViewQueryService.QueryOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.QueryOnViewResponse, error) {
		return client.QueryOnView(ctx, req)
	})
}

func (qvc *queryViewClient) RequeryOnView(ctx context.Context, nodeID int64, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return executeViewQueryRPC(ctx, qvc, nodeID, "ViewQueryService.RequeryOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.RequeryOnViewResponse, error) {
		return client.RequeryOnView(ctx, req)
	})
}

func executeViewQueryRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	nodeID int64,
	method string,
	call func(context.Context, viewpb.ViewQueryServiceClient) (T, error),
) (T, error) {
	if !qvc.owner.lifetime.Add(typeutil.LifetimeStateWorking) {
		var zero T
		return zero, viewerror.NewOnShutdownError("querynode client is closing")
	}
	defer qvc.owner.lifetime.Done()

	client, err := qvc.service.GetService(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	resp, err := call(contextutil.WithPickServerID(ctx, nodeID), client)
	return resp, viewerror.ConvertViewError(method, err)
}

func (qvc *queryViewClient) close() {
	qvc.service.Close()
}
