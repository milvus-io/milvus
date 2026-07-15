package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// QueryViewSyncClient is the QueryView sync domain client under HandlerClient.
type QueryViewSyncClient interface {
	// SyncQueryView opens a pchannel-scoped QueryView sync stream to the StreamingNode
	// that owns the current pchannel assignment.
	SyncQueryView(ctx context.Context, pchannel string) (viewpb.ViewSyncService_SyncQueryViewClient, error)
}

type queryViewSyncClient struct {
	owner           *handlerClientImpl
	viewSyncService lazygrpc.Service[viewpb.ViewSyncServiceClient]
}

func newQueryViewSyncClient(owner *handlerClientImpl, conn lazygrpc.Conn) QueryViewSyncClient {
	return &queryViewSyncClient{
		owner:           owner,
		viewSyncService: lazygrpc.WithServiceCreator(conn, viewpb.NewViewSyncServiceClient),
	}
}

func (hc *handlerClientImpl) QueryViewSyncClient() QueryViewSyncClient {
	return hc.queryViewSyncClient
}

func (c *queryViewSyncClient) SyncQueryView(ctx context.Context, pchannel string) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.String("handler", "ViewSyncService.SyncQueryView"))
	result, err := c.owner.createHandlerAfterStreamingNodeReady(ctx, logger, pchannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		client, err := c.viewSyncService.GetService(ctx)
		if err != nil {
			return nil, err
		}
		stream, err := client.SyncQueryView(worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, assign.Channel))
		if err != nil {
			return nil, streamingstatus.ConvertStreamingError("ViewSyncService.SyncQueryView", err)
		}
		return stream, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(viewpb.ViewSyncService_SyncQueryViewClient), nil
}
