package client

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"go.uber.org/zap"
)

// NewQueryViewServiceClient creates a new QueryViewServiceClient.
func NewQueryViewServiceClient(
	qnService lazygrpc.Service[viewpb.QueryViewSyncServiceClient],
	snService lazygrpc.Service[viewpb.QueryViewSyncServiceClient],
	channelAssgnmentWatcher assignment.Watcher,
) QueryViewServiceClient {
	return &queryViewServiceClientImpl{
		qnService:                qnService,
		snService:                snService,
		channelAssignmentWatcher: channelAssgnmentWatcher,
	}
}

// queryViewServiceClientImpl is the client implementation of the query view service.
type queryViewServiceClientImpl struct {
	log.Binder
	qnService                lazygrpc.Service[viewpb.QueryViewSyncServiceClient]
	snService                lazygrpc.Service[viewpb.QueryViewSyncServiceClient]
	channelAssignmentWatcher assignment.Watcher
}

// CreateSyncer syncs the query view service.
func (c *queryViewServiceClientImpl) CreateSyncer(req SyncOption) QueryViewServiceSyncer {
	syncer := newGRPCSyncer(c, &req, c.Logger().With(zap.String("workNode", req.WorkNode.String())))
	return syncer
}

func (c *queryViewServiceClientImpl) createNewSyncStreamClient(ctx context.Context, workNode qviews.WorkNode) (viewpb.QueryViewSyncService_SyncClient, error) {
	switch workNode := workNode.(type) {
	case qviews.QueryNode:
		return c.createNewQueryNodeClient(ctx, workNode)
	case qviews.StreamingNode:
		return c.createNewStreamingNodeClient(ctx, workNode)
	default:
		panic("unknown work node type")
	}
}

func (c *queryViewServiceClientImpl) createNewQueryNodeClient(ctx context.Context, workNode qviews.QueryNode) (viewpb.QueryViewSyncService_SyncClient, error) {
	service, err := c.qnService.GetService(ctx)
	if err != nil {
		return nil, err
	}
	ctx = contextutil.WithPickServerID(ctx, workNode.ID)
	newStreamClient, err := service.Sync(ctx)
	if err == nil {
		return newStreamClient, nil
	}
	if picker.IsErrSubConnNoExist(err) {
		return nil, errors.Mark(err, ErrNodeGone)
	}
	return nil, err
}

func (c *queryViewServiceClientImpl) createNewStreamingNodeClient(ctx context.Context, workNode qviews.StreamingNode) (viewpb.QueryViewSyncService_SyncClient, error) {
	service, err := c.snService.GetService(ctx)
	if err != nil {
		return nil, err
	}

	// block until create new streaming client success.
	pchannel := funcutil.ToPhysicalChannel(workNode.PChannel)
	for {
		assign := c.channelAssignmentWatcher.Get(ctx, pchannel)
		if assign != nil {
			ctx = contextutil.WithPickServerID(ctx, assign.Node.ServerID)
			newStreamingClient, err := service.Sync(ctx)
			if err == nil {
				return newStreamingClient, nil
			}
		}
		// Block until new assignment term is coming.
		if err := c.channelAssignmentWatcher.Watch(ctx, pchannel, assign); err != nil {
			return nil, err
		}
	}
}
