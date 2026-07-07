package syncer

import (
	"context"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	snmanager "github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ ViewSyncClient = (*DefaultViewSyncClient)(nil)

// WALLocatedProvider provides the latest pchannel-to-StreamingNode lookup.
type WALLocatedProvider interface {
	GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool)
}

// DefaultViewSyncClient combines QueryNode service discovery, StreamingNode
// service discovery, and channel WAL-location lookup for ReliableSyncer.
type DefaultViewSyncClient struct {
	queryNodes     qnmanager.ManagerClient
	streamingNodes snmanager.ManagerClient
	channels       WALLocatedProvider
}

func NewDefaultViewSyncClient(
	queryNodes qnmanager.ManagerClient,
	streamingNodes snmanager.ManagerClient,
	channels WALLocatedProvider,
) *DefaultViewSyncClient {
	return &DefaultViewSyncClient{
		queryNodes:     queryNodes,
		streamingNodes: streamingNodes,
		channels:       channels,
	}
}

func (c *DefaultViewSyncClient) RegisterNodeChangedNotifier(notifier func()) {
	if c.queryNodes == nil {
		return
	}
	c.queryNodes.RegisterNodeChangedNotifier(notifier)
}

func (c *DefaultViewSyncClient) IsNodeAlive(ctx context.Context, node qviews.WorkNode) bool {
	switch n := node.(type) {
	case qviews.QueryNode:
		if c.queryNodes == nil {
			return false
		}
		nodes, err := c.queryNodes.GetAllQueryNodes(ctx)
		if err != nil {
			return false
		}
		_, ok := nodes[n.ID]
		return ok
	case qviews.StreamingNode:
		return true
	default:
		return false
	}
}

func (c *DefaultViewSyncClient) OpenSyncStream(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	switch n := node.(type) {
	case qviews.QueryNode:
		if c.queryNodes == nil {
			return nil, merr.WrapErrServiceInternalMsg("querynode manager client is nil")
		}
		client, err := c.queryNodes.CreateViewSyncClient(ctx, n.ID)
		if err != nil {
			return nil, err
		}
		return client.SyncQueryView(ctx)
	case qviews.StreamingNode:
		if c.streamingNodes == nil {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode manager client is nil")
		}
		if c.channels == nil {
			return nil, merr.WrapErrServiceInternalMsg("channel manager is nil")
		}
		streamingNodeID, ok := c.channels.GetLatestWALLocated(ctx, n.PChannel)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode assignment not found for pchannel %s", n.PChannel)
		}
		client, err := c.streamingNodes.CreateViewSyncClient(ctx, streamingNodeID)
		if err != nil {
			return nil, err
		}
		return client.SyncQueryView(ctx)
	default:
		return nil, merr.WrapErrServiceInternalMsg("unknown work node type %T", node)
	}
}

func (c *DefaultViewSyncClient) Close() {}
