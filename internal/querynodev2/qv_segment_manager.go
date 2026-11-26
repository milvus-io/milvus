package querynodev2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (node *QueryNode) NewQueryViewSegmentManager(ctx context.Context, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, metadata qnview.SegmentLoadInfoProvider) qnview.SegmentManager {
	if node == nil {
		return nil
	}
	manager := qvresource.NewQueryViewSegmentManager(ctx, node.manager, node.loader, meta, streams, metadata)
	if manager == nil {
		return nil
	}
	node.queryViewSegments.Store(manager)
	return manager
}

// SetQueryViewHandler binds the RPC handler to the node's shutdown lifecycle.
func (node *QueryNode) SetQueryViewHandler(handler *qnview.QNQueryViewHandler) {
	node.queryViewHandler.Store(handler)
}

// QueryViewResourceWatermarks only reads an initialized loader and participates
// in the node lifetime so shutdown cannot destroy it while sampling.
func (node *QueryNode) QueryViewResourceWatermarks() *viewpb.NodeResourceWatermarks {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return nil
	}
	defer node.lifetime.Done()
	return node.loader.GetResourceWatermarks()
}
