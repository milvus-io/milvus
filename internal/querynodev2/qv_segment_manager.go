package querynodev2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

func (node *QueryNode) NewQueryViewSegmentManager(ctx context.Context, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, streamFactories ...qnview.SegmentLoadInfoStreamFactory) qnview.SegmentManager {
	if node == nil {
		return nil
	}
	return qvresource.NewQueryViewSegmentManager(ctx, node.manager, node.loader, meta, streams, streamFactories...)
}
