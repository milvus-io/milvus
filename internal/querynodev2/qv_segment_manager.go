package querynodev2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (node *QueryNode) NewQueryViewSegmentManager(ctx context.Context, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, streamFactory qnview.SegmentLoadInfoStreamFactory) (qnview.SegmentManager, error) {
	if node == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot construct QueryView segment manager: missing QueryNode")
	}
	return qvresource.NewQueryViewSegmentManager(ctx, node.loadResourceBudget, node.chunkManager, meta, streams, streamFactory)
}
