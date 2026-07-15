package querynodev2

import (
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

func (node *QueryNode) NewQueryViewSegmentManager(meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager) qnview.SegmentManager {
	if node == nil {
		return nil
	}
	return qvresource.NewQueryViewSegmentManager(node.manager, node.loader, meta, streams)
}
