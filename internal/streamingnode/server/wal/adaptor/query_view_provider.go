package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

var _ snview.QueryViewHandlerProvider = (*walAdaptorImpl)(nil)
var _ viewquery.TaskProvider = (*walAdaptorImpl)(nil)

func (w *walAdaptorImpl) QueryViewHandler() worknodehandler.QueryViewHandler {
	return w.queryViewHandler
}

func (w *walAdaptorImpl) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	h, err := w.queryViewTaskHandler(shardID)
	if err != nil {
		return nil, err
	}
	return h.AcquireSearchSegmentTasks(ctx, shardID, version, mvcc, req)
}

func (w *walAdaptorImpl) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	h, err := w.queryViewTaskHandler(shardID)
	if err != nil {
		return nil, err
	}
	return h.AcquireQuerySegmentTasks(ctx, shardID, version, mvcc, req)
}

func (w *walAdaptorImpl) queryViewTaskHandler(shardID qviews.ShardID) (*snview.SNQueryViewHandler, error) {
	if !w.IsAvailable() {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	if funcutil.ToPhysicalChannel(shardID.VChannel) != w.Channel().Name {
		return nil, viewerror.NewViewNotFound("query view shard %s is not on wal %s", shardID.String(), w.Channel().String())
	}
	if w.queryViewHandler == nil {
		return nil, viewerror.NewViewNotFound("query view handler is unavailable")
	}
	return w.queryViewHandler, nil
}
