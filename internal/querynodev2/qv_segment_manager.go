package querynodev2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/qvresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
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

// ScheduleQueryViewRead keeps the caller's view pinned until the scheduler
// finishes or rejects its task, including when the RPC context is canceled.
func (node *QueryNode) ScheduleQueryViewRead(task scheduler.Task) error {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return err
	}
	defer node.lifetime.Done()
	if err := node.scheduler.Add(task); err != nil {
		return err
	}
	return task.Wait()
}
