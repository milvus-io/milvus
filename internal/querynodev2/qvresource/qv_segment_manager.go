package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type queryViewPhysicalSegmentLoader struct {
	collections qvCollectionManager
	segments    qvSegmentManager
	loader      qvSegmentLoader
}

func NewQueryViewPhysicalSegmentLoader(manager *segments.Manager, loader segments.Loader) qnview.PhysicalSegmentLoader {
	return newQueryViewPhysicalSegmentLoader(manager.Collection, manager.Segment, realQVSegmentLoader{
		loader:         loader,
		collections:    manager.Collection,
		segmentManager: manager.Segment,
	})
}

func NewQueryViewSegmentManager(ctx context.Context, manager *segments.Manager, loader segments.Loader, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, watcherFactories ...qnview.SegmentLoadInfoWatcherFactory) qnview.SegmentManager {
	if manager == nil || loader == nil || meta == nil || streams == nil {
		return nil
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(manager, loader)
	nodeScheduler := nodescheduler.Get()
	var watcher qnview.SegmentLoadInfoWatcher
	physicalManager := qnview.NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndWatcher(
		nodeScheduler,
		qnview.NewQueryViewSegmentLoadScheduler(meta, physicalLoader, newQueryViewSegmentResourceEstimator(loader)),
		watcher,
	)
	if len(watcherFactories) > 0 && watcherFactories[0] != nil {
		watcher = watcherFactories[0].NewSegmentLoadInfoWatcher(ctx, physicalManager.ApplyLoadInfoSnapshot)
		physicalManager.SetSegmentLoadInfoWatcher(watcher)
	}
	collectionRuntime := newQueryViewCollectionRuntimeManager(meta, manager.Collection)
	return qnview.NewQueryViewSegmentReadinessManagerWithScheduler(nodeScheduler, physicalManager, qvtransformlogbuffer.New(streams), collectionRuntime)
}

func newQueryViewPhysicalSegmentLoader(collections qvCollectionManager, segments qvSegmentManager, loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{
		collections: collections,
		segments:    segments,
		loader:      loader,
	}
}
