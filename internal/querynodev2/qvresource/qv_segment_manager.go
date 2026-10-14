package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryViewPhysicalSegmentLoader struct {
	segments qvSegmentManager
	loader   qvSegmentLoader
}

func NewQueryViewPhysicalSegmentLoader(manager *segments.Manager, loader segments.Loader) qnview.PhysicalSegmentLoader {
	return newQueryViewPhysicalSegmentLoader(manager.Segment, realQVSegmentLoader{
		loader:         loader,
		segmentManager: manager.Segment,
	})
}

func NewQueryViewSegmentManager(ctx context.Context, manager *segments.Manager, loader segments.Loader, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, streamFactories ...qnview.SegmentLoadInfoStreamFactory) qnview.SegmentManager {
	if manager == nil || loader == nil || meta == nil || streams == nil {
		return nil
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(manager, loader)
	nodeScheduler := nodescheduler.Get()
	var segmentLoadInfoStream qnview.SegmentLoadInfoStream
	if len(streamFactories) > 0 && streamFactories[0] != nil {
		segmentLoadInfoStream = streamFactories[0].NewSegmentLoadInfoStream(ctx)
	}
	physicalManager := qnview.NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(
		nodeScheduler,
		physicalLoader,
		segmentLoadInfoStream,
		newQueryViewSegmentResourceEstimator(loader),
	)
	collectionRuntime := newQueryViewCollectionRuntimeManager(meta, streaming.WAL().SchemaClient())
	return qnview.NewQueryViewSegmentReadinessManagerWithScheduler(
		nodeScheduler,
		physicalManager,
		qvtransformlogbuffer.New(
			streams,
			paramtable.Get().QueryNodeCfg.QueryViewTransformLogDrainConcurrency.GetAsInt(),
		),
		paramtable.Get().QueryNodeCfg.QueryViewSegmentCatchupConcurrency.GetAsInt(),
		collectionRuntime,
	)
}

func newQueryViewPhysicalSegmentLoader(segments qvSegmentManager, loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{
		segments: segments,
		loader:   loader,
	}
}
