package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
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

func NewQueryViewSegmentManager(ctx context.Context, manager *segments.Manager, loader segments.Loader, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, metadata qnview.SegmentLoadInfoProvider) *qnview.SegmentResourceManager {
	if manager == nil || loader == nil || meta == nil || streams == nil || metadata == nil {
		return nil
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(manager, loader)
	nodeScheduler := nodescheduler.Get()
	collectionRuntime := newQueryViewCollectionRuntimeManager(meta, streaming.WAL().SchemaClient())
	return qnview.NewSegmentResourceManager(ctx, nodeScheduler, physicalLoader,
		newQueryViewSegmentResourceEstimator(loader), metadata,
		qvtransformlogbuffer.NewConfigured(ctx, streams), collectionRuntime)
}

func newQueryViewPhysicalSegmentLoader(segments qvSegmentManager, loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{
		segments: segments,
		loader:   loader,
	}
}
