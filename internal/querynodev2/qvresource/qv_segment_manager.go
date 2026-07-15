package qvresource

import (
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
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

func NewQueryViewSegmentManager(manager *segments.Manager, loader segments.Loader, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager) qnview.SegmentManager {
	if manager == nil || loader == nil || meta == nil || streams == nil {
		return nil
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(manager, loader)
	physicalManager := qnview.NewViewScopedPhysicalSegmentManager(meta, physicalLoader, newQueryViewSegmentResourceEstimator(loader))
	collectionRuntime := newQueryViewCollectionRuntimeManager(meta, manager.Collection)
	return qnview.NewQueryViewSegmentReadinessManager(physicalManager, qvtransformlogbuffer.New(streams), collectionRuntime)
}

func newQueryViewPhysicalSegmentLoader(collections qvCollectionManager, segments qvSegmentManager, loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{
		collections: collections,
		segments:    segments,
		loader:      loader,
	}
}
