package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryViewPhysicalSegmentLoader struct{ loader qvSegmentLoader }

func NewQueryViewPhysicalSegmentLoader(cm storage.ChunkManager) qnview.PhysicalSegmentLoader {
	return newQueryViewPhysicalSegmentLoader(realQVSegmentLoader{cm: cm})
}

func NewQueryViewSegmentManager(ctx context.Context, budget *segments.LoadResourceBudget, cm storage.ChunkManager, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, streamFactories ...qnview.SegmentLoadInfoStreamFactory) qnview.SegmentManager {
	if budget == nil || cm == nil || meta == nil || streams == nil {
		return nil
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(cm)
	nodeScheduler := nodescheduler.Get()
	var segmentLoadInfoStream qnview.SegmentLoadInfoStream
	if len(streamFactories) > 0 && streamFactories[0] != nil {
		segmentLoadInfoStream = streamFactories[0].NewSegmentLoadInfoStream(ctx)
	}
	physicalManager := qnview.NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(
		nodeScheduler,
		physicalLoader,
		segmentLoadInfoStream,
		newQueryViewSegmentResourceEstimator(budget),
	)
	collectionRuntime := newQueryViewCollectionRuntimeManager(meta)
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

func newQueryViewPhysicalSegmentLoader(loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{loader: loader}
}
