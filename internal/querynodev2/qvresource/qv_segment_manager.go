package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryViewPhysicalSegmentLoader struct{ loader qvSegmentLoader }

func NewQueryViewPhysicalSegmentLoader(cm storage.ChunkManager) qnview.PhysicalSegmentLoader {
	return newQueryViewPhysicalSegmentLoader(realQVSegmentLoader{cm: cm})
}

// NewQueryViewSegmentManager validates all required dependencies before starting
// resource workers. Stream connectivity is managed asynchronously by the factory.
func NewQueryViewSegmentManager(ctx context.Context, budget *segments.LoadResourceBudget, cm storage.ChunkManager, meta qnview.QueryViewLoadMetadataProvider, streams wal.TransformLogStreamManager, streamFactory qnview.SegmentLoadInfoStreamFactory) (qnview.SegmentManager, error) {
	var missing string
	switch {
	case budget == nil:
		missing = "load resource budget"
	case cm == nil:
		missing = "chunk manager"
	case meta == nil:
		missing = "load metadata provider"
	case streams == nil:
		missing = "TransformLog stream manager"
	case streamFactory == nil:
		missing = "SegmentLoadInfoStreamFactory"
	}
	if missing != "" {
		return nil, merr.WrapErrServiceInternalMsg("cannot construct QueryView segment manager: missing %s", missing)
	}
	segmentLoadInfoStream := streamFactory.NewSegmentLoadInfoStream(ctx)
	if segmentLoadInfoStream == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot construct QueryView segment manager: SegmentLoadInfoStreamFactory returned nil stream")
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(cm)
	nodeScheduler := nodescheduler.Get()
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
	), nil
}

func newQueryViewPhysicalSegmentLoader(loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{loader: loader}
}
