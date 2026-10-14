package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func mergeTaskContext(schedulerCtx context.Context, taskCtx context.Context) (context.Context, context.CancelFunc) {
	if taskCtx == nil {
		taskCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(taskCtx)
	stop := context.AfterFunc(schedulerCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

type schedulerTaskFunc func(context.Context) error

func (f schedulerTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

var _ nodescheduler.Task = schedulerTaskFunc(nil)

func updateCollectionIndexMeta(ctx context.Context, collection CollectionRuntime, indexes []*indexpb.IndexInfo) error {
	updater, ok := collection.(CollectionIndexMetaUpdater)
	if !ok {
		return nil
	}
	return updater.UpdateIndexMeta(ctx, indexes)
}

type transformStartSegment struct {
	TransformSegment
	startAfter uint64
}

func (s *transformStartSegment) UnwrapTransformSegment() TransformSegment {
	return s.TransformSegment
}

func (s *transformStartSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *transformStartSegment) QuerySegment() segments.Segment {
	readable, ok := s.TransformSegment.(ReadableSealedSegment)
	if !ok {
		return nil
	}
	return readable.QuerySegment()
}

func (s *transformStartSegment) Collection() *segments.Collection {
	readable, ok := s.TransformSegment.(ReadableSealedSegment)
	if !ok {
		return nil
	}
	return readable.Collection()
}
