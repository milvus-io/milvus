package qnview

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newSegmentLoadTask(loader PhysicalSegmentLoader, estimator SegmentResourceEstimator, task SegmentLoadTask) *SegmentLoadTask {
	task.loader = loader
	task.estimator = estimator
	return &task
}

func (t *SegmentLoadTask) Execute(schedulerCtx context.Context) error {
	if t.OnFinished != nil {
		defer t.OnFinished()
	}
	ctx, cancel := mergeTaskContext(schedulerCtx, t.Context)
	defer cancel()
	if ctx.Err() != nil {
		return nil
	}
	segment, err := t.load(ctx)
	if err != nil {
		if errors.Is(err, merr.ErrSegmentRequestResourceFailed) && ctx.Err() == nil {
			return err
		}
		if t.OnUnrecoverable != nil {
			t.OnUnrecoverable(err)
		}
		// Loading failures are terminal for this metadata revision. Returning nil
		// prevents a scheduler-level retry from racing the manager's reset path.
		return nil
	}
	if t.OnLoaded != nil {
		t.OnLoaded(segment)
	}
	return nil
}

func (t *SegmentLoadTask) load(ctx context.Context) (TransformSegment, error) {
	loadInfo, indexes, err := t.loadInfo()
	if err != nil {
		return nil, err
	}
	if err := updateCollectionIndexMeta(ctx, t.Collection, indexes); err != nil {
		return nil, err
	}
	reservation, err := t.reserve(ctx, loadInfo)
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	segment, err := t.loader.Load(ctx, loadInfo, t.Collection)
	if err != nil {
		return nil, err
	}
	if t.TransformStartAfterTimeTick > 0 {
		segment = &transformStartSegment{TransformSegment: segment, startAfter: t.TransformStartAfterTimeTick}
	}
	return segment, nil
}

func (t *SegmentLoadTask) loadInfo() (*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	if t.Snapshot.LoadInfo != nil {
		return t.Snapshot.LoadInfo, t.Snapshot.IndexInfos, nil
	}
	return nil, nil, merr.WrapErrServiceInternalMsg(
		"query view segment load requires a metadata stream snapshot, segmentID=%d", t.SegmentID)
}

func (t *SegmentLoadTask) reserve(ctx context.Context, info *querypb.SegmentLoadInfo) (ResourceReservation, error) {
	if t.estimator == nil {
		return nil, nil
	}
	return t.estimator.Reserve(ctx, info, t.Collection)
}

func newSegmentUpdateTask(loader PhysicalSegmentLoader, task SegmentUpdateTask) *SegmentUpdateTask {
	task.loader = loader
	return &task
}

func (t *SegmentUpdateTask) Execute(schedulerCtx context.Context) error {
	ctx, cancel := mergeTaskContext(schedulerCtx, t.Context)
	defer cancel()
	if ctx.Err() != nil {
		t.fail(ctx.Err())
		return nil
	}
	if err := t.update(ctx); err != nil {
		if ctx.Err() != nil {
			t.fail(ctx.Err())
			return nil
		}
		// A non-nil task error asks the injected QueryNode scheduler to delay and
		// retry this same task instance.
		return err
	}
	return nil
}

func (t *SegmentUpdateTask) update(ctx context.Context) error {
	action := classifySegmentUpdate(t.Current, t.Snapshot.Revision)
	if action == SegmentUpdateNone {
		if t.OnUpdated != nil {
			t.OnUpdated(t.Current)
		}
		return nil
	}
	if err := updateCollectionIndexMeta(ctx, t.Collection, t.Snapshot.IndexInfos); err != nil {
		return err
	}
	if err := t.loader.Update(ctx, t.Segment, t.Collection, t.Snapshot, action); err != nil {
		return err
	}
	if t.OnUpdated != nil {
		t.OnUpdated(t.Snapshot.Revision)
	}
	return nil
}

func (t *SegmentUpdateTask) fail(err error) {
	if t.OnFailed != nil {
		t.OnFailed(err)
	}
}

func mergeTaskContext(schedulerCtx, taskCtx context.Context) (context.Context, context.CancelFunc) {
	if schedulerCtx == nil {
		schedulerCtx = context.Background()
	}
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

func classifySegmentUpdate(current, next SegmentLoadInfoRevision) SegmentUpdateAction {
	if next.Empty() || current == next {
		return SegmentUpdateNone
	}
	return SegmentUpdateReopen | SegmentUpdateLoadIndex
}

type schedulerTaskFunc func(context.Context) error

func (f schedulerTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

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
