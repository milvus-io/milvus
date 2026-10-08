package qnview

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func newSegmentLoadTask(loader PhysicalSegmentLoader, estimator SegmentResourceEstimator, task SegmentLoadTask) *SegmentLoadTask {
	task.loader = loader
	task.estimator = estimator
	return &task
}

func (t *SegmentLoadTask) Execute(schedulerCtx context.Context) (err error) {
	defer func() {
		if !errors.Is(err, nodescheduler.ErrDelay) && t.OnFinished != nil {
			t.OnFinished()
		}
	}()
	ctx, cancel := mergeTaskContext(schedulerCtx, t.Context)
	defer cancel()
	if ctx.Err() != nil {
		return nil
	}
	segment, err := t.load(ctx)
	if err != nil {
		if errors.Is(err, nodescheduler.ErrDelay) {
			return err
		}
		if t.OnUnrecoverable != nil {
			t.OnUnrecoverable(err)
		}
		return err
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
	reservation, err := prepareSegmentResources(ctx, t.Collection, loadInfo, indexes, t.estimator)
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	if loader, ok := t.loader.(PlannedPhysicalSegmentLoader); ok {
		return loader.LoadWithPlan(ctx, SegmentLoadPlan{
			Collection: t.Collection, LoadInfo: loadInfo,
			TransformStartAfterTimeTick: t.TransformStartAfterTimeTick,
		})
	}
	segment, err := t.loader.Load(ctx, loadInfo, t.Collection)
	if err != nil {
		return nil, err
	}
	// Legacy implementations may already return the requested baseline. Never
	// repair a mismatch with a getter-only decorator: that splits replay and
	// applied progress and can silently skip required history.
	if segment != nil && (segment.TransformStartAfterTimeTick() != t.TransformStartAfterTimeTick || segment.AppliedTransformTimeTick() != t.TransformStartAfterTimeTick) {
		_ = segment.Release(ctx)
		return nil, merr.WrapErrServiceInternalMsg("segment loader did not initialize planned transform frontier, segmentID=%d", t.SegmentID)
	}
	return segment, nil
}

func (t *SegmentLoadTask) loadInfo() (*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	if t.Snapshot.LoadInfo != nil {
		return t.Snapshot.LoadInfo, t.Snapshot.IndexInfos, nil
	}
	return nil, nil, merr.WrapErrServiceInternalMsg("query view segment load requires watch snapshot, segmentID=%d", t.SegmentID)
}

// Load and Reopen share admission. Only this preparation stage may delay the
// task; once native work starts, an error terminates the attempt.
func prepareSegmentResources(ctx context.Context, collection CollectionRuntime, info *querypb.SegmentLoadInfo, indexes []*indexpb.IndexInfo, estimator SegmentResourceEstimator) (ResourceReservation, error) {
	if info == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view segment load requires watch snapshot")
	}
	if err := updateCollectionIndexMeta(ctx, collection, indexes); err != nil {
		return nil, err
	}
	if estimator == nil {
		return nil, nil
	}
	reservation, err := estimator.Reserve(ctx, info, collection)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		mlog.Debug(ctx, "segment resource admission delayed", mlog.FieldSegmentID(info.GetSegmentID()), mlog.Err(err))
		return nil, nodescheduler.ErrDelay
	}
	return reservation, nil
}

func newSegmentUpdateTask(loader PhysicalSegmentLoader, task SegmentUpdateTask, estimators ...SegmentResourceEstimator) *SegmentUpdateTask {
	task.loader = loader
	if len(estimators) > 0 {
		task.estimator = estimators[0]
	}
	return &task
}

func (t *SegmentUpdateTask) Execute(schedulerCtx context.Context) (err error) {
	defer func() {
		if !errors.Is(err, nodescheduler.ErrDelay) && t.OnFinished != nil {
			t.OnFinished()
		}
	}()
	ctx, cancel := mergeTaskContext(schedulerCtx, t.Context)
	defer cancel()
	if ctx.Err() != nil {
		t.fail(ctx.Err())
		return nil
	}
	if err := t.update(ctx); err != nil {
		if errors.Is(err, nodescheduler.ErrDelay) {
			return err
		}
		t.fail(err)
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
	reservation, err := prepareSegmentResources(ctx, t.Collection, t.Snapshot.LoadInfo, t.Snapshot.IndexInfos, t.estimator)
	if err != nil {
		return err
	}
	if reservation != nil {
		defer reservation.Release()
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

func classifySegmentUpdate(current, next SegmentLoadInfoRevision) SegmentUpdateAction {
	if next.Empty() || current == next {
		return SegmentUpdateNone
	}
	return SegmentUpdateReopen
}

type schedulerTaskFunc func(context.Context) error

func (f schedulerTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

var (
	_ nodescheduler.Task = schedulerTaskFunc(nil)
	_ nodescheduler.Task = (*SegmentLoadTask)(nil)
	_ nodescheduler.Task = (*SegmentUpdateTask)(nil)
)

func updateCollectionIndexMeta(ctx context.Context, collection CollectionRuntime, indexes []*indexpb.IndexInfo) error {
	updater, ok := collection.(CollectionIndexMetaUpdater)
	if !ok {
		return nil
	}
	return updater.UpdateIndexMeta(ctx, indexes)
}
