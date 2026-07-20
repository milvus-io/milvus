package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type QueryViewSegmentLoadScheduler struct {
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator
}

func NewQueryViewSegmentLoadScheduler(meta QueryViewLoadMetadataProvider, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *QueryViewSegmentLoadScheduler {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return &QueryViewSegmentLoadScheduler{
		loader:    loader,
		estimator: estimator,
	}
}

func (s *QueryViewSegmentLoadScheduler) Submit(task SegmentLoadTask) {
	go s.load(task)
}

func (s *QueryViewSegmentLoadScheduler) Update(task SegmentUpdateTask) {
	go s.update(task)
}

func (s *QueryViewSegmentLoadScheduler) Cancel(int64) {}

func (s *QueryViewSegmentLoadScheduler) load(task SegmentLoadTask) {
	ctx := task.Context
	if ctx == nil {
		ctx = context.Background()
	}
	segment, err := s.loadMissing(ctx, task)
	if err != nil {
		if task.OnUnrecoverable != nil {
			task.OnUnrecoverable(err)
		}
		return
	}
	if task.OnLoaded != nil {
		task.OnLoaded(segment)
	}
}

func (s *QueryViewSegmentLoadScheduler) loadMissing(ctx context.Context, task SegmentLoadTask) (TransformSegment, error) {
	loadInfo, indexes, err := s.loadInfo(ctx, task)
	if err != nil {
		return nil, err
	}
	if err := updateCollectionIndexMeta(ctx, task.Collection, indexes); err != nil {
		return nil, err
	}
	reservation, err := s.reserve(ctx, loadInfo, task.Collection)
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	segment, err := s.loader.Load(ctx, loadInfo, task.Collection)
	if err != nil {
		return nil, err
	}
	if task.TransformStartAfterTimeTick > 0 {
		segment = &transformStartSegment{
			TransformSegment: segment,
			startAfter:       task.TransformStartAfterTimeTick,
		}
	}
	return segment, nil
}

func (s *QueryViewSegmentLoadScheduler) loadInfo(ctx context.Context, task SegmentLoadTask) (*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	if task.Snapshot.LoadInfo != nil {
		return task.Snapshot.LoadInfo, task.Snapshot.IndexInfos, nil
	}
	return nil, nil, merr.WrapErrServiceInternalMsg("query view segment load requires watch snapshot, segmentID=%d", task.SegmentID)
}

func (s *QueryViewSegmentLoadScheduler) update(task SegmentUpdateTask) {
	ctx := task.Context
	if ctx == nil {
		ctx = context.Background()
	}
	action := classifySegmentUpdate(task.Current, task.Snapshot.Revision)
	if action == SegmentUpdateNone {
		if task.OnUpdated != nil {
			task.OnUpdated(task.Current)
		}
		return
	}
	if err := updateCollectionIndexMeta(ctx, task.Collection, task.Snapshot.IndexInfos); err != nil {
		if task.OnFailed != nil {
			task.OnFailed(err)
		}
		return
	}
	if err := s.loader.Update(ctx, task.Segment, task.Collection, task.Snapshot, action); err != nil {
		if task.OnFailed != nil {
			task.OnFailed(err)
		}
		return
	}
	if task.OnUpdated != nil {
		task.OnUpdated(task.Snapshot.Revision)
	}
}

func classifySegmentUpdate(current, next SegmentLoadInfoRevision) SegmentUpdateAction {
	if next.Empty() || current == next {
		return SegmentUpdateNone
	}
	return SegmentUpdateReopen | SegmentUpdateLoadIndex
}

func (s *QueryViewSegmentLoadScheduler) reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error) {
	if s.estimator == nil {
		return nil, nil
	}
	return s.estimator.Reserve(ctx, info, collection)
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
