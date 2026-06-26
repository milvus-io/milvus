package qnview

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type QueryViewSegmentLoadScheduler struct {
	meta      QueryViewLoadMetadataProvider
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator
}

func NewQueryViewSegmentLoadScheduler(meta QueryViewLoadMetadataProvider, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *QueryViewSegmentLoadScheduler {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return &QueryViewSegmentLoadScheduler{
		meta:      meta,
		loader:    loader,
		estimator: estimator,
	}
}

func (s *QueryViewSegmentLoadScheduler) Submit(task SegmentLoadTask) {
	go s.load(task)
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
	loadInfos, indexes, err := s.meta.GetQueryViewSegmentLoadInfo(ctx, task.Meta.GetCollectionId(), task.SegmentID)
	if err != nil {
		return nil, err
	}
	if len(loadInfos) != 1 {
		return nil, fmt.Errorf("segment load info should contain exactly one segment, got %d", len(loadInfos))
	}
	if err := updateCollectionIndexMeta(ctx, task.Collection, indexes); err != nil {
		return nil, err
	}
	reservation, err := s.reserve(ctx, loadInfos[0], task.Collection)
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	segment, err := s.loader.Load(ctx, loadInfos[0], task.Collection)
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
