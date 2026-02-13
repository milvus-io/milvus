package segments

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querynodev2/segments/limiter"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type doOnSegmentFunc func(ctx context.Context, segment Segment) error

func doOnSegment(ctx context.Context, mgr *Manager, seg Segment, do doOnSegmentFunc) error {
	// record search time and cache miss
	var err error
	accessRecord := metricsutil.NewQuerySegmentAccessRecord(getSegmentMetricLabel(seg))
	defer func() {
		accessRecord.Finish(err)
	}()
	if seg.IsLazyLoad() {
		ctx, cancel := withLazyLoadTimeoutContext(ctx)
		defer cancel()

		var missing bool
		missing, err = mgr.DiskCache.Do(ctx, seg.ID(), do)
		if missing {
			accessRecord.CacheMissing()
		}
		if err != nil {
			log.Ctx(ctx).Warn("failed to do query disk cache", zap.Int64("segID", seg.ID()), zap.Error(err))
		}
		return err
	}
	return do(ctx, seg)
}

// doOnSegments Be careful to use this, since no any pool is used.
func doOnSegments(ctx context.Context, mgr *Manager, segments []Segment, do doOnSegmentFunc) error {
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(limiter.GetCurrentSegmentSQConcurrentLimit())
	for _, segment := range segments {
		seg := segment
		errGroup.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return doOnSegment(ctx, mgr, seg, do)
		})
	}
	return errGroup.Wait()
}
