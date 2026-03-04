package segments

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
)

type doOnSegmentFunc func(ctx context.Context, segment Segment) error

func doOnSegment(ctx context.Context, mgr *Manager, seg Segment, do doOnSegmentFunc) error {
	// record search time and cache miss
	var err error
	accessRecord := metricsutil.NewQuerySegmentAccessRecord(getSegmentMetricLabel(seg))
	defer func() {
		accessRecord.Finish(err)
	}()
	return do(ctx, seg)
}

// doOnSegments Be careful to use this, since no any pool is used.
func doOnSegments(ctx context.Context, mgr *Manager, segments []Segment, do doOnSegmentFunc) error {
	errGroup, ctx := errgroup.WithContext(ctx)
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
