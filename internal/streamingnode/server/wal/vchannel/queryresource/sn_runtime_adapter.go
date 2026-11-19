package queryresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *QueryRuntime) WaitMVCCVisible(ctx context.Context, growingTimetick uint64, transformingTimetick uint64) error {
	runtime, ok := r.growingRuntime()
	if !ok {
		return merr.WrapErrServiceInternalMsg("growing runtime module is not available")
	}
	return runtime.WaitMVCCVisible(ctx, growingTimetick, transformingTimetick)
}

// WaitLineageVisible waits until the residual of a split source applied the
// deletes of every target stream through the requested tick.
func (r *QueryRuntime) WaitLineageVisible(ctx context.Context, ticks map[string]uint64) error {
	runtime, ok := r.growingRuntime()
	if !ok {
		return merr.WrapErrServiceInternalMsg("growing runtime module is not available")
	}
	waiter, ok := runtime.(lineageRuntime)
	if !ok {
		return merr.WrapErrServiceInternalMsg("growing runtime module cannot wait for split lineage streams")
	}
	return waiter.WaitLineageVisible(ctx, ticks)
}

type lineageRuntime interface {
	WaitLineageVisible(ctx context.Context, ticks map[string]uint64) error
}

func (r *QueryRuntime) AcquireGrowingSegmentHandles(ctx context.Context, dataVersion qviews.DataVersion, partitionIDs []int64) ([]snview.GrowingSegmentHandle, error) {
	runtime, ok := r.growingRuntime()
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("growing runtime module is not available")
	}
	return runtime.AcquireGrowingSegmentHandles(ctx, dataVersion, partitionIDs)
}

func (r *QueryRuntime) MayHaveVisibleGrowingSegments(dataVersion qviews.DataVersion, growingTimetick uint64, transformingTimetick uint64, partitionIDs []int64) bool {
	runtime, ok := r.growingRuntime()
	if !ok {
		return true
	}
	return runtime.MayHaveVisibleGrowingSegments(dataVersion, growingTimetick, transformingTimetick, partitionIDs)
}

type growingRuntime interface {
	WaitMVCCVisible(ctx context.Context, growingTimetick uint64, transformingTimetick uint64) error
	AcquireGrowingSegmentHandles(ctx context.Context, dataVersion qviews.DataVersion, partitionIDs []int64) ([]snview.GrowingSegmentHandle, error)
	MayHaveVisibleGrowingSegments(dataVersion qviews.DataVersion, growingTimetick uint64, transformingTimetick uint64, partitionIDs []int64) bool
}

func (r *QueryRuntime) growingRuntime() (growingRuntime, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, module := range r.modules {
		runtime, ok := module.(growingRuntime)
		if ok {
			return runtime, true
		}
	}
	return nil, false
}
