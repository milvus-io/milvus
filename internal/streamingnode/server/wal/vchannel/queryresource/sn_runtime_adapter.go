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
