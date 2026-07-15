package growingruntime

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *Runtime) WaitMVCCVisible(ctx context.Context, growingTimetick uint64, transformTimetick uint64) error {
	if r == nil {
		return merr.WrapErrServiceInternalMsg("growing runtime is nil")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.mvccCond.Broadcast()
			r.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	r.mu.Lock()
	defer r.mu.Unlock()
	for !r.mvccVisibleLocked(growingTimetick, transformTimetick) {
		if r.closed {
			return merr.WrapErrServiceUnavailable("growing runtime is closed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mvccCond.Wait()
	}
	return ctx.Err()
}

func (r *Runtime) mvccVisibleLocked(growingTimetick uint64, transformTimetick uint64) bool {
	return r.appliedGrowingTimeTick.Load() >= growingTimetick &&
		r.appliedTransformTimeTick.Load() >= transformTimetick
}

func (r *Runtime) markGrowingTimeTick(timetick uint64) {
	r.markTimeTick(&r.appliedGrowingTimeTick, timetick)
}

func (r *Runtime) markTransformTimeTick(timetick uint64) {
	r.markTimeTick(&r.appliedTransformTimeTick, timetick)
}

func (r *Runtime) markTimeTick(value interface {
	Load() uint64
	CompareAndSwap(old uint64, new uint64) bool
}, timetick uint64,
) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	advanced := advanceTimeTick(value, timetick)
	if !advanced {
		return
	}
	r.mvccCond.Broadcast()
}
