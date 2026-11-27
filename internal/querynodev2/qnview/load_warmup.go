package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// segmentWarmUse belongs to one request, not to the View that located its read.
// A canceled request must not abort a cache load shared by a live query or warm.
type segmentWarmUse struct {
	request context.Context
	cancel  context.CancelFunc
}

// Prewarm borrows a fixed set of read states just like a query. View replacement
// cannot extend or cancel the acquired work. Errors terminate this request;
// no reconcile, retry timer or node recovery can recreate it.
func (m *SegmentResourceManager) Prewarm(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) error {
	handles, err := m.AcquireSealedSegmentHandles(ctx, key, view, 0)
	if err != nil {
		return err
	}
	type borrowedWarm struct {
		handle *resourceSegmentHandle
		use    *segmentWarmUse
		ctx    context.Context
		stop   func() bool
	}
	borrowed := make([]borrowedWarm, 0, len(handles))
	for _, handle := range handles {
		h := handle.(*resourceSegmentHandle)
		warmCtx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:gosec // canceled after other cache consumers drain
		use := &segmentWarmUse{request: ctx, cancel: cancel}
		m.mu.Lock()
		if h.base.warmUses == nil {
			h.base.warmUses = make(map[*segmentWarmUse]struct{})
		}
		h.base.warmUses[use] = struct{}{}
		h.resource.cancelIdleWarmsLocked()
		m.mu.Unlock()
		stop := context.AfterFunc(ctx, func() {
			m.mu.Lock()
			h.resource.cancelIdleWarmsLocked()
			m.mu.Unlock()
		})
		borrowed = append(borrowed, borrowedWarm{handle: h, use: use, ctx: warmCtx, stop: stop})
	}
	defer func() {
		for _, warm := range borrowed {
			warm.stop()
			m.mu.Lock()
			delete(warm.handle.base.warmUses, warm.use)
			m.mu.Unlock()
			warm.use.cancel()
			warm.handle.Release()
		}
	}()
	for _, warm := range borrowed {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := warm.handle.read.Prewarm(warm.ctx, nil); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Called with manager.mu held. Only explicit request cancellation or node
// shutdown cancels warming. Ordinary View retirement preserves borrowed reads.
func (r *segmentResource) cancelIdleWarmsLocked() {
	for _, base := range r.bases {
		if base.operationRefs > len(base.warmUses) {
			continue
		}
		active := false
		if !r.manager.closed {
			for use := range base.warmUses {
				if use.request.Err() == nil {
					active = true
					break
				}
			}
		}
		if !active {
			for use := range base.warmUses {
				use.cancel()
			}
		}
	}
}
