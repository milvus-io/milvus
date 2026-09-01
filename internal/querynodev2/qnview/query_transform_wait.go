package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
)

func (m *QueryViewSegmentReadinessManager) WaitTransformVisible(ctx context.Context, key qviews.QueryViewKey, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	view := m.views[key]
	if view == nil {
		m.mu.Unlock()
		return viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	guard := view.transformGuard
	m.mu.Unlock()
	if guard == nil {
		return viewerror.NewViewInvalidated("query view %s transform guard is not ready", key.String())
	}
	return guard.WaitTransformVisible(ctx, timetick)
}
