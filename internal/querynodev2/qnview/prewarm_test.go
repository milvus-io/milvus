//go:build test && dynamic

package qnview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type prewarmSegmentManager struct {
	*mockSegmentManager
	prewarm func(context.Context, qviews.QueryViewKey, *viewpb.QueryViewOfQueryNode) error
}

func (m *prewarmSegmentManager) Prewarm(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) error {
	return m.prewarm(ctx, key, view)
}

func TestPrewarmHandlerRetainsResourcesThroughViewDrop(t *testing.T) {
	manager := &prewarmSegmentManager{mockSegmentManager: newMockSegmentManager()}
	h := NewQNQueryViewHandler(manager)
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{{View: view}})
	req, _ := manager.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
	failure := viewerror.NewUnknownError("injected prewarm failure")
	manager.prewarm = func(ctx context.Context, actual qviews.QueryViewKey, _ *viewpb.QueryViewOfQueryNode) error {
		require.Equal(t, key, actual)
		h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1)}})
		require.Zero(t, manager.releasedCount())
		require.NoError(t, ctx.Err())
		return failure
	}
	err := h.Prewarm(t.Context(), &viewpb.PrewarmOnViewRequest{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()})
	require.Same(t, failure, err)
	require.Equal(t, 1, manager.releasedCount(), "failed requests also release their lease")
}
