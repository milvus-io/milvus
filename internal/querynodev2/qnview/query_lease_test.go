//go:build test && dynamic

package qnview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
)

func TestQNHandler_AcquireReadyViewReturnsExactVersion(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view},
	})
	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	lease, err := h.AcquireReadyView(context.Background(), view.ShardID(), key.QueryViewVersion)
	require.NoError(t, err)
	defer lease.Release()
	assert.True(t, key.QueryViewVersion.EQ(lease.Version))
	assert.True(t, proto.Equal(view.IntoProto().GetMeta(), lease.Meta))
	require.Len(t, view.IntoProto().GetQueryNode(), 1)
	assert.True(t, proto.Equal(view.IntoProto().GetQueryNode()[0], lease.View))
}

func TestQNHandler_QueryViewLeaseDefersSegmentRelease(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	lease, err := h.AcquireReadyView(context.Background(), view.ShardID(), key.QueryViewVersion)
	require.NoError(t, err)

	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})
	assert.Equal(t, 0, mgr.releasedCount())

	lease.Release()
	assert.Equal(t, 1, mgr.releasedCount())
	mgr.invokeReleaseCallback(key)
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
}
