package snview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type prewarmResourceManager struct {
	*mockResourceManager
	prewarm func(context.Context, qviews.QueryViewKey) error
}

func (m *prewarmResourceManager) Prewarm(ctx context.Context, key qviews.QueryViewKey) error {
	return m.prewarm(ctx, key)
}

func TestPrewarmHandlerRetainsServingViewAcrossDown(t *testing.T) {
	h, shard, entry, _, reports := leasedUpView(t, time.Minute)
	key := newPreparingSNView(1).QueryViewKey()
	h.resMgr = &prewarmResourceManager{mockResourceManager: h.resMgr.(*mockResourceManager), prewarm: func(ctx context.Context, actual qviews.QueryViewKey) error {
		require.Equal(t, key, actual)
		pushLeaseDown(h, reports)
		shard.mu.Lock()
		require.EqualValues(t, 1, entry.queryRefs)
		entry.leaseExpireAt = time.Now().Add(-time.Second)
		shard.advancePendingDownLocked(key.QueryViewVersion, entry)
		require.Equal(t, qviews.QueryViewStateUp, entry.sm.State())
		shard.mu.Unlock()
		require.NoError(t, ctx.Err())
		return nil
	}}
	require.NoError(t, h.Prewarm(t.Context(), &viewpb.PrewarmOnViewRequest{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()}))
	shard.mu.Lock()
	defer shard.mu.Unlock()
	require.Zero(t, entry.queryRefs)
	require.Equal(t, qviews.QueryViewStateDown, entry.sm.State())
}
