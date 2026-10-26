package snview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestUnknownTeardownDetachesEmptyShard(t *testing.T) {
	for _, state := range []viewpb.QueryViewState{viewpb.QueryViewState_QueryViewStateDown, viewpb.QueryViewState_QueryViewStateDropped} {
		t.Run(state.String(), func(t *testing.T) {
			h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), newMockResourceManager(), nil)
			view := newSNViewWithState(1, state)
			stale := h.getOrCreateShard(view.ShardID())
			reports := &reportCollector{}
			h.ApplyViews([]handler.ApplyView{{View: view, OnReport: reports.onReport}})
			require.Equal(t, qviews.QueryViewStateDropped, reports.last().State())
			require.Empty(t, h.shards)
			require.True(t, stale.detached)
			require.False(t, stale.ApplyViews([]handler.ApplyView{{View: newPreparingSNView(1)}}))
			h.ApplyViews([]handler.ApplyView{{View: newPreparingSNView(1)}})
			replacement := h.shards[view.ShardID()]
			require.NotSame(t, stale, replacement)
			stale.onEmpty(stale)
			require.Same(t, replacement, h.shards[view.ShardID()])
		})
	}
}

func TestUnknownReplicaSkipsShardsWithoutUpViews(t *testing.T) {
	h, up, _, _, _ := leasedUpView(t, 0)
	preparing := newPreparingSNView(1).IntoProto()
	preparing.Meta.ReplicaId = up.shardID.ReplicaID + 1
	h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(preparing)}})
	unknown := qviews.ShardID{ReplicaID: qviews.UnknownReplicaID, VChannel: up.shardID.VChannel}
	for i := 0; i < 32; i++ {
		lease, err := h.AcquireLatestUpView(context.Background(), unknown)
		require.NoError(t, err)
		require.Equal(t, up.shardID.ReplicaID, lease.Meta.GetReplicaId())
		lease.Release()
	}
	_, err := h.AcquireLatestUpView(context.Background(), qviews.NewShardIDFromQVMeta(preparing.Meta))
	require.True(t, viewerror.AsViewError(err).IsViewNotFound(), "an explicit replica must not fall back")
	_, err = h.AcquireLatestUpView(context.Background(), qviews.ShardID{ReplicaID: preparing.Meta.ReplicaId + 1, VChannel: unknown.VChannel})
	require.True(t, viewerror.AsViewError(err).IsViewNotFound(), "a missing explicit replica must not fall back")
	delete(h.shards, up.shardID)
	_, err = h.AcquireLatestUpView(context.Background(), unknown)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = h.AcquireLatestUpView(ctx, unknown)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMissingQueryRuntimeInvalidatesView(t *testing.T) {
	h, shard, _, _, _ := leasedUpView(t, 0)
	patch := mockey.Mock((*mockResourceManager).QueryRuntime).Return(nil, false).Build()
	defer patch.UnPatch()
	_, err := h.queryRuntime(qviews.QueryViewKey{ShardID: shard.shardID, QueryViewVersion: newPreparingSNView(1).QueryViewKey().QueryViewVersion})
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	require.NotContains(t, err.Error(), "%s")
}
