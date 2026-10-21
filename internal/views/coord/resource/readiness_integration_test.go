package resource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type refreshSource struct{}

func (*refreshSource) RefreshDataView(context.Context, int64) (*viewpb.DataVersion, error) {
	panic("mock with mockey")
}

func TestReadinessResourceIntegration(t *testing.T) {
	r := managementResources(t)
	r.RefreshVersions = &sync.Map{}
	r.LoadManager = loadmgr.NewCollectionLoadManager(r.LoadConfigs, nil)
	r.Reconciler = &refreshSource{}
	ready := false
	var duringSnapshot func()
	snapshot := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, shards []qviews.ShardID) *coordview.ShardViewSnapshot {
		stats := map[qviews.ShardID]*coordview.ShardStats{}
		if ready {
			for _, shard := range shards {
				stats[shard] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 2}}, UpLoadInfoVersion: r.LoadConfigs.GetConfigVersion(1)}
			}
		}
		result := coordview.NewShardViewSnapshot(1, stats)
		if duringSnapshot != nil {
			duringSnapshot()
		}
		return result
	}).Build()
	defer snapshot.UnPatch()
	refresh := mockey.Mock((*refreshSource).RefreshDataView).Return(&viewpb.DataVersion{StreamingVersion: 2}, nil).Build()
	defer refresh.UnPatch()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.Refresh(ctx, 1), context.DeadlineExceeded, "refresh waits for the committed version to serve")
	ready = true
	require.NoError(t, r.Refresh(t.Context(), 1))
	require.NoError(t, r.WaitDataVersion(t.Context(), 1, qviews.DataVersion{StreamingVersion: 1}))
	version, ok := r.RefreshVersions.Load(int64(1))
	require.True(t, ok)
	require.Equal(t, qviews.DataVersion{StreamingVersion: 2}, version, "older refresh must not lower the fence")
	require.NoError(t, r.AddPartition(t.Context(), 1, 10))
	require.NoError(t, r.AddPartition(t.Context(), 1, 10))
	require.Equal(t, []int64{10}, r.LoadConfigs.GetConfig(1).PartitionIDs)
	channels := []string{"v0", "v1"}
	require.NoError(t, loadstatus.WaitAtSchema(t.Context(), r.LoadConfigs, r.ShardViews, r.Readiness, 1, channels, 0))
	ctx, cancelSchema := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelSchema()
	require.ErrorIs(t, loadstatus.Wait(ctx, r.LoadConfigs, r.ShardViews, r.Readiness, 1, channels, 1, qviews.DataVersion{}), context.DeadlineExceeded)
	cfg := r.LoadConfigs.GetConfig(1)
	duringSnapshot = func() {
		require.NoError(t, r.LoadConfigs.Remove(t.Context(), 1))
		require.NoError(t, r.LoadConfigs.Put(t.Context(), cfg))
	}
	require.ErrorIs(t, loadstatus.WaitAtSchema(t.Context(), r.LoadConfigs, r.ShardViews, r.Readiness, 1, channels, 0), merr.ErrCollectionNotLoaded, "coalesced release/reload invalidates the earlier waiter")
	duringSnapshot = nil
	require.NoError(t, r.LoadConfigs.Remove(t.Context(), 1))
	require.NoError(t, r.WaitDataVersion(t.Context(), 1, qviews.DataVersion{StreamingVersion: 3}))
	require.NoError(t, r.AddPartition(t.Context(), 1, 11), "partition creation must not reload a released collection")
	require.False(t, r.LoadConfigs.Contains(1))
	require.ErrorIs(t, loadstatus.WaitAtSchema(t.Context(), r.LoadConfigs, r.ShardViews, r.Readiness, 1, channels, 0), merr.ErrCollectionNotLoaded)
	ctx, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	require.ErrorIs(t, r.Refresh(ctx, 1), context.Canceled)
	require.ErrorIs(t, loadstatus.WaitAtSchema(ctx, r.LoadConfigs, r.ShardViews, r.Readiness, 1, channels, 0), context.Canceled)
	r.Readiness.Close()
	require.ErrorIs(t, loadstatus.WaitAtSchema(t.Context(), r.LoadConfigs, r.ShardViews, r.Readiness, 1, channels, 0), merr.ErrServiceUnavailable)
}
