package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestColdLoadWarmupWaitsForReadinessWithoutGatingQueries(t *testing.T) {
	paramtable.Init()
	s, _, sync := newReadinessTestServer(t)
	r := s.qviewsRuntime
	t.Cleanup(r.stopWarmups)
	channels := []string{"by-dev-rootcoord-dml_100v0"}
	cfg := r.loadConfigStore.GetConfig(100)
	calls := make(chan *viewpb.PrewarmOnViewRequest, 2)
	finish := make(chan struct{})
	r.prewarm = func(ctx context.Context, _ qviews.WorkNode, req *viewpb.PrewarmOnViewRequest) error {
		calls <- req
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.warmCollectionOnce(t.Context(), 100, channels, r.readyChanges.Subscribe(100))
	require.Never(t, func() bool { return len(sync.sent) != 0 }, 50*time.Millisecond, time.Millisecond)
	id := qviews.ShardID{ReplicaID: 1000, VChannel: channels[0]}
	builder := qviews.NewQueryViewAtCoordBuilder(1000, &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1},
		Shards: []*viewpb.DataViewOfShard{{Vchannel: channels[0]}},
	}, channels[0]).SetLoadInfoVersion(cfg.Version)
	require.NoError(t, r.shardViewRegistry.Ensure(id).AddPreparing(t.Context(), builder))
	preparing := receiveReadinessSync(t, sync, qviews.QueryViewStatePreparing)
	ready := preparing.View.IntoProto()
	ready.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(ready))
	up := receiveReadinessSync(t, sync, qviews.QueryViewStateUp)
	up.OnSyncResponse(up.View)
	select {
	case req := <-calls:
		require.Equal(t, preparing.View.IntoProto().GetMeta().GetVersion(), req.GetVersion(), "prewarm uses the existing serving View")
	case <-time.After(3 * time.Second):
		t.Fatal("prewarm request not dispatched")
	}
	require.True(t, loadstatus.Get(r.loadConfigStore, r.shardViewRegistry, 100, channels).Ready(), "query readiness does not wait for prewarm")
	require.Same(t, cfg, r.loadConfigStore.GetConfig(100))
	close(finish)
	r.warmWG.Wait()
}

func TestPendingWarmupDoesNotSurviveReleaseReloadOrShutdown(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "release and reload"}[release], func(t *testing.T) {
			paramtable.Init()
			s, _, sync := newReadinessTestServer(t)
			r := s.qviewsRuntime
			t.Cleanup(r.stopWarmups)
			sub := r.readyChanges.Subscribe(100)
			if release {
				cfg := r.loadConfigStore.GetConfig(100).Clone()
				r.loadConfigStore.StopPlanning(100)
				require.NoError(t, r.loadConfigStore.Put(t.Context(), cfg))
			}
			r.warmCollectionOnce(t.Context(), 100, []string{"by-dev-rootcoord-dml_100v0"}, sub)
			if !release {
				r.stopWarmups()
			}
			done := make(chan struct{})
			go func() { r.warmWG.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("withdrawn warmup remained pending")
			}
			require.Empty(t, sync.sent)
		})
	}
}

func TestRunningWarmupIgnoresViewChangesButStopsOnRelease(t *testing.T) {
	paramtable.Init()
	s, _, _ := newReadinessTestServer(t, testPersistedQueryView(100, qviews.ShardID{ReplicaID: 1000, VChannel: "by-dev-rootcoord-dml_100v0"}))
	r := s.qviewsRuntime
	t.Cleanup(r.stopWarmups)
	started := make(chan context.Context, 1)
	r.prewarm = func(ctx context.Context, _ qviews.WorkNode, _ *viewpb.PrewarmOnViewRequest) error {
		started <- ctx
		<-ctx.Done()
		return ctx.Err()
	}
	r.warmCollectionOnce(t.Context(), 100, []string{"by-dev-rootcoord-dml_100v0"}, r.readyChanges.Subscribe(100))
	var warmCtx context.Context
	select {
	case warmCtx = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("prewarm was not dispatched")
	}
	r.readyChanges.Notify(100)
	require.Never(t, func() bool { return warmCtx.Err() != nil }, 30*time.Millisecond, time.Millisecond)
	r.loadConfigStore.StopPlanning(100)
	done := make(chan struct{})
	go func() { r.warmWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("release did not cancel prewarm")
	}
	require.ErrorIs(t, warmCtx.Err(), context.Canceled)
}
