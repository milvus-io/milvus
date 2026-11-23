package querycoordv2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	streamcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type visibilitySNResources struct{}

func (*visibilitySNResources) Acquire(req snview.AcquireResource) { go req.OnReady() }
func (*visibilitySNResources) Release(req snview.ReleaseResource) { go req.OnDropped() }

// Drive real Coord and SN state machines and keep the same replica subscriptions
// open throughout. Only QN readiness, persistence of the views and transport are simulated.
func TestClusterReplicaVisibilityAcrossCollections(t *testing.T) {
	ctx := t.Context()
	deps := loadmgrtest.Dependencies(t)
	store, err := loadmgr.RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	catalog := streamcatalog.NewCataLog(nil)
	for _, p := range []*mockey.Mocker{
		mockey.Mock(mockey.GetMethod(catalog, "SaveQueryViews")).Return(nil).Build(),
		mockey.Mock((*fakeQueryViewCatalog).SaveQueryViews).Return(nil).Build(),
		mockey.Mock((*meta.ResourceManager).ContainResourceGroup).Return(true).Build(),
	} {
		t.Cleanup(func() { p.UnPatch() })
	}
	sn := snview.RecoverPChannelSNQueryViewHandler(ctx, "p", catalog, &visibilitySNResources{}, nil)
	t.Cleanup(sn.CloseForHandoff)
	stream := sn.AcquireReplicaWatchStream(ctx, "p", nil)
	t.Cleanup(func() { _ = stream.Close() })
	transport := syncer.NewReliableSyncer(&fakeRuntimeViewSyncClient{}, nil, nil)
	t.Cleanup(func() { _ = transport.Close() })
	qnCommands := make(chan syncer.SyncView, 32)
	send := mockey.Mock(mockey.GetMethod(transport, "SyncViews")).To(func(_ context.Context, batch syncer.SyncGroup) error {
		for _, commands := range batch.ViewsByNode {
			for _, command := range commands {
				if _, ok := command.View.WorkNode().(qviews.QueryNode); ok {
					qnCommands <- command
				} else {
					sn.ApplyViews([]handler.ApplyView{{View: command.View, OnReport: func(r qviews.QueryViewAtWorkNode) { command.OnSyncResponse(r) }}})
				}
			}
		}
		return nil
	}).Build()
	t.Cleanup(func() { send.UnPatch() })
	registry, err := coordview.RecoverShardViewRegistryWithDataViews(ctx, &fakeQueryViewCatalog{}, transport, newRuntimeDataViewProvider(t), store)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	channels := func(_ context.Context, id int64) ([]string, error) {
		return []string{fmt.Sprintf("p_%dv0", id), fmt.Sprintf("p_%dv1", id)}, nil
	}
	w := &LoadConfigWatcher{s: &Server{qviewsRuntime: &qviewsRuntime{loadConfigStore: store, shardViewRegistry: registry, resources: &resource.Resources{VChannels: channels}}}, notifier: syncutil.NewAsyncTaskNotifier[struct{}]()}
	nextID := int64(100)
	manager := loadmgr.NewCollectionLoadManager(store, nil, qviewsReplicaResources{ResourceManager: &meta.ResourceManager{}, allocateID: func() (int64, error) { nextID++; return nextID, nil }})
	subscriptions := map[string]wal.ReplicaWatchSubscription{}
	expected := map[string]map[string]int64{}
	var slow qviews.ShardID
	prepare := func(id int64, shard qviews.ShardID, version uint64) {
		b := qviews.NewQueryViewAtCoordBuilderFromMetadata(shard, qviews.QueryViewBuilderMetadata{CollectionID: id, DataVersion: qviews.DataVersion{StreamingVersion: 1}, TransformStartAfterTimetick: 1}).
			SetAssignments(map[int64]map[int64][]int64{10: {20: {30}}}).SetLoadInfoVersion(version)
		require.NoError(t, registry.Ensure(shard).AddPreparing(ctx, b))
	}
	report := func(command syncer.SyncView, state viewpb.QueryViewState) {
		pb := command.View.IntoProto()
		pb.Meta.State = state
		command.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(pb))
	}
	for _, id := range []int64{1, 2} {
		require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{CollectionID: id, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: id, ResourceGroup: "old"}}}))
		require.NoError(t, manager.UpdateClusterReplicaConfig(ctx, id, 3, []string{"old", "A", "B"}))
		cs, _ := channels(ctx, id)
		for _, ch := range cs {
			sub, err := stream.Subscribe(ctx, ch, func(qviews.ReplicaSnapshot) {})
			require.NoError(t, err)
			subscriptions[ch] = sub
			expected[ch] = map[string]int64{}
			for _, r := range store.GetConfig(id).Replicas {
				shard := qviews.ShardID{ReplicaID: r.ReplicaID, VChannel: ch}
				prepare(id, shard, store.GetConfigVersion(id))
				expected[ch][r.ResourceGroup] = r.ReplicaID
				if id == 2 && ch == cs[1] && r.ResourceGroup == "B" {
					slow = shard
				}
			}
		}
	}
	var missing syncer.SyncView
	for range 12 {
		select {
		case cmd := <-qnCommands:
			if cmd.View.ShardID() == slow {
				missing = cmd
				report(cmd, viewpb.QueryViewState_QueryViewStatePreparing)
			} else {
				report(cmd, viewpb.QueryViewState_QueryViewStateReady)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("missing QN preparation command")
		}
	}
	checkRoutes := func(groups ...string) {
		t.Helper()
		require.Eventually(t, func() bool {
			for ch, sub := range subscriptions {
				snapshot := sub.Snapshot()
				if !snapshot.Valid || len(snapshot.Replicas) != len(groups) {
					return false
				}
				found := make(map[int64]bool)
				for _, r := range snapshot.Replicas {
					found[r.ReplicaID] = true
				}
				for _, group := range groups {
					if !found[expected[ch][group]] {
						return false
					}
				}
			}
			return true
		}, 5*time.Second, 10*time.Millisecond, "routing groups %v", groups)
	}
	checkRoutes("old")
	require.Eventually(t, func() bool {
		held := 0
		for _, s := range registry.Snapshot().StatsMap() {
			if s.HeldReadyLoadInfoVersion != 0 {
				held++
			}
		}
		return held == 7
	}, 5*time.Second, 10*time.Millisecond)
	w.unappliedGroups = map[string]bool{"A": true}
	w.promoteReadyGroups()
	checkRoutes("old")
	clear(w.unappliedGroups)
	w.s.qviewsRuntime.resources.VChannels = func(context.Context, int64) ([]string, error) { return nil, context.DeadlineExceeded }
	w.promoteReadyGroups()
	checkRoutes("old")
	w.s.qviewsRuntime.resources.VChannels = channels
	w.promoteReadyGroups()
	checkRoutes("old", "A")
	w.promoteReadyGroups()
	checkRoutes("old", "A")
	report(missing, viewpb.QueryViewState_QueryViewStateReady)
	w.promoteReadyGroups()
	checkRoutes("old", "A", "B")
	require.Empty(t, store.PendingVisibility())
}
