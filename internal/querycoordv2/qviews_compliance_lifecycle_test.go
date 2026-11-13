package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Only transport and persistence are mocked; the registry and state machines
// publish the actual snapshots consumed by the production compliance check.
type complianceSyncer struct{ syncer.ReliableSyncer }

func (*complianceSyncer) SyncViews(context.Context, syncer.SyncGroup) error { return nil }

func TestQueryViewComplianceCleanupLifecycle(t *testing.T) {
	for _, growingOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "sealed", true: "growing only"}[growingOnly], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			pending := make(chan syncer.SyncView, 16)
			transport := mockey.Mock((*complianceSyncer).SyncViews).To(func(_ *complianceSyncer, _ context.Context, group syncer.SyncGroup) error {
				for _, views := range group.ViewsByNode {
					for _, view := range views {
						pending <- view
					}
				}
				return nil
			}).Build()
			t.Cleanup(func() { transport.UnPatch() })
			deleting, allowDelete := make(chan struct{}, 1), make(chan struct{})
			persist := mockey.Mock((*fakeQueryViewCatalog).SaveQueryViews).To(func(_ *fakeQueryViewCatalog, ctx context.Context, views []*viewpb.QueryViewOfShard) error {
				for _, view := range views {
					if view.Meta.State == viewpb.QueryViewState_QueryViewStateDropped {
						deleting <- struct{}{}
						select {
						case <-allowDelete:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return nil
			}).Build()
			t.Cleanup(func() { persist.UnPatch() })
			registry, err := coordview.RecoverShardViewRegistry(ctx, &fakeQueryViewCatalog{}, &complianceSyncer{})
			require.NoError(t, err)
			t.Cleanup(registry.Close)
			// Ensure a failed assertion also unblocks the background persistence.
			t.Cleanup(func() { close(allowDelete) })
			store, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
			require.NoError(t, err)
			require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 11, ResourceGroup: "blue"}}}))
			s := &Server{meta: &meta.Meta{ResourceManager: &meta.ResourceManager{}}, qviewsRuntime: &qviewsRuntime{loadConfigStore: store, shardViewRegistry: registry}}
			rg := mockey.Mock((*meta.ResourceManager).GetResourceGroupByNodeID).Return("blue").Build()
			t.Cleanup(func() { rg.UnPatch() })
			shard := qviews.ShardID{ReplicaID: 11, VChannel: "p_1v0"}
			builder := qviews.NewQueryViewAtCoordBuilder(11, &viewpb.DataViewOfCollection{
				CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1},
				Shards: []*viewpb.DataViewOfShard{{Vchannel: shard.VChannel}},
			}, shard.VChannel).SetLoadInfoVersion(store.GetConfigVersion(1))
			nodes := 1
			want := map[string]int{"": 1}
			if !growingOnly {
				builder.SetAssignments(map[int64]map[int64][]int64{1: {10: {100}}})
				nodes = 2
				want = map[string]int{"blue": 1}
			}
			manager := registry.Ensure(shard)
			require.NoError(t, manager.AddPreparing(ctx, builder))
			report := func(expected, response qviews.QueryViewState) {
				t.Helper()
				select {
				case sync := <-pending:
					require.Equal(t, expected, sync.View.State())
					view := sync.View.IntoProto()
					view.Meta.State = viewpb.QueryViewState(response)
					sync.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(view))
				case <-ctx.Done():
					t.Fatal("timed out waiting for view sync")
				}
			}
			for range nodes {
				report(qviews.QueryViewStatePreparing, qviews.QueryViewStateReady)
			}
			report(qviews.QueryViewStateUp, qviews.QueryViewStateUp)
			require.NotNil(t, manager.Stats().UpVersion)
			cfg := store.GetConfig(1).Clone()
			cfg.Replicas = []*loadmgr.ReplicaAssignment{{ReplicaID: 22, ResourceGroup: "green"}}
			require.NoError(t, store.Put(ctx, cfg))
			check := func() {
				t.Helper()
				require.Equal(t, want, s.GetLeakedResourcesByCollectionPerRG(ctx, 1, nil))
			}
			check()
			require.NoError(t, manager.RequestRelease(ctx))
			check() // Down is still retained.
			report(qviews.QueryViewStateDown, qviews.QueryViewStateDown)
			check() // Dropping is still retained, even though Segments is empty.
			require.Empty(t, manager.Stats().Segments)
			for range nodes {
				report(qviews.QueryViewStateDropped, qviews.QueryViewStateDropped)
			}
			select {
			case <-deleting:
			case <-ctx.Done():
				t.Fatal("final deletion was not attempted")
			}
			check() // Dropped awaits durable catalog deletion.
			allowDelete <- struct{}{}
			require.Eventually(t, func() bool {
				return len(s.GetLeakedResourcesByCollectionPerRG(ctx, 1, nil)) == 0
			}, 5*time.Second, time.Millisecond)
		})
	}
}
