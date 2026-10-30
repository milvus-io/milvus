package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAlterCallbacksApplyLoadConfigDirectly(t *testing.T) {
	for _, database := range []bool{false, true} {
		name := "collection"
		if database {
			name = "database"
		}
		t.Run(name, func(t *testing.T) {
			var persistErr error
			for _, p := range []*mockey.Mocker{
				mockey.Mock(catalogkv.Catalog.GetCollections).Return(nil, nil).Build(),
				mockey.Mock(catalogkv.Catalog.GetPartitions).Return(nil, nil).Build(),
				mockey.Mock(catalogkv.Catalog.GetReplicas).Return(nil, nil).Build(),
				mockey.Mock(catalogkv.Catalog.SaveCollection).To(func(catalogkv.Catalog, context.Context, *querypb.CollectionLoadInfo, ...*querypb.PartitionLoadInfo) error {
					return persistErr
				}).Build(),
				mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
				mockey.Mock((*MetaTable).AlterCollection).Return(nil).Build(),
				mockey.Mock((*MetaTable).AlterDatabase).Return(nil).Build(),
				mockey.Mock((*DDLCallback).ExpireCaches).Return(nil).Build(),
			} {
				t.Cleanup(func() { p.UnPatch() })
			}
			store, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgrtest.Dependencies(t))
			require.NoError(t, err)
			cfg := &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{10}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 11, ResourceGroup: "rg"}}}
			require.NoError(t, store.Put(t.Context(), cfg))
			published := 0
			store.RegisterObserver(func(id int64, released bool) {
				require.EqualValues(t, 1, id)
				require.False(t, released)
				require.Len(t, store.GetConfig(id).Replicas, 2)
				published++
			})
			allocated := int64(11)
			for _, p := range []*mockey.Mocker{
				mockey.Mock((*testReplicaResources).ContainResourceGroup).Return(true).Build(),
				mockey.Mock((*testReplicaResources).AllocateReplicaID).To(func(*testReplicaResources, context.Context) (int64, error) {
					allocated++
					return allocated, nil
				}).Build(),
			} {
				t.Cleanup(func() { p.UnPatch() })
			}
			r := &qvresource.Resources{LoadConfigs: store, LoadManager: loadmgr.NewCollectionLoadManager(store, nil, &testReplicaResources{})}
			p := mockey.Mock(qvresource.Resource).Return(r).Build()
			t.Cleanup(func() { p.UnPatch() })
			// No MixCoord client or broadcaster is installed. The callbacks must
			// use the existing manager directly under their enclosing DDL guard.
			c := &DDLCallback{Core: &Core{meta: &MetaTable{}, broker: &mockBroker{
				BroadcastAlteredCollectionFunc: func(context.Context, int64) error { return nil },
			}}}
			channel := funcutil.GetControlChannel("test")
			apply := func() error {
				if database {
					msg := message.NewAlterDatabaseMessageBuilderV2().WithHeader(&messagespb.AlterDatabaseMessageHeader{DbId: 1, DbName: "db"}).
						WithBody(&messagespb.AlterDatabaseMessageBody{AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterDatabase{
							CollectionIds: []int64{1, 2}, ReplicaNumber: 2, ResourceGroups: []string{"rg"},
						}}).WithControlChannelBroadcast().MustBuildBroadcast()
					return c.alterDatabaseV1AckCallback(t.Context(), message.BroadcastResultAlterDatabaseMessageV2{
						Message: message.MustAsBroadcastAlterDatabaseMessageV2(msg), Results: map[string]*message.AppendResult{channel: {}},
					})
				}
				msg := message.NewAlterCollectionMessageBuilderV2().WithHeader(&messagespb.AlterCollectionMessageHeader{CollectionId: 1}).
					WithBody(&messagespb.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{
						AlterLoadConfig: &messagespb.AlterLoadConfigOfAlterCollection{ReplicaNumber: 2, ResourceGroups: []string{"rg"}},
					}}).WithControlChannelBroadcast().MustBuildBroadcast()
				return c.alterCollectionV2AckCallback(t.Context(), message.BroadcastResultAlterCollectionMessageV2{Message: message.MustAsBroadcastAlterCollectionMessageV2(msg)})
			}
			persistErr = merr.WrapErrServiceUnavailableMsg("catalog unavailable")
			require.ErrorIs(t, apply(), persistErr)
			require.Len(t, store.GetConfig(1).Replicas, 1)
			require.Zero(t, published)
			persistErr = nil
			require.NoError(t, apply())
			require.NoError(t, apply(), "callback replay is a no-op")
			require.Equal(t, 1, published)
			require.EqualValues(t, 13, allocated, "failed persistence may consume an ID; successful replay allocates none")
			require.Equal(t, []int64{10}, store.GetConfig(1).PartitionIDs)
			require.False(t, store.Contains(2), "ALTER must not load an unloaded collection")
		})
	}
}

// Dependency implementations are patched with mockey in each test.
type testReplicaResources struct{}

func (*testReplicaResources) AllocateReplicaID(context.Context) (int64, error) {
	panic("mock with mockey")
}

func (*testReplicaResources) ContainResourceGroup(context.Context, string) bool {
	panic("mock with mockey")
}

func mockReplicaConfigUpdate(t *testing.T, err error) {
	t.Helper()
	for _, p := range []*mockey.Mocker{
		mockey.Mock(qvresource.Resource).Return(&qvresource.Resources{LoadManager: &loadmgr.CollectionLoadManager{}}).Build(),
		mockey.Mock((*loadmgr.CollectionLoadManager).UpdateReplicaConfig).Return(err).Build(),
	} {
		t.Cleanup(func() { p.UnPatch() })
	}
}
