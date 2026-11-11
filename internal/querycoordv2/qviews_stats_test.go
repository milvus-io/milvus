package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/shardstats"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQViewsStatsRecoveryFailureCleanup(t *testing.T) {
	mockey.PatchConvey("registry recovery failure closes stats and discovery", t, func() {
		paramtable.Init()
		catalog := queryview.NewCollectionLoadInfoCatalog(nil)
		mockey.Mock(mockey.GetMethod(catalog, "List")).Return([]*viewpb.CollectionLoadConfig{{CollectionID: 1, Version: 1, Replicas: []*viewpb.LoadReplicaConfig{{ReplicaId: 7}}}}, nil).Build()
		started := make(chan struct{})
		mockey.Mock((*fakeQueryViewCatalog).ListQueryViews).To(func(*fakeQueryViewCatalog, context.Context) ([]*viewpb.QueryViewOfShard, error) {
			<-started
			return nil, context.Canceled
		}).Build()
		var original func(*loadmgr.LoadConfigStore) *shardstats.ReplicaStatsStore
		var store *shardstats.ReplicaStatsStore
		id := qviews.ShardID{ReplicaID: 7, VChannel: "p_1v0"}
		mockey.Mock(shardstats.New).Origin(&original).To(func(configs *loadmgr.LoadConfigStore) *shardstats.ReplicaStatsStore {
			store = original(configs)
			store.OnShardStats([]qviews.ShardStats{{ShardID: id, RequestCount: 1}})
			_, found := store.GetShardStats(id)
			require.True(t, found)
			return store
		}).Build()
		stopped := make(chan struct{})
		mockey.Mock((*fakeRuntimeViewSyncClient).WatchPChannels).To(func(_ *fakeRuntimeViewSyncClient, ctx context.Context, _ func([]string)) error {
			defer close(stopped)
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}).Build()
		runtime, err := newQViewsRuntime(t.Context(), qviewsRuntimeDependencies{
			loadConfigDependencies: loadmgr.LoadConfigStoreDependencies{Catalog: catalog},
			queryViewCatalog:       &fakeQueryViewCatalog{}, viewSyncClient: &fakeRuntimeViewSyncClient{},
			queryNodeClient: &fakeRuntimeQueryNodeClient{}, resourceGroupManager: &fakeRuntimeResourceGroupManager{},
			dataViewProvider: newRuntimeDataViewProvider(t),
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, runtime)
		require.NotNil(t, store)
		store.OnShardStats([]qviews.ShardStats{{ShardID: id, RequestCount: 2}})
		_, found := store.GetShardStats(id)
		require.False(t, found)
		select {
		case <-stopped:
		default:
			t.Fatal("runtime returned before discovery stopped")
		}
	})
}
