package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQViewsFeedbackRecoveryFailureCleanup(t *testing.T) {
	mockey.PatchConvey("registry recovery failure closes feedback and initial streams", t, func() {
		paramtable.Init()
		catalog := queryview.NewCollectionLoadInfoCatalog(nil)
		mockey.Mock(mockey.GetMethod(catalog, "List")).Return([]*viewpb.CollectionLoadConfig{{CollectionID: 1, Version: 1, Replicas: []*viewpb.LoadReplicaConfig{{ReplicaId: 7}}}}, nil).Build()
		started := make(chan struct{})
		mockey.Mock((*fakeQueryViewCatalog).ListQueryViews).To(func(*fakeQueryViewCatalog, context.Context) ([]*viewpb.QueryViewOfShard, error) {
			<-started
			return nil, context.Canceled
		}).Build()
		var original func(*loadmgr.LoadConfigStore) *viewfeedback.ReplicaFeedbackStore
		var store *viewfeedback.ReplicaFeedbackStore
		id := qviews.ShardID{ReplicaID: 7, VChannel: "p_1v0"}
		mockey.Mock(viewfeedback.New).Origin(&original).To(func(configs *loadmgr.LoadConfigStore) *viewfeedback.ReplicaFeedbackStore {
			store = original(configs)
			store.OnViewFeedback(runtimeTestFeedbackWindow(id, 1))
			_, found := store.GetViewFeedback(id)
			require.True(t, found)
			return store
		}).Build()
		stopped := make(chan struct{})
		mockey.Mock((*fakeRuntimeViewSyncClient).OpenSyncStream).To(func(_ *fakeRuntimeViewSyncClient, ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			defer close(stopped)
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build()
		runtime, err := newQViewsRuntime(t.Context(), qviewsRuntimeDependencies{
			loadConfigDependencies: loadmgr.LoadConfigStoreDependencies{Catalog: catalog},
			pchannels:              []string{"p"},
			queryViewCatalog:       &fakeQueryViewCatalog{}, viewSyncClient: &fakeRuntimeViewSyncClient{},
			queryNodeClient: &fakeRuntimeQueryNodeClient{}, resourceGroupManager: &fakeRuntimeResourceGroupManager{},
			dataViewProvider: newRuntimeDataViewProvider(t),
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, runtime)
		require.NotNil(t, store)
		store.OnViewFeedback(runtimeTestFeedbackWindow(id, 2))
		_, found := store.GetViewFeedback(id)
		require.False(t, found)
		select {
		case <-stopped:
		default:
			t.Fatal("runtime returned before initial stream stopped")
		}
	})
}

func runtimeTestFeedbackWindow(id qviews.ShardID, count uint64) qviews.ViewFeedbackWindow {
	now := time.Now().UnixNano()
	return qviews.ViewFeedbackWindow{StartUnixNanos: now - int64(time.Second), EndUnixNanos: now, Shards: map[qviews.ShardID]qviews.ViewFeedback{id: {ShardID: id, RequestCount: count}}}
}
