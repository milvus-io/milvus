package loadmgr

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func newEmptyLoadConfigStore(t *testing.T, catalog catalogkv.Catalog) *LoadConfigStore {
	t.Helper()
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetCollections).Return(nil, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetReplicas).Return(nil, nil), 1)
	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	return store
}

func sampleAlterLoadConfigHeader() *messagespb.AlterLoadConfigMessageHeader {
	return &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     1,
		CollectionId:             100,
		PartitionIds:             []int64{10},
		LoadFields:               []*messagespb.LoadFieldConfig{{FieldId: 200, IndexId: 300}},
		UserSpecifiedReplicaMode: true,
		Replicas: []*messagespb.LoadReplicaConfig{
			{ReplicaId: 1000, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaId: 1001, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
		},
	}
}

func sampleAlterLoadConfigResult() message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := funcutil.GetControlChannel("test")
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(sampleAlterLoadConfigHeader()).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{controlChannel: {}},
	}
}

func TestCollectionLoadManager_UpdateLoadConfig(t *testing.T) {
	catalog := catalogkv.NewCatalog(nil)
	store := newEmptyLoadConfigStore(t, catalog)
	var notified []int64
	manager := NewCollectionLoadManager(
		store,
		func(collectionID int64) { notified = append(notified, collectionID) },
	)

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil), 1)

	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigResult()))

	cfg := store.Snapshot().ConfigsMap()[100]
	require.NotNil(t, cfg)
	require.Len(t, cfg.Replicas, 2)
	assert.Equal(t, int64(1000), cfg.Replicas[0].ReplicaID)
	assert.Equal(t, "rg1", cfg.Replicas[0].ResourceGroup)
	assert.Equal(t, []int64{100}, notified)
}

func TestCollectionLoadManager_ReleaseCollectionKeepsRegistryForReconcile(t *testing.T) {
	catalog := catalogkv.NewCatalog(nil)
	store := newEmptyLoadConfigStore(t, catalog)
	var notified []int64
	manager := NewCollectionLoadManager(
		store,
		func(collectionID int64) { notified = append(notified, collectionID) },
	)

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil), 1)
	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigResult()))

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseReplicas).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseCollection).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	require.NoError(t, manager.ReleaseCollection(context.Background(), &messagespb.DropLoadConfigMessageHeader{
		CollectionId: 100,
	}))

	assert.NotContains(t, store.Snapshot().ConfigsMap(), int64(100))
	assert.Equal(t, []int64{100, 100}, notified)
}
