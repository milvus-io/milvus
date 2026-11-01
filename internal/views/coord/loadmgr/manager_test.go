package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

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
	store, _ := newTestStore(t)
	var notified []int64
	manager := NewCollectionLoadManager(store, func(id int64) { notified = append(notified, id) }, nil)
	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigResult()))
	cfg := store.GetConfig(100)
	require.Len(t, cfg.Replicas, 2)
	assert.Equal(t, int64(1000), cfg.Replicas[0].ReplicaID)
	assert.Equal(t, []int64{100}, notified)
}

func TestCollectionLoadManager_ReleaseCollectionKeepsRegistryForReconcile(t *testing.T) {
	store, _ := newTestStore(t)
	manager := NewCollectionLoadManager(store, nil, nil)
	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigResult()))
	version := store.GetConfigVersion(100)
	require.NoError(t, manager.ReleaseCollection(context.Background(), &messagespb.DropLoadConfigMessageHeader{CollectionId: 100}))
	require.False(t, store.Contains(100))
	_, err := store.GetLoadInfo(t.Context(), 100, version)
	require.NoError(t, err, "Release must retain metadata until node cleanup")
}
