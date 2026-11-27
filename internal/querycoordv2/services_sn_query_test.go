package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewLoadInfoUsesExactSnapshots(t *testing.T) {
	ctx := context.Background()
	deps := loadmgrtest.Dependencies(t)
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}, {FieldID: 101, IndexID: 201}, {FieldID: 102, IndexID: 202}}
	definitions := append([]*indexpb.IndexInfo(nil), indexes...)
	deps.Indexes = func(_ int64, ids ...int64) []*indexpb.IndexInfo {
		if len(ids) == 0 {
			return indexes
		}
		return definitions // GetLoadInfo filters exact IDs, never current field membership.
	}
	store, err := loadmgr.RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{
		CollectionID: 1, PartitionIDs: []int64{10},
		LoadFields:     []*viewpb.LoadFieldConfig{{FieldId: 100}},
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}},
	}
	require.NoError(t, store.Put(ctx, cfg))
	first := store.GetConfigVersion(1)
	indexes = []*indexpb.IndexInfo{{FieldID: 101, IndexID: 301}}
	definitions = append(definitions, indexes...)
	require.NoError(t, store.Put(ctx, cfg))
	s := &Server{qviewsRuntime: &qviewsRuntime{loadConfigStore: store}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	historical, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: first})
	require.NoError(t, err)
	require.NoError(t, merr.Error(historical.Status))
	require.Len(t, historical.IndexInfoList, 2, "history includes required fields outside preload hints")
	require.EqualValues(t, 200, historical.IndexInfoList[0].IndexID)
	latest, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.NoError(t, merr.Error(latest.Status))
	require.Len(t, latest.IndexInfoList, 1)
	require.EqualValues(t, 301, latest.IndexInfoList[0].IndexID)
	require.Greater(t, latest.Version, first)
	store.StopPlanning(1)
	historical, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: first})
	require.NoError(t, err)
	require.NoError(t, merr.Error(historical.Status))
	latest, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(latest.Status), merr.ErrCollectionNotLoaded)
	missing, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 999})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(missing.Status), merr.ErrServiceNotReady)
	for _, id := range []int64{0, 2} {
		resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: id})
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.Status))
	}
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	latest, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.Error(t, merr.Error(latest.Status))
}

func TestQueryViewLoadConfigFeedsAdmissionChecks(t *testing.T) {
	cfg := &loadmgr.LoadConfig{
		CollectionID: 1, DbID: 2, PartitionIDs: []int64{10, 11}, UserSpecifiedReplicaMode: true,
		LoadFields:     []*viewpb.LoadFieldConfig{{FieldId: 100, IndexIds: []int64{200}}},
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 3, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaID: 4, ResourceGroup: "rg2", Priority: commonpb.LoadPriority_LOW},
		},
	}
	current := qviewsCurrentLoadConfig(cfg)
	require.Equal(t, int64(1), current.Collection.GetCollectionID())
	require.True(t, current.Collection.GetUserSpecifiedReplicaMode())
	require.Equal(t, map[int64]int64{100: 200}, current.Collection.GetFieldIndexID())
	require.Equal(t, loadmgr.FieldsToMessage(cfg.RequiredFields), current.Collection.GetRequiredFields())
	current.Collection.RequiredFields[0].FieldId++
	require.EqualValues(t, 100, cfg.RequiredFields[0].FieldId, "legacy projection does not mutate the immutable View config")
	require.ElementsMatch(t, []int64{10, 11}, current.GetPartitionIDs())
	require.Equal(t, map[string]int{"rg1": 1, "rg2": 1}, current.GetReplicaNumber())
	require.Equal(t, commonpb.LoadPriority_LOW, current.Replicas[4].LoadPriority())
	require.Nil(t, qviewsCurrentLoadConfig(nil).Collection)
}

func TestServingLoadInfoUsesOnePublicationSnapshot(t *testing.T) {
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{10}}))
	oldVersion := store.GetConfigVersion(1)
	provider := &fakeRuntimeDataViewProvider{}
	data := &viewpb.DataViewOfCollection{CollectionId: 1, ShardSetEpoch: 2, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: "p_1v0"}}}
	get := mockey.Mock((*fakeRuntimeDataViewProvider).Get).To(func(_ *fakeRuntimeDataViewProvider, ctx context.Context, _ int64, _ *viewpb.DataVersion) (qviews.DataViewRef, error) {
		// Load changes after the first latest-load lookup but before publication.
		require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{20}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 3}}}))
		return &runtimeDataViewRef{view: data}, nil
	}).Build()
	defer get.UnPatch()
	resolve := mockey.Mock((*coordview.ShardViewRegistry).ResolveServingGeneration).To(func(_ *coordview.ShardViewRegistry, _ context.Context, got *viewpb.DataViewOfCollection, version uint64, replicas []int64) (*viewpb.ServingGeneration, error) {
		require.Same(t, data, got)
		require.Greater(t, version, oldVersion)
		require.Equal(t, []int64{3}, replicas)
		return &viewpb.ServingGeneration{CollectionId: 1, LoadInfoVersion: version, ReadSchemaVersion: 0, ShardSetEpoch: 2, Shards: data.Shards}, nil
	}).Build()
	defer resolve.UnPatch()
	balance := cache.New(balancer.DefaultBalanceConfig())
	balance.PublishDataView(1, api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 1, DataVersion: qviews.DataVersion{StreamingVersion: 1}, ShardSetEpoch: 2, Shards: []*api.ShardDataView{{VChannel: "p_1v0"}}}))
	runtime := &qviewsRuntime{loadConfigStore: store, balanceCache: balance, shardViewRegistry: &coordview.ShardViewRegistry{}, resources: &qvresource.Resources{DataViews: provider}}
	server := &Server{qviewsRuntime: runtime}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	response, err := server.GetQueryViewLoadInfo(t.Context(), &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.NoError(t, merr.Error(response.Status))
	require.Greater(t, response.Version, oldVersion)
	require.Equal(t, []int64{20}, response.PartitionIDs)
	require.EqualValues(t, 2, response.ShardSetEpoch)
	require.Zero(t, response.ReadSchemaVersion)
	require.Equal(t, data.Shards, response.ServingShards)
	cfg := store.GetConfig(1)
	store.StopPlanning(1)
	_, err = runtime.resolveServingGeneration(t.Context(), data, cfg)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable, "Release invalidates the captured load config before publication")
}
