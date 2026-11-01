package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
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
