package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewLoadInfoUsesExactSnapshots(t *testing.T) {
	ctx := context.Background()

	s := &Server{qviewsRuntime: &qviewsRuntime{loadConfigStore: &loadmgr.LoadConfigStore{}, loadInfos: &loadmgr.LoadInfoStore{}}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	released := false
	patch := mockey.Mock((*loadmgr.LoadConfigStore).GetConfigWithVersion).To(func(_ *loadmgr.LoadConfigStore, id int64) (*loadmgr.LoadConfig, uint64) {
		if id != 1 || released {
			return nil, 0
		}
		return &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{10}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 102}}}, 3
	}).Build()
	defer patch.UnPatch()
	snapshot := mockey.Mock((*loadmgr.LoadInfoStore).Get).To(func(_ *loadmgr.LoadInfoStore, _ context.Context, id int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		if version != 3 && version != 2 {
			return nil, merr.WrapErrServiceNotReadyMsg("snapshot missing")
		}
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: id, Version: version, PartitionIDs: []int64{10}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 102}}}, nil
	}).Build()
	defer snapshot.UnPatch()
	historical, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 2})
	require.NoError(t, err)
	require.NoError(t, merr.Error(historical.Status))
	require.EqualValues(t, 2, historical.Version, "historical views must not be rejected because desired config advanced")
	resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 3})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, int64(1), resp.GetCollectionID())
	require.Equal(t, []int64{10}, resp.GetPartitionIDs())
	require.Len(t, resp.GetLoadFields(), 2)
	require.Equal(t, int64(100), resp.LoadFields[0].FieldId)
	require.Equal(t, int64(102), resp.LoadFields[1].FieldId)
	require.Equal(t, uint64(3), resp.GetVersion(), "return the requested current configuration")
	stale, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(stale.GetStatus()), merr.ErrServiceNotReady)
	for _, id := range []int64{0, 2} {
		resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: id})
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.GetStatus()))
	}
	released = true
	historical, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 2})
	require.NoError(t, err)
	require.NoError(t, merr.Error(historical.Status), "live old views retain their snapshot after release")
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.Status), merr.ErrCollectionNotLoaded)
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.GetStatus()))
}

func TestQueryViewLoadConfigFeedsAdmissionChecks(t *testing.T) {
	cfg := &loadmgr.LoadConfig{
		CollectionID: 1, DbID: 2, PartitionIDs: []int64{10, 11}, UserSpecifiedReplicaMode: true,
		LoadFields:     []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 200}},
		RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 3, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaID: 4, ResourceGroup: "rg2", Priority: commonpb.LoadPriority_LOW},
		},
	}
	current := qviewsCurrentLoadConfig(cfg)
	require.Equal(t, int64(1), current.Collection.GetCollectionID())
	require.True(t, current.Collection.GetUserSpecifiedReplicaMode())
	require.Equal(t, map[int64]int64{100: 200}, current.Collection.GetFieldIndexID())
	require.Equal(t, cfg.RequiredFields, current.Collection.GetRequiredFields())
	require.NotSame(t, cfg.RequiredFields[0], current.Collection.RequiredFields[0])
	require.ElementsMatch(t, []int64{10, 11}, current.GetPartitionIDs())
	require.Equal(t, map[string]int{"rg1": 1, "rg2": 1}, current.GetReplicaNumber())
	require.Equal(t, commonpb.LoadPriority_LOW, current.Replicas[4].LoadPriority())
	require.Nil(t, qviewsCurrentLoadConfig(nil).Collection)
}
