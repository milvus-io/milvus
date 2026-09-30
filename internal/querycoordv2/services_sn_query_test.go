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

func TestQueryViewLoadInfoUsesCurrentLoadedMetadata(t *testing.T) {
	ctx := context.Background()

	s := &Server{qviewsRuntime: &qviewsRuntime{loadConfigStore: &loadmgr.LoadConfigStore{}}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	patch := mockey.Mock((*loadmgr.LoadConfigStore).GetConfigWithVersion).To(func(_ *loadmgr.LoadConfigStore, id int64) (*loadmgr.LoadConfig, uint64) {
		if id != 1 {
			return nil, 0
		}
		return &loadmgr.LoadConfig{CollectionID: 1, PartitionIDs: []int64{10}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 102}}}, 3
	}).Build()
	defer patch.UnPatch()
	resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, int64(1), resp.GetCollectionID())
	require.Equal(t, []int64{10}, resp.GetPartitionIDs())
	require.Len(t, resp.GetLoadFields(), 2)
	require.Equal(t, int64(100), resp.LoadFields[0].FieldId)
	require.Equal(t, int64(102), resp.LoadFields[1].FieldId)
	require.Equal(t, uint64(3), resp.GetVersion(), "return the actual current version, not the requested historical version")
	for _, id := range []int64{0, 2} {
		resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: id})
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.GetStatus()))
	}
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.GetStatus()))
}

func TestQueryViewLoadConfigFeedsAdmissionChecks(t *testing.T) {
	cfg := &loadmgr.LoadConfig{
		CollectionID: 1, DbID: 2, PartitionIDs: []int64{10, 11}, UserSpecifiedReplicaMode: true,
		LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 200}},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 3, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaID: 4, ResourceGroup: "rg2", Priority: commonpb.LoadPriority_LOW},
		},
	}
	current := qviewsCurrentLoadConfig(cfg)
	require.Equal(t, int64(1), current.Collection.GetCollectionID())
	require.True(t, current.Collection.GetUserSpecifiedReplicaMode())
	require.Equal(t, map[int64]int64{100: 200}, current.Collection.GetFieldIndexID())
	require.ElementsMatch(t, []int64{10, 11}, current.GetPartitionIDs())
	require.Equal(t, map[string]int{"rg1": 1, "rg2": 1}, current.GetReplicaNumber())
	require.Equal(t, commonpb.LoadPriority_LOW, current.Replicas[4].LoadPriority())
	require.Nil(t, qviewsCurrentLoadConfig(nil).Collection)
}
