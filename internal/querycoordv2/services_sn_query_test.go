package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewLoadInfoUsesCurrentLoadedMetadata(t *testing.T) {
	ctx := context.Background()
	manager := meta.NewCollectionManager(nil)
	s := &Server{meta: &meta.Meta{CollectionManager: manager}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, manager.PutCollectionWithoutSave(ctx, &meta.Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 1, LoadFields: []int64{100, 102}}}))
	require.NoError(t, manager.PutPartitionWithoutSave(ctx, &meta.Partition{PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 1, PartitionID: 10}}))
	resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, int64(1), resp.GetCollectionID())
	require.Equal(t, []int64{10}, resp.GetPartitionIDs())
	require.Len(t, resp.GetLoadFields(), 2)
	require.Equal(t, int64(100), resp.LoadFields[0].FieldId)
	require.Equal(t, int64(102), resp.LoadFields[1].FieldId)
	require.Zero(t, resp.GetVersion(), "current metadata must not claim a historical version")
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
