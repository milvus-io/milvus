package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoCatalogForTest struct{ queryview.LoadInfoCatalog }

func (*loadInfoCatalogForTest) GetLoadInfo(context.Context, int64, uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	panic("patch with mockey")
}

func TestQueryViewLoadInfoRequiresExactHistory(t *testing.T) {
	ctx := context.Background()
	stored := &querypb.GetQueryViewLoadInfoResponse{CollectionID: 1, Version: 9, PartitionIDs: []int64{10}, RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 42}}}
	patch := mockey.Mock((*loadInfoCatalogForTest).GetLoadInfo).To(func(_ *loadInfoCatalogForTest, _ context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		if collectionID == 1 && version == 9 {
			return stored, nil
		}
		return nil, nil
	}).Build()
	defer patch.UnPatch()
	s := &Server{loadInfoCatalog: &loadInfoCatalogForTest{}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status))
	require.EqualValues(t, 9, resp.Version)
	require.Equal(t, []int64{10}, resp.PartitionIDs)
	require.Zero(t, resp.RequiredFields[0].IndexId)
	require.EqualValues(t, 42, stored.RequiredFields[0].IndexId, "RPC must not mutate the retained snapshot")
	for _, req := range []*querypb.GetQueryViewLoadInfoRequest{{CollectionID: 1, Version: 10}, {CollectionID: 2, Version: 9}} {
		resp, err = s.GetQueryViewLoadInfo(ctx, req)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceNotReady)
	}
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{})
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.Status))
	s.loadInfoCatalog = nil
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceNotReady)
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.Status))
}
