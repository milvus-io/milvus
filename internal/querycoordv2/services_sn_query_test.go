package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
	s := &Server{loadInfos: loadmgr.NewLoadInfoStore(&loadInfoCatalogForTest{}, func(int64) []*indexpb.IndexInfo { return nil })}
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
	s.loadInfos = nil
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceNotReady)
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.Status))
}

func TestQueryViewLoadInfoCurrentStateIndependentOfHistory(t *testing.T) {
	ctx := context.Background()
	collection := &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 1, LoadFields: []int64{100}},
		LoadPercentage:     0,
	}
	collectionPatch := mockey.Mock((*meta.CollectionManager).GetCollection).To(func(_ *meta.CollectionManager, _ context.Context, id int64) *meta.Collection {
		require.EqualValues(t, 1, id)
		return collection
	}).Build()
	defer collectionPatch.UnPatch()
	partitionPatch := mockey.Mock((*meta.CollectionManager).GetPartitionIDsByCollection).Return([]int64{10}).Build()
	defer partitionPatch.UnPatch()

	s := &Server{meta: &meta.Meta{CollectionManager: meta.NewCollectionManager(nil, nil)}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	current := &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1}
	resp, err := s.GetQueryViewLoadInfo(ctx, current)
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status), "loading counts as a live configuration, even without history")
	require.EqualValues(t, 1, resp.CollectionID)
	require.Equal(t, []int64{10}, resp.PartitionIDs)
	require.Len(t, resp.LoadFields, 1)
	require.EqualValues(t, 100, resp.LoadFields[0].FieldId)
	require.Zero(t, resp.LoadFields[0].IndexId)

	stored := &querypb.GetQueryViewLoadInfoResponse{CollectionID: 1, Version: 9, PartitionIDs: []int64{20}, RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}}
	catalogCalls := 0
	catalogPatch := mockey.Mock((*loadInfoCatalogForTest).GetLoadInfo).To(func(_ *loadInfoCatalogForTest, _ context.Context, id int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		catalogCalls++
		require.EqualValues(t, 1, id)
		require.EqualValues(t, 9, version)
		return stored, nil
	}).Build()
	defer catalogPatch.UnPatch()
	s.loadInfos = loadmgr.NewLoadInfoStore(&loadInfoCatalogForTest{}, func(int64) []*indexpb.IndexInfo { return nil })
	collection = nil // Release removes live state but retains historical snapshots.
	resp, err = s.GetQueryViewLoadInfo(ctx, current)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.Status), merr.ErrCollectionNotLoaded)
	require.Zero(t, catalogCalls, "retained history must not turn a released collection into loaded")
	resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status))
	require.EqualValues(t, 9, resp.Version)
	require.Equal(t, []int64{20}, resp.PartitionIDs)
	require.Equal(t, 1, catalogCalls, "an existing View can still resolve its exact history after release")
}

func TestQueryViewLoadInfoResolvesCurrentRequiredIndexes(t *testing.T) {
	ctx := context.Background()
	stored := &querypb.GetQueryViewLoadInfoResponse{
		Status: merr.Success(), CollectionID: 1, Version: 9,
		LoadFields:     []*messagespb.LoadFieldConfig{{FieldId: 100}},
		RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}},
		IndexInfoList:  []*indexpb.IndexInfo{{FieldID: 100, IndexID: 42}},
	}
	catalogPatch := mockey.Mock((*loadInfoCatalogForTest).GetLoadInfo).Return(stored, nil).Build()
	defer catalogPatch.UnPatch()
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}, {FieldID: 101, IndexID: 201}, {FieldID: 102, IndexID: 202}}
	s := &Server{loadInfos: loadmgr.NewLoadInfoStore(&loadInfoCatalogForTest{}, func(id int64) []*indexpb.IndexInfo {
		require.EqualValues(t, 1, id)
		return indexes
	})}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	req := &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9}
	resp, err := s.GetQueryViewLoadInfo(ctx, req)
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status))
	require.Len(t, resp.IndexInfoList, 2, "indexes follow required fields, including fields outside preload hints")
	require.EqualValues(t, 200, resp.IndexInfoList[0].IndexID)
	require.EqualValues(t, 201, resp.IndexInfoList[1].IndexID)
	resp.IndexInfoList[0].IndexID = 999
	resp.RequiredFields[0].FieldId = 999
	require.EqualValues(t, 200, indexes[0].IndexID)
	require.EqualValues(t, 100, stored.RequiredFields[0].FieldId)
	require.EqualValues(t, 42, stored.IndexInfoList[0].IndexID)

	indexes = []*indexpb.IndexInfo{{FieldID: 101, IndexID: 301}, {FieldID: 102, IndexID: 302}}
	resp, err = s.GetQueryViewLoadInfo(ctx, req)
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.Status))
	require.Len(t, resp.IndexInfoList, 1, "cached history must refresh indexes and observe removed indexes")
	require.EqualValues(t, 301, resp.IndexInfoList[0].IndexID)
	require.EqualValues(t, 100, resp.RequiredFields[0].FieldId)
	require.Equal(t, 1, catalogPatch.Times(), "only the immutable scope is cached")
}

func TestQueryViewLoadInfoRejectsInvalidHistory(t *testing.T) {
	for _, tc := range []struct {
		name string
		info *querypb.GetQueryViewLoadInfoResponse
		err  error
		want error
	}{
		{name: "missing scope", info: &querypb.GetQueryViewLoadInfoResponse{CollectionID: 1, Version: 9}, want: merr.ErrDataIntegrity},
		{name: "missing snapshot", want: merr.ErrServiceNotReady},
		{name: "catalog failure", err: merr.WrapErrServiceUnavailableMsg("catalog busy"), want: merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock((*loadInfoCatalogForTest).GetLoadInfo).Return(tc.info, tc.err).Build()
			defer patch.UnPatch()
			s := &Server{loadInfos: loadmgr.NewLoadInfoStore(&loadInfoCatalogForTest{}, func(int64) []*indexpb.IndexInfo {
				t.Fatal("invalid history must fail before resolving indexes")
				return nil
			})}
			s.UpdateStateCode(commonpb.StateCode_Healthy)
			resp, err := s.GetQueryViewLoadInfo(t.Context(), &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(resp.Status), tc.want)
		})
	}
}
