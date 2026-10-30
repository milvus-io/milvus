package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoIndexSourceForTest struct{ types.MixCoord }

func (*loadInfoIndexSourceForTest) QueryViewCollectionIndexInfos(int64) []*indexpb.IndexInfo {
	panic("patch with mockey")
}

func TestQueryViewLoadInfoInitializationAndRecovery(t *testing.T) {
	client, _ := kvfactory.GetEtcdAndPath()
	storage := etcdkv.NewEtcdKV(client, "query-view-load-info-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "")) })
	ctx := t.Context()
	snapshot := &querypb.GetQueryViewLoadInfoResponse{
		Status: merr.Success(), CollectionID: 1, Version: 9,
		PartitionIDs: []int64{10}, RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
	}
	require.NoError(t, queryview.NewLoadInfoCatalog(storage).SaveLoadInfo(ctx, snapshot))
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}, {FieldID: 101, IndexID: 201}}
	patch := mockey.Mock((*loadInfoIndexSourceForTest).QueryViewCollectionIndexInfos).To(func(_ *loadInfoIndexSourceForTest, id int64) []*indexpb.IndexInfo {
		require.EqualValues(t, 1, id)
		return indexes
	}).Build()
	defer patch.UnPatch()

	for _, withIndexSource := range []bool{true, false} {
		s := &Server{ctx: ctx, kv: storage, nodeMgr: session.NewNodeManager()}
		if withIndexSource {
			s.mixCoord = &loadInfoIndexSourceForTest{}
		}
		require.NoError(t, s.initMeta())
		s.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.Status))
		require.EqualValues(t, 9, resp.Version)
		require.Equal(t, []int64{10}, resp.PartitionIDs)
		if withIndexSource {
			require.Len(t, resp.IndexInfoList, 1)
			require.EqualValues(t, 200, resp.IndexInfoList[0].IndexID)
		} else {
			require.Empty(t, resp.IndexInfoList)
		}
		resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.Status), merr.ErrCollectionNotLoaded,
			"recovering historical scopes must not recreate current load state")
	}
}
