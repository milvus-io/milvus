package querycoordv2

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoIndexSourceForTest struct{ types.MixCoord }

func (*loadInfoIndexSourceForTest) QueryViewCollectionIndexInfos(int64, ...int64) []*indexpb.IndexInfo {
	panic("patch with mockey")
}

func TestQueryViewLoadInfoInitializationAndRecovery(t *testing.T) {
	client, _ := kvfactory.GetEtcdAndPath()
	storage := etcdkv.NewEtcdKV(client, "query-view-load-info-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, storage.RemoveWithPrefix(context.Background(), "")) })
	ctx := t.Context()
	snapshot := &viewpb.CollectionLoadConfig{
		CollectionID: 1, Version: 9,
		PartitionIds: []int64{10}, RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100, IndexIds: []int64{200}}},
	}
	encoded, err := proto.Marshal(snapshot)
	require.NoError(t, err)
	require.NoError(t, storage.Save(ctx, fmt.Sprintf("%s/1/9", queryview.VersionedLoadInfoPrefix), string(encoded)))
	indexes := []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}, {FieldID: 101, IndexID: 201}}
	patch := mockey.Mock((*loadInfoIndexSourceForTest).QueryViewCollectionIndexInfos).To(func(_ *loadInfoIndexSourceForTest, id int64, ids ...int64) []*indexpb.IndexInfo {
		require.EqualValues(t, 1, id)
		require.Equal(t, []int64{200}, ids)
		return indexes
	}).Build()
	defer patch.UnPatch()

	for _, withIndexSource := range []bool{true, false} {
		s := &Server{ctx: ctx, kv: storage, nodeMgr: session.NewNodeManager()}
		if withIndexSource {
			s.mixCoord = &loadInfoIndexSourceForTest{}
		}
		require.NoError(t, s.initMeta())
		// The integrated QueryView runtime owns load metadata; initMeta only
		// initializes the legacy coordinator metadata needed during recovery.
		deps := newDefaultQViewsRuntimeDependencies(storage, client, s.store, nil, s.mixCoord)
		t.Cleanup(func() {
			deps.queryNodeManager.Close()
			deps.streamingNodeHandler.Close()
			deps.streamingCoordClient.Close()
		})
		configs, err := loadmgr.RecoverLoadConfigStore(ctx, s.store, deps.loadInfos)
		require.NoError(t, err)
		s.qviewsRuntime = &qviewsRuntime{loadConfigStore: configs, loadInfos: deps.loadInfos}
		s.UpdateStateCode(commonpb.StateCode_Healthy)
		resp, err := s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1, Version: 9})
		require.NoError(t, err)
		if withIndexSource {
			require.NoError(t, merr.Error(resp.Status))
			require.EqualValues(t, 9, resp.Version)
			require.Equal(t, []int64{10}, resp.PartitionIDs)
			require.Len(t, resp.IndexInfoList, 1)
			require.EqualValues(t, 200, resp.IndexInfoList[0].IndexID)
		} else {
			require.ErrorIs(t, merr.Error(resp.Status), merr.ErrServiceNotReady)
		}
		resp, err = s.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: 1})
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.Status), merr.ErrCollectionNotLoaded,
			"recovering historical scopes must not recreate current load state")
	}
}
