package proxy

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestProxyInvalidateServingPublicationScope(t *testing.T) {
	paramtable.Init()
	client := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, nil, nil, nil)
	var invalidated []int64
	patch := mockey.Mock(queryclient.InvalidateServingGeneration).To(func(actual queryclient.Client, id int64) {
		require.Same(t, client, actual)
		invalidated = append(invalidated, id)
	}).Build()
	defer patch.UnPatch()

	for _, test := range []struct {
		name string
		kind commonpb.MsgType
		id   int64
		want []int64
	}{
		{"collection", commonpb.MsgType_AlterCollection, 100, []int64{100}},
		{"missing collection id", commonpb.MsgType_AlterCollection, 0, nil},
		{"invalid collection id", commonpb.MsgType_AlterCollection, -1, nil},
		{"drop database", commonpb.MsgType_DropDatabase, 100, []int64{0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalidated = nil
			cache := NewMockCache(t)
			shards := shardclient.NewMockShardClientManager(t)
			if test.kind == commonpb.MsgType_DropDatabase {
				cache.EXPECT().RemoveDatabase(mock.Anything, "db").Return().Once()
				shards.EXPECT().RemoveDatabase("db").Return().Once()
			} else {
				cache.EXPECT().InvalidateCollectionMeta(mock.Anything, "db", "collection", test.id, false).Return(nil).Once()
				if test.id != 0 {
					shards.EXPECT().InvalidateShardLeaderCache([]int64{test.id}).Return().Once()
				}
			}
			node := &Proxy{metaCache: cache, shardMgr: shards, viewQueryClient: client}
			require.NoError(t, node.initRateCollector())
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			status, err := node.InvalidateCollectionMetaCache(t.Context(), &proxypb.InvalidateCollMetaCacheRequest{
				Base: &commonpb.MsgBase{MsgType: test.kind}, DbName: "db", CollectionName: "collection", CollectionID: test.id,
			})
			require.NoError(t, err)
			require.NoError(t, merr.Error(status))
			require.Equal(t, test.want, invalidated)
		})
	}
}
