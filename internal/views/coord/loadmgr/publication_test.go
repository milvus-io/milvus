package loadmgr

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadConfigPublicationCommitOwnershipAndFailure(t *testing.T) {
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*querycoord.Catalog).GetCollections).Return([]*querypb.CollectionLoadInfo{{CollectionID: 1}}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetReplicas).Return([]*querypb.Replica{}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).ReleaseReplicas).Return(nil).Build(),
		mockey.Mock((*querycoord.Catalog).ReleaseCollection).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	store, err := RecoverLoadConfigStore(t.Context(), querycoord.NewCatalog(nil))
	require.NoError(t, err)
	var published []*LoadConfig
	var revisions []uint64
	stop := store.RegisterLoadConfigListener(func(id int64, cfg *LoadConfig, rev uint64) {
		require.Equal(t, int64(1), id)
		published = append(published, cfg)
		revisions = append(revisions, rev)
	})
	require.Len(t, published, 1)
	save := mockey.Mock((*querycoord.Catalog).SaveCollection).Return(nil).Build()
	cfg := &LoadConfig{CollectionID: 1, PartitionIDs: []int64{100}}
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Len(t, published, 2)
	cfg.PartitionIDs[0] = 999
	require.Equal(t, []int64{100}, published[1].PartitionIDs)
	require.Empty(t, published[0].PartitionIDs)
	save.UnPatch()
	failed := mockey.Mock((*querycoord.Catalog).SaveCollection).Return(merr.WrapErrServiceUnavailableMsg("catalog unavailable")).Build()
	require.Error(t, store.Put(t.Context(), published[1]))
	require.Len(t, published, 2)
	failed.UnPatch()
	require.NoError(t, store.Remove(t.Context(), 1))
	require.Len(t, published, 3)
	require.Nil(t, published[2])
	require.Less(t, revisions[0], revisions[1])
	require.Less(t, revisions[1], revisions[2])
	stop()
	replay := store.RegisterLoadConfigListener(func(int64, *LoadConfig, uint64) { t.Error("removed config replayed") })
	replay()
}
