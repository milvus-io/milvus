package loadmgr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadConfigPublicationCommitOwnershipAndFailure(t *testing.T) {
	store, hooks := newTestStore(t)
	cfg := sampleConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	var published []*LoadConfig
	var versions []uint64
	unsubscribe := store.RegisterLoadConfigListener(func(id int64, cfg *LoadConfig, version uint64) {
		require.EqualValues(t, 100, id)
		published = append(published, cfg)
		versions = append(versions, version)
	})
	require.Len(t, published, 1)
	cfg.PartitionIDs = []int64{30}
	require.NoError(t, store.Put(t.Context(), cfg))
	cfg.PartitionIDs[0] = 99
	require.Equal(t, []int64{30}, published[1].PartitionIDs)
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error {
		return merr.WrapErrServiceUnavailableMsg("catalog unavailable")
	}
	require.Error(t, store.Put(t.Context(), cfg))
	require.Len(t, published, 2)
	store.StopPlanning(100)
	require.Len(t, published, 3)
	require.Nil(t, published[2])
	require.Zero(t, versions[2])
	require.Greater(t, versions[1], versions[0])
	unsubscribe()
	replay := store.RegisterLoadConfigListener(func(int64, *LoadConfig, uint64) { t.Error("releasing config replayed") })
	replay()
}
