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

func TestLoadConfigAdmissionKeepsFirstCommitAcrossReload(t *testing.T) {
	for _, unchanged := range []bool{false, true} {
		t.Run(map[bool]string{false: "first load", true: "unchanged load"}[unchanged], func(t *testing.T) {
			store, _ := newTestStore(t)
			cfg := sampleConfig()
			if unchanged {
				require.NoError(t, store.Put(t.Context(), cfg))
			}
			committed, unsubscribe := store.WatchNextLoadConfig(100)
			defer unsubscribe()
			require.Empty(t, committed, "registration must not capture pre-admission state")
			require.NoError(t, store.Put(t.Context(), cfg))
			admitted := store.GetConfigVersion(100)
			// Model a release and another load after the callback releases its DDL
			// lock, but before the original Broadcast caller reads its result.
			store.StopPlanning(100)
			require.NoError(t, store.Remove(t.Context(), 100))
			require.NoError(t, store.Put(t.Context(), cfg))
			require.Greater(t, store.GetConfigVersion(100), admitted)
			require.Len(t, committed, 1)
			require.Equal(t, admitted, <-committed)
		})
	}
}

func TestLoadConfigAdmissionIgnoresFailedAndUnrelatedCommits(t *testing.T) {
	store, hooks := newTestStore(t)
	committed, unsubscribe := store.WatchNextLoadConfig(100)
	defer unsubscribe()
	other := sampleConfig()
	other.CollectionID = 200
	require.NoError(t, store.Put(t.Context(), other))
	require.Empty(t, committed)
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error {
		return merr.WrapErrServiceUnavailableMsg("injected persistence failure")
	}
	require.Error(t, store.Put(t.Context(), sampleConfig()))
	require.Empty(t, committed)
	hooks.beforeSave = nil
	require.NoError(t, store.Put(t.Context(), sampleConfig()))
	require.Len(t, committed, 1)
	require.Equal(t, store.GetConfigVersion(100), <-committed)

	abandoned, stop := store.WatchNextLoadConfig(100)
	stop()
	stop() // cleanup is idempotent
	require.NoError(t, store.Put(t.Context(), sampleConfig()))
	require.Empty(t, abandoned)
}
