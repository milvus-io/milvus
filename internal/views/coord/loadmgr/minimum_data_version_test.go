package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMinimumDataVersionHistoryAndInheritance(t *testing.T) {
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	minimum := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 2}
	require.NoError(t, store.AdvanceMinimumDataVersion(t.Context(), 1, minimum))
	require.False(t, store.Contains(1), "a refresh cannot load an unloaded collection")
	require.NoError(t, store.Put(t.Context(), versionedTestConfig()))
	legacy := store.GetConfigVersion(1)
	require.NoError(t, store.AdvanceMinimumDataVersion(t.Context(), 1, minimum))
	version := store.GetConfigVersion(1)
	require.Greater(t, version, legacy)
	require.Equal(t, minimum, store.GetConfig(1).Clone().MinimumDataVersion)
	for _, boundary := range []qviews.DataVersion{minimum, {StreamingVersion: 9, CompactVersion: 99}} {
		require.NoError(t, store.AdvanceMinimumDataVersion(t.Context(), 1, boundary))
		require.Equal(t, version, store.GetConfigVersion(1))
	}
	_, err = store.AcquireLoadInfo(t.Context(), 1, version, qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1})
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	for _, boundary := range []qviews.DataVersion{minimum, {StreamingVersion: 11}} {
		release, err := store.AcquireLoadInfo(t.Context(), 1, version, boundary)
		require.NoError(t, err)
		release()
		release()
	}
	release, err := store.AcquireLoadInfo(t.Context(), 1, legacy, qviews.DataVersion{})
	require.NoError(t, err, "historical Views validate their own LoadInfo minimum")
	defer release()
	cfg := versionedTestConfig() // a WAL config has no minimum field
	cfg.PartitionIDs = append(cfg.PartitionIDs, 11)
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Equal(t, minimum, store.GetConfig(1).MinimumDataVersion)
	restarted, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.Equal(t, minimum, restarted.GetConfig(1).MinimumDataVersion)
	releaseRecovered, err := restarted.AcquireLoadInfo(t.Context(), 1, legacy, qviews.DataVersion{})
	require.NoError(t, err)
	releaseRecovered()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, store.AdvanceMinimumDataVersion(ctx, 1, minimum), context.Canceled)
}

func TestMinimumDataVersionPublicationFailure(t *testing.T) {
	hooks := &versionedTestHooks{}
	store, err := RecoverLoadConfigStore(t.Context(), versionedTestDependencies(t, hooks))
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), versionedTestConfig()))
	version := store.GetConfigVersion(1)
	hooks.beforeSave = func(_ *viewpb.CollectionLoadConfig) error { return context.DeadlineExceeded }
	require.ErrorIs(t, store.AdvanceMinimumDataVersion(t.Context(), 1, qviews.DataVersion{StreamingVersion: 10}), context.DeadlineExceeded)
	require.Equal(t, version, store.GetConfigVersion(1))
	require.Equal(t, qviews.DataVersion{}, store.GetConfig(1).MinimumDataVersion)
}
