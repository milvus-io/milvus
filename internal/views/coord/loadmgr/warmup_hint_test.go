package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestWarmupEpochPersistenceAndClone(t *testing.T) {
	cfg := sampleConfig()
	cfg.WarmupEpoch = 42
	clone := cfg.Clone()
	require.EqualValues(t, 42, clone.WarmupEpoch)
	clone.WarmupEpoch = 43
	require.EqualValues(t, 42, cfg.WarmupEpoch)
	recovered := buildFromPersisted(cfg.toCollectionLoadConfigProto())
	require.EqualValues(t, 42, recovered.WarmupEpoch)
	fromMessage := FromAlterLoadConfigMessage(&messagespb.AlterLoadConfigMessageHeader{CollectionId: 100, WarmupEpoch: 42})
	require.EqualValues(t, 42, fromMessage.WarmupEpoch)
}

func TestWarmupEpochIsMonotonicUntilRelease(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)
	collectionID := sampleConfig().CollectionID
	epoch := func() int64 { return store.GetConfig(collectionID).WarmupEpoch }
	put := func(e int64) {
		cfg := sampleConfig()
		cfg.WarmupEpoch = e
		require.NoError(t, store.Put(ctx, cfg))
		require.EqualValues(t, e, cfg.WarmupEpoch, "Put must not mutate the caller's object")
	}

	put(0)
	require.Zero(t, epoch())
	put(42) // a warmup hint on a loaded collection warms in place.
	require.EqualValues(t, 42, epoch())
	put(43) // every warmup hint gets a newer epoch.
	require.EqualValues(t, 43, epoch())
	put(0) // other DDL messages omit the epoch.
	require.EqualValues(t, 43, epoch())
	put(41) // an older request never rolls the epoch back.
	require.EqualValues(t, 43, epoch())

	store.StopPlanning(collectionID)
	require.NoError(t, store.Remove(ctx, collectionID))
	put(0)
	require.Zero(t, epoch(), "release clears the epoch")
}

func TestWarmupEpochOnlyChangeKeepsCollectionVersion(t *testing.T) {
	ctx := context.Background()
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	put := func(mutate func(*LoadConfig)) {
		cfg := sampleConfig()
		mutate(cfg)
		require.NoError(t, store.Put(ctx, cfg))
	}
	collectionID := sampleConfig().CollectionID

	put(func(*LoadConfig) {})
	version := store.GetConfigVersion(collectionID)
	storeVersion := store.Snapshot().Version()
	put(func(cfg *LoadConfig) { cfg.WarmupEpoch = 42 })
	require.Equal(t, version, store.GetConfigVersion(collectionID), "the readiness gate must not wait for a warmup")
	require.Greater(t, store.Snapshot().Version(), storeVersion, "the balancer still observes the change")
	require.EqualValues(t, 42, store.GetConfig(collectionID).WarmupEpoch)

	// The epoch is durable under the kept version.
	recovered, err := RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	require.Equal(t, version, recovered.GetConfigVersion(collectionID))
	require.EqualValues(t, 42, recovered.GetConfig(collectionID).WarmupEpoch)

	put(func(cfg *LoadConfig) { cfg.WarmupEpoch = 43; cfg.PartitionIDs = []int64{10} })
	require.NotEqual(t, version, store.GetConfigVersion(collectionID), "a change of the served data still changes the version")
}
