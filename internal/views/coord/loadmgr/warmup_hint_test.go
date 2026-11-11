package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLegacyWarmupHintIsNotRecoveredOrReplayed(t *testing.T) {
	ctx := context.Background()
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	cfg := sampleConfig()
	require.NoError(t, store.Put(ctx, cfg))
	persisted := store.GetConfig(cfg.CollectionID).toCollectionLoadConfigProto()
	legacy := protowire.AppendVarint(protowire.AppendTag(nil, 9, protowire.VarintType), 42)
	persisted.ProtoReflect().SetUnknown(legacy)
	require.NoError(t, deps.Catalog.Save(ctx, persisted))
	recovered, err := RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	require.Empty(t, recovered.GetConfig(cfg.CollectionID).toCollectionLoadConfigProto().ProtoReflect().GetUnknown())
	require.Equal(t, legacy, []byte(persisted.ProtoReflect().GetUnknown()), "recovery must not mutate the source")
	require.NoError(t, recovered.Put(ctx, cfg))
	require.Equal(t, persisted.Version, recovered.GetConfigVersion(cfg.CollectionID))

	header := &messagespb.AlterLoadConfigMessageHeader{CollectionId: 100}
	header.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 9, protowire.VarintType), 42))
	replayed := FromAlterLoadConfigMessage(header)
	require.Empty(t, replayed.toCollectionLoadConfigProto().ProtoReflect().GetUnknown(), "legacy WAL and CDC records cannot trigger new work")
}

func TestOptionalWarmupDoesNotWaitBehindDDL(t *testing.T) {
	store, _ := newTestStore(t)
	require.NoError(t, store.Put(t.Context(), sampleConfig()))
	cfg := store.GetConfig(sampleConfig().CollectionID)
	called := false
	accept := func() error { called = true; return nil }
	require.NoError(t, store.WithCurrentConfig(cfg, func() error {
		require.ErrorIs(t, store.TryWithCurrentConfig(cfg, accept), merr.ErrServiceUnavailable)
		return nil
	}))
	require.False(t, called)
	require.NoError(t, store.TryWithCurrentConfig(cfg, accept))
	require.True(t, called)
}
