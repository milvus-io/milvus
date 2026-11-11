package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
