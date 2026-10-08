package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestSchemaGCCatalogRoundTripRetryAndFence(t *testing.T) {
	ctx := context.Background()
	catalog := newTestEtcdCatalog(t, "schema-gc")
	checkpoint := &streamingpb.WALCheckpoint{TimeTick: 30, Term: 1}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{ConsumeCheckpoint: checkpoint}))
	meta := &streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 30,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1, Schemas: []*streamingpb.CollectionSchemaOfVChannel{
			{Schema: &schemapb.CollectionSchema{Version: 1}, CheckpointTimeTick: 10, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED},
			{Schema: &schemapb.CollectionSchema{Version: 2}, CheckpointTimeTick: 20, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL},
		}},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: checkpoint, VChannels: map[string]*streamingpb.VChannelMeta{"v1": meta},
	}))
	restored, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, restored, 1)
	require.True(t, proto.Equal(meta, restored[0]), "tombstone must survive restart before physical deletion")
	// A concurrent schema update was frozen after cleanup: its copy still has
	// the old tombstone. Explicit removal must win over that stale schema key.
	meta.CollectionInfo.Schemas = append(meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
		Schema: &schemapb.CollectionSchema{Version: 3}, CheckpointTimeTick: 30, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
	})
	deletion := &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: checkpoint, VChannels: map[string]*streamingpb.VChannelMeta{"v1": meta},
		RemovedVChannelSchemas: map[string][]uint64{"v1": {10}},
	}
	failure := errors.New("injected schema delete failure")
	patch := mockey.Mock((*recoverySnapshotKV).MultiSaveAndRemove).Return(failure).Build()
	t.Cleanup(func() { patch.UnPatch() })
	require.ErrorIs(t, catalog.SaveRecoverySnapshot(ctx, "p1", deletion), failure)
	patch.UnPatch()
	restored, err = catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, restored[0].CollectionInfo.Schemas, 2, "failure must leave retriable tombstones")
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", deletion))
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", deletion), "same deletion is idempotent")
	restored, err = catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, restored[0].CollectionInfo.Schemas, 2)
	require.Equal(t, uint64(20), restored[0].CollectionInfo.Schemas[0].CheckpointTimeTick)
	require.Equal(t, uint64(30), restored[0].CollectionInfo.Schemas[1].CheckpointTimeTick)
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 30, Term: 2},
	}))
	err = catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: checkpoint, RemovedVChannelSchemas: map[string][]uint64{"v1": {20}},
	})
	require.ErrorContains(t, err, "fenced")
	restored, err = catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, restored[0].CollectionInfo.Schemas, 2, "stale publisher must not delete schema keys")
}
