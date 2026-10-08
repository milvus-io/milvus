package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestSchemaGCRecoveryValidation(t *testing.T) {
	meta := &streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 20,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1, Schemas: []*streamingpb.CollectionSchemaOfVChannel{
			{Schema: &schemapb.CollectionSchema{Version: 1}, CheckpointTimeTick: 10, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED},
			{Schema: &schemapb.CollectionSchema{Version: 2}, CheckpointTimeTick: 20, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL},
		}},
	}
	metas := map[string]*streamingpb.VChannelMeta{"v1": meta}
	require.NoError(t, validateRecoveredViewMeta(metas, nil, false))
	meta.CollectionInfo.Schemas[1].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
	require.ErrorContains(t, validateRecoveredViewMeta(metas, nil, false), "latest schema")
	meta.CollectionInfo.Schemas[1].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL
	meta.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_UNKNOWN
	require.ErrorContains(t, validateRecoveredViewMeta(metas, nil, false), "unknown schema state")
}

func TestSchemaGCBuildSnapshotWithConcurrentVChannelUpdate(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(20), TimeTick: 20, Term: 1}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	meta := &streamingpb.VChannelMeta{Vchannel: "v1"}
	removed := &streamingpb.VChannelMeta{Vchannel: "v1", CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
		Schemas: []*streamingpb.CollectionSchemaOfVChannel{{CheckpointTimeTick: 10, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED}},
	}}
	deletion := newOrderedDirtySnapshot(moduleapi.ModuleNameVChannel, moduleapi.SnapshotKey{VChannel: "v1"}, moduleapi.SnapshotOpDeleteSchemas, removed)
	for _, op := range []moduleapi.SnapshotOp{moduleapi.SnapshotOpUpsertBase, moduleapi.SnapshotOpUpsert} {
		batch := &dirtyPersistSnapshot{Checkpoint: checkpoint, ModuleDirtySnaps: []moduleapi.DirtySnapshot{
			deletion, newOrderedDirtySnapshot(moduleapi.ModuleNameVChannel, moduleapi.SnapshotKey{VChannel: "v1"}, op, meta),
		}}
		snapshot, err := storage.buildRecoverySnapshot(batch)
		require.NoError(t, err)
		require.Equal(t, []uint64{10}, snapshot.RemovedVChannelSchemas["v1"])
		require.Equal(t, checkpoint.Term, snapshot.ConsumeCheckpoint.Term)
		batch.ModuleDirtySnaps = append(batch.ModuleDirtySnaps, deletion)
		_, err = storage.buildRecoverySnapshot(batch)
		require.ErrorContains(t, err, "duplicate dirty snapshot")
	}
}
