package vchannel

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func retireSplitCopy(taskID int64, tick uint64, local bool) message.ImmutableMessage {
	b := message.NewRetireSplitShardMessageBuilderV2().WithVChannel(splitSource).
		WithHeader(&message.RetireSplitShardMessageHeader{CollectionId: 1, SplitTaskId: taskID}).
		WithBody(&message.RetireSplitShardMessageBody{})
	if local {
		b.WithUnreplicable()
	}
	return b.MustBuildMutable().WithTimeTick(tick).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
}

func TestSplitRetirementKeepsItsWriteFenceAcrossRecovery(t *testing.T) {
	m := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: newSplitSourceMeta()})
	observeOnManager(m, newSplitShardCopy(splitSource, splitTaskID, 200))
	// Neither a replicable retirement nor a retirement of another task is valid.
	observeOnManager(m, retireSplitCopy(splitTaskID, 250, false))
	observeOnManager(m, retireSplitCopy(splitTaskID+1, 260, true))
	require.True(t, m.Module(splitSource).IsActive())
	observeOnManager(m, retireSplitCopy(splitTaskID, 300, true))
	meta := vchannelMetaOf(t, m, splitSource)
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, meta.GetState())
	require.Equal(t, uint64(300), meta.GetCheckpointTimeTick())
	require.Equal(t, uint64(200), meta.GetSplitLineage().GetSwitchTimeTick())
	require.False(t, m.Module(splitSource).IsActive())
	state := m.RecoverySnapshot().VChannels[splitSource]
	require.Equal(t, splitTaskID, state.SplitTaskID)
	require.Empty(t, state.PartitionIDs, "retired sources must not recreate segment allocators")
	require.Nil(t, state.Schema, "retired sources must not prewarm analyzers")

	// Crash before publication: replay the durable record on an active source.
	before := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: newSplitSourceMeta()})
	observeOnManager(before, newSplitShardCopy(splitSource, splitTaskID, 200))
	observeOnManager(before, retireSplitCopy(splitTaskID, 300, true))
	require.True(t, proto.Equal(meta, vchannelMetaOf(t, before, splitSource)))
	// Crash after publication: restore the fence before any replay; later
	// duplicates, a replayed genesis and forwarded schemas cannot resurrect S.
	after := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: meta})
	require.Equal(t, splitTaskID, after.RecoverySnapshot().VChannels[splitSource].SplitTaskID)
	observeOnManager(after, retireSplitCopy(splitTaskID, 350, true))
	observeOnManager(after, newSplitShardCopy(splitSource, splitTaskID, 400))
	observeOnManager(after, forwardedSplitSchema(5, 450))
	require.True(t, proto.Equal(meta, vchannelMetaOf(t, after, splitSource)))
	require.Empty(t, after.ConsumeDirtySnapshots())
}

func forwardedSplitSchema(version int32, tick uint64) message.ImmutableMessage {
	return message.NewSchemaChangeMessageBuilderV2().WithVChannel(splitSource).WithUnreplicable().
		WithHeader(&message.SchemaChangeMessageHeader{CollectionId: 1}).
		WithBody(&message.SchemaChangeMessageBody{Schema: splitSchema(version)}).
		MustBuildMutable().WithTimeTick(tick).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
}

func TestForwardedSplitSchemaCallbackRetryDoesNotGrowHistory(t *testing.T) {
	m := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: newSplitSourceMeta()})
	observeOnManager(m, newSplitShardCopy(splitSource, splitTaskID, 200))
	observeOnManager(m, forwardedSplitSchema(4, 300))
	observeOnManager(m, forwardedSplitSchema(4, 350))
	meta := vchannelMetaOf(t, m, splitSource)
	require.Len(t, meta.GetCollectionInfo().GetSchemas(), 2)
	require.Equal(t, uint64(350), meta.GetCheckpointTimeTick())
	entry, err := m.Module(splitSource).GetSchemaVersion(1, 4)
	require.NoError(t, err)
	require.True(t, proto.Equal(splitSchema(4), entry.GetSchema()))
	require.Equal(t, splitTaskID, m.RecoverySnapshot().VChannels[splitSource].SplitTaskID)
	// Replay after restart also deduplicates by semantic schema, even though
	// the new local append has a later timestamp.
	recovered := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: meta})
	observeOnManager(recovered, forwardedSplitSchema(4, 400))
	require.Len(t, vchannelMetaOf(t, recovered, splitSource).GetCollectionInfo().GetSchemas(), 2)
}
