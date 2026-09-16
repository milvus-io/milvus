package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestControlReplayPreservesSalvageAcrossCrash(t *testing.T) {
	config := func(primary string, followers ...string) *commonpb.ReplicateConfiguration {
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{{ClusterId: primary, Pchannels: []string{"test-pchannel"}}},
		}
		for _, follower := range followers {
			cfg.Clusters = append(cfg.Clusters, &commonpb.MilvusCluster{ClusterId: follower, Pchannels: []string{"test-pchannel"}})
			cfg.CrossClusterTopology = append(cfg.CrossClusterTopology, &commonpb.CrossClusterTopology{
				SourceClusterId: primary, TargetClusterId: follower,
			})
		}
		return cfg
	}
	controlMessage := func(tt uint64, cfg *commonpb.ReplicateConfiguration, promote bool) message.ImmutableMessage {
		return message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{ReplicateConfiguration: cfg, ForcePromote: promote}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).WithAllVChannel().MustBuildMutable().
			WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(100)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
	}
	initial := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100), TimeTick: 100,
		Magic:           utility.RecoveryMagicRecoveryStorageV2,
		ReplicateConfig: config("source", "local"),
		ReplicateCheckpoint: &commonpb.ReplicateCheckpoint{
			ClusterId: "source", Pchannel: "test-pchannel", TimeTick: 500,
			MessageId: message.MustMarshalMessageID(walimplstest.NewTestMessageID(500)),
		},
	}
	original := newTestRecoveryStorage(t, initial)
	t.Cleanup(original.closeRecoveryResources)
	original.currentClusterID = "local"
	// The first config change retains local's source; force-promote then
	// captures source progress before clearing the replication checkpoint.
	update := controlMessage(110, config("source", "local", "third"), false)
	promote := controlMessage(120, config("local"), true)
	original.updatePChannelControl(update)
	original.updatePChannelControl(promote)
	require.Equal(t, uint64(500), original.pendingSalvageCheckpoint.TimeTick)

	frozen := original.consumeDirtySnapshot()
	require.NotNil(t, frozen)
	require.True(t, frozen.CheckpointDirty)
	snapshot, err := original.buildRecoverySnapshot(frozen)
	require.NoError(t, err)
	require.Equal(t, uint64(100), snapshot.ConsumeCheckpoint.TimeTick)
	require.Equal(t, uint64(120), snapshot.ConsumeCheckpoint.ControlCheckpointTimeTick)
	require.Equal(t, uint64(500), snapshot.SalvageCheckpoint.TimeTick)

	for _, committed := range []bool{false, true} {
		name := "before metadata publication"
		if committed {
			name = "after metadata publication"
		}
		t.Run(name, func(t *testing.T) {
			stored := initial.IntoProto()
			if committed {
				stored = snapshot.ConsumeCheckpoint
			}
			data, err := proto.Marshal(stored)
			require.NoError(t, err)
			loaded := &streamingpb.WALCheckpoint{}
			require.NoError(t, proto.Unmarshal(data, loaded))
			replay := newTestRecoveryStorage(t, utility.NewWALCheckpointFromProto(loaded))
			t.Cleanup(replay.closeRecoveryResources)
			replay.currentClusterID = "local"
			replay.updatePChannelControl(update)
			replay.updatePChannelControl(promote)
			require.True(t, proto.Equal(original.pchannelControl, replay.pchannelControl))
			if committed {
				require.Nil(t, replay.pendingSalvageCheckpoint, "do not overwrite the already persisted source boundary")
				require.Nil(t, replay.consumeDirtySnapshot(), "covered control replay has no new catalog effects")
			} else {
				require.Equal(t, uint64(500), replay.pendingSalvageCheckpoint.TimeTick,
					"unpublished effects replay from the old state and must reproduce the same salvage boundary")
			}
			// New control events after the saved frontier must still apply.
			replay.updatePChannelControl(controlMessage(130, config("source", "local"), false))
			require.Equal(t, uint64(130), replay.pchannelControl.CheckpointTimeTick)
			require.Equal(t, "source", replay.pchannelControl.ReplicateCheckpoint.ClusterId)
			require.Equal(t, uint64(100), replay.checkpoint.TimeTick)
		})
	}
}

func TestControlFrontierOnlyChangeIsPersisted(t *testing.T) {
	initial := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100), TimeTick: 100,
		Magic: utility.RecoveryMagicRecoveryStorageV2, ControlCheckpointTimeTick: 120,
	}
	rs := newTestRecoveryStorage(t, initial)
	t.Cleanup(rs.closeRecoveryResources)
	require.Nil(t, rs.consumeDirtySnapshot())
	// Identical state at a later event must still carry its newer replay boundary.
	rs.pchannelControl.CheckpointTimeTick = 130
	frozen := rs.consumeDirtySnapshot()
	require.NotNil(t, frozen)
	require.True(t, frozen.CheckpointDirty)
	require.Equal(t, uint64(100), frozen.Checkpoint.TimeTick)
	require.Equal(t, uint64(130), frozen.Checkpoint.ControlCheckpointTimeTick)
	rs.pchannelControl.CheckpointTimeTick = 140
	require.Equal(t, uint64(130), frozen.Checkpoint.ControlCheckpointTimeTick, "frozen frontier must be immutable")
	// Saving the older frozen snapshot must leave the newer observation dirty.
	rs.checkpoint = frozen.Checkpoint.Clone()
	next := rs.consumeDirtySnapshot()
	require.NotNil(t, next)
	require.Equal(t, uint64(140), next.Checkpoint.ControlCheckpointTimeTick)
}
