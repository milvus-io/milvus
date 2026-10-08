package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestNewWALCheckpointFromProto(t *testing.T) {
	assert.Nil(t, NewWALCheckpointFromProto(nil))
	assert.Nil(t, NewWALCheckpointFromProto(nil).IntoProto())

	messageID := rmq.NewRmqID(1)
	timeTick := uint64(12345)
	recoveryMagic := int64(1)
	protoCheckpoint := &streamingpb.WALCheckpoint{
		MessageId:     messageID.IntoProto(),
		TimeTick:      timeTick,
		RecoveryMagic: recoveryMagic,
	}
	checkpoint := NewWALCheckpointFromProto(protoCheckpoint)

	assert.True(t, messageID.EQ(checkpoint.MessageID))
	assert.Equal(t, timeTick, checkpoint.TimeTick)
	assert.Equal(t, recoveryMagic, checkpoint.Magic)

	proto := checkpoint.IntoProto()
	checkpoint2 := NewWALCheckpointFromProto(proto)
	assert.True(t, messageID.EQ(checkpoint2.MessageID))
	assert.Equal(t, timeTick, checkpoint2.TimeTick)
	assert.Equal(t, recoveryMagic, checkpoint2.Magic)

	checkpoint3 := checkpoint.Clone()
	assert.True(t, messageID.EQ(checkpoint3.MessageID))
	assert.Equal(t, timeTick, checkpoint3.TimeTick)
	assert.Equal(t, recoveryMagic, checkpoint3.Magic)

	// The control fields advance atomically with the checkpoint: they round
	// trip through the proto and survive Clone.
	protoCheckpoint.ReplicateConfig = &commonpb.ReplicateConfiguration{}
	protoCheckpoint.ReplicateCheckpoint = &commonpb.ReplicateCheckpoint{
		ClusterId: "by-dev",
		Pchannel:  "p1",
		MessageId: rmq.NewRmqID(2).IntoProto(),
		TimeTick:  123456,
	}
	withControl := NewWALCheckpointFromProto(protoCheckpoint)
	assert.True(t, messageID.EQ(withControl.MessageID))
	assert.NotNil(t, withControl.ReplicateConfig)
	assert.Equal(t, "by-dev", withControl.ReplicateCheckpoint.GetClusterId())
	assert.Equal(t, uint64(123456), withControl.ReplicateCheckpoint.GetTimeTick())

	roundtrip := NewWALCheckpointFromProto(withControl.IntoProto())
	assert.Equal(t, "by-dev", roundtrip.ReplicateCheckpoint.GetClusterId())
	assert.Equal(t, rmq.NewRmqID(2).IntoProto(), roundtrip.ReplicateCheckpoint.GetMessageId())
	assert.Equal(t, uint64(123456), roundtrip.ReplicateCheckpoint.GetTimeTick())

	cloned := withControl.Clone()
	assert.Equal(t, "by-dev", cloned.ReplicateCheckpoint.GetClusterId())

	// PChannelControlFromCheckpoint decodes the embedded control state with the
	// checkpoint position as its frontier.
	control := PChannelControlFromCheckpoint(withControl)
	assert.Equal(t, timeTick, control.GetCheckpointTimeTick())
	assert.Equal(t, "by-dev", control.GetReplicateCheckpoint().GetClusterId())
	assert.Equal(t, "p1", control.GetReplicateCheckpoint().GetPchannel())
	assert.NotNil(t, control.GetReplicateConfig())

	// ApplyControl freezes control state into a checkpoint.
	applyTarget := withControl.Clone()
	applyTarget.AlterWalState = nil
	applyTarget.ReplicateCheckpoint = nil
	applyTarget.ApplyControl(control)
	assert.Equal(t, uint64(123456), applyTarget.ReplicateCheckpoint.GetTimeTick())
	assert.Equal(t, rmq.NewRmqID(2).IntoProto(), applyTarget.ReplicateCheckpoint.GetMessageId())
}

func TestControlCheckpointRoundTripPreservesIndependentFrontier(t *testing.T) {
	control := &streamingpb.PChannelRecoveryControlMeta{
		CheckpointTimeTick: 120,
		ReplicateConfig: &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{{ClusterId: "local"}},
		},
		AlterWalState: &streamingpb.AlterWALState{
			TimeTick: 115, Stage: streamingpb.AlterWALStage_FLUSHING,
		},
	}
	cp := &WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 100, Term: 3}
	cp.ApplyControl(control)
	control.CheckpointTimeTick = 130
	control.AlterWalState.Stage = streamingpb.AlterWALStage_ADVANCE_CHECKPOINT
	control.ReplicateConfig.Clusters[0].ClusterId = "changed"

	cloned := cp.Clone()
	cp.ControlCheckpointTimeTick = 140
	cp.AlterWalState.TimeTick = 135
	encoded, err := proto.Marshal(cloned.IntoProto())
	require.NoError(t, err)
	stored := &streamingpb.WALCheckpoint{}
	require.NoError(t, proto.Unmarshal(encoded, stored))
	recovered := NewWALCheckpointFromProto(stored)
	require.Equal(t, uint64(100), recovered.TimeTick)
	require.True(t, rmq.NewRmqID(10).EQ(recovered.MessageID))
	require.Equal(t, int64(3), recovered.Term)
	require.Equal(t, uint64(120), recovered.ControlCheckpointTimeTick)
	state := PChannelControlFromCheckpoint(recovered)
	require.Equal(t, uint64(120), state.CheckpointTimeTick)
	require.Equal(t, "local", state.ReplicateConfig.Clusters[0].ClusterId)
	require.Equal(t, uint64(115), state.AlterWalState.TimeTick)
	require.Equal(t, streamingpb.AlterWALStage_FLUSHING, state.AlterWalState.Stage)

	// A covered AlterWAL event may advance its stage without moving either frontier.
	state.AlterWalState.Stage = streamingpb.AlterWALStage_ADVANCE_CHECKPOINT
	recovered.ApplyControl(state)
	reopened := PChannelControlFromCheckpoint(NewWALCheckpointFromProto(recovered.IntoProto()))
	require.Equal(t, uint64(120), reopened.CheckpointTimeTick)
	require.Equal(t, streamingpb.AlterWALStage_ADVANCE_CHECKPOINT, reopened.AlterWalState.Stage)
}

func TestControlCheckpointRecoveryUsesGlobalFloor(t *testing.T) {
	for _, controlTimeTick := range []uint64{0, 80, 100, 120} {
		stored := &streamingpb.WALCheckpoint{
			MessageId: rmq.NewRmqID(10).IntoProto(), TimeTick: 100,
			ControlCheckpointTimeTick: controlTimeTick,
		}
		cp := NewWALCheckpointFromProto(stored)
		state := PChannelControlFromCheckpoint(cp)
		require.Equal(t, max(uint64(100), controlTimeTick), state.CheckpointTimeTick)
		require.Equal(t, uint64(100), cp.TimeTick, "control never changes the global replay position")
	}
}
