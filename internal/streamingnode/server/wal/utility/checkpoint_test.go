package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
