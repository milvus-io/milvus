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

	protoCheckpoint.ReplicateConfig = &commonpb.ReplicateConfiguration{}
	protoCheckpoint.ReplicateCheckpoint = &commonpb.ReplicateCheckpoint{
		ClusterId: "by-dev",
		Pchannel:  "p1",
		MessageId: nil,
		TimeTick:  0,
	}
	newCheckpoint := NewWALCheckpointFromProto(protoCheckpoint)
	assert.True(t, messageID.EQ(newCheckpoint.MessageID))
	control := PChannelRecoveryControlMetaFromLegacyCheckpoint(protoCheckpoint)
	assert.Equal(t, timeTick, control.GetCheckpointTimeTick())
	assert.Equal(t, "by-dev", control.GetReplicateCheckpoint().GetClusterId())
	assert.Equal(t, "p1", control.GetReplicateCheckpoint().GetPchannel())
	assert.NotNil(t, control.GetReplicateConfig())

	protoCheckpoint.ReplicateCheckpoint.MessageId = rmq.NewRmqID(2).IntoProto()
	protoCheckpoint.ReplicateCheckpoint.TimeTick = 123456

	control = PChannelRecoveryControlMetaFromLegacyCheckpoint(protoCheckpoint)
	assert.Equal(t, uint64(123456), control.GetReplicateCheckpoint().GetTimeTick())
	assert.Equal(t, rmq.NewRmqID(2).IntoProto(), control.GetReplicateCheckpoint().GetMessageId())

	// New checkpoints intentionally contain only the global WAL position.
	proto = newCheckpoint.IntoProto()
	assert.Nil(t, proto.GetReplicateConfig())
	assert.Nil(t, proto.GetReplicateCheckpoint())
}
