package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func TestNewWALCheckpointFromProto(t *testing.T) {
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
	assert.Equal(t, "by-dev", newCheckpoint.ReplicateCheckpoint.ClusterID)
	assert.Equal(t, "p1", newCheckpoint.ReplicateCheckpoint.PChannel)
	assert.Equal(t, uint64(0), newCheckpoint.ReplicateCheckpoint.TimeTick)
	assert.Nil(t, newCheckpoint.ReplicateCheckpoint.MessageID)
	assert.NotNil(t, newCheckpoint.ReplicateConfig)

	protoCheckpoint.ReplicateCheckpoint.MessageId = rmq.NewRmqID(2).IntoProto()
	protoCheckpoint.ReplicateCheckpoint.TimeTick = 123456

	newCheckpoint = NewWALCheckpointFromProto(protoCheckpoint)
	assert.Equal(t, "by-dev", newCheckpoint.ReplicateCheckpoint.ClusterID)
	assert.Equal(t, "p1", newCheckpoint.ReplicateCheckpoint.PChannel)
	assert.Equal(t, uint64(123456), newCheckpoint.ReplicateCheckpoint.TimeTick)
	assert.True(t, rmq.NewRmqID(2).EQ(newCheckpoint.ReplicateCheckpoint.MessageID))
	assert.NotNil(t, newCheckpoint.ReplicateConfig)
}
