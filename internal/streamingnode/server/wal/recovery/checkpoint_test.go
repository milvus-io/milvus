package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
	checkpoint := newWALCheckpointFromProto(protoCheckpoint)

	assert.True(t, messageID.EQ(checkpoint.MessageID))
	assert.Equal(t, timeTick, checkpoint.TimeTick)
	assert.Equal(t, recoveryMagic, checkpoint.Magic)

	proto := checkpoint.IntoProto()
	checkpoint2 := newWALCheckpointFromProto(proto)
	assert.True(t, messageID.EQ(checkpoint2.MessageID))
	assert.Equal(t, timeTick, checkpoint2.TimeTick)
	assert.Equal(t, recoveryMagic, checkpoint2.Magic)

	checkpoint3 := checkpoint.Clone()
	assert.True(t, messageID.EQ(checkpoint3.MessageID))
	assert.Equal(t, timeTick, checkpoint3.TimeTick)
	assert.Equal(t, recoveryMagic, checkpoint3.Magic)
}
