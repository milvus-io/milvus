package message_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/mock_message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

func TestMessage(t *testing.T) {
	b := message.NewBuilder()
	mutableMessage := b.WithMessageType(message.MessageTypeTimeTick).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{"key": "value"}).
		BuildMutable()

	assert.Equal(t, "payload", string(mutableMessage.Payload()))
	assert.True(t, mutableMessage.Properties().Exist("key"))
	v, ok := mutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, mutableMessage.MessageType())
	assert.Equal(t, 21, mutableMessage.EstimateSize())
	mutableMessage.WithTimeTick(123)
	v, ok = mutableMessage.Properties().Get("_tt")
	assert.True(t, ok)
	assert.Equal(t, "123", v)

	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().EQ(msgID).Return(true)

	b = message.NewBuilder()
	immutableMessage := b.WithMessageID(msgID).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{
			"key": "value",
			"_t":  "1",
			"_tt": "456",
			"_v":  "1",
		}).
		BuildImmutable()

	assert.True(t, immutableMessage.MessageID().EQ(msgID))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 27, immutableMessage.EstimateSize())
	assert.Equal(t, message.Version(1), immutableMessage.Version())
	assert.Equal(t, uint64(456), immutableMessage.TimeTick())

	b = message.NewBuilder()
	immutableMessage = b.WithMessageID(msgID).
		WithPayload([]byte("payload")).
		WithProperty("key", "value").
		WithProperty("_t", "1").
		BuildImmutable()

	assert.True(t, immutableMessage.MessageID().EQ(msgID))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 18, immutableMessage.EstimateSize())
	assert.Equal(t, message.Version(0), immutableMessage.Version())
	assert.Panics(t, func() {
		immutableMessage.TimeTick()
	})

	assert.Panics(t, func() {
		message.NewBuilder().WithMessageID(msgID).BuildMutable()
	})
	assert.Panics(t, func() {
		message.NewBuilder().BuildImmutable()
	})
}
