package message_test

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
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
	tt, n := proto.DecodeVarint([]byte(v))
	assert.Equal(t, uint64(123), tt)
	assert.Equal(t, len([]byte(v)), n)

	lcMsgID := mock_message.NewMockMessageID(t)
	lcMsgID.EXPECT().Marshal().Return([]byte("lcMsgID"))
	mutableMessage.WithLastConfirmed(lcMsgID)
	v, ok = mutableMessage.Properties().Get("_lc")
	assert.True(t, ok)
	assert.Equal(t, v, "lcMsgID")

	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().EQ(msgID).Return(true)
	msgID.EXPECT().WALName().Return("testMsgID")
	message.RegisterMessageIDUnmsarshaler("testMsgID", func(data []byte) (message.MessageID, error) {
		if string(data) == "lcMsgID" {
			return msgID, nil
		}
		panic(fmt.Sprintf("unexpected data: %s", data))
	})

	b = message.NewBuilder()
	immutableMessage := b.WithMessageID(msgID).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{
			"key": "value",
			"_t":  "1",
			"_tt": string(proto.EncodeVarint(456)),
			"_v":  "1",
			"_lc": "lcMsgID",
		}).
		BuildImmutable()

	assert.True(t, immutableMessage.MessageID().EQ(msgID))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 36, immutableMessage.EstimateSize())
	assert.Equal(t, message.Version(1), immutableMessage.Version())
	assert.Equal(t, uint64(456), immutableMessage.TimeTick())
	assert.NotNil(t, immutableMessage.LastConfirmedMessageID())

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
		immutableMessage.LastConfirmedMessageID()
	})

	assert.Panics(t, func() {
		message.NewBuilder().WithMessageID(msgID).BuildMutable()
	})
	assert.Panics(t, func() {
		message.NewBuilder().BuildImmutable()
	})
}
