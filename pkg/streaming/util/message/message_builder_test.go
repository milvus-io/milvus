package message_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestMessage(t *testing.T) {
	b := message.NewMutableMessageBuilder()
	mutableMessage := b.
		WithMessageType(message.MessageTypeTimeTick).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{"key": "value"}).
		BuildMutable()

	assert.Equal(t, "payload", string(mutableMessage.Payload()))
	assert.True(t, mutableMessage.Properties().Exist("key"))
	v, ok := mutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, mutableMessage.MessageType())
	assert.Equal(t, 24, mutableMessage.EstimateSize())
	mutableMessage.WithTimeTick(123)
	v, ok = mutableMessage.Properties().Get("_tt")
	assert.True(t, ok)
	tt, err := message.DecodeUint64(v)
	assert.Equal(t, uint64(123), tt)
	assert.NoError(t, err)

	lcMsgID := mock_message.NewMockMessageID(t)
	lcMsgID.EXPECT().Marshal().Return("lcMsgID")
	mutableMessage.WithLastConfirmed(lcMsgID)
	v, ok = mutableMessage.Properties().Get("_lc")
	assert.True(t, ok)
	assert.Equal(t, v, "lcMsgID")

	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().EQ(msgID).Return(true)
	msgID.EXPECT().WALName().Return("testMsgID")
	message.RegisterMessageIDUnmsarshaler("testMsgID", func(data string) (message.MessageID, error) {
		if data == "lcMsgID" {
			return msgID, nil
		}
		panic(fmt.Sprintf("unexpected data: %s", data))
	})

	immutableMessage := message.NewImmutableMesasge(msgID,
		[]byte("payload"),
		map[string]string{
			"key": "value",
			"_t":  "1200",
			"_tt": message.EncodeUint64(456),
			"_v":  "1",
			"_lc": "lcMsgID",
		})

	assert.True(t, immutableMessage.MessageID().EQ(msgID))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 39, immutableMessage.EstimateSize())
	assert.Equal(t, message.Version(1), immutableMessage.Version())
	assert.Equal(t, uint64(456), immutableMessage.TimeTick())
	assert.NotNil(t, immutableMessage.LastConfirmedMessageID())

	immutableMessage = message.NewImmutableMesasge(
		msgID,
		[]byte("payload"),
		map[string]string{
			"key": "value",
			"_t":  "1200",
		})

	assert.True(t, immutableMessage.MessageID().EQ(msgID))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 21, immutableMessage.EstimateSize())
	assert.Equal(t, message.Version(0), immutableMessage.Version())
	assert.Panics(t, func() {
		immutableMessage.TimeTick()
	})
	assert.Panics(t, func() {
		immutableMessage.LastConfirmedMessageID()
	})

	assert.Panics(t, func() {
		message.NewMutableMessageBuilder().BuildMutable()
	})
}
