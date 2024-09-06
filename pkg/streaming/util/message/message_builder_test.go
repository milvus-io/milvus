package message_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestMessage(t *testing.T) {
	b := message.NewTimeTickMessageBuilderV1()
	mutableMessage, err := b.WithHeader(&message.TimeTickMessageHeader{}).
		WithProperties(map[string]string{"key": "value"}).
		WithProperty("key2", "value2").
		WithVChannel("v1").
		WithBody(&msgpb.TimeTickMsg{}).BuildMutable()
	assert.NoError(t, err)

	payload, err := proto.Marshal(&message.TimeTickMessageHeader{})
	assert.NoError(t, err)

	assert.True(t, bytes.Equal(payload, mutableMessage.Payload()))
	assert.True(t, mutableMessage.Properties().Exist("key"))
	v, ok := mutableMessage.Properties().Get("key")
	assert.True(t, mutableMessage.Properties().Exist("key2"))
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, mutableMessage.MessageType())
	assert.Equal(t, 31, mutableMessage.EstimateSize())
	mutableMessage.WithTimeTick(123)
	mutableMessage.WithBarrierTimeTick(456)
	mutableMessage.WithWALTerm(1)
	v, ok = mutableMessage.Properties().Get("_tt")
	assert.True(t, ok)
	tt, err := message.DecodeUint64(v)
	assert.Equal(t, uint64(123), tt)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123), mutableMessage.TimeTick())
	assert.Equal(t, uint64(456), mutableMessage.BarrierTimeTick())

	lcMsgID := walimplstest.NewTestMessageID(1)
	mutableMessage.WithLastConfirmed(lcMsgID)
	v, ok = mutableMessage.Properties().Get("_lc")
	assert.True(t, ok)
	assert.Equal(t, v, "1")

	v, ok = mutableMessage.Properties().Get("_vc")
	assert.True(t, ok)
	assert.Equal(t, "v1", v)
	assert.Equal(t, "v1", mutableMessage.VChannel())

	msgID := walimplstest.NewTestMessageID(1)
	immutableMessage := message.NewImmutableMesasge(msgID,
		[]byte("payload"),
		map[string]string{
			"key": "value",
			"_t":  "1",
			"_tt": message.EncodeUint64(456),
			"_v":  "1",
			"_lc": "1",
		})

	assert.True(t, immutableMessage.MessageID().EQ(msgID))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, message.MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 30, immutableMessage.EstimateSize())
	assert.Equal(t, message.Version(1), immutableMessage.Version())
	assert.Equal(t, uint64(456), immutableMessage.TimeTick())
	assert.NotNil(t, immutableMessage.LastConfirmedMessageID())

	immutableMessage = message.NewImmutableMesasge(
		msgID,
		[]byte("payload"),
		map[string]string{
			"key": "value",
			"_t":  "1",
		})

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
		message.NewTimeTickMessageBuilderV1().BuildMutable()
	})
}

func TestLastConfirmed(t *testing.T) {
	flush, _ := message.NewFlushMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).
		BuildMutable()

	imFlush := flush.WithTimeTick(1).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	assert.True(t, imFlush.LastConfirmedMessageID().EQ(walimplstest.NewTestMessageID(1)))
}
