package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type mockMessage struct {
	topic      string
	properties map[string]string
	payload    []byte
	id         MessageID
}

func (m *mockMessage) Topic() string {
	return m.topic
}

func (m *mockMessage) Properties() map[string]string {
	return m.properties
}

func (m *mockMessage) Payload() []byte {
	return m.payload
}

func (m *mockMessage) ID() MessageID {
	return m.id
}

func TestGetMsgType(t *testing.T) {
	t.Run("Test with properties", func(t *testing.T) {
		properties := map[string]string{
			MsgTypeKey: "Insert",
		}
		msg := &mockMessage{
			properties: properties,
		}
		msgType, err := GetMsgType(msg)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_Insert, msgType)
	})

	t.Run("Test with payload", func(t *testing.T) {
		header := &commonpb.MsgHeader{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Insert,
			},
		}
		payload, err := proto.Marshal(header)
		assert.NoError(t, err)

		msg := &mockMessage{
			payload: payload,
		}
		msgType, err := GetMsgType(msg)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_Insert, msgType)
	})

	t.Run("Test with empty payload and properties", func(t *testing.T) {
		msg := &mockMessage{}
		msgType, err := GetMsgType(msg)
		assert.Error(t, err)
		assert.Equal(t, commonpb.MsgType_Undefined, msgType)
	})
}
