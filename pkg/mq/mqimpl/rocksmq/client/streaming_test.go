package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/server"
)

func TestStreaming(t *testing.T) {
	payload, err := marshalStreamingMessage(&common.ProducerMessage{
		Payload: []byte("payload"),
		Properties: map[string]string{
			"key": "value",
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, payload)

	msg, err := unmarshalStreamingMessage("topic", server.ConsumerMessage{
		MsgID:   1,
		Payload: payload,
	})
	assert.NoError(t, err)
	assert.Equal(t, string(msg.Payload()), "payload")
	assert.Equal(t, msg.Properties()["key"], "value")
	msg, err = unmarshalStreamingMessage("topic", server.ConsumerMessage{
		MsgID:   1,
		Payload: payload[1:],
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errNotStreamingServiceMessage)
	assert.Nil(t, msg)
}
