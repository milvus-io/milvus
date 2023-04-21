package nmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNmqMessage_All(t *testing.T) {
	topic := "t"
	payload := []byte("test payload")
	msg := Message{MsgID: 12, Topic: topic, Payload: payload}
	nm := &nmqMessage{msg: msg}
	assert.Equal(t, topic, nm.Topic())
	assert.Equal(t, uint64(12), nm.ID().(*nmqID).messageID)
	assert.Equal(t, payload, nm.Payload())
	assert.Nil(t, nm.Properties())
}
