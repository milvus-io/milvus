package rmq

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestMessageID(t *testing.T) {
	assert.Equal(t, int64(1), message.MessageID(rmqID(1)).(interface{ RmqID() int64 }).RmqID())
	assert.Equal(t, walName, rmqID(1).WALName())

	assert.True(t, rmqID(1).LT(rmqID(2)))
	assert.True(t, rmqID(1).EQ(rmqID(1)))
	assert.True(t, rmqID(1).LTE(rmqID(1)))
	assert.True(t, rmqID(1).LTE(rmqID(2)))
	assert.False(t, rmqID(2).LT(rmqID(1)))
	assert.False(t, rmqID(2).EQ(rmqID(1)))
	assert.False(t, rmqID(2).LTE(rmqID(1)))
	assert.True(t, rmqID(2).LTE(rmqID(2)))

	msgID, err := UnmarshalMessageID(rmqID(1).Marshal())
	assert.NoError(t, err)
	assert.Equal(t, rmqID(1), msgID)

	_, err = UnmarshalMessageID(string([]byte{0x01, 0x02, 0x03, 0x04}))
	assert.Error(t, err)
}
