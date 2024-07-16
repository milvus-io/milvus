package rmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageID(t *testing.T) {
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
