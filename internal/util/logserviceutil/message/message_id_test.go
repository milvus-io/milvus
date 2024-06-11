package message

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestRegisterMessageIDUnmarshaler(t *testing.T) {
	msgID := NewTestMessageID(123)

	id, err := UnmarshalMessageID("test", []byte("123"))
	assert.True(t, id.EQ(msgID))
	assert.NoError(t, err)

	id, err = UnmarshalMessageID("test", []byte("1234a"))
	assert.Nil(t, id)
	assert.Error(t, err)

	assert.Panics(t, func() {
		UnmarshalMessageID("test1", []byte("123"))
	})

	assert.Panics(t, func() {
		RegisterMessageIDUnmsarshaler("test", func(b []byte) (MessageID, error) {
			if bytes.Equal(b, []byte("123")) {
				return msgID, nil
			}
			return nil, errors.New("invalid")
		})
	})
}
