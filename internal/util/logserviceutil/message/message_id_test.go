package message_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/mock_message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

func TestRegisterMessageIDUnmarshaler(t *testing.T) {
	msgID := mock_message.NewMockMessageID(t)

	message.RegisterMessageIDUnmsarshaler("test", func(b []byte) (message.MessageID, error) {
		if bytes.Equal(b, []byte("123")) {
			return msgID, nil
		}
		return nil, errors.New("invalid")
	})

	id, err := message.UnmarshalMessageID("test", []byte("123"))
	assert.NotNil(t, id)
	assert.NoError(t, err)

	id, err = message.UnmarshalMessageID("test", []byte("1234"))
	assert.Nil(t, id)
	assert.Error(t, err)

	assert.Panics(t, func() {
		message.UnmarshalMessageID("test1", []byte("123"))
	})

	assert.Panics(t, func() {
		message.RegisterMessageIDUnmsarshaler("test", func(b []byte) (message.MessageID, error) {
			if bytes.Equal(b, []byte("123")) {
				return msgID, nil
			}
			return nil, errors.New("invalid")
		})
	})
}
