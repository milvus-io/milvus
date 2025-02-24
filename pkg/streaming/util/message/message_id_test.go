package message_test

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestRegisterMessageIDUnmarshaler(t *testing.T) {
	msgID := mock_message.NewMockMessageID(t)

	message.RegisterMessageIDUnmsarshaler("test", func(b string) (message.MessageID, error) {
		if b == "123" {
			return msgID, nil
		}
		return nil, errors.New("invalid")
	})

	id, err := message.UnmarshalMessageID("test", "123")
	assert.NotNil(t, id)
	assert.NoError(t, err)

	id, err = message.UnmarshalMessageID("test", "1234")
	assert.Nil(t, id)
	assert.Error(t, err)

	assert.Panics(t, func() {
		message.UnmarshalMessageID("test1", "123")
	})

	assert.Panics(t, func() {
		message.RegisterMessageIDUnmsarshaler("test", func(b string) (message.MessageID, error) {
			if b == "123" {
				return msgID, nil
			}
			return nil, errors.New("invalid")
		})
	})
}
