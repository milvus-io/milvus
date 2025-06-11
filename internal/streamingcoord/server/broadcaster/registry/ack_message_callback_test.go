package registry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestMessageCallbackRegistration(t *testing.T) {
	// Reset callbacks before test
	resetMessageAckCallbacks()

	// Test registering a callback
	called := false
	callback := func(ctx context.Context, msg message.MutableMessage) error {
		called = true
		return nil
	}

	// Register callback for DropPartition message type
	RegisterMessageAckCallback(message.MessageTypeDropPartition, callback)

	// Verify callback was registered
	callbackFuture, ok := messageAckCallbacks[message.MessageTypeDropPartition]
	assert.True(t, ok)
	assert.NotNil(t, callbackFuture)

	// Create a mock message
	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeDropPartition)

	// Call the callback
	err := CallMessageAckCallback(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, called)

	resetMessageAckCallbacks()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = CallMessageAckCallback(ctx, msg)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}
