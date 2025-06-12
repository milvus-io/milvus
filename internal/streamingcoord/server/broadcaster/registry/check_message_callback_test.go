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

func TestCheckMessageCallbackRegistration(t *testing.T) {
	// Reset callbacks before test
	resetMessageCheckCallbacks()

	// Test registering a callback
	called := false
	callback := func(ctx context.Context, msg message.BroadcastMutableMessage) error {
		called = true
		return nil
	}

	// Register callback for DropPartition message type
	RegisterMessageCheckCallback(message.MessageTypeImport, callback)

	// Verify callback was registered
	callbackFuture, ok := messageCheckCallbacks[message.MessageTypeImport]
	assert.True(t, ok)
	assert.NotNil(t, callbackFuture)

	// Create a mock message
	msg := mock_message.NewMockBroadcastMutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeImport)

	// Call the callback
	err := CallMessageCheckCallback(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, called)

	resetMessageCheckCallbacks()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = CallMessageCheckCallback(ctx, msg)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}
