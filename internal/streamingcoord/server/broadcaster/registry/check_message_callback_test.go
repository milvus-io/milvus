package registry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestCheckMessageCallbackRegistration(t *testing.T) {
	// Reset callbacks before test
	resetMessageCheckCallbacks()

	// Test registering a callback
	called := false
	callback := func(ctx context.Context, msg message.BroadcastImportMessageV1) (message.BroadcastMutableMessage, error) {
		called = true
		return msg.BroadcastMessage(), nil
	}

	// Register callback for DropPartition message type
	RegisterImportV1CheckCallback(callback)

	// Verify callback was registered
	callbackFuture, ok := messageCheckCallbacks[message.MessageTypeImportV1]
	assert.True(t, ok)
	assert.NotNil(t, callbackFuture)

	// Create a mock message
	msg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&message.ImportMsg{}).
		WithBroadcast([]string{"v1"}).MustBuildBroadcast()

	// Call the callback
	newMsg, err := CallMessageCheckCallback(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, called)
	assert.NotNil(t, newMsg)

	resetMessageCheckCallbacks()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	newMsg, err = CallMessageCheckCallback(ctx, msg)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Nil(t, newMsg)
}
