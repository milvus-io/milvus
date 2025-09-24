package registry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestMessageCallbackRegistration(t *testing.T) {
	// Reset callbacks before test
	resetMessageAckCallbacks()

	// Test registering a callback
	called := false
	callback := func(ctx context.Context, msg message.BroadcastResultDropPartitionMessageV1) error {
		called = true
		return nil
	}

	RegisterDropPartitionV1AckCallback(callback)

	// Verify callback was registered
	callbackFuture, ok := messageAckCallbacks[message.MessageTypeDropPartitionV1]
	assert.True(t, ok)
	assert.NotNil(t, callbackFuture)

	// Create a mock message
	msg := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{}).
		WithBody(&message.DropPartitionRequest{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	// Call the callback
	err := CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{
		"v1": {
			MessageID:              walimplstest.NewTestMessageID(1),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
			TimeTick:               1,
		},
	})
	assert.NoError(t, err)
	assert.True(t, called)

	resetMessageAckCallbacks()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = CallMessageAckCallback(ctx, msg, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}
