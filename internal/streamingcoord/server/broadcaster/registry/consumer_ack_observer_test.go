package registry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestConsumerAckObserver(t *testing.T) {
	ResetConsumerAckObservers()
	t.Cleanup(func() { ResetConsumerAckObservers() })
	ctx := context.Background()
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeSplitShard)

	// A message type that is not observed passes at once.
	other := mock_message.NewMockImmutableMessage(t)
	other.EXPECT().MessageType().Return(message.MessageTypeManualFlush)
	require.NoError(t, CallConsumerAckObserver(ctx, other))
	require.Panics(t, func() { RegisterConsumerAckObserver(message.MessageTypeManualFlush, nil) })

	// The acknowledgement of an observed type waits for the registration: it
	// fails with the context of the caller, and is sent again.
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	require.ErrorIs(t, CallConsumerAckObserver(waitCtx, msg), context.DeadlineExceeded)
	cancel()

	waiting := make(chan error, 1)
	go func() { waiting <- CallConsumerAckObserver(ctx, msg) }()
	select {
	case err := <-waiting:
		require.FailNow(t, "the acknowledgement passed before the observer was registered", "%v", err)
	case <-time.After(20 * time.Millisecond):
	}

	calls := 0
	observerErr := errors.New("mock")
	RegisterConsumerAckObserver(message.MessageTypeSplitShard, func(_ context.Context, got message.ImmutableMessage) error {
		calls++
		require.Equal(t, msg, got)
		if calls == 1 {
			return observerErr
		}
		return nil
	})
	require.ErrorIs(t, <-waiting, observerErr)
	require.NoError(t, CallConsumerAckObserver(ctx, msg))
	require.Equal(t, 2, calls)

	// Registered again, the observer is replaced; a nil one observes nothing.
	RegisterConsumerAckObserver(message.MessageTypeSplitShard, nil)
	require.NoError(t, CallConsumerAckObserver(ctx, msg))
	require.Equal(t, 2, calls)
}
