package registry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestResolvedAckOnceCallback(t *testing.T) {
	ResetRegistration()
	t.Cleanup(ResetRegistration)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	callback, err := ResolveMessageAckOnceCallback(ctx, message.MessageTypeTruncateCollectionV2)
	require.Nil(t, callback)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	var calls atomic.Int32
	RegisterTruncateCollectionV2AckOnceCallback(func(context.Context, message.AckResultTruncateCollectionMessageV2) error {
		calls.Add(1)
		return context.Canceled
	})
	callback, err = ResolveMessageAckOnceCallback(context.Background(), message.MessageTypeTruncateCollectionV2)
	require.NoError(t, err)
	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(&message.TruncateCollectionMessageHeader{}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast([]string{"v1"}).MustBuildBroadcast().WithBroadcastID(1).
		SplitIntoMutableMessage()[0].WithTimeTick(1).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	require.ErrorIs(t, CallResolvedMessageAckOnceCallbacks(context.Background(), callback, msg, msg), context.Canceled)
	require.Equal(t, int32(2), calls.Load())
	require.NoError(t, CallResolvedMessageAckOnceCallbacks(context.Background(), callback))
	require.NoError(t, CallResolvedMessageAckOnceCallbacks(context.Background(), nil, msg))

	callback, err = ResolveMessageAckOnceCallback(context.Background(), message.MessageTypeDropCollectionV1)
	require.NoError(t, err)
	require.Nil(t, callback)
}
