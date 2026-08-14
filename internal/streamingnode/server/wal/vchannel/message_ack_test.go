package vchannel

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type testMessageObserver interface {
	ObserveMessage(context.Context, message.RetainedImmutableMessage)
}

func observeTestMessage(
	ctx context.Context,
	t *testing.T,
	observer testMessageObserver,
	raw message.ImmutableMessage,
) message.ImmutableMessage {
	t.Helper()
	owner := message.NewOwnedImmutableMessage(raw, nil)
	dispatch := owner.Clone()
	observer.ObserveMessage(ctx, dispatch)
	dispatch.Release()
	owner.Release()
	return raw
}
