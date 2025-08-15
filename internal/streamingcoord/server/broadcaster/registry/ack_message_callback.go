package registry

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// MessageAckCallback is the callback function for the message type.
type (
	MessageAckCallback[H proto.Message, B proto.Message] = func(ctx context.Context, params message.SpecializedImmutableMessage[H, B]) error
	messageInnerAckCallback                              = func(ctx context.Context, msgs message.ImmutableMessage) error
)

// messageAckCallbacks is the map of message type to the callback function.
var messageAckCallbacks map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerAckCallback]

// registerMessageAckCallback registers the callback function for the message type.
func registerMessageAckCallback[H proto.Message, B proto.Message](callback MessageAckCallback[H, B]) {
	typ := message.MustGetMessageTypeWithVersion[H, B]()
	future, ok := messageAckCallbacks[typ]
	if !ok {
		panic(fmt.Sprintf("the future of message callback for type %s is not registered", typ))
	}
	if future.Ready() {
		// only for test, the register callback should be called once and only once
		return
	}
	future.Set(func(ctx context.Context, msgs message.ImmutableMessage) error {
		specializedMsg := message.MustAsSpecializedImmutableMessage[H, B](msgs)
		return callback(ctx, specializedMsg)
	})
}

// CallMessageAckCallback calls the callback function for the message type.
func CallMessageAckCallback(ctx context.Context, msg message.ImmutableMessage) error {
	callbackFuture, ok := messageAckCallbacks[msg.MessageTypeWithVersion()]
	if !ok {
		// No callback need tobe called, return nil
		return nil
	}
	callback, err := callbackFuture.GetWithContext(ctx)
	if err != nil {
		return errors.Wrap(err, "when waiting callback registered")
	}
	return callback(ctx, msg)
}
