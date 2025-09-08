package registry

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

type (
	// MessageCheckCallback is the callback function for the message type.
	MessageCheckCallback[H proto.Message, B proto.Message] = func(ctx context.Context, msg message.SpecializedBroadcastMessage[H, B]) error
	messageInnerCheckCallback                              = func(ctx context.Context, msg message.BroadcastMutableMessage) error
)

// messageCheckCallbacks is the map of message type to the callback function.
var messageCheckCallbacks map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerCheckCallback]

// registerMessageCheckCallback registers the callback function for the message type.
func registerMessageCheckCallback[H proto.Message, B proto.Message](callback MessageCheckCallback[H, B]) {
	typ := message.MustGetMessageTypeWithVersion[H, B]()
	future, ok := messageCheckCallbacks[typ]
	if !ok {
		panic(fmt.Sprintf("the future of check message callback for type %s is not registered", typ))
	}
	if future.Ready() {
		// only for test, the register callback should be called once and only once
		return
	}
	future.Set(func(ctx context.Context, msg message.BroadcastMutableMessage) error {
		specializedMsg := message.MustAsSpecializedBroadcastMessage[H, B](msg)
		return callback(ctx, specializedMsg)
	})
}

// CallMessageCheckCallback calls the callback function for the message type.
func CallMessageCheckCallback(ctx context.Context, msg message.BroadcastMutableMessage) error {
	callbackFuture, ok := messageCheckCallbacks[msg.MessageTypeWithVersion()]
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
