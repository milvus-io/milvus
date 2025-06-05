package registry

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// MessageCheckCallback is the callback function for the message type.
type MessageCheckCallback = func(ctx context.Context, msg message.BroadcastMutableMessage) error

// resetMessageCheckCallbacks resets the message check callbacks.
func resetMessageCheckCallbacks() {
	messageCheckCallbacks = map[message.MessageType]*syncutil.Future[MessageCheckCallback]{
		message.MessageTypeImport: syncutil.NewFuture[MessageCheckCallback](),
	}
}

// messageCheckCallbacks is the map of message type to the callback function.
var messageCheckCallbacks map[message.MessageType]*syncutil.Future[MessageCheckCallback]

// RegisterMessageCheckCallback registers the callback function for the message type.
func RegisterMessageCheckCallback(typ message.MessageType, callback MessageCheckCallback) {
	future, ok := messageCheckCallbacks[typ]
	if !ok {
		panic(fmt.Sprintf("the future of check message callback for type %s is not registered", typ))
	}
	if future.Ready() {
		// only for test, the register callback should be called once and only once
		return
	}
	future.Set(callback)
}

// CallMessageCheckCallback calls the callback function for the message type.
func CallMessageCheckCallback(ctx context.Context, msg message.BroadcastMutableMessage) error {
	callbackFuture, ok := messageCheckCallbacks[msg.MessageType()]
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
