package registry

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// init the message ack callbacks
func init() {
	resetMessageAckCallbacks()
	resetMessageCheckCallbacks()
}

// resetMessageAckCallbacks resets the message ack callbacks.
func resetMessageAckCallbacks() {
	messageAckCallbacks = map[message.MessageType]*syncutil.Future[MessageAckCallback]{
		message.MessageTypeDropPartition: syncutil.NewFuture[MessageAckCallback](),
	}
}

// MessageAckCallback is the callback function for the message type.
type MessageAckCallback = func(ctx context.Context, msg message.MutableMessage) error

// messageAckCallbacks is the map of message type to the callback function.
var messageAckCallbacks map[message.MessageType]*syncutil.Future[MessageAckCallback]

// RegisterMessageAckCallback registers the callback function for the message type.
func RegisterMessageAckCallback(typ message.MessageType, callback MessageAckCallback) {
	future, ok := messageAckCallbacks[typ]
	if !ok {
		panic(fmt.Sprintf("the future of message callback for type %s is not registered", typ))
	}
	if future.Ready() {
		// only for test, the register callback should be called once and only once
		return
	}
	future.Set(callback)
}

// CallMessageAckCallback calls the callback function for the message type.
func CallMessageAckCallback(ctx context.Context, msg message.MutableMessage) error {
	callbackFuture, ok := messageAckCallbacks[msg.MessageType()]
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
