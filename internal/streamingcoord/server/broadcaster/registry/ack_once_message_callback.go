package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"google.golang.org/protobuf/proto"
)

// MessageAckOnceCallback is the callback function for the message type.
// It will be called when the message is acked by streamingnode at one channel at a time.
type (
	MessageAckOnceCallback[H proto.Message, B proto.Message] = func(ctx context.Context, result message.AckResult[H, B]) error
	messageInnerAckOnceCallback                              = func(ctx context.Context, msg message.ImmutableMessage) error
)

// messageAckOnceCallbacks is the map of message type to the ack once callback function.
var messageAckOnceCallbacks map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerAckOnceCallback]

// registerMessageAckOnceCallback registers the ack once callback function for the message type.
func registerMessageAckOnceCallback[H proto.Message, B proto.Message](callback MessageAckOnceCallback[H, B]) {
	typ := message.MustGetMessageTypeWithVersion[H, B]()
	future, ok := messageAckOnceCallbacks[typ]
	if !ok {
		panic(fmt.Sprintf("the future of ack once callback for type %s is not registered", typ))
	}
	if future.Ready() {
		// only for test, the register callback should be called once and only once
		return
	}
	future.Set(func(ctx context.Context, msg message.ImmutableMessage) error {
		return callback(ctx, message.AckResult[H, B]{
			Message: message.MustAsSpecializedImmutableMessage[H, B](msg),
		})
	})
}

// CallMessageAckOnceCallbacks calls the ack callback function for the message type in batch.
func CallMessageAckOnceCallbacks(ctx context.Context, msgs ...message.ImmutableMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	if len(msgs) == 1 {
		return callMessageAckOnceCallback(ctx, msgs[0])
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(msgs))
	errc := make(chan error, len(msgs))
	for _, msg := range msgs {
		msg := msg
		go func() {
			defer wg.Done()
			errc <- callMessageAckOnceCallback(ctx, msg)
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

// callMessageAckOnceCallback calls the ack once callback function for the message type.
func callMessageAckOnceCallback(ctx context.Context, msg message.ImmutableMessage) error {
	callbackFuture, ok := messageAckOnceCallbacks[msg.MessageTypeWithVersion()]
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
