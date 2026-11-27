package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ConsumerAckObserver is called when a StreamingNode acknowledges one copy of
// a broadcast message from the consuming side, that is, when the local work
// the copy started on its vchannel is durable.
//
// It differs from an ack-once callback in when it runs. A broadcast without
// AckSyncUp is acknowledged by the broadcaster as soon as its copies are
// appended, and its ack callbacks run then. The acknowledgement of the
// consuming side arrives later and changes nothing in the task; an observer is
// the only code that sees it.
//
// An observer runs for every acknowledgement the broadcaster receives for a
// live task. A replicated split source may deliver proof even after completion.
// A completed broadcast must never recreate removed coordinator state. Observers
// must be idempotent. An error is returned to the StreamingNode, which sends
// the acknowledgement again.
type ConsumerAckObserver func(ctx context.Context, msg message.ImmutableMessage, completed bool) error

// consumerAckObserved is the fixed set of message types whose consumer-side
// acknowledgements are observed. An acknowledgement of one of them waits until
// the observer is registered, like an ack callback waits for its registration:
// the broadcaster accepts acknowledgements before the coordinators have
// finished starting, and an acknowledgement that passed unobserved would
// never be sent again.
var consumerAckObserved = []message.MessageType{
	message.MessageTypeSplitShard,
}

type consumerAckObserverSlot struct {
	// registered is closed by the first registration.
	registered chan struct{}
	observer   ConsumerAckObserver
}

var (
	consumerAckObserversMu sync.RWMutex
	consumerAckObservers   = newConsumerAckObserverSlots(consumerAckObserved...)
)

func newConsumerAckObserverSlots(types ...message.MessageType) map[message.MessageType]*consumerAckObserverSlot {
	slots := make(map[message.MessageType]*consumerAckObserverSlot, len(types))
	for _, typ := range types {
		slots[typ] = &consumerAckObserverSlot{registered: make(chan struct{})}
	}
	return slots
}

// RegisterConsumerAckObserver sets the observer of the consumer-side
// acknowledgements of one of the observed message types. A nil observer
// registers one that observes nothing. Registering again replaces the
// observer.
func RegisterConsumerAckObserver(typ message.MessageType, observer ConsumerAckObserver) {
	consumerAckObserversMu.Lock()
	defer consumerAckObserversMu.Unlock()
	slot, ok := consumerAckObservers[typ]
	if !ok {
		panic(fmt.Sprintf("the consumer acks of message type %s are not observed", typ))
	}
	slot.observer = observer
	select {
	case <-slot.registered:
	default:
		close(slot.registered)
	}
}

// CallConsumerAckObserver calls the observer of the type of the message. For
// an observed type it waits until the observer is registered.
func CallConsumerAckObserver(ctx context.Context, msg message.ImmutableMessage, completed bool) error {
	consumerAckObserversMu.RLock()
	slot, ok := consumerAckObservers[msg.MessageType()]
	consumerAckObserversMu.RUnlock()
	if !ok {
		return nil
	}
	select {
	case <-ctx.Done():
		return merr.Wrap(ctx.Err(), "when waiting the consumer ack observer registered")
	case <-slot.registered:
	}
	consumerAckObserversMu.RLock()
	observer := slot.observer
	consumerAckObserversMu.RUnlock()
	if observer == nil {
		return nil
	}
	return observer(ctx, msg, completed)
}
