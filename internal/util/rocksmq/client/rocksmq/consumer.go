package rocksmq

import (
	"context"
)

type SubscriptionInitialPosition int

const (
	SubscriptionPositionLatest SubscriptionInitialPosition = iota
	SubscriptionPositionEarliest
)

type ConsumerOptions struct {
	// The topic that this consumer will subscribe on
	Topic string

	// The subscription name for this consumer
	SubscriptionName string

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitialPosition

	// Message for this consumer
	// When a message is received, it will be pushed to this channel for consumption
	MessageChannel chan ConsumerMessage
}

type ConsumerMessage struct {
	Payload []byte
}

type Consumer interface {
	// returns the substription for the consumer
	Subscription() string

	// Receive a single message
	Receive(ctx context.Context) (ConsumerMessage, error)

	// TODO: Chan returns a channel to consume messages from
	// Chan() <-chan ConsumerMessage
}
