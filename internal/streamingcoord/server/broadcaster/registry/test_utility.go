//go:build test
// +build test

package registry

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

func ResetRegistration() {
	resetMessageAckCallbacks()
	resetMessageAckOnceCallbacks()
	resetAppendFirstAppliedResolvers()
}

// ResetConsumerAckObservers drops every consumer ack observer and observes
// the given message types in addition to the fixed set. Only for tests.
func ResetConsumerAckObservers(extra ...message.MessageType) {
	consumerAckObserversMu.Lock()
	defer consumerAckObserversMu.Unlock()
	consumerAckObservers = newConsumerAckObserverSlots(append(append([]message.MessageType{}, consumerAckObserved...), extra...)...)
}
