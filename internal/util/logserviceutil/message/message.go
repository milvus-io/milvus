package message

import (
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

var (
	_ BasicMessage     = (*messageImpl)(nil)
	_ MutableMessage   = (*messageImpl)(nil)
	_ ImmutableMessage = (*immutableMessageImpl)(nil)
)

// NewMQProducerMessageFromMutableMessage creates a new mq producer message from mutable message.
// TODO: remove in the future.
func NewMQProducerMessageFromMutableMessage(m MutableMessage) *mqwrapper.ProducerMessage {
	return &mqwrapper.ProducerMessage{
		Payload:    m.Payload(),
		Properties: m.Properties().ToRawMap(),
	}
}

// NewImmutableMessageFromMQConsumedMessage creates a new immutable message from mq producer message.
// TODO: remove in the future.
func NewImmutableMessageFromMQConsumedMessage(msg mqwrapper.Message) ImmutableMessage {
	return &immutableMessageImpl{
		id: msg.ID(),
		messageImpl: messageImpl{
			payload:    msg.Payload(),
			properties: propertiesImpl(msg.Properties()),
		},
	}
}

// NewMessageFromPBMessage creates a new message from pb message.
func NewMessageFromPBMessage(msg *logpb.Message) MutableMessage {
	return &messageImpl{
		payload:    msg.Payload,
		properties: propertiesImpl(msg.Properties),
	}
}

// NewImmutableMessageFromPBMessage creates a new immutable message from pb message.
func NewImmutableMessageFromPBMessage(id *logpb.MessageID, msg *logpb.Message) ImmutableMessage {
	return &immutableMessageImpl{
		id: NewMessageIDFromPBMessageID(id),
		messageImpl: messageImpl{
			payload:    msg.Payload,
			properties: propertiesImpl(msg.Properties),
		},
	}
}

// BasicMessage is the basic interface of message.
type BasicMessage interface {
	// MessageType returns the type of message.
	MessageType() MessageType

	// Message payload.
	Payload() []byte

	// EstimateSize returns the estimated size of message.
	EstimateSize() int
}

// MutableMessage is the mutable message interface.
// Message can be modified before it is persistent by wal.
type MutableMessage interface {
	BasicMessage

	WithTimeTick(tt uint64) MutableMessage

	// Properties returns the message properties.
	Properties() Properties
}

// ImmutableMessage is the read-only message interface.
// Once a message is persistent by wal, it will be immutable.
// And the message id will be assigned.
type ImmutableMessage interface {
	BasicMessage

	// TimeTick returns the time tick of current message.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	TimeTick() uint64

	// MessageID returns the message id.
	MessageID() MessageID

	// Properties returns the message read only properties.
	Properties() RProperties

	// Version returns the message format version.
	// 0: old version before lognode.
	// from 1: new version after lognode.
	Version() Version
}
