package message

var (
	_ BasicMessage     = (*messageImpl)(nil)
	_ MutableMessage   = (*messageImpl)(nil)
	_ ImmutableMessage = (*immutableMessageImpl)(nil)
)

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
