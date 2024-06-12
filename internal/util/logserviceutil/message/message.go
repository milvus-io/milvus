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

	// WithLastConfirmed sets the last confirmed message id of current message.
	// !!! preserved for log system internal usage, don't call it outside of log system.
	WithLastConfirmed(id MessageID) MutableMessage

	// WithTimeTick sets the time tick of current message.
	// !!! preserved for log system internal usage, don't call it outside of log system.
	WithTimeTick(tt uint64) MutableMessage

	// Properties returns the message properties.
	Properties() Properties
}

// ImmutableMessage is the read-only message interface.
// Once a message is persistent by wal, it will be immutable.
// And the message id will be assigned.
type ImmutableMessage interface {
	BasicMessage

	// WALName returns the name of message related wal.
	WALName() string

	// TimeTick returns the time tick of current message.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	TimeTick() uint64

	// LastConfirmedMessageID returns the last confirmed message id of current message.
	// last confirmed message is always a timetick message.
	// Read from this message id will guarantee the time tick greater than this message is consumed.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	LastConfirmedMessageID() MessageID

	// MessageID returns the message id of current message.
	MessageID() MessageID

	// Properties returns the message read only properties.
	Properties() RProperties

	// Version returns the message format version.
	// 0: old version before lognode.
	// from 1: new version after lognode.
	Version() Version
}
