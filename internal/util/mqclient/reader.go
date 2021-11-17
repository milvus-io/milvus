package mqclient

import (
	"context"
)

// ReaderMessage package Reader and Message as a struct to use
type ReaderMessage struct {
	Reader
	Message
}

// ReaderOptions abstraction Reader options to use.
type ReaderOptions struct {
	// Topic specify the topic this consumer will subscribe on.
	// This argument is required when constructing the reader.
	Topic string

	// Name set the reader name.
	Name string

	// Attach a set of application defined properties to the reader
	// This properties will be visible in the topic stats
	Properties map[string]string

	// StartMessageID initial reader positioning is done by specifying a message id. The options are:
	//  * `MessageID` : Start reading from a particular message id, the reader will position itself on that
	//                  specific position. The first message to be read will be the message next to the specified
	//                  messageID
	StartMessageID MessageID

	// If true, the reader will start at the `StartMessageID`, included.
	// Default is `false` and the reader will start from the "next" message
	StartMessageIDInclusive bool
}

// Reader can be used to scan through all the messages currently available in a topic.
type Reader interface {
	// Topic from which this reader is reading from
	Topic() string

	// Next read the next message in the topic, blocking until a message is available
	Next(context.Context) (Message, error)

	// HasNext check if there is any message available to read from the current position
	HasNext() bool

	// Close the reader and stop the broker to push more messages
	Close()

	// Reset the subscription associated with this reader to a specific message id.
	Seek(MessageID) error
}
