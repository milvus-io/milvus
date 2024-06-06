package message

import (
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// messageIDUnmarshaler is the map for message id unmarshaler.
var messageIDUnmarshaler typeutil.ConcurrentMap[string, MessageIDUnmarshaler]

// RegisterMessageIDUnmsarshaler register the message id unmarshaler.
func RegisterMessageIDUnmsarshaler(name string, unmarshaler MessageIDUnmarshaler) {
	_, loaded := messageIDUnmarshaler.GetOrInsert(name, unmarshaler)
	if loaded {
		panic("MessageID Unmarshaler already registered: " + name)
	}
}

// MessageIDUnmarshaler is the unmarshaler for message id.
type MessageIDUnmarshaler = func(b []byte) (MessageID, error)

// UnmsarshalMessageID unmarshal the message id.
func UnmarshalMessageID(name string, b []byte) (MessageID, error) {
	unmarshaler, ok := messageIDUnmarshaler.Get(name)
	if !ok {
		panic("MessageID Unmarshaler not registered: " + name)
	}
	return unmarshaler(b)
}

// MessageID is the interface for message id.
type MessageID interface {
	// WALName returns the name of message id related wal.
	WALName() string

	// LT less than.
	LT(MessageID) bool

	// LTE less than or equal to.
	LTE(MessageID) bool

	// EQ Equal to.
	EQ(MessageID) bool

	// Marshal marshal the message id.
	Marshal() []byte
}
