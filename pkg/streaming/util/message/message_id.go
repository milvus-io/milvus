package message

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	// messageIDUnmarshaler is the map for message id unmarshaler.
	messageIDUnmarshaler typeutil.ConcurrentMap[string, MessageIDUnmarshaler]

	ErrInvalidMessageID = errors.New("invalid message id")
)

// RegisterMessageIDUnmsarshaler register the message id unmarshaler.
func RegisterMessageIDUnmsarshaler(name string, unmarshaler MessageIDUnmarshaler) {
	_, loaded := messageIDUnmarshaler.GetOrInsert(name, unmarshaler)
	if loaded {
		panic("MessageID Unmarshaler already registered: " + name)
	}
}

// MessageIDUnmarshaler is the unmarshaler for message id.
type MessageIDUnmarshaler = func(b string) (MessageID, error)

// UnmsarshalMessageID unmarshal the message id.
func UnmarshalMessageID(name string, b string) (MessageID, error) {
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
	Marshal() string

	// Convert into string for logging.
	String() string
}
