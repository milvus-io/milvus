package message

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	// messageIDUnmarshaler is the map for message id unmarshaler.
	messageIDUnmarshaler typeutil.ConcurrentMap[WALName, MessageIDUnmarshaler]

	ErrInvalidMessageID = errors.New("invalid message id")
)

// RegisterMessageIDUnmsarshaler register the message id unmarshaler.
func RegisterMessageIDUnmsarshaler(walName WALName, unmarshaler MessageIDUnmarshaler) {
	_, loaded := messageIDUnmarshaler.GetOrInsert(walName, unmarshaler)
	if loaded {
		panic("MessageID Unmarshaler already registered: " + walName.String())
	}
}

// MessageIDUnmarshaler is the unmarshaler for message id.
type MessageIDUnmarshaler = func(b string) (MessageID, error)

// MustMarshalMessageID marshal the message id, panic if failed.
func MustMarshalMessageID(msgID MessageID) *commonpb.MessageID {
	if msgID == nil {
		return nil
	}
	return msgID.IntoProto()
}

// MustUnmarshalMessageID unmarshal the message id, panic if failed.
func MustUnmarshalMessageID(msgID *commonpb.MessageID) MessageID {
	if msgID == nil {
		return nil
	}
	id, err := UnmarshalMessageID(msgID)
	if err != nil {
		panic(fmt.Sprintf("unmarshal message id failed: %s, wal: %s, bytes: %s", err.Error(), msgID.WALName.String(), msgID.Id))
	}
	return id
}

// UnmsarshalMessageID unmarshal the message id.
func UnmarshalMessageID(msgID *commonpb.MessageID) (MessageID, error) {
	name := WALName(msgID.WALName)
	if name == WALNameUnknown {
		name = MustGetDefaultWALName()
	}
	unmarshaler, ok := messageIDUnmarshaler.Get(name)
	if !ok {
		panic("MessageID Unmarshaler not registered: " + name.String())
	}
	return unmarshaler(msgID.Id)
}

// MessageID is the interface for message id.
type MessageID interface {
	// WALName returns the name of message id related wal.
	WALName() WALName

	// LT less than.
	LT(MessageID) bool

	// LTE less than or equal to.
	LTE(MessageID) bool

	// EQ Equal to.
	EQ(MessageID) bool

	// Marshal marshal the message id.
	Marshal() string

	// IntoProto marshal the message id to proto.
	IntoProto() *commonpb.MessageID

	// Convert into string for logging.
	String() string
}
