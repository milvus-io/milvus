package message

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

type MessageType messagespb.MessageType

// String implements fmt.Stringer interface.
func (t MessageType) String() string {
	return messagespb.MessageType_name[int32(t)]
}

// marshal marshal MessageType to string.
func (t MessageType) marshal() string {
	return strconv.FormatInt(int64(t), 10)
}

// Valid checks if the MessageType is valid.
func (t MessageType) Valid() bool {
	typ := int32(t)
	_, ok := messagespb.MessageType_name[typ]
	return t != MessageTypeUnknown && ok
}

// IsExclusiveRequired checks if the MessageType is exclusive append required.
// An exclusive required message type is that the message's timetick should keep same order with message id.
// And when the message is appending, other messages with the same vchannel cannot append concurrently.
func (t MessageType) IsExclusiveRequired() bool {
	_, ok := exclusiveRequiredMessageType[t]
	return ok
}

// CanEnableCipher checks if the MessageType can enable cipher.
func (t MessageType) CanEnableCipher() bool {
	_, ok := cipherMessageType[t]
	return ok
}

// IsSysmtem checks if the MessageType is a system type.
func (t MessageType) IsSystem() bool {
	_, ok := systemMessageType[t]
	return ok
}

// IsSelfControlled checks if the MessageType is self controlled.
func (t MessageType) IsSelfControlled() bool {
	_, ok := selfControlledMessageType[t]
	return ok
}

// unmarshalMessageType unmarshal MessageType from string.
func unmarshalMessageType(s string) MessageType {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return MessageTypeUnknown
	}
	return MessageType(i)
}
