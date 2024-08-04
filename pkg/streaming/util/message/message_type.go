package message

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
)

type MessageType messagespb.MessageType

const (
	MessageTypeUnknown          MessageType = MessageType(messagespb.MessageType_Unknown)
	MessageTypeTimeTick         MessageType = MessageType(messagespb.MessageType_TimeTick)
	MessageTypeInsert           MessageType = MessageType(messagespb.MessageType_Insert)
	MessageTypeDelete           MessageType = MessageType(messagespb.MessageType_Delete)
	MessageTypeFlush            MessageType = MessageType(messagespb.MessageType_Flush)
	MessageTypeCreateCollection MessageType = MessageType(messagespb.MessageType_CreateCollection)
	MessageTypeDropCollection   MessageType = MessageType(messagespb.MessageType_DropCollection)
	MessageTypeCreatePartition  MessageType = MessageType(messagespb.MessageType_CreatePartition)
	MessageTypeDropPartition    MessageType = MessageType(messagespb.MessageType_DropPartition)
)

var messageTypeName = map[MessageType]string{
	MessageTypeUnknown:          "UNKNOWN",
	MessageTypeTimeTick:         "TIME_TICK",
	MessageTypeInsert:           "INSERT",
	MessageTypeDelete:           "DELETE",
	MessageTypeFlush:            "FLUSH",
	MessageTypeCreateCollection: "CREATE_COLLECTION",
	MessageTypeDropCollection:   "DROP_COLLECTION",
	MessageTypeCreatePartition:  "CREATE_PARTITION",
	MessageTypeDropPartition:    "DROP_PARTITION",
}

// String implements fmt.Stringer interface.
func (t MessageType) String() string {
	return messageTypeName[t]
}

// marshal marshal MessageType to string.
func (t MessageType) marshal() string {
	return strconv.FormatInt(int64(t), 10)
}

// Valid checks if the MessageType is valid.
func (t MessageType) Valid() bool {
	_, ok := messageTypeName[t]
	return t != MessageTypeUnknown && ok
}

// unmarshalMessageType unmarshal MessageType from string.
func unmarshalMessageType(s string) MessageType {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return MessageTypeUnknown
	}
	return MessageType(i)
}
