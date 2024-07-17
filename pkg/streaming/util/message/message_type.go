package message

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type MessageType int32

const (
	MessageTypeUnknown          MessageType = MessageType(commonpb.MsgType_Undefined)
	MessageTypeTimeTick         MessageType = MessageType(commonpb.MsgType_TimeTick)
	MessageTypeInsert           MessageType = MessageType(commonpb.MsgType_Insert)
	MessageTypeDelete           MessageType = MessageType(commonpb.MsgType_Delete)
	MessageTypeFlush            MessageType = MessageType(commonpb.MsgType_Flush)
	MessageTypeCreateCollection MessageType = MessageType(commonpb.MsgType_CreateCollection)
	MessageTypeDropCollection   MessageType = MessageType(commonpb.MsgType_DropCollection)
	MessageTypeCreatePartition  MessageType = MessageType(commonpb.MsgType_CreatePartition)
	MessageTypeDropPartition    MessageType = MessageType(commonpb.MsgType_DropPartition)
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
