package message

import "strconv"

type MessageType int32

const (
	MessageTypeUnknown          MessageType = 0
	MessageTypeTimeTick         MessageType = 1
	MessageTypeInsert           MessageType = 2
	MessageTypeDelete           MessageType = 3
	MessageTypeCreateCollection MessageType = 4
	MessageTypeDropCollection   MessageType = 5
	MessageTypeCreatePartition  MessageType = 6
	MessageTypeDropPartition    MessageType = 7
)

var messageTypeName = map[MessageType]string{
	MessageTypeUnknown:          "MESSAGE_TYPE_UNKNOWN",
	MessageTypeTimeTick:         "MESSAGE_TYPE_TIME_TICK",
	MessageTypeInsert:           "MESSAGE_TYPE_INSERT",
	MessageTypeDelete:           "MESSAGE_TYPE_DELETE",
	MessageTypeCreateCollection: "MESSAGE_TYPE_CREATE_COLLECTION",
	MessageTypeDropCollection:   "MESSAGE_TYPE_DROP_COLLECTION",
	MessageTypeCreatePartition:  "MESSAGE_TYPE_CREATE_PARTITION",
	MessageTypeDropPartition:    "MESSAGE_TYPE_DROP_PARTITION",
}

// String implements fmt.Stringer interface.
func (t MessageType) String() string {
	return messageTypeName[t]
}

// marshal marshal MessageType to string.
func (t MessageType) marshal() string {
	return strconv.FormatInt(int64(t), 10)
}

// unmarshalMessageType unmarshal MessageType from string.
func unmarshalMessageType(s string) MessageType {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return MessageTypeUnknown
	}
	return MessageType(i)
}
