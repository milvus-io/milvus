package message

import "strconv"

type MessageType int32

const (
	MessageTypeUnknown  MessageType = 0
	MessageTypeTimeTick MessageType = 1
)

var messageTypeName = map[MessageType]string{
	MessageTypeUnknown:  "MESSAGE_TYPE_UNKNOWN",
	MessageTypeTimeTick: "MESSAGE_TYPE_TIME_TICK",
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
