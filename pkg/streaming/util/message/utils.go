package message

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

// AsImmutableTxnMessage converts an ImmutableMessage to ImmutableTxnMessage
var AsImmutableTxnMessage = func(msg ImmutableMessage) ImmutableTxnMessage {
	underlying, ok := msg.(*immutableTxnMessageImpl)
	if !ok {
		return nil
	}
	return underlying
}

// NewMessageTypeWithVersion creates a new MessageTypeWithVersion.
func NewMessageTypeWithVersion(t MessageType, v Version) MessageTypeWithVersion {
	return MessageTypeWithVersion{MessageType: t, Version: v}
}

// GetSerializeType returns the specialized message type for the given message type and version.
func GetSerializeType(mv MessageTypeWithVersion) (MessageSpecializedType, bool) {
	if mv.Version == VersionOld {
		// There's some old messages that is coming from old arch of msgstream.
		// We need to convert them to versionV1 to find the specialized type.
		mv.Version = VersionV1
	}
	typ, ok := messageTypeVersionSpecializedMap[mv]
	return typ, ok
}

// GetMessageTypeWithVersion returns the message type with version for the given message type and version.
func GetMessageTypeWithVersion[H proto.Message, B proto.Message]() (MessageTypeWithVersion, bool) {
	var h H
	var b B
	styp := MessageSpecializedType{
		HeaderType: reflect.TypeOf(h),
		BodyType:   reflect.TypeOf(b),
	}
	mv, ok := messageSpecializedTypeVersionMap[styp]
	return mv, ok
}

// MustGetMessageTypeWithVersion returns the message type with version for the given message type and version, panics on error.
func MustGetMessageTypeWithVersion[H proto.Message, B proto.Message]() MessageTypeWithVersion {
	mv, ok := GetMessageTypeWithVersion[H, B]()
	if !ok {
		panic("message type not found")
	}
	return mv
}

// ReplicateHeader is the header of replicate message.
type ReplicateHeader struct {
	ClusterID              string
	MessageID              MessageID
	LastConfirmedMessageID MessageID
	TimeTick               uint64
	VChannel               string
}
