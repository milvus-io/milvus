package adaptor

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var messageTypeToCommonpbMsgType = map[message.MessageType]commonpb.MsgType{
	message.MessageTypeTimeTick:         commonpb.MsgType_TimeTick,
	message.MessageTypeInsert:           commonpb.MsgType_Insert,
	message.MessageTypeDelete:           commonpb.MsgType_Delete,
	message.MessageTypeFlush:            commonpb.MsgType_FlushSegment,
	message.MessageTypeManualFlush:      commonpb.MsgType_ManualFlush,
	message.MessageTypeCreateCollection: commonpb.MsgType_CreateCollection,
	message.MessageTypeDropCollection:   commonpb.MsgType_DropCollection,
	message.MessageTypeCreatePartition:  commonpb.MsgType_CreatePartition,
	message.MessageTypeDropPartition:    commonpb.MsgType_DropPartition,
}

// MustGetCommonpbMsgTypeFromMessageType returns the commonpb.MsgType from message.MessageType.
func MustGetCommonpbMsgTypeFromMessageType(t message.MessageType) commonpb.MsgType {
	if v, ok := messageTypeToCommonpbMsgType[t]; ok {
		return v
	}
	panic("unsupported message type")
}
