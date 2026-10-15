package utility

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
)

// NewMessagePosition preserves the safe seek position of a complete message,
// including a committed transaction. The channel is supplied by the consumer
// because a PChannel-wide Flush has no VChannel of its own.
func NewMessagePosition(msg message.ImmutableMessage, vchannel string) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: vchannel,
		Timestamp:   msg.TimeTick(),
		MsgID:       adaptor.MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		WALName:     commonpb.WALName(msg.WALName()),
	}
}
