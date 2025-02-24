package timetick

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
)

func NewTimeTickMsg(ts uint64, lastConfirmedMessageID message.MessageID, sourceID int64) (message.MutableMessage, error) {
	// TODO: time tick should be put on properties, for compatibility, we put it on message body now.
	// Common message's time tick is set on interceptor.
	// TimeTickMsg's time tick should be set here.
	msg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
				commonpbutil.WithMsgID(0),
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(sourceID),
			),
		}).
		WithAllVChannel().
		BuildMutable()
	if err != nil {
		return nil, err
	}
	if lastConfirmedMessageID != nil {
		return msg.WithTimeTick(ts).WithLastConfirmed(lastConfirmedMessageID), nil
	}
	return msg.WithTimeTick(ts).WithLastConfirmedUseMessageID(), nil
}
