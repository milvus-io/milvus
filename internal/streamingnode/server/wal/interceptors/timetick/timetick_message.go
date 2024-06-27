package timetick

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
)

func newTimeTickMsg(ts uint64, sourceID int64) (message.MutableMessage, error) {
	// TODO: time tick should be put on properties, for compatibility, we put it on message body now.
	msgstreamMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: msgpb.TimeTickMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
				commonpbutil.WithMsgID(0),
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(sourceID),
			),
		},
	}
	bytes, err := msgstreamMsg.Marshal(msgstreamMsg)
	if err != nil {
		return nil, errors.Wrap(err, "marshal time tick message failed")
	}

	payload, ok := bytes.([]byte)
	if !ok {
		return nil, errors.New("marshal time tick message as []byte failed")
	}

	// Common message's time tick is set on interceptor.
	// TimeTickMsg's time tick should be set here.
	msg := message.NewMutableMessageBuilder().
		WithMessageType(message.MessageTypeTimeTick).
		WithPayload(payload).
		BuildMutable().
		WithTimeTick(ts)
	return msg, nil
}
