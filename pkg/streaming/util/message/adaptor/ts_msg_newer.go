package adaptor

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var (
	_ msgstream.TsMsg = &tsMsgImpl{}
	_ msgstream.TsMsg = &FlushMessageBody{}
)

type tsMsgImpl struct {
	msgstream.BaseMsg
	ts      uint64
	sz      int
	msgType commonpb.MsgType
}

func (t *tsMsgImpl) ID() msgstream.UniqueID {
	panic("should never use")
}

func (t *tsMsgImpl) SetID(id msgstream.UniqueID) {
	panic("should never use")
}

func (t *tsMsgImpl) Type() commonpb.MsgType {
	return t.msgType
}

func (t *tsMsgImpl) SourceID() int64 {
	panic("should never use")
}

func (t *tsMsgImpl) Marshal(msgstream.TsMsg) (msgstream.MarshalType, error) {
	panic("should never use")
}

func (t *tsMsgImpl) Unmarshal(msgstream.MarshalType) (msgstream.TsMsg, error) {
	panic("should never use")
}

func (t *tsMsgImpl) Size() int {
	return t.sz
}

func (t *tsMsgImpl) SetTs(ts uint64) {
	t.ts = ts
}

type CreateSegmentMessageBody struct {
	*tsMsgImpl
	CreateSegmentMessage message.ImmutableCreateSegmentMessageV2
}

func NewCreateSegmentMessageBody(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	createMsg, err := message.AsImmutableCreateSegmentMessageV2(msg)
	if err != nil {
		return nil, err
	}
	return &CreateSegmentMessageBody{
		tsMsgImpl: &tsMsgImpl{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: msg.TimeTick(),
				EndTimestamp:   msg.TimeTick(),
			},
			ts:      msg.TimeTick(),
			sz:      msg.EstimateSize(),
			msgType: MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()),
		},
		CreateSegmentMessage: createMsg,
	}, nil
}

type FlushMessageBody struct {
	*tsMsgImpl
	FlushMessage message.ImmutableFlushMessageV2
}

func NewFlushMessageBody(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	flushMsg, err := message.AsImmutableFlushMessageV2(msg)
	if err != nil {
		return nil, err
	}
	return &FlushMessageBody{
		tsMsgImpl: &tsMsgImpl{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: msg.TimeTick(),
				EndTimestamp:   msg.TimeTick(),
			},
			ts:      msg.TimeTick(),
			sz:      msg.EstimateSize(),
			msgType: MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()),
		},
		FlushMessage: flushMsg,
	}, nil
}

type ManualFlushMessageBody struct {
	*tsMsgImpl
	ManualFlushMessage message.ImmutableManualFlushMessageV2
}

func NewManualFlushMessageBody(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	flushMsg, err := message.AsImmutableManualFlushMessageV2(msg)
	if err != nil {
		return nil, err
	}
	return &ManualFlushMessageBody{
		tsMsgImpl: &tsMsgImpl{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: msg.TimeTick(),
				EndTimestamp:   msg.TimeTick(),
			},
			ts:      msg.TimeTick(),
			sz:      msg.EstimateSize(),
			msgType: MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()),
		},
		ManualFlushMessage: flushMsg,
	}, nil
}

type SchemaChangeMessageBody struct {
	*tsMsgImpl
	SchemaChangeMessage message.ImmutableSchemaChangeMessageV2
	BroadcastID         uint64
}

func NewSchemaChangeMessageBody(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	schChgMsg, err := message.AsImmutableCollectionSchemaChangeV2(msg)
	if err != nil {
		return nil, err
	}
	return &SchemaChangeMessageBody{
		tsMsgImpl: &tsMsgImpl{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: msg.TimeTick(),
				EndTimestamp:   msg.TimeTick(),
			},
			ts:      msg.TimeTick(),
			sz:      msg.EstimateSize(),
			msgType: MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()),
		},
		SchemaChangeMessage: schChgMsg,
		BroadcastID:         msg.BroadcastHeader().BroadcastID,
	}, nil
}
