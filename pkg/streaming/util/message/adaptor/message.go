package adaptor

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var unmashalerDispatcher = (&msgstream.ProtoUDFactory{}).NewUnmarshalDispatcher()

// FromMessageToMsgPack converts message to msgpack.
// Same TimeTick must be sent with same msgpack.
// !!! Msgs must be keep same time tick.
// TODO: remove this function after remove the msgstream implementation.
func NewMsgPackFromMessage(msgs ...message.ImmutableMessage) (*msgstream.MsgPack, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	allTsMsgs := make([]msgstream.TsMsg, 0, len(msgs))

	var finalErr error
	for _, msg := range msgs {
		var tsMsg msgstream.TsMsg
		var err error
		switch msg.Version() {
		case message.VersionOld:
			tsMsg, err = fromMessageToTsMsgVOld(msg)
		case message.VersionV1:
			tsMsg, err = fromMessageToTsMsgV1(msg)
		default:
			panic("unsupported message version")
		}
		if err != nil {
			finalErr = errors.CombineErrors(finalErr, errors.Wrapf(err, "Failed to convert message to msgpack, %v", msg.MessageID()))
			continue
		}
		allTsMsgs = append(allTsMsgs, tsMsg)
	}
	if len(allTsMsgs) == 0 {
		return nil, finalErr
	}

	// msgs is sorted by time tick.
	// Postition use the last confirmed message id.
	// 1. So use the first tsMsgs's Position can read all messages which timetick is greater or equal than the first tsMsgs's BeginTs.
	//    In other words, from the StartPositions, you can read the full msgPack.
	// 2. Use the last tsMsgs's Position as the EndPosition, you can read all messages following the msgPack.
	return &msgstream.MsgPack{
		BeginTs:        allTsMsgs[0].BeginTs(),
		EndTs:          allTsMsgs[len(allTsMsgs)-1].EndTs(),
		Msgs:           allTsMsgs,
		StartPositions: []*msgstream.MsgPosition{allTsMsgs[0].Position()},
		EndPositions:   []*msgstream.MsgPosition{allTsMsgs[len(allTsMsgs)-1].Position()},
	}, finalErr
}

func fromMessageToTsMsgVOld(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	panic("Not implemented")
}

// fromMessageToTsMsgV1 converts message to ts message.
func fromMessageToTsMsgV1(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	tsMsg, err := unmashalerDispatcher.Unmarshal(msg.Payload(), commonpb.MsgType(msg.MessageType()))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal message")
	}
	tsMsg.SetTs(msg.TimeTick())
	tsMsg.SetPosition(&msgpb.MsgPosition{
		ChannelName: msg.VChannel(),
		// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
		MsgID:     MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		MsgGroup:  "", // Not important any more.
		Timestamp: msg.TimeTick(),
	})

	return recoverMessageFromHeader(tsMsg, msg)
}

// recoverMessageFromHeader recovers message from header.
func recoverMessageFromHeader(tsMsg msgstream.TsMsg, msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMessage, err := message.AsImmutableInsertMessage(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to insert message")
		}
		// insertMsg has multiple partition and segment assignment is done by insert message header.
		// so recover insert message from header before send it.
		return recoverInsertMsgFromHeader(tsMsg.(*msgstream.InsertMsg), insertMessage.MessageHeader(), msg.TimeTick())
	default:
		return tsMsg, nil
	}
}

// recoverInsertMsgFromHeader recovers insert message from header.
func recoverInsertMsgFromHeader(insertMsg *msgstream.InsertMsg, header *message.InsertMessageHeader, timetick uint64) (msgstream.TsMsg, error) {
	if insertMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}
	// header promise a batch insert on vchannel in future, so header has multiple partition.
	var assignment *message.PartitionSegmentAssignment
	for _, p := range header.Partitions {
		if p.GetPartitionId() == insertMsg.GetPartitionID() {
			assignment = p
			break
		}
	}
	if assignment.GetSegmentAssignment().GetSegmentId() == 0 {
		panic("unreachable code, partition id is not exist")
	}

	insertMsg.SegmentID = assignment.GetSegmentAssignment().GetSegmentId()
	// timetick should has been assign at streaming node.
	// so overwrite the timetick on insertRequest.
	timestamps := make([]uint64, insertMsg.GetNumRows())
	for i := 0; i < len(timestamps); i++ {
		timestamps[i] = timetick
	}
	insertMsg.Timestamps = timestamps
	return insertMsg, nil
}
