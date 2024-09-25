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
		// Parse a transaction message into multiple tsMsgs.
		if msg.MessageType() == message.MessageTypeTxn {
			tsMsgs, err := parseTxnMsg(msg)
			if err != nil {
				finalErr = errors.CombineErrors(finalErr, errors.Wrapf(err, "Failed to convert txn message to msgpack, %v", msg.MessageID()))
				continue
			}
			allTsMsgs = append(allTsMsgs, tsMsgs...)
			continue
		}

		tsMsg, err := parseSingleMsg(msg)
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
	beginTs := allTsMsgs[0].BeginTs()
	endTs := allTsMsgs[len(allTsMsgs)-1].EndTs()
	startPosition := allTsMsgs[0].Position()
	endPosition := allTsMsgs[len(allTsMsgs)-1].Position()
	// filter the TimeTick message.
	tsMsgs := make([]msgstream.TsMsg, 0, len(allTsMsgs))
	for _, msg := range allTsMsgs {
		if msg.Type() == commonpb.MsgType_TimeTick {
			continue
		}
		tsMsgs = append(tsMsgs, msg)
	}
	return &msgstream.MsgPack{
		BeginTs:        beginTs,
		EndTs:          endTs,
		Msgs:           tsMsgs,
		StartPositions: []*msgstream.MsgPosition{startPosition},
		EndPositions:   []*msgstream.MsgPosition{endPosition},
	}, finalErr
}

// parseTxnMsg converts a txn message to ts message list.
func parseTxnMsg(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	txnMsg := message.AsImmutableTxnMessage(msg)
	if txnMsg == nil {
		panic("unreachable code, message must be a txn message")
	}

	tsMsgs := make([]msgstream.TsMsg, 0, txnMsg.Size())
	err := txnMsg.RangeOver(func(im message.ImmutableMessage) error {
		var tsMsg msgstream.TsMsg
		tsMsg, err := parseSingleMsg(im)
		if err != nil {
			return err
		}
		tsMsgs = append(tsMsgs, tsMsg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tsMsgs, nil
}

// parseSingleMsg converts message to ts message.
func parseSingleMsg(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	switch msg.Version() {
	case message.VersionOld:
		return fromMessageToTsMsgVOld(msg)
	case message.VersionV1:
		return fromMessageToTsMsgV1(msg)
	case message.VersionV2:
		return fromMessageToTsMsgV2(msg)
	default:
		panic("unsupported message version")
	}
}

func fromMessageToTsMsgVOld(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	panic("Not implemented")
}

// fromMessageToTsMsgV1 converts message to ts message.
func fromMessageToTsMsgV1(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	tsMsg, err := unmashalerDispatcher.Unmarshal(msg.Payload(), MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()))
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

// fromMessageToTsMsgV2 converts message to ts message.
func fromMessageToTsMsgV2(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	var tsMsg msgstream.TsMsg
	var err error
	switch msg.MessageType() {
	case message.MessageTypeFlush:
		tsMsg, err = NewFlushMessageBody(msg)
	case message.MessageTypeManualFlush:
		tsMsg, err = NewManualFlushMessageBody(msg)
	default:
		panic("unsupported message type")
	}
	if err != nil {
		return nil, err
	}
	tsMsg.SetTs(msg.TimeTick())
	tsMsg.SetPosition(&msgpb.MsgPosition{
		ChannelName: msg.VChannel(),
		// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
		MsgID:     MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		MsgGroup:  "", // Not important any more.
		Timestamp: msg.TimeTick(),
	})
	return tsMsg, nil
}

// recoverMessageFromHeader recovers message from header.
func recoverMessageFromHeader(tsMsg msgstream.TsMsg, msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMessage, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to insert message")
		}
		// insertMsg has multiple partition and segment assignment is done by insert message header.
		// so recover insert message from header before send it.
		return recoverInsertMsgFromHeader(tsMsg.(*msgstream.InsertMsg), insertMessage.Header(), msg.TimeTick())
	case message.MessageTypeDelete:
		deleteMessage, err := message.AsImmutableDeleteMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to delete message")
		}
		return recoverDeleteMsgFromHeader(tsMsg.(*msgstream.DeleteMsg), deleteMessage.Header(), msg.TimeTick())
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
	insertMsg.Base.Timestamp = timetick
	return insertMsg, nil
}

func recoverDeleteMsgFromHeader(deleteMsg *msgstream.DeleteMsg, header *message.DeleteMessageHeader, timetick uint64) (msgstream.TsMsg, error) {
	if deleteMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}
	timestamps := make([]uint64, len(deleteMsg.Timestamps))
	for i := 0; i < len(timestamps); i++ {
		timestamps[i] = timetick
	}
	deleteMsg.Timestamps = timestamps
	return deleteMsg, nil
}
