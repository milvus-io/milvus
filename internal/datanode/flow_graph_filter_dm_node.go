package datanode

import (
	"context"
	"math"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type filterDmNode struct {
	BaseNode
	ddMsg *ddMsg
}

func (fdmNode *filterDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filterDmNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {

	if len(in) != 2 {
		log.Error("Invalid operate message input in filterDmNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	ddMsg, ok := in[1].(*ddMsg)
	if !ok {
		log.Error("type assertion failed for ddMsg")
		// TODO: add error handling
	}

	fdmNode.ddMsg = ddMsg

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		flushMessages:  make([]*flushMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
		startPositions: make([]*internalpb2.MsgPosition, 0),
	}

	iMsg.flushMessages = append(iMsg.flushMessages, ddMsg.flushMessages...)

	for _, msg := range msgStreamMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_Insert:
			resMsg := fdmNode.filterInvalidInsertMessage(msg.(*msgstream.InsertMsg))
			if resMsg != nil {
				iMsg.insertMessages = append(iMsg.insertMessages, resMsg)
			}
		// case commonpb.MsgType_kDelete:
		// dmMsg.deleteMessages = append(dmMsg.deleteMessages, (*msg).(*msgstream.DeleteTask))
		default:
			log.Error("Not supporting message type", zap.Any("Type", msg.Type()))
		}
	}

	iMsg.startPositions = append(iMsg.startPositions, msgStreamMsg.StartPositions()...)
	iMsg.gcRecord = ddMsg.gcRecord
	var res Msg = &iMsg
	return []Msg{res}, ctx
}

func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	// No dd record, do all insert requests.
	records, ok := fdmNode.ddMsg.collectionRecords[msg.CollectionID]
	if !ok {
		return msg
	}

	// TODO: If the last record is drop type, all insert requests are invalid.
	//if !records[len(records)-1].createOrDrop {
	//	return nil
	//}

	// Filter insert requests before last record.
	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		// TODO: what if the messages are misaligned? Here, we ignore those messages and print error
		log.Error("misaligned messages detected")
		return nil
	}
	tmpTimestamps := make([]Timestamp, 0)
	tmpRowIDs := make([]int64, 0)
	tmpRowData := make([]*commonpb.Blob, 0)

	// calculate valid time range
	timeBegin := Timestamp(0)
	timeEnd := Timestamp(math.MaxUint64)
	for _, record := range records {
		if record.createOrDrop && timeBegin < record.timestamp {
			timeBegin = record.timestamp
		}
		if !record.createOrDrop && timeEnd > record.timestamp {
			timeEnd = record.timestamp
		}
	}

	for i, t := range msg.Timestamps {
		if t >= timeBegin && t <= timeEnd {
			tmpTimestamps = append(tmpTimestamps, t)
			tmpRowIDs = append(tmpRowIDs, msg.RowIDs[i])
			tmpRowData = append(tmpRowData, msg.RowData[i])
		}
	}

	if len(tmpRowIDs) <= 0 {
		return nil
	}

	msg.Timestamps = tmpTimestamps
	msg.RowIDs = tmpRowIDs
	msg.RowData = tmpRowData
	return msg
}

func newFilteredDmNode() *filterDmNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDmNode{
		BaseNode: baseNode,
	}
}
