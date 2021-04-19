package querynode

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type filterDmNode struct {
	baseNode
	collectionID UniqueID
	replica      ReplicaInterface
}

func (fdmNode *filterDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filterDmNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {
	//log.Debug("Do filterDmNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in filterDmNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	if msgStreamMsg == nil {
		return []Msg{}, ctx
	}

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}
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
			log.Warn("Non supporting", zap.Int32("message type", int32(msg.Type())))
		}
	}

	var res Msg = &iMsg

	return []Msg{res}, ctx
}

func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	// check if collection and partition exist
	collection := fdmNode.replica.hasCollection(msg.CollectionID)
	partition := fdmNode.replica.hasPartition(msg.PartitionID)
	if !collection || !partition {
		return nil
	}

	// check if the collection from message is target collection
	if msg.CollectionID != fdmNode.collectionID {
		return nil
	}

	// check if the segment is in excluded segments
	excludedSegments, err := fdmNode.replica.getExcludedSegments(fdmNode.collectionID)
	log.Debug("excluded segments", zap.String("segmentIDs", fmt.Sprintln(excludedSegments)))
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	for _, id := range excludedSegments {
		if msg.SegmentID == id {
			return nil
		}
	}

	// TODO: If the last record is drop type, all insert requests are invalid.
	//if !records[len(records)-1].createOrDrop {
	//	return nil
	//}

	// Filter insert requests before last record.
	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		// TODO: what if the messages are misaligned? Here, we ignore those messages and print error
		log.Error("Error, misaligned messages detected")
		return nil
	}

	if len(msg.Timestamps) <= 0 {
		return nil
	}

	return msg
}

func newFilteredDmNode(replica ReplicaInterface, collectionID UniqueID) *filterDmNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDmNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		replica:      replica,
	}
}
