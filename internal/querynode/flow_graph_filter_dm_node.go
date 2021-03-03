package querynode

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type filterDmNode struct {
	baseNode
	replica collectionReplica
}

func (fdmNode *filterDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filterDmNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {
	//fmt.Println("Do filterDmNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in filterDmNode, input length = ", len(in))
		// TODO: add error handling
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Println("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
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
		case commonpb.MsgType_kInsert:
			resMsg := fdmNode.filterInvalidInsertMessage(msg.(*msgstream.InsertMsg))
			if resMsg != nil {
				iMsg.insertMessages = append(iMsg.insertMessages, resMsg)
			}
		// case commonpb.MsgType_kDelete:
		// dmMsg.deleteMessages = append(dmMsg.deleteMessages, (*msg).(*msgstream.DeleteTask))
		default:
			log.Println("Non supporting message type:", msg.Type())
		}
	}

	var res Msg = &iMsg

	return []Msg{res}, ctx
}

func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	// TODO: open this check
	// check if partition dm enable
	enableCollection := fdmNode.replica.hasCollection(msg.CollectionID)
	enablePartition := fdmNode.replica.hasPartition(msg.PartitionID)
	if !enableCollection || !enablePartition {
		return nil
	}

	// TODO: If the last record is drop type, all insert requests are invalid.
	//if !records[len(records)-1].createOrDrop {
	//	return nil
	//}

	// Filter insert requests before last record.
	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		// TODO: what if the messages are misaligned? Here, we ignore those messages and print error
		log.Println("Error, misaligned messages detected")
		return nil
	}

	tmpTimestamps := make([]Timestamp, 0)
	tmpRowIDs := make([]int64, 0)
	tmpRowData := make([]*commonpb.Blob, 0)

	for i, t := range msg.Timestamps {
		tmpTimestamps = append(tmpTimestamps, t)
		tmpRowIDs = append(tmpRowIDs, msg.RowIDs[i])
		tmpRowData = append(tmpRowData, msg.RowData[i])
	}

	if len(tmpRowIDs) <= 0 {
		return nil
	}

	msg.Timestamps = tmpTimestamps
	msg.RowIDs = tmpRowIDs
	msg.RowData = tmpRowData
	return msg
}

func newFilteredDmNode(replica collectionReplica) *filterDmNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDmNode{
		baseNode: baseNode,
		replica:  replica,
	}
}
