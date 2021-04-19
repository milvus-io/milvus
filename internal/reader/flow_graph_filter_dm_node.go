package reader

import (
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type filterDmNode struct {
	BaseNode
}

func (fdmNode *filterDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filterDmNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do filterDmNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in filterDmNode, input length = ", len(in))
		// TODO: add error handling
	}

	msMsg, ok := (*in[0]).(*MsgStreamMsg)
	if !ok {
		log.Println("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	// TODO: add time range check

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
	}
	for _, msg := range msMsg.TsMessages() {
		switch msg.Type() {
		case internalPb.MsgType_kInsert:
			iMsg.insertMessages = append(iMsg.insertMessages, msg.(*msgstream.InsertMsg))
		// case internalPb.MsgType_kDelete:
		// dmMsg.deleteMessages = append(dmMsg.deleteMessages, (*msg).(*msgstream.DeleteTask))
		default:
			log.Println("Non supporting message type:", msg.Type())
		}
	}

	var res Msg = &iMsg
	return []*Msg{&res}
}

func newFilteredDmNode() *filterDmNode {
	maxQueueLength := Params.flowGraphMaxQueueLength()
	maxParallelism := Params.flowGraphMaxParallelism()

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDmNode{
		BaseNode: baseNode,
	}
}
