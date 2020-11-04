package reader

import (
	"log"

	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type msgStreamNode struct {
	BaseNode
	msgStreamMsg msgStreamMsg
}

func (msNode *msgStreamNode) Name() string {
	return "msNode"
}

func (msNode *msgStreamNode) Operate(in []*Msg) []*Msg {
	if len(in) != 1 {
		log.Println("Invalid operate message input in msgStreamNode")
		// TODO: add error handling
	}

	streamMsg, ok := (*in[0]).(*msgStreamMsg)
	if !ok {
		log.Println("type assertion failed for msgStreamMsg")
		// TODO: add error handling
	}

	// TODO: add time range check

	var dmMsg = dmMsg{
		insertMessages: make([]*msgstream.InsertTask, 0),
		// deleteMessages: make([]*msgstream.DeleteTask, 0),
		timeRange: streamMsg.timeRange,
	}
	for _, msg := range streamMsg.tsMessages {
		switch (*msg).Type() {
		case internalPb.MsgType_kInsert:
			dmMsg.insertMessages = append(dmMsg.insertMessages, (*msg).(*msgstream.InsertTask))
		// case msgstream.KDelete:
		//	dmMsg.deleteMessages = append(dmMsg.deleteMessages, (*msg).(*msgstream.DeleteTask))
		default:
			log.Println("Non supporting message type:", (*msg).Type())
		}
	}
	var res Msg = &dmMsg
	return []*Msg{&res}
}

func newMsgStreamNode() *msgStreamNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &msgStreamNode{
		BaseNode: baseNode,
	}
}
