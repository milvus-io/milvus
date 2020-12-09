package writenode

import (
	"log"
)

type (
	writeNode struct {
		BaseNode
	}
)

func (wNode *writeNode) Name() string {
	return "wNode"
}

func (wNode *writeNode) Operate(in []*Msg) []*Msg {
	log.Println("=========== WriteNode Operating")

	if len(in) != 1 {
		log.Println("Invalid operate message input in writetNode, input length = ", len(in))
		// TODO: add error handling
	}

	iMsg, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	log.Println("=========== insertMsg length:", len(iMsg.insertMessages))
	for _, task := range iMsg.insertMessages {
		if len(task.RowIDs) != len(task.Timestamps) || len(task.RowIDs) != len(task.RowData) {
			log.Println("Error, misaligned messages detected")
			continue
		}
		log.Println("Timestamp: ", task.Timestamps[0])
		log.Printf("t(%d) : %v ", task.Timestamps[0], task.RowData[0])
	}

	var res Msg = &serviceTimeMsg{
		timeRange: iMsg.timeRange,
	}

	// TODO
	return []*Msg{&res}

}

func newWriteNode() *writeNode {
	maxQueueLength := Params.flowGraphMaxQueueLength()
	maxParallelism := Params.flowGraphMaxParallelism()

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &writeNode{
		BaseNode: baseNode,
	}
}
