package reader

import (
	"log"
)

type dmNode struct {
	BaseNode
	dmMsg dmMsg
}

func (dmNode *dmNode) Name() string {
	return "dmNode"
}

func (dmNode *dmNode) Operate(in []*Msg) []*Msg {
	// fmt.Println("Do dmNode operation")

	// TODO: add filtered by schema update
	// But for now, we think all the messages are valid

	if len(in) != 1 {
		log.Println("Invalid operate message input in filteredDmNode")
		// TODO: add error handling
	}

	dmMsg, ok := (*in[0]).(*dmMsg)
	if !ok {
		log.Println("type assertion failed for dmMsg")
		// TODO: add error handling
	}

	var fdmMsg = filteredDmMsg{
		insertMessages: dmMsg.insertMessages,
		timeRange:      dmMsg.timeRange,
	}

	var res Msg = &fdmMsg
	return []*Msg{&res}
}

func newDmNode() *dmNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &dmNode{
		BaseNode: baseNode,
	}
}
