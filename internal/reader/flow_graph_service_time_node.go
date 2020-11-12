package reader

import (
	"log"
)

type serviceTimeNode struct {
	BaseNode
	node *QueryNode
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Operate(in []*Msg) []*Msg {
	// fmt.Println("Do serviceTimeNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in serviceTimeNode, input length = ", len(in))
		// TODO: add error handling
	}

	serviceTimeMsg, ok := (*in[0]).(*serviceTimeMsg)
	if !ok {
		log.Println("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	// update service time
	stNode.node.tSafe = serviceTimeMsg.timeRange.timestampMax
	return nil
}

func newServiceTimeNode(node *QueryNode) *serviceTimeNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &serviceTimeNode{
		BaseNode: baseNode,
		node:     node,
	}
}
