package reader

import (
	"log"
)

type serviceTimeNode struct {
	BaseNode
	node           *QueryNode
	serviceTimeMsg serviceTimeMsg
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Operate(in []*Msg) []*Msg {
	// fmt.Println("Do serviceTimeNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in serviceTimeNode")
		// TODO: add error handling
	}

	serviceTimeMsg, ok := (*in[0]).(*serviceTimeMsg)
	if !ok {
		log.Println("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	stNode.node.queryNodeTime.updateSearchServiceTime(serviceTimeMsg.timeRange)
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
