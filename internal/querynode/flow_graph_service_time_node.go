package querynodeimp

import (
	"log"
)

type serviceTimeNode struct {
	baseNode
	replica collectionReplica
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do serviceTimeNode operation")

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
	stNode.replica.getTSafe().set(serviceTimeMsg.timeRange.timestampMax)
	//fmt.Println("update tSafe to:", getPhysicalTime(serviceTimeMsg.timeRange.timestampMax))

	var res Msg = &gcMsg{
		gcRecord:  serviceTimeMsg.gcRecord,
		timeRange: serviceTimeMsg.timeRange,
	}
	return []*Msg{&res}
}

func newServiceTimeNode(replica collectionReplica) *serviceTimeNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &serviceTimeNode{
		baseNode: baseNode,
		replica:  replica,
	}
}
