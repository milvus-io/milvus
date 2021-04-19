package querynodeimp

import (
	"log"
)

type gcNode struct {
	baseNode
	replica collectionReplica
}

func (gcNode *gcNode) Name() string {
	return "gcNode"
}

func (gcNode *gcNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do gcNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in gcNode, input length = ", len(in))
		// TODO: add error handling
	}

	gcMsg, ok := (*in[0]).(*gcMsg)
	if !ok {
		log.Println("type assertion failed for gcMsg")
		// TODO: add error handling
	}

	// drop collections
	for _, collectionID := range gcMsg.gcRecord.collections {
		err := gcNode.replica.removeCollection(collectionID)
		if err != nil {
			log.Println(err)
		}
	}

	// drop partitions
	for _, partition := range gcMsg.gcRecord.partitions {
		err := gcNode.replica.removePartition(partition.collectionID, partition.partitionTag)
		if err != nil {
			log.Println(err)
		}
	}

	return nil
}

func newGCNode(replica collectionReplica) *gcNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &gcNode{
		baseNode: baseNode,
		replica:  replica,
	}
}
