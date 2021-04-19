package querynode

import (
	"context"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
)

type gcNode struct {
	baseNode
	replica ReplicaInterface
}

func (gcNode *gcNode) Name() string {
	return "gcNode"
}

func (gcNode *gcNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {
	//log.Debug("Do gcNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in gcNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	_, ok := in[0].(*gcMsg)
	if !ok {
		log.Error("type assertion failed for gcMsg")
		// TODO: add error handling
	}

	// Use `releasePartition` and `releaseCollection`,
	// because if we drop collections or partitions here, query service doesn't know this behavior,
	// which would lead the wrong result of `showCollections` or `showPartition`

	//// drop collections
	//for _, collectionID := range gcMsg.gcRecord.collections {
	//	err := gcNode.replica.removeCollection(collectionID)
	//	if err != nil {
	//		log.Println(err)
	//	}
	//}
	//
	//// drop partitions
	//for _, partition := range gcMsg.gcRecord.partitions {
	//	err := gcNode.replica.removePartition(partition.partitionID)
	//	if err != nil {
	//		log.Println(err)
	//	}
	//}

	return nil, ctx
}

func newGCNode(replica ReplicaInterface) *gcNode {
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
