package datanode

import (
	"context"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
)

type gcNode struct {
	BaseNode
	replica Replica
}

func (gcNode *gcNode) Name() string {
	return "gcNode"
}

func (gcNode *gcNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {

	if len(in) != 1 {
		log.Error("Invalid operate message input in gcNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	gcMsg, ok := in[0].(*gcMsg)
	if !ok {
		log.Error("type assertion failed for gcMsg")
		// TODO: add error handling
	}

	if gcMsg == nil {
		return []Msg{}, ctx
	}

	// drop collections
	for _, collectionID := range gcMsg.gcRecord.collections {
		err := gcNode.replica.removeCollection(collectionID)
		if err != nil {
			log.Error("replica remove collection wrong", zap.Error(err))
		}
	}

	return nil, ctx
}

func newGCNode(replica Replica) *gcNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &gcNode{
		BaseNode: baseNode,
		replica:  replica,
	}
}
