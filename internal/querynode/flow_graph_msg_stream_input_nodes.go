package querynode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func (dsService *dataSyncService) newDmInputNode(ctx context.Context) *flowgraph.InputNode {
	// query node doesn't need to consume any topic
	insertStream, _ := dsService.msFactory.NewTtMsgStream(ctx)
	dsService.dmStream = insertStream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&insertStream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}
