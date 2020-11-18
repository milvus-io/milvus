package reader

import "github.com/zilliztech/milvus-distributed/internal/util/flowgraph"

type deleteNode struct {
	BaseNode
	deleteMsg deleteMsg
}

func (dNode *deleteNode) Name() string {
	return "dNode"
}

func (dNode *deleteNode) Operate(in []*Msg) []*Msg {
	return in
}

func newDeleteNode() *deleteNode {
	maxQueueLength := flowgraph.Params.FlowGraphMaxQueueLength()
	maxParallelism := flowgraph.Params.FlowGraphMaxParallelism()

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		BaseNode: baseNode,
	}
}
