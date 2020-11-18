package reader

import "github.com/zilliztech/milvus-distributed/internal/util/flowgraph"

type schemaUpdateNode struct {
	BaseNode
	schemaUpdateMsg schemaUpdateMsg
}

func (suNode *schemaUpdateNode) Name() string {
	return "suNode"
}

func (suNode *schemaUpdateNode) Operate(in []*Msg) []*Msg {
	return in
}

func newSchemaUpdateNode() *schemaUpdateNode {
	maxQueueLength := flowgraph.Params.FlowGraphMaxQueueLength()
	maxParallelism := flowgraph.Params.FlowGraphMaxParallelism()

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &schemaUpdateNode{
		BaseNode: baseNode,
	}
}
