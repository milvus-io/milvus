package reader

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
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		BaseNode: baseNode,
	}
}
