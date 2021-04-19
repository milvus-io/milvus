package reader

type insertNode struct {
	BaseNode
	insertMsg insertMsg
}

func (iNode *insertNode) Name() string {
	return "iNode"
}

func (iNode *insertNode) Operate(in []*Msg) []*Msg {
	return in
}

func newInsertNode() *insertNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &insertNode{
		BaseNode: baseNode,
	}
}
