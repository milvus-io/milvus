package reader

type dmNode struct {
	BaseNode
	dmMsg dmMsg
}

func (dmNode *dmNode) Name() string {
	return "dmNode"
}

func (dmNode *dmNode) Operate(in []*Msg) []*Msg {
	return in
}

func newDmNode() *dmNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &dmNode{
		BaseNode: baseNode,
	}
}
