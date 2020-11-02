package reader

type filteredDmNode struct {
	BaseNode
	filteredDmMsg filteredDmMsg
}

func (fdmNode *filteredDmNode) Name() string {
	return "dmNode"
}

func (fdmNode *filteredDmNode) Operate(in []*Msg) []*Msg {
	return in
}

func newFilteredDmNode() *filteredDmNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filteredDmNode{
		BaseNode: baseNode,
	}
}
