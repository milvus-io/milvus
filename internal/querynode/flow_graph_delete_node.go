package querynodeimp

type deleteNode struct {
	baseNode
	deleteMsg deleteMsg
}

func (dNode *deleteNode) Name() string {
	return "dNode"
}

func (dNode *deleteNode) Operate(in []*Msg) []*Msg {
	return in
}

func newDeleteNode() *deleteNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deleteNode{
		baseNode: baseNode,
	}
}
