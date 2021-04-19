package reader

type serviceTimeNode struct {
	BaseNode
	serviceTimeMsg serviceTimeMsg
}

func (stNode *serviceTimeNode) Name() string {
	return "iNode"
}

func (stNode *serviceTimeNode) Operate(in []*Msg) []*Msg {
	return in
}

func newServiceTimeNode() *serviceTimeNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &serviceTimeNode{
		BaseNode: baseNode,
	}
}
