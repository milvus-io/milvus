package reader

type msgStreamNode struct {
	BaseNode
	msgStreamMsg msgStreamMsg
}

func (msNode *msgStreamNode) Name() string {
	return "msNode"
}

func (msNode *msgStreamNode) Operate(in []*Msg) []*Msg {
	return in
}

func newMsgStreamNode() *msgStreamNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &msgStreamNode{
		BaseNode: baseNode,
	}
}
