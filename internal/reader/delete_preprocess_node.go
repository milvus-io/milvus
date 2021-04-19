package reader

type deletePreprocessNode struct {
	BaseNode
	deletePreprocessMsg deletePreprocessMsg
}

func (dpNode *deletePreprocessNode) Name() string {
	return "dpNode"
}

func (dpNode *deletePreprocessNode) Operate(in []*Msg) []*Msg {
	return in
}

func newDeletePreprocessNode() *deletePreprocessNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &deletePreprocessNode{
		BaseNode: baseNode,
	}
}
