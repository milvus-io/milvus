package reader

type key2SegNode struct {
	BaseNode
	key2SegMsg key2SegMsg
}

func (ksNode *key2SegNode) Name() string {
	return "ksNode"
}

func (ksNode *key2SegNode) Operate(in []*Msg) []*Msg {
	return in
}

func newKey2SegNode() *key2SegNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &key2SegNode{
		BaseNode: baseNode,
	}
}
