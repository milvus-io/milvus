package planner

type NodeCount struct {
	baseNode
}

func (n *NodeCount) String() string {
	return "NodeCount"
}

func (n *NodeCount) GetChildren() []Node {
	return nil
}

func (n *NodeCount) Accept(v Visitor) interface{} {
	return v.VisitCount(n)
}

func NewNodeCount(text string) *NodeCount {
	return &NodeCount{
		baseNode: newBaseNode(text),
	}
}
