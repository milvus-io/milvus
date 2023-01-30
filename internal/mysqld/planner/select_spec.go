package planner

type NodeSelectSpec struct {
	baseNode
	// TODO(longjiquan)
}

func (n *NodeSelectSpec) String() string {
	return "NodeSelectSpec"
}

func (n *NodeSelectSpec) GetChildren() []Node {
	return nil
}

func (n *NodeSelectSpec) Accept(v Visitor) interface{} {
	return v.VisitSelectSpec(n)
}
