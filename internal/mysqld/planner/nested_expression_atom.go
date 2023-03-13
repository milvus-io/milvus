package planner

type NodeNestedExpressionAtom struct {
	baseNode
	Expressions []*NodeExpression
}

func (n *NodeNestedExpressionAtom) String() string {
	return "NodeNestedExpressionAtom"
}

func (n *NodeNestedExpressionAtom) GetChildren() []Node {
	children := make([]Node, 0, len(n.Expressions))
	for _, child := range n.Expressions {
		children = append(children, child)
	}
	return children
}

func (n *NodeNestedExpressionAtom) Accept(v Visitor) interface{} {
	return v.VisitNestedExpressionAtom(n)
}

func NewNodeNestedExpressionAtom(text string, exprs []*NodeExpression) *NodeNestedExpressionAtom {
	return &NodeNestedExpressionAtom{
		baseNode:    newBaseNode(text),
		Expressions: exprs,
	}
}
