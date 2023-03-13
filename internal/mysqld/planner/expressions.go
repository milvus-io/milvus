package planner

type NodeExpressions struct {
	baseNode
	Expressions []*NodeExpression
}

func (n *NodeExpressions) String() string {
	return "NodeExpressions"
}

func (n *NodeExpressions) GetChildren() []Node {
	children := make([]Node, 0, len(n.Expressions))
	for _, child := range n.Expressions {
		children = append(children, child)
	}
	return children
}

func (n *NodeExpressions) Accept(v Visitor) interface{} {
	return v.VisitExpressions(n)
}

func NewNodeExpressions(text string, exprs []*NodeExpression) *NodeExpressions {
	return &NodeExpressions{
		baseNode:    newBaseNode(text),
		Expressions: exprs,
	}
}
