package planner

type NodeNotExpression struct {
	baseNode
	Expression *NodeExpression
}

func (n *NodeNotExpression) String() string {
	return "NodeNotExpression"
}

func (n *NodeNotExpression) GetChildren() []Node {
	return []Node{n.Expression}
}

func (n *NodeNotExpression) Accept(v Visitor) interface{} {
	return v.VisitNotExpression(n)
}

func NewNodeNotExpression(text string, expr *NodeExpression) *NodeNotExpression {
	return &NodeNotExpression{
		baseNode:   newBaseNode(text),
		Expression: expr,
	}
}
