package planner

import "fmt"

type NodeUnaryExpressionAtom struct {
	baseNode
	Expr *NodeExpressionAtom
	Op   UnaryOperator
}

func (n *NodeUnaryExpressionAtom) String() string {
	return fmt.Sprintf("NodeUnaryExpressionAtom, Op: %v", n.Op)
}

func (n *NodeUnaryExpressionAtom) GetChildren() []Node {
	return []Node{n.Expr}
}

func (n *NodeUnaryExpressionAtom) Accept(v Visitor) interface{} {
	return v.VisitUnaryExpressionAtom(n)
}

func NewNodeUnaryExpressionAtom(text string, expr *NodeExpressionAtom, op UnaryOperator) *NodeUnaryExpressionAtom {
	return &NodeUnaryExpressionAtom{
		baseNode: newBaseNode(text),
		Expr:     expr,
		Op:       op,
	}
}
