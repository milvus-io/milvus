package planner

import "fmt"

type NodeLogicalExpression struct {
	baseNode
	Left  *NodeExpression
	Right *NodeExpression
	Op    LogicalOperator
}

func (n *NodeLogicalExpression) String() string {
	return fmt.Sprintf("NodeLogicalExpression, Op: %v", n.Op)
}

func (n *NodeLogicalExpression) GetChildren() []Node {
	return []Node{n.Left, n.Right}
}

func (n *NodeLogicalExpression) Accept(v Visitor) interface{} {
	return v.VisitLogicalExpression(n)
}

func NewNodeLogicalExpression(text string, left, right *NodeExpression, op LogicalOperator) *NodeLogicalExpression {
	return &NodeLogicalExpression{
		baseNode: newBaseNode(text),
		Left:     left,
		Right:    right,
		Op:       op,
	}
}
