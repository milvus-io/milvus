package planner

import "fmt"

type NodeBinaryComparisonPredicate struct {
	baseNode
	Left  *NodePredicate
	Right *NodePredicate
	Op    ComparisonOperator
}

func (n *NodeBinaryComparisonPredicate) String() string {
	return fmt.Sprintf("NodeBinaryComparisonPredicate, Op: %v", n.Op)
}

func (n *NodeBinaryComparisonPredicate) GetChildren() []Node {
	return []Node{n.Left, n.Right}
}

func (n *NodeBinaryComparisonPredicate) Accept(v Visitor) interface{} {
	return v.VisitBinaryComparisonPredicate(n)
}

func NewNodeBinaryComparisonPredicate(text string, left, right *NodePredicate, op ComparisonOperator) *NodeBinaryComparisonPredicate {
	return &NodeBinaryComparisonPredicate{
		baseNode: newBaseNode(text),
		Left:     left,
		Right:    right,
		Op:       op,
	}
}
