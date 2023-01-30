package planner

import "fmt"

type NodeInPredicate struct {
	baseNode
	Predicate   *NodePredicate
	Expressions *NodeExpressions
	Op          InOperator
}

func (n *NodeInPredicate) String() string {
	return fmt.Sprintf("NodeInPredicate, Op: %v", n.Op)
}

func (n *NodeInPredicate) GetChildren() []Node {
	return []Node{n.Predicate, n.Expressions}
}

func (n *NodeInPredicate) Accept(v Visitor) interface{} {
	return v.VisitInPredicate(n)
}

func NewNodeInPredicate(text string, predicate *NodePredicate, exprs *NodeExpressions, op InOperator) *NodeInPredicate {
	return &NodeInPredicate{
		baseNode:    newBaseNode(text),
		Predicate:   predicate,
		Expressions: exprs,
		Op:          op,
	}
}
