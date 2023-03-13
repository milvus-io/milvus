package planner

import "fmt"

type NodeIsExpression struct {
	baseNode
	Predicate *NodePredicate
	TV        TestValue
	Op        IsOperator
}

func (n *NodeIsExpression) String() string {
	return fmt.Sprintf("NodeIsExpression, Op: %v, Test Value: %v", n.Op, n.TV)
}

func (n *NodeIsExpression) GetChildren() []Node {
	return []Node{n.Predicate}
}

func (n *NodeIsExpression) Accept(v Visitor) interface{} {
	return v.VisitIsExpression(n)
}

func NewNodeIsExpression(text string, predicate *NodePredicate, tv TestValue, op IsOperator) *NodeIsExpression {
	n := &NodeIsExpression{
		baseNode:  newBaseNode(text),
		Predicate: predicate,
		TV:        tv,
		Op:        op,
	}
	return n
}
