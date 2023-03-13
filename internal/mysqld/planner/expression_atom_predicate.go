package planner

import "github.com/moznion/go-optional"

type NodeExpressionAtomPredicate struct {
	baseNode
	Assignment     optional.Option[*NodeLocalID]
	ExpressionAtom *NodeExpressionAtom
}

func (n *NodeExpressionAtomPredicate) String() string {
	return "NodeExpressionAtomPredicate"
}

func (n *NodeExpressionAtomPredicate) GetChildren() []Node {
	return []Node{n.ExpressionAtom}
}

func (n *NodeExpressionAtomPredicate) Accept(v Visitor) interface{} {
	return v.VisitExpressionAtomPredicate(n)
}

type NodeExpressionAtomPredicateOption func(*NodeExpressionAtomPredicate)

func (n *NodeExpressionAtomPredicate) apply(opts ...NodeExpressionAtomPredicateOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithAssignment(assign *NodeLocalID) NodeExpressionAtomPredicateOption {
	return func(n *NodeExpressionAtomPredicate) {
		n.Assignment = optional.Some(assign)
	}
}

func NewNodeExpressionAtomPredicate(text string, exprAtom *NodeExpressionAtom, opts ...NodeExpressionAtomPredicateOption) *NodeExpressionAtomPredicate {
	n := &NodeExpressionAtomPredicate{
		baseNode:       newBaseNode(text),
		ExpressionAtom: exprAtom,
	}
	n.apply(opts...)
	return n
}
