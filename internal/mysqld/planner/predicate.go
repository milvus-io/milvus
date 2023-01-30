package planner

import "github.com/moznion/go-optional"

type NodePredicate struct {
	baseNode
	InPredicate               optional.Option[*NodeInPredicate]
	BinaryComparisonPredicate optional.Option[*NodeBinaryComparisonPredicate]
	ExpressionAtomPredicate   optional.Option[*NodeExpressionAtomPredicate]
}

func (n *NodePredicate) String() string {
	return "NodePredicate"
}

func (n *NodePredicate) GetChildren() []Node {
	if n.InPredicate.IsSome() {
		return []Node{n.InPredicate.Unwrap()}
	}
	if n.BinaryComparisonPredicate.IsSome() {
		return []Node{n.BinaryComparisonPredicate.Unwrap()}
	}
	if n.ExpressionAtomPredicate.IsSome() {
		return []Node{n.ExpressionAtomPredicate.Unwrap()}
	}
	return nil
}

func (n *NodePredicate) Accept(v Visitor) interface{} {
	return v.VisitPredicate(n)
}

type NodePredicateOption func(*NodePredicate)

func (n *NodePredicate) apply(opts ...NodePredicateOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithInPredicate(p *NodeInPredicate) NodePredicateOption {
	return func(n *NodePredicate) {
		n.InPredicate = optional.Some(p)
	}
}

func WithNodeBinaryComparisonPredicate(p *NodeBinaryComparisonPredicate) NodePredicateOption {
	return func(n *NodePredicate) {
		n.BinaryComparisonPredicate = optional.Some(p)
	}
}

func WithNodeExpressionAtomPredicate(p *NodeExpressionAtomPredicate) NodePredicateOption {
	return func(n *NodePredicate) {
		n.ExpressionAtomPredicate = optional.Some(p)
	}
}

func NewNodePredicate(text string, opts ...NodePredicateOption) *NodePredicate {
	n := &NodePredicate{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
