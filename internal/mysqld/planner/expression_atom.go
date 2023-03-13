package planner

import "github.com/moznion/go-optional"

type NodeExpressionAtom struct {
	baseNode
	Constant       optional.Option[*NodeConstant]
	FullColumnName optional.Option[*NodeFullColumnName]
	UnaryExpr      optional.Option[*NodeUnaryExpressionAtom]
	NestedExpr     optional.Option[*NodeNestedExpressionAtom]
}

func (n *NodeExpressionAtom) String() string {
	return "NodeExpression"
}

func (n *NodeExpressionAtom) GetChildren() []Node {
	if n.Constant.IsSome() {
		return []Node{n.Constant.Unwrap()}
	}
	if n.FullColumnName.IsSome() {
		return []Node{n.FullColumnName.Unwrap()}
	}
	if n.UnaryExpr.IsSome() {
		return []Node{n.UnaryExpr.Unwrap()}
	}
	if n.NestedExpr.IsSome() {
		return []Node{n.NestedExpr.Unwrap()}
	}
	return nil
}

func (n *NodeExpressionAtom) Accept(v Visitor) interface{} {
	return v.VisitExpressionAtom(n)
}

type NodeExpressionAtomOption func(*NodeExpressionAtom)

func (n *NodeExpressionAtom) apply(opts ...NodeExpressionAtomOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func ExpressionAtomWithConstant(c *NodeConstant) NodeExpressionAtomOption {
	return func(n *NodeExpressionAtom) {
		n.Constant = optional.Some(c)
	}
}

func ExpressionAtomWithFullColumnName(c *NodeFullColumnName) NodeExpressionAtomOption {
	return func(n *NodeExpressionAtom) {
		n.FullColumnName = optional.Some(c)
	}
}

func ExpressionAtomWithUnaryExpr(c *NodeUnaryExpressionAtom) NodeExpressionAtomOption {
	return func(n *NodeExpressionAtom) {
		n.UnaryExpr = optional.Some(c)
	}
}

func ExpressionAtomWithNestedExpr(c *NodeNestedExpressionAtom) NodeExpressionAtomOption {
	return func(n *NodeExpressionAtom) {
		n.NestedExpr = optional.Some(c)
	}
}

func NewNodeExpressionAtom(text string, opts ...NodeExpressionAtomOption) *NodeExpressionAtom {
	n := &NodeExpressionAtom{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
