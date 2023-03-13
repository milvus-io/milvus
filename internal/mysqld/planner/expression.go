package planner

import "github.com/moznion/go-optional"

type NodeExpression struct {
	baseNode
	NotExpr     optional.Option[*NodeNotExpression]
	LogicalExpr optional.Option[*NodeLogicalExpression]
	IsExpr      optional.Option[*NodeIsExpression]
	Predicate   optional.Option[*NodePredicate]
}

func (n *NodeExpression) String() string {
	return "NodeExpression"
}

func (n *NodeExpression) GetChildren() []Node {
	if n.NotExpr.IsSome() {
		return []Node{n.NotExpr.Unwrap()}
	}
	if n.LogicalExpr.IsSome() {
		return []Node{n.LogicalExpr.Unwrap()}
	}
	if n.IsExpr.IsSome() {
		return []Node{n.IsExpr.Unwrap()}
	}
	if n.Predicate.IsSome() {
		return []Node{n.Predicate.Unwrap()}
	}
	return nil
}

func (n *NodeExpression) Accept(v Visitor) interface{} {
	return v.VisitExpression(n)
}

type NodeExpressionOption func(*NodeExpression)

func (n *NodeExpression) apply(opts ...NodeExpressionOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithNotExpr(expr *NodeNotExpression) NodeExpressionOption {
	return func(n *NodeExpression) {
		n.NotExpr = optional.Some(expr)
	}
}

func WithLogicalExpr(expr *NodeLogicalExpression) NodeExpressionOption {
	return func(n *NodeExpression) {
		n.LogicalExpr = optional.Some(expr)
	}
}

func WithIsExpr(expr *NodeIsExpression) NodeExpressionOption {
	return func(n *NodeExpression) {
		n.IsExpr = optional.Some(expr)
	}
}

func WithPredicate(predicate *NodePredicate) NodeExpressionOption {
	return func(n *NodeExpression) {
		n.Predicate = optional.Some(predicate)
	}
}

func NewNodeExpression(text string, opts ...NodeExpressionOption) *NodeExpression {
	n := &NodeExpression{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
