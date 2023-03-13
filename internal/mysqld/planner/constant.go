package planner

import "github.com/moznion/go-optional"

type NodeConstant struct {
	baseNode
	StringLiteral  optional.Option[string]
	DecimalLiteral optional.Option[int64]
	BooleanLiteral optional.Option[bool]
	RealLiteral    optional.Option[float64]
}

func (n *NodeConstant) String() string {
	return "NodeConstant: " + n.GetText()
}

func (n *NodeConstant) GetChildren() []Node {
	return nil
}

func (n *NodeConstant) Accept(v Visitor) interface{} {
	return v.VisitConstant(n)
}

type NodeConstantOption func(*NodeConstant)

func (n *NodeConstant) apply(opts ...NodeConstantOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithStringLiteral(s string) NodeConstantOption {
	return func(n *NodeConstant) {
		n.StringLiteral = optional.Some(s)
	}
}

func WithDecimalLiteral(s int64) NodeConstantOption {
	return func(n *NodeConstant) {
		n.DecimalLiteral = optional.Some(s)
	}
}

func WithBooleanLiteral(s bool) NodeConstantOption {
	return func(n *NodeConstant) {
		n.BooleanLiteral = optional.Some(s)
	}
}

func WithRealLiteral(s float64) NodeConstantOption {
	return func(n *NodeConstant) {
		n.RealLiteral = optional.Some(s)
	}
}

func NewNodeConstant(text string, opts ...NodeConstantOption) *NodeConstant {
	n := &NodeConstant{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
