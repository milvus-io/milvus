package planner

import "github.com/moznion/go-optional"

type NodeSelectElement struct {
	baseNode
	Star           optional.Option[*NodeSelectElementStar]
	FullColumnName optional.Option[*NodeFullColumnName]
	FunctionCall   optional.Option[*NodeFunctionCall]
}

func (n *NodeSelectElement) String() string {
	return "NodeSelectElement"
}

func (n *NodeSelectElement) GetChildren() []Node {
	if n.Star.IsSome() {
		return []Node{n.Star.Unwrap()}
	}
	if n.FullColumnName.IsSome() {
		return []Node{n.FullColumnName.Unwrap()}
	}
	if n.FunctionCall.IsSome() {
		return []Node{n.FunctionCall.Unwrap()}
	}
	return nil
}

func (n *NodeSelectElement) Accept(v Visitor) interface{} {
	return v.VisitSelectElement(n)
}

type NodeSelectElementOption func(*NodeSelectElement)

func (n *NodeSelectElement) apply(opts ...NodeSelectElementOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithStar() NodeSelectElementOption {
	return func(n *NodeSelectElement) {
		n.Star = optional.Some(&NodeSelectElementStar{})
	}
}

func WithFullColumnName(c *NodeFullColumnName) NodeSelectElementOption {
	return func(n *NodeSelectElement) {
		n.FullColumnName = optional.Some(c)
	}
}

func WithFunctionCall(c *NodeFunctionCall) NodeSelectElementOption {
	return func(n *NodeSelectElement) {
		n.FunctionCall = optional.Some(c)
	}
}

func NewNodeSelectElement(text string, opts ...NodeSelectElementOption) *NodeSelectElement {
	n := &NodeSelectElement{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
