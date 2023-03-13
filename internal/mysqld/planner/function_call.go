package planner

import "github.com/moznion/go-optional"

type NodeFunctionCall struct {
	baseNode
	Agg   optional.Option[*NodeAggregateWindowedFunction]
	Alias optional.Option[string]
}

func (n *NodeFunctionCall) String() string {
	if n.Alias.IsSome() {
		return "NodeFunctionCall, Alias: " + n.Alias.Unwrap()
	}
	return "NodeFunctionCall"
}

func (n *NodeFunctionCall) GetChildren() []Node {
	if n.Agg.IsSome() {
		return []Node{n.Agg.Unwrap()}
	}
	return nil
}

func (n *NodeFunctionCall) Accept(v Visitor) interface{} {
	return v.VisitFunctionCall(n)
}

type NodeFunctionCallOption func(*NodeFunctionCall)

func (n *NodeFunctionCall) apply(opts ...NodeFunctionCallOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func FunctionCallWithAlias(alias string) NodeFunctionCallOption {
	return func(n *NodeFunctionCall) {
		n.Alias = optional.Some(alias)
	}
}

func WithAgg(agg *NodeAggregateWindowedFunction) NodeFunctionCallOption {
	return func(n *NodeFunctionCall) {
		n.Agg = optional.Some(agg)
	}
}

func NewNodeFunctionCall(text string, opts ...NodeFunctionCallOption) *NodeFunctionCall {
	n := &NodeFunctionCall{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
