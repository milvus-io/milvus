package planner

import (
	"github.com/moznion/go-optional"
)

type NodeAggregateWindowedFunction struct {
	baseNode
	AggCount optional.Option[*NodeCount]
}

func (n *NodeAggregateWindowedFunction) String() string {
	return "NodeAggregateWindowedFunction"
}

func (n *NodeAggregateWindowedFunction) GetChildren() []Node {
	if n.AggCount.IsSome() {
		return []Node{n.AggCount.Unwrap()}
	}
	return nil
}

func (n *NodeAggregateWindowedFunction) Accept(v Visitor) interface{} {
	return v.VisitAggregateWindowedFunction(n)
}

type NodeAggregateWindowedFunctionOption func(*NodeAggregateWindowedFunction)

func (n *NodeAggregateWindowedFunction) apply(opts ...NodeAggregateWindowedFunctionOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithAggCount(c *NodeCount) NodeAggregateWindowedFunctionOption {
	return func(n *NodeAggregateWindowedFunction) {
		n.AggCount = optional.Some(c)
	}
}

func NewNodeAggregateWindowedFunction(text string, opts ...NodeAggregateWindowedFunctionOption) *NodeAggregateWindowedFunction {
	n := &NodeAggregateWindowedFunction{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
