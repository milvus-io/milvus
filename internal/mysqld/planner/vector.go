package planner

import "github.com/moznion/go-optional"

type NodeVector struct {
	FloatVector optional.Option[*NodeFloatVector]
}

type NodeVectorOption func(*NodeVector)

func (n *NodeVector) apply(opts ...NodeVectorOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithFloatVector(v *NodeFloatVector) NodeVectorOption {
	return func(n *NodeVector) {
		n.FloatVector = optional.Some(v)
	}
}

func NewNodeVector(opts ...NodeVectorOption) *NodeVector {
	n := &NodeVector{}
	n.apply(opts...)
	return n
}
