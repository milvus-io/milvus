package planner

import (
	"fmt"

	"github.com/moznion/go-optional"
)

type NodeANNSClause struct {
	baseNode
	Column  *NodeFullColumnName
	Vectors []*NodeVector
	Params  optional.Option[*NodeKVPairs]
}

func (n *NodeANNSClause) String() string {
	s := fmt.Sprintf("NodeANNSClause, Column: %s, Nq: %d", n.Column.String(), len(n.Vectors))
	if n.Params.IsSome() {
		s += fmt.Sprintf(", Params: %s", n.Params.Unwrap().String())
	}
	return s
}

func (n *NodeANNSClause) GetChildren() []Node {
	// return []Node{n.Column}
	return nil
}

func (n *NodeANNSClause) Accept(v Visitor) interface{} {
	return v.VisitANNSClause(n)
}

type NodeANNSClauseOption func(*NodeANNSClause)

func NodeANNSClauseWithParams(p *NodeKVPairs) NodeANNSClauseOption {
	return func(n *NodeANNSClause) {
		n.Params = optional.Some(p)
	}
}

func (n *NodeANNSClause) apply(opts ...NodeANNSClauseOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func NewNodeANNSClause(text string, column *NodeFullColumnName, vectors []*NodeVector, opts ...NodeANNSClauseOption) *NodeANNSClause {
	n := &NodeANNSClause{
		baseNode: newBaseNode(text),
		Column:   column,
		Vectors:  vectors,
	}
	n.apply(opts...)
	return n
}
