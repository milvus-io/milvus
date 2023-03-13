package planner

import "github.com/moznion/go-optional"

type NodeFromClause struct {
	baseNode
	TableSources []*NodeTableSource
	Where        optional.Option[*NodeExpression]
}

func (n *NodeFromClause) String() string {
	return "NodeFromClause"
}

func (n *NodeFromClause) GetChildren() []Node {
	children := make([]Node, 0, len(n.TableSources)+1)
	for _, child := range n.TableSources {
		children = append(children, child)
	}
	if n.Where.IsSome() {
		children = append(children, n.Where.Unwrap())
	}
	return children
}

func (n *NodeFromClause) Accept(v Visitor) interface{} {
	return v.VisitFromClause(n)
}

type NodeFromClauseOption func(*NodeFromClause)

func (n *NodeFromClause) apply(opts ...NodeFromClauseOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithWhere(where *NodeExpression) NodeFromClauseOption {
	return func(n *NodeFromClause) {
		n.Where = optional.Some(where)
	}
}

func NewNodeFromClause(text string, tableSources []*NodeTableSource, opts ...NodeFromClauseOption) *NodeFromClause {
	n := &NodeFromClause{
		baseNode:     newBaseNode(text),
		TableSources: tableSources,
	}
	n.apply(opts...)
	return n
}
