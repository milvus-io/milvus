package planner

import "github.com/moznion/go-optional"

type NodeSqlStatement struct {
	baseNode
	DmlStatement optional.Option[*NodeDmlStatement]
}

func (n *NodeSqlStatement) String() string {
	return "NodeSqlStatement"
}

func (n *NodeSqlStatement) GetChildren() []Node {
	if n.DmlStatement.IsSome() {
		return []Node{n.DmlStatement.Unwrap()}
	}
	return nil
}

func (n *NodeSqlStatement) Accept(v Visitor) interface{} {
	return v.VisitSqlStatement(n)
}

type NodeSqlStatementOption func(*NodeSqlStatement)

func (n *NodeSqlStatement) apply(opts ...NodeSqlStatementOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithDmlStatement(s *NodeDmlStatement) NodeSqlStatementOption {
	return func(n *NodeSqlStatement) {
		n.DmlStatement = optional.Some(s)
	}
}

func NewNodeSqlStatement(text string, opts ...NodeSqlStatementOption) *NodeSqlStatement {
	n := &NodeSqlStatement{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
