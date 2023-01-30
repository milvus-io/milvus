package planner

import "github.com/moznion/go-optional"

type NodeDmlStatement struct {
	baseNode
	SelectStatement optional.Option[*NodeSelectStatement]
}

func (n *NodeDmlStatement) String() string {
	return "NodeDmlStatement"
}

func (n *NodeDmlStatement) GetChildren() []Node {
	if n.SelectStatement.IsSome() {
		return []Node{n.SelectStatement.Unwrap()}
	}
	return nil
}

func (n *NodeDmlStatement) Accept(v Visitor) interface{} {
	return v.VisitDmlStatement(n)
}

type NodeDmlStatementOption func(*NodeDmlStatement)

func (n *NodeDmlStatement) apply(opts ...NodeDmlStatementOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithSelectStatement(s *NodeSelectStatement) NodeDmlStatementOption {
	return func(n *NodeDmlStatement) {
		n.SelectStatement = optional.Some(s)
	}
}

func NewNodeDmlStatement(text string, opts ...NodeDmlStatementOption) *NodeDmlStatement {
	n := &NodeDmlStatement{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
