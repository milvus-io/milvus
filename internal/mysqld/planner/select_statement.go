package planner

import "github.com/moznion/go-optional"

type NodeSelectStatement struct {
	baseNode
	SimpleSelect optional.Option[*NodeSimpleSelect]
}

func (n *NodeSelectStatement) String() string {
	return "NodeSelectStatement"
}

func (n *NodeSelectStatement) GetChildren() []Node {
	if n.SimpleSelect.IsSome() {
		return []Node{n.SimpleSelect.Unwrap()}
	}
	return nil
}

func (n *NodeSelectStatement) Accept(v Visitor) interface{} {
	return v.VisitSelectStatement(n)
}

type NodeSelectStatementOption func(*NodeSelectStatement)

func (n *NodeSelectStatement) apply(opts ...NodeSelectStatementOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithSimpleSelect(s *NodeSimpleSelect) NodeSelectStatementOption {
	return func(n *NodeSelectStatement) {
		n.SimpleSelect = optional.Some(s)
	}
}

func NewNodeSelectStatement(text string, opts ...NodeSelectStatementOption) *NodeSelectStatement {
	n := &NodeSelectStatement{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
