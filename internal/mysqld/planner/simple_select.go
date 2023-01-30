package planner

import "github.com/moznion/go-optional"

type NodeSimpleSelect struct {
	baseNode
	Query      optional.Option[*NodeQuerySpecification]
	LockClause optional.Option[*NodeLockClause]
}

func (n *NodeSimpleSelect) String() string {
	return "NodeSimpleSelect"
}

func (n *NodeSimpleSelect) GetChildren() []Node {
	children := make([]Node, 0, 2)
	if n.Query.IsSome() {
		children = append(children, n.Query.Unwrap())
	}
	if n.LockClause.IsSome() {
		children = append(children, n.LockClause.Unwrap())
	}
	return children
}

func (n *NodeSimpleSelect) Accept(v Visitor) interface{} {
	//TODO implement me
	panic("implement me")
}

type NodeSimpleSelectOption func(*NodeSimpleSelect)

func (n *NodeSimpleSelect) apply(opts ...NodeSimpleSelectOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithQuery(query *NodeQuerySpecification) NodeSimpleSelectOption {
	return func(n *NodeSimpleSelect) {
		n.Query = optional.Some(query)
	}
}

func WithLockClause(clause *NodeLockClause) NodeSimpleSelectOption {
	return func(n *NodeSimpleSelect) {
		n.LockClause = optional.Some(clause)
	}
}

func NewNodeSimpleSelect(text string, opts ...NodeSimpleSelectOption) *NodeSimpleSelect {
	n := &NodeSimpleSelect{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
