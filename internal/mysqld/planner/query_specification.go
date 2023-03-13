package planner

import "github.com/moznion/go-optional"

type NodeQuerySpecification struct {
	baseNode
	SelectSpecs    []*NodeSelectSpec
	SelectElements []*NodeSelectElement
	From           optional.Option[*NodeFromClause]
	Limit          optional.Option[*NodeLimitClause]
}

func (n *NodeQuerySpecification) String() string {
	return "NodeQuerySpecification"
}

func (n *NodeQuerySpecification) GetChildren() []Node {
	children := make([]Node, 0, len(n.SelectSpecs)+len(n.SelectElements)+2)
	for _, child := range n.SelectSpecs {
		children = append(children, child)
	}
	for _, child := range n.SelectElements {
		children = append(children, child)
	}
	if n.From.IsSome() {
		children = append(children, n.From.Unwrap())
	}
	if n.Limit.IsSome() {
		children = append(children, n.Limit.Unwrap())
	}
	return children
}

func (n *NodeQuerySpecification) Accept(v Visitor) interface{} {
	return v.VisitQuerySpecification(n)
}

type NodeQuerySpecificationOption func(*NodeQuerySpecification)

func (n *NodeQuerySpecification) apply(opts ...NodeQuerySpecificationOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithFrom(from *NodeFromClause) NodeQuerySpecificationOption {
	return func(n *NodeQuerySpecification) {
		n.From = optional.Some(from)
	}
}

func WithLimit(Limit *NodeLimitClause) NodeQuerySpecificationOption {
	return func(n *NodeQuerySpecification) {
		n.Limit = optional.Some(Limit)
	}
}

func NewNodeQuerySpecification(
	text string,
	selectSpecs []*NodeSelectSpec,
	selectElements []*NodeSelectElement,
	opts ...NodeQuerySpecificationOption,
) *NodeQuerySpecification {
	n := &NodeQuerySpecification{
		baseNode:       newBaseNode(text),
		SelectSpecs:    selectSpecs,
		SelectElements: selectElements,
	}
	n.apply(opts...)
	return n
}
