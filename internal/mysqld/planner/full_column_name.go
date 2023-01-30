package planner

import (
	"fmt"

	"github.com/moznion/go-optional"
)

type NodeFullColumnName struct {
	baseNode
	Name  string
	Alias optional.Option[string]
}

func (n *NodeFullColumnName) String() string {
	if n.Alias.IsSome() {
		return fmt.Sprintf("NodeFullColumnName, Name: %s, Alias: %s", n.Name, n.Alias.Unwrap())
	}
	return fmt.Sprintf("NodeFullColumnName, Name: %s", n.Name)
}

func (n *NodeFullColumnName) GetChildren() []Node {
	return nil
}

func (n *NodeFullColumnName) Accept(v Visitor) interface{} {
	return v.VisitFullColumnName(n)
}

type NodeFullColumnNameOption func(*NodeFullColumnName)

func (n *NodeFullColumnName) apply(opts ...NodeFullColumnNameOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func FullColumnNameWithAlias(alias string) NodeFullColumnNameOption {
	return func(n *NodeFullColumnName) {
		n.Alias = optional.Some(alias)
	}
}

func NewNodeFullColumnName(text, name string, opts ...NodeFullColumnNameOption) *NodeFullColumnName {
	n := &NodeFullColumnName{
		baseNode: newBaseNode(text),
		Name:     name,
	}
	n.apply(opts...)
	return n
}
