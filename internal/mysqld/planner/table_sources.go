package planner

import (
	"fmt"

	"github.com/moznion/go-optional"
)

type NodeTableSource struct {
	baseNode
	TableName optional.Option[string]
}

func (n *NodeTableSource) String() string {
	if n.TableName.IsSome() {
		return fmt.Sprintf("NodeTableSource: %s", n.TableName.Unwrap())
	}
	return "NodeTableSource"
}

func (n *NodeTableSource) GetChildren() []Node {
	return nil
}

func (n *NodeTableSource) Accept(v Visitor) interface{} {
	return v.VisitTableSource(n)
}

type NodeTableSourceOption func(*NodeTableSource)

func (n *NodeTableSource) apply(opts ...NodeTableSourceOption) {
	for _, opt := range opts {
		opt(n)
	}
}

func WithTableName(name string) NodeTableSourceOption {
	return func(n *NodeTableSource) {
		n.TableName = optional.Some(name)
	}
}

func NewNodeTableSource(text string, opts ...NodeTableSourceOption) *NodeTableSource {
	n := &NodeTableSource{
		baseNode: newBaseNode(text),
	}
	n.apply(opts...)
	return n
}
