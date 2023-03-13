package planner

import "fmt"

type Node interface {
	fmt.Stringer
	GetText() string
	GetChildren() []Node
	Accept(visitor Visitor) interface{}
}

type baseNode struct {
	text string
}

func (n baseNode) GetText() string {
	return n.text
}

func newBaseNode(text string) baseNode {
	return baseNode{text: text}
}
