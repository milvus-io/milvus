package planner

import (
	"github.com/shivamMg/ppds/tree"
)

type TreeUtils interface {
	PreOrderTraverse(n Node, v Visitor)
	PrettyPrint(n Node)
	PrettyPrintHr(n Node)
	PrettyPrintHrn(n Node)
}

// wrappedNode implements tree.Node.
type wrappedNode struct {
	n Node
}

func (w wrappedNode) Data() interface{} {
	return w.n.String() + ", text: " + w.n.GetText()
}

func (w wrappedNode) Children() []tree.Node {
	children := w.n.GetChildren()
	r := make([]tree.Node, 0, len(children))
	for _, child := range children {
		r = append(r, newWrappedNode(child))
	}
	return r
}

func newWrappedNode(n Node) *wrappedNode {
	return &wrappedNode{n: n}
}

type treeUtilsImpl struct{}

func (t treeUtilsImpl) PreOrderTraverse(n Node, v Visitor) {
	n.Accept(v)
	children := n.GetChildren()
	for _, child := range children {
		t.PreOrderTraverse(child, v)
	}
}

func (t treeUtilsImpl) PrettyPrint(n Node) {
	tree.Print(newWrappedNode(n))
}

func (t treeUtilsImpl) PrettyPrintHr(n Node) {
	tree.PrintHr(newWrappedNode(n))
}

func (t treeUtilsImpl) PrettyPrintHrn(n Node) {
	tree.PrintHrn(newWrappedNode(n))
}

func NewTreeUtils() TreeUtils {
	return &treeUtilsImpl{}
}
