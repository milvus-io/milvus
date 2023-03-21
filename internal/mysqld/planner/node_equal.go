package planner

import "reflect"

func Equal(n1, n2 Node) bool {
	v := NewJSONVisitor()
	j1, j2 := n1.Accept(v), n2.Accept(v)
	return reflect.DeepEqual(j1, j2)
}
