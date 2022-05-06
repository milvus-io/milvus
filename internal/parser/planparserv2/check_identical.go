package planparserv2

import (
	"reflect"

	"github.com/milvus-io/milvus/internal/proto/planpb"
)

// CheckIdentical check if two exprs are identical.
func CheckIdentical(expr, other *planpb.Expr) bool {
	v := NewShowExprVisitor()
	js1 := v.VisitExpr(expr)
	js2 := v.VisitExpr(other)
	return reflect.DeepEqual(js1, js2)
}
