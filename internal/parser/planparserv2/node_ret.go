package planparserv2

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

type ExprWithType struct {
	expr     *planpb.Expr
	dataType schemapb.DataType
	// ExprWithType can be a node only when nodeDependent is set to false.
	// For example, a column expression or a value expression itself cannot be an expression node independently.
	// Unless our execution backend can support them.
	nodeDependent bool
}

func getError(obj interface{}) error {
	err, ok := obj.(error)
	if !ok {
		// obj is not an error.
		return nil
	}
	return err
}

func getExpr(obj interface{}) *ExprWithType {
	n, ok := obj.(*ExprWithType)
	if !ok {
		// obj is not of *ExprWithType
		return nil
	}
	return n
}

func getGenericValue(obj interface{}) *planpb.GenericValue {
	expr := getExpr(obj)
	if expr == nil {
		return nil
	}
	return expr.expr.GetValueExpr().GetValue()
}

func getValueExpr(obj interface{}) *planpb.ValueExpr {
	expr := getExpr(obj)
	if expr == nil {
		return nil
	}
	return expr.expr.GetValueExpr()
}

func isTemplateExpr(expr *planpb.ValueExpr) bool {
	return expr != nil && expr.GetTemplateVariableName() != ""
}
