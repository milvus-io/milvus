package planparserv2

import (
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

type ExprWithType struct {
	expr     *planpb.Expr
	dataType schemapb.DataType
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
