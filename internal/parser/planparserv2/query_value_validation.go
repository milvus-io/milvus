package planparserv2

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newNaNQueryError() error {
	return merr.WrapErrParameterInvalidMsg("NaN is not supported in query expressions")
}

func queryValueHasNaN(value *planpb.GenericValue) bool {
	switch v := value.GetVal().(type) {
	case *planpb.GenericValue_FloatVal:
		return math.IsNaN(v.FloatVal)
	case *planpb.GenericValue_ArrayVal:
		for _, element := range v.ArrayVal.GetArray() {
			if queryValueHasNaN(element) {
				return true
			}
		}
	}
	return false
}

func validateQueryValues(expr *planpb.Expr) error {
	hasNaN := walkExpr(expr, func(node *planpb.Expr) bool {
		var values []*planpb.GenericValue
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_ValueExpr:
			values = []*planpb.GenericValue{e.ValueExpr.GetValue()}
		case *planpb.Expr_TermExpr:
			values = e.TermExpr.GetValues()
		case *planpb.Expr_UnaryRangeExpr:
			values = append([]*planpb.GenericValue{e.UnaryRangeExpr.GetValue()}, e.UnaryRangeExpr.GetExtraValues()...)
		case *planpb.Expr_BinaryRangeExpr:
			values = []*planpb.GenericValue{e.BinaryRangeExpr.GetLowerValue(), e.BinaryRangeExpr.GetUpperValue()}
		case *planpb.Expr_BinaryArithOpEvalRangeExpr:
			values = []*planpb.GenericValue{e.BinaryArithOpEvalRangeExpr.GetRightOperand(), e.BinaryArithOpEvalRangeExpr.GetValue()}
		case *planpb.Expr_JsonContainsExpr:
			values = e.JsonContainsExpr.GetElements()
		case *planpb.Expr_TimestamptzArithCompareExpr:
			values = []*planpb.GenericValue{e.TimestamptzArithCompareExpr.GetCompareValue()}
		case *planpb.Expr_RandomSampleExpr:
			return math.IsNaN(float64(e.RandomSampleExpr.GetSampleFactor()))
		case *planpb.Expr_GisfunctionFilterExpr:
			return math.IsNaN(e.GisfunctionFilterExpr.GetDistance())
		}
		for _, value := range values {
			if queryValueHasNaN(value) {
				return true
			}
		}
		return false
	})
	if hasNaN {
		return newNaNQueryError()
	}
	return nil
}

// Reject before comparison or logical constant folding can discard the NaN.
func validateFoldedConstant(expr *ExprWithType) interface{} {
	if queryValueHasNaN(getGenericValue(expr)) {
		return newNaNQueryError()
	}
	return expr
}
