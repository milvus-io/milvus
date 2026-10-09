package planparserv2

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func queryValueHasNonFiniteFloat(value *planpb.GenericValue) bool {
	switch v := value.GetVal().(type) {
	case *planpb.GenericValue_FloatVal:
		return math.IsNaN(v.FloatVal) || math.IsInf(v.FloatVal, 0)
	case *planpb.GenericValue_ArrayVal:
		for _, element := range v.ArrayVal.GetArray() {
			if queryValueHasNonFiniteFloat(element) {
				return true
			}
		}
	}
	return false
}

func validateIntegerArrayQueryValues(column *planpb.ColumnInfo, values []*planpb.GenericValue) error {
	if column.GetDataType() != schemapb.DataType_Array || !typeutil.IsIntegerType(column.GetElementType()) {
		return nil
	}
	for _, value := range values {
		if queryValueHasNonFiniteFloat(value) {
			return merr.WrapErrQueryPlanMsg("non-finite floating-point query values cannot be cast to integer array element type %s", column.GetElementType().String())
		}
	}
	return nil
}

// NaN is an ordinary, non-NULL scalar query value. Parameters that represent
// probabilities or distances still require finite numbers.
func validateQueryValues(expr *planpb.Expr) error {
	var typeError error
	invalid := walkExpr(expr, func(node *planpb.Expr) bool {
		var value float64
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_UnaryRangeExpr:
			typeError = validateIntegerArrayQueryValues(e.UnaryRangeExpr.GetColumnInfo(), []*planpb.GenericValue{e.UnaryRangeExpr.GetValue()})
			return typeError != nil
		case *planpb.Expr_TermExpr:
			typeError = validateIntegerArrayQueryValues(e.TermExpr.GetColumnInfo(), e.TermExpr.GetValues())
			return typeError != nil
		case *planpb.Expr_JsonContainsExpr:
			typeError = validateIntegerArrayQueryValues(e.JsonContainsExpr.GetColumnInfo(), e.JsonContainsExpr.GetElements())
			return typeError != nil
		case *planpb.Expr_RandomSampleExpr:
			value = float64(e.RandomSampleExpr.GetSampleFactor())
		case *planpb.Expr_GisfunctionFilterExpr:
			value = e.GisfunctionFilterExpr.GetDistance()
		default:
			return false
		}
		return math.IsNaN(value) || math.IsInf(value, 0)
	})
	if typeError != nil {
		return typeError
	}
	if invalid {
		return merr.WrapErrParameterInvalidMsg("sample factors and distances must be finite")
	}
	return nil
}
