package rewriter

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// normalizeEmptyArrayComparisons lowers whole ARRAY == [] and ARRAY != [] to
// array_length(ARRAY) == 0 and array_length(ARRAY) != 0. This is semantic
// normalization rather than an optional optimization: an empty array carries
// no element type, while ARRAY length has the same nullable semantics and an
// executable plan representation.
func normalizeEmptyArrayComparisons(expr *planpb.Expr) *planpb.Expr {
	if expr == nil {
		return nil
	}

	switch real := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		real.BinaryExpr.Left = normalizeEmptyArrayComparisons(real.BinaryExpr.GetLeft())
		real.BinaryExpr.Right = normalizeEmptyArrayComparisons(real.BinaryExpr.GetRight())
		return expr
	case *planpb.Expr_UnaryExpr:
		real.UnaryExpr.Child = normalizeEmptyArrayComparisons(real.UnaryExpr.GetChild())
		return expr
	case *planpb.Expr_BinaryArithExpr:
		real.BinaryArithExpr.Left = normalizeEmptyArrayComparisons(real.BinaryArithExpr.GetLeft())
		real.BinaryArithExpr.Right = normalizeEmptyArrayComparisons(real.BinaryArithExpr.GetRight())
		return expr
	case *planpb.Expr_CallExpr:
		for i, parameter := range real.CallExpr.GetFunctionParameters() {
			real.CallExpr.FunctionParameters[i] = normalizeEmptyArrayComparisons(parameter)
		}
		return expr
	case *planpb.Expr_RandomSampleExpr:
		real.RandomSampleExpr.Predicate = normalizeEmptyArrayComparisons(real.RandomSampleExpr.GetPredicate())
		return expr
	case *planpb.Expr_UnaryRangeExpr:
		return normalizeEmptyArrayUnaryRange(expr, real.UnaryRangeExpr)
	default:
		return expr
	}
}

func normalizeEmptyArrayUnaryRange(original *planpb.Expr, unaryRange *planpb.UnaryRangeExpr) *planpb.Expr {
	if unaryRange == nil || unaryRange.GetColumnInfo() == nil {
		return original
	}
	columnInfo := unaryRange.GetColumnInfo()
	if columnInfo.GetDataType() != schemapb.DataType_Array ||
		len(columnInfo.GetNestedPath()) != 0 {
		return original
	}
	if unaryRange.GetOp() != planpb.OpType_Equal && unaryRange.GetOp() != planpb.OpType_NotEqual {
		return original
	}
	array := unaryRange.GetValue().GetArrayVal()
	if array == nil || len(array.GetArray()) != 0 {
		return original
	}

	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithOpEvalRangeExpr{
			BinaryArithOpEvalRangeExpr: &planpb.BinaryArithOpEvalRangeExpr{
				ColumnInfo: columnInfo,
				ArithOp:    planpb.ArithOpType_ArrayLength,
				Op:         unaryRange.GetOp(),
				Value: &planpb.GenericValue{
					Val: &planpb.GenericValue_Int64Val{Int64Val: 0},
				},
			},
		},
	}
}
