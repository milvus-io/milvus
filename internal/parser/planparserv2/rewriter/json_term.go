package rewriter

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

var jsonTermExecutorKindOrder = []string{"bool", "numeric", "string", "array"}

func jsonTermExecutorKind(value *planpb.GenericValue) string {
	kind := valueCaseWithNil(value)
	if isNumericCase(kind) {
		return "numeric"
	}
	return kind
}

// normalizeTermExprs enforces the execution invariant that every TermExpr can
// be dispatched to one scalar executor. JSON terms are partitioned by executor
// kind; int64 and float literals share one numeric executor and one candidate
// matcher, so mixed numeric IN parses each row once. Whole-ARRAY membership is
// lowered to array equality branches because segcore has no array-valued
// TermExpr executor. This is correctness normalization, not an optional
// optimization.
func normalizeTermExprs(expr *planpb.Expr) *planpb.Expr {
	if expr == nil {
		return nil
	}

	switch real := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		real.BinaryExpr.Left = normalizeTermExprs(real.BinaryExpr.GetLeft())
		real.BinaryExpr.Right = normalizeTermExprs(real.BinaryExpr.GetRight())
		return expr
	case *planpb.Expr_UnaryExpr:
		real.UnaryExpr.Child = normalizeTermExprs(real.UnaryExpr.GetChild())
		return expr
	case *planpb.Expr_BinaryArithExpr:
		real.BinaryArithExpr.Left = normalizeTermExprs(real.BinaryArithExpr.GetLeft())
		real.BinaryArithExpr.Right = normalizeTermExprs(real.BinaryArithExpr.GetRight())
		return expr
	case *planpb.Expr_CallExpr:
		for i, parameter := range real.CallExpr.GetFunctionParameters() {
			real.CallExpr.FunctionParameters[i] = normalizeTermExprs(parameter)
		}
		return expr
	case *planpb.Expr_RandomSampleExpr:
		real.RandomSampleExpr.Predicate = normalizeTermExprs(real.RandomSampleExpr.GetPredicate())
		return expr
	case *planpb.Expr_ElementFilterExpr:
		real.ElementFilterExpr.ElementExpr = normalizeTermExprs(real.ElementFilterExpr.GetElementExpr())
		real.ElementFilterExpr.Predicate = normalizeTermExprs(real.ElementFilterExpr.GetPredicate())
		return expr
	case *planpb.Expr_MatchExpr:
		real.MatchExpr.Predicate = normalizeTermExprs(real.MatchExpr.GetPredicate())
		return expr
	case *planpb.Expr_TermExpr:
		return normalizeTermExpr(expr, real.TermExpr)
	default:
		return expr
	}
}

func normalizeTermExpr(original *planpb.Expr, term *planpb.TermExpr) *planpb.Expr {
	if term == nil || term.GetColumnInfo() == nil || term.GetIsInField() || len(term.GetValues()) == 0 {
		return original
	}

	columnInfo := term.GetColumnInfo()
	if columnInfo.GetDataType() == schemapb.DataType_Array &&
		len(columnInfo.GetNestedPath()) == 0 && !columnInfo.GetIsElementLevel() {
		parts := make([]*planpb.Expr, 0, len(term.GetValues()))
		for _, value := range term.GetValues() {
			if valueCaseWithNil(value) != "array" {
				return original
			}
			parts = append(parts, newUnaryRangeExpr(
				columnInfo, planpb.OpType_Equal, value))
		}
		return foldBinary(planpb.BinaryExpr_LogicalOr, parts)
	}

	if columnInfo.GetDataType() != schemapb.DataType_JSON {
		return original
	}

	buckets := make(map[string][]*planpb.GenericValue)
	for _, value := range term.GetValues() {
		kind := jsonTermExecutorKind(value)
		buckets[kind] = append(buckets[kind], value)
	}

	// A homogeneous scalar JSON term is already executable. Array-valued JSON
	// membership is lowered to equality branches because TermExpr has no array
	// executor.
	if len(buckets) == 1 {
		if _, hasArrays := buckets["array"]; !hasArrays {
			return original
		}
	}

	parts := make([]*planpb.Expr, 0, len(buckets))
	for _, kind := range jsonTermExecutorKindOrder {
		values := buckets[kind]
		if len(values) == 0 {
			continue
		}
		if kind == "array" {
			for _, value := range values {
				parts = append(parts, newUnaryRangeExpr(
					term.GetColumnInfo(), planpb.OpType_Equal, value))
			}
			continue
		}
		if len(values) == 1 {
			parts = append(parts, newUnaryRangeExpr(
				term.GetColumnInfo(), planpb.OpType_Equal, values[0]))
		} else {
			parts = append(parts, newTermExpr(term.GetColumnInfo(), values))
		}
	}

	// Preserve an unexpected kind instead of dropping user values. The final
	// planner validation/segcore guard remains responsible for rejecting kinds
	// that cannot be executed.
	for kind, values := range buckets {
		known := false
		for _, orderedKind := range jsonTermExecutorKindOrder {
			if kind == orderedKind {
				known = true
				break
			}
		}
		if !known && len(values) > 0 {
			parts = append(parts, newTermExpr(term.GetColumnInfo(), values))
		}
	}

	if len(parts) == 0 {
		return original
	}
	return foldBinary(planpb.BinaryExpr_LogicalOr, parts)
}

func valueGroupKey(col *planpb.ColumnInfo, value *planpb.GenericValue) (string, bool) {
	if col == nil || !canApplyHomogeneousTermRewrite(value) {
		return "", false
	}
	kind := valueCase(value)
	key := columnKey(col)
	// Statically typed columns are cast before rewriting. JSON is the only
	// column type whose predicates must remain partitioned by literal kind.
	if col.GetDataType() == schemapb.DataType_JSON {
		key += "|" + kind
	}
	return key, true
}

func termGroupKey(term *planpb.TermExpr) (string, bool) {
	if term == nil || term.GetColumnInfo() == nil || !canApplyHomogeneousTermRewrite(term.GetValues()...) {
		return "", false
	}
	kind := valueCase(term.GetValues()[0])
	key := columnKey(term.GetColumnInfo())
	if term.GetColumnInfo().GetDataType() != schemapb.DataType_JSON {
		return key, true
	}
	return key + "|" + kind, true
}
