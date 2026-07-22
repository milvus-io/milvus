package rewriter

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

var jsonTermKindOrder = []string{"bool", "int64", "float", "string", "array"}

// normalizeJSONTermExprs enforces the execution invariant that every JSON
// TermExpr contains one concrete GenericValue kind.  This is correctness
// normalization, not an optional optimization: segcore selects the executor
// type from the first term value and therefore cannot safely consume a mixed
// list.
func normalizeJSONTermExprs(expr *planpb.Expr) *planpb.Expr {
	if expr == nil {
		return nil
	}

	switch real := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		real.BinaryExpr.Left = normalizeJSONTermExprs(real.BinaryExpr.GetLeft())
		real.BinaryExpr.Right = normalizeJSONTermExprs(real.BinaryExpr.GetRight())
		return expr
	case *planpb.Expr_UnaryExpr:
		real.UnaryExpr.Child = normalizeJSONTermExprs(real.UnaryExpr.GetChild())
		return expr
	case *planpb.Expr_BinaryArithExpr:
		real.BinaryArithExpr.Left = normalizeJSONTermExprs(real.BinaryArithExpr.GetLeft())
		real.BinaryArithExpr.Right = normalizeJSONTermExprs(real.BinaryArithExpr.GetRight())
		return expr
	case *planpb.Expr_CallExpr:
		for i, parameter := range real.CallExpr.GetFunctionParameters() {
			real.CallExpr.FunctionParameters[i] = normalizeJSONTermExprs(parameter)
		}
		return expr
	case *planpb.Expr_RandomSampleExpr:
		real.RandomSampleExpr.Predicate = normalizeJSONTermExprs(real.RandomSampleExpr.GetPredicate())
		return expr
	case *planpb.Expr_TermExpr:
		return normalizeJSONTermExpr(expr, real.TermExpr)
	default:
		return expr
	}
}

func normalizeJSONTermExpr(original *planpb.Expr, term *planpb.TermExpr) *planpb.Expr {
	if term == nil || term.GetColumnInfo() == nil || term.GetIsInField() ||
		term.GetColumnInfo().GetDataType() != schemapb.DataType_JSON || len(term.GetValues()) == 0 {
		return original
	}

	buckets := make(map[string][]*planpb.GenericValue)
	for _, value := range term.GetValues() {
		kind := valueCaseWithNil(value)
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
	for _, kind := range jsonTermKindOrder {
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
		for _, orderedKind := range jsonTermKindOrder {
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
	if col == nil || value == nil || value.GetVal() == nil {
		return "", false
	}
	kind := valueCase(value)
	if kind == "array" || kind == "other" {
		return "", false
	}
	key := columnKey(col)
	// Statically typed columns are cast before rewriting. JSON is the only
	// column type whose predicates must remain partitioned by literal kind.
	if col.GetDataType() == schemapb.DataType_JSON {
		key += "|" + kind
	}
	return key, true
}

func termGroupKey(term *planpb.TermExpr) (string, bool) {
	if term == nil || term.GetColumnInfo() == nil || len(term.GetValues()) == 0 {
		return "", false
	}
	kind := valueCaseWithNil(term.GetValues()[0])
	if kind == "nil" || kind == "other" || kind == "array" {
		return "", false
	}
	key := columnKey(term.GetColumnInfo())
	if term.GetColumnInfo().GetDataType() != schemapb.DataType_JSON {
		return key, true
	}
	// this for loop is for defensive. in practice after normalization, all values
	// in a term should have the same kind. but we still check it here to be safe.
	for _, value := range term.GetValues()[1:] {
		if valueCaseWithNil(value) != kind {
			return "", false
		}
	}
	return key + "|" + kind, true
}
