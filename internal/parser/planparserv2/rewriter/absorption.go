package rewriter

import (
	"reflect"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// combineOrAbsorption implements the absorption law for OR:
// P OR (P AND Q) → P
// If any OR-level part is structurally identical to any AND-child of another
// OR-level part, the compound part (P AND Q) is absorbed.
func (v *visitor) combineOrAbsorption(parts []*planpb.Expr) []*planpb.Expr {
	if len(parts) < 2 {
		return parts
	}

	absorbed := make([]bool, len(parts))

	for i := 0; i < len(parts); i++ {
		if absorbed[i] {
			continue
		}
		for j := 0; j < len(parts); j++ {
			if i == j || absorbed[j] {
				continue
			}
			// Check if parts[i] is a sub-expression of parts[j]'s AND tree.
			// If parts[j] = (A AND B AND ...) and parts[i] equals any child,
			// then parts[j] is absorbed by parts[i].
			if containsInAnd(parts[j], parts[i]) {
				absorbed[j] = true
			}
		}
	}

	out := make([]*planpb.Expr, 0, len(parts))
	for i, e := range parts {
		if !absorbed[i] {
			out = append(out, e)
		}
	}
	if len(out) == len(parts) {
		return parts
	}
	return out
}

// combineAndAbsorption implements the absorption law for AND:
// P AND (P OR Q) → P
// If any AND-level part is structurally identical to any OR-child of another
// AND-level part, the compound part (P OR Q) is absorbed.
func (v *visitor) combineAndAbsorption(parts []*planpb.Expr) []*planpb.Expr {
	if len(parts) < 2 {
		return parts
	}

	absorbed := make([]bool, len(parts))

	for i := 0; i < len(parts); i++ {
		if absorbed[i] {
			continue
		}
		for j := 0; j < len(parts); j++ {
			if i == j || absorbed[j] {
				continue
			}
			// Check if parts[i] is a sub-expression of parts[j]'s OR tree.
			if containsInOr(parts[j], parts[i]) {
				absorbed[j] = true
			}
		}
	}

	out := make([]*planpb.Expr, 0, len(parts))
	for i, e := range parts {
		if !absorbed[i] {
			out = append(out, e)
		}
	}
	if len(out) == len(parts) {
		return parts
	}
	return out
}

// containsInAnd returns true if expr is an AND tree and needle matches
// one of its leaf children structurally.
func containsInAnd(expr, needle *planpb.Expr) bool {
	be := expr.GetBinaryExpr()
	if be == nil || be.GetOp() != planpb.BinaryExpr_LogicalAnd {
		return false
	}
	// Collect AND children
	children := make([]*planpb.Expr, 0, 4)
	collectAnd(expr, &children)
	for _, child := range children {
		if exprStructEqual(child, needle) {
			return true
		}
	}
	return false
}

// containsInOr returns true if expr is an OR tree and needle matches
// one of its leaf children structurally.
func containsInOr(expr, needle *planpb.Expr) bool {
	be := expr.GetBinaryExpr()
	if be == nil || be.GetOp() != planpb.BinaryExpr_LogicalOr {
		return false
	}
	children := make([]*planpb.Expr, 0, 4)
	collectOr(expr, &children)
	for _, child := range children {
		if exprStructEqual(child, needle) {
			return true
		}
	}
	return false
}

// exprStructEqual compares two expressions for structural equality.
// Uses protobuf reflection for a deep comparison.
func exprStructEqual(a, b *planpb.Expr) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Fast path: check type first
	aType := reflect.TypeOf(a.GetExpr())
	bType := reflect.TypeOf(b.GetExpr())
	if aType != bType {
		return false
	}

	switch aExpr := a.GetExpr().(type) {
	case *planpb.Expr_UnaryRangeExpr:
		bExpr := b.GetUnaryRangeExpr()
		if bExpr == nil {
			return false
		}
		aa, bb := aExpr.UnaryRangeExpr, bExpr
		return aa.GetOp() == bb.GetOp() &&
			columnKey(aa.GetColumnInfo()) == columnKey(bb.GetColumnInfo()) &&
			equalsGenericSafe(aa.GetValue(), bb.GetValue())

	case *planpb.Expr_BinaryRangeExpr:
		bExpr := b.GetBinaryRangeExpr()
		if bExpr == nil {
			return false
		}
		aa, bb := aExpr.BinaryRangeExpr, bExpr
		return columnKey(aa.GetColumnInfo()) == columnKey(bb.GetColumnInfo()) &&
			aa.GetLowerInclusive() == bb.GetLowerInclusive() &&
			aa.GetUpperInclusive() == bb.GetUpperInclusive() &&
			equalsGenericSafe(aa.GetLowerValue(), bb.GetLowerValue()) &&
			equalsGenericSafe(aa.GetUpperValue(), bb.GetUpperValue())

	case *planpb.Expr_TermExpr:
		bExpr := b.GetTermExpr()
		if bExpr == nil {
			return false
		}
		aa, bb := aExpr.TermExpr, bExpr
		if columnKey(aa.GetColumnInfo()) != columnKey(bb.GetColumnInfo()) {
			return false
		}
		aVals, bVals := aa.GetValues(), bb.GetValues()
		if len(aVals) != len(bVals) {
			return false
		}
		for i := range aVals {
			if !equalsGeneric(aVals[i], bVals[i]) {
				return false
			}
		}
		return true

	case *planpb.Expr_BinaryExpr:
		bExpr := b.GetBinaryExpr()
		if bExpr == nil {
			return false
		}
		return aExpr.BinaryExpr.GetOp() == bExpr.GetOp() &&
			exprStructEqual(aExpr.BinaryExpr.GetLeft(), bExpr.GetLeft()) &&
			exprStructEqual(aExpr.BinaryExpr.GetRight(), bExpr.GetRight())

	case *planpb.Expr_UnaryExpr:
		bExpr := b.GetUnaryExpr()
		if bExpr == nil {
			return false
		}
		return aExpr.UnaryExpr.GetOp() == bExpr.GetOp() &&
			exprStructEqual(aExpr.UnaryExpr.GetChild(), bExpr.GetChild())

	case *planpb.Expr_AlwaysTrueExpr:
		return b.GetAlwaysTrueExpr() != nil

	default:
		return false
	}
}

// equalsGenericSafe handles nil values.
func equalsGenericSafe(a, b *planpb.GenericValue) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return equalsGeneric(a, b)
}
