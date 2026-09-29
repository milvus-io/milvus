package rewriter

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func RewriteExpr(e *planpb.Expr) *planpb.Expr {
	optimizeEnabled := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
	return RewriteExprWithConfig(e, optimizeEnabled)
}

func RewriteExprWithConfig(e *planpb.Expr, optimizeEnabled bool) *planpb.Expr {
	if e == nil {
		return nil
	}
	e = normalizeTermExprs(e)
	e = normalizeEmptyArrayComparisons(e)
	v := &visitor{optimizeEnabled: optimizeEnabled}
	res := v.visitExpr(e)
	if out, ok := res.(*planpb.Expr); ok && out != nil {
		return out
	}
	return e
}

type visitor struct {
	optimizeEnabled bool
}

func (v *visitor) visitExpr(expr *planpb.Expr) interface{} {
	switch real := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		return v.visitBinaryExpr(real.BinaryExpr)
	case *planpb.Expr_UnaryExpr:
		return v.visitUnaryExpr(real.UnaryExpr)
	case *planpb.Expr_TermExpr:
		return v.visitTermExpr(real.TermExpr)
	case *planpb.Expr_ValueExpr:
		return v.visitValueExpr(real.ValueExpr, expr)
	// no optimization for other types
	default:
		return expr
	}
}

func (v *visitor) visitBinaryExpr(expr *planpb.BinaryExpr) interface{} {
	if !v.optimizeEnabled {
		left := v.visitExpr(expr.GetLeft()).(*planpb.Expr)
		right := v.visitExpr(expr.GetRight()).(*planpb.Expr)
		return &planpb.Expr{
			Expr: &planpb.Expr_BinaryExpr{
				BinaryExpr: &planpb.BinaryExpr{
					Left:  left,
					Right: right,
					Op:    expr.GetOp(),
				},
			},
		}
	}
	switch expr.GetOp() {
	case planpb.BinaryExpr_LogicalOr:
		// Do not recurse into the operand tree before flattening: a left-deep
		// chain a OR b OR ... OR z (as produced by ANTLR) has O(N) OR nodes,
		// each of which would re-flatten its whole subtree -> O(N^2) total.
		// Flatten the whole same-op chain once, then optimize each operand
		// independently (same-op descendants have already been collected).
		parts := flattenLogicalExpr(expr)
		for i, p := range parts {
			if res, ok := v.visitExpr(p).(*planpb.Expr); ok {
				parts[i] = res
			}
		}
		parts = combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAny)
		parts = v.combineOrEqualsToIn(parts)
		parts = v.combineOrTextMatchToMerged(parts)
		parts = v.combineOrRangePredicates(parts)
		parts = v.combineOrBinaryRanges(parts)
		parts = v.combineOrInWithIn(parts)
		parts = v.combineOrInWithEqual(parts)
		return foldBinary(planpb.BinaryExpr_LogicalOr, parts)
	case planpb.BinaryExpr_LogicalAnd:
		parts := flattenLogicalExpr(expr)
		for i, p := range parts {
			if res, ok := v.visitExpr(p).(*planpb.Expr); ok {
				parts[i] = res
			}
		}
		parts = combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAll)
		parts = v.combineAndRangePredicates(parts)
		parts = v.combineAndBinaryRanges(parts)
		parts = v.combineAndInWithIn(parts)
		parts = v.combineAndInWithRange(parts)
		parts = v.combineAndInWithEqual(parts)
		parts = v.combineAndNotEqualsToNotIn(parts)
		return foldBinary(planpb.BinaryExpr_LogicalAnd, parts)
	default:
		left := v.visitExpr(expr.GetLeft()).(*planpb.Expr)
		right := v.visitExpr(expr.GetRight()).(*planpb.Expr)
		return &planpb.Expr{
			Expr: &planpb.Expr_BinaryExpr{
				BinaryExpr: &planpb.BinaryExpr{
					Left:  left,
					Right: right,
					Op:    expr.GetOp(),
				},
			},
		}
	}
}

// BalanceLogicalExpr balances the same-op AND/OR chain rooted at expr, leaving
// its operands unchanged. It preserves source order and template flags without
// folding constants, so it is safe to use before deferred template validation.
func BalanceLogicalExpr(expr *planpb.Expr) *planpb.Expr {
	binary := expr.GetBinaryExpr()
	if binary == nil || (binary.GetOp() != planpb.BinaryExpr_LogicalAnd && binary.GetOp() != planpb.BinaryExpr_LogicalOr) {
		return expr
	}
	return buildBalancedBinary(binary.GetOp(), flattenLogicalExpr(binary))
}

// flattenLogicalExpr collects a same-op chain once, in source order, without
// recursing through a potentially left-deep parser tree.
func flattenLogicalExpr(expr *planpb.BinaryExpr) []*planpb.Expr {
	parts := make([]*planpb.Expr, 0, 4)
	stack := []*planpb.Expr{expr.GetRight(), expr.GetLeft()}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == nil {
			continue
		}
		be := cur.GetBinaryExpr()
		if be != nil && be.GetOp() == expr.GetOp() {
			stack = append(stack, be.GetRight(), be.GetLeft())
			continue
		}
		parts = append(parts, cur)
	}
	return parts
}

func (v *visitor) visitUnaryExpr(expr *planpb.UnaryExpr) interface{} {
	if !v.optimizeEnabled {
		child := v.visitExpr(expr.GetChild()).(*planpb.Expr)
		return &planpb.Expr{
			Expr: &planpb.Expr_UnaryExpr{
				UnaryExpr: &planpb.UnaryExpr{
					Op:    expr.GetOp(),
					Child: child,
				},
			},
		}
	}

	// Handle NOT(TermExpr) before visiting child.
	// Skip bool types here — they are handled via visitTermExpr bool optimization + NOT simplification.
	if expr.GetOp() == planpb.UnaryExpr_Not {
		if te := expr.GetChild().GetTermExpr(); te != nil {
			sortTermValues(te)
			col := te.GetColumnInfo()
			if v.optimizeEnabled && effectiveDataType(col) == schemapb.DataType_Bool {
				if !canFoldPredicateToBoolConstant(col) && boolValuesCoverDomain(te.GetValues()) {
					return notExpr(&planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: te}})
				}
				// Let other bool NOT IN flow through to visitTermExpr for bool-specific optimization.
			} else if col != nil && len(te.GetValues()) == 1 {
				if !canRewriteNotEqual(col, te.GetValues()[0]) {
					return notExpr(&planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: te}})
				}
				// Single-value NOT IN → != (avoids SIMD setup overhead for trivial case)
				return newUnaryRangeExpr(col, planpb.OpType_NotEqual, te.GetValues()[0])
			}
		}
	}

	child := v.visitExpr(expr.GetChild()).(*planpb.Expr)

	if expr.GetOp() == planpb.UnaryExpr_Not {
		// NOT (NOT AlwaysTrue) → AlwaysTrue
		if IsAlwaysFalseExpr(child) {
			return newAlwaysTrueExpr()
		}
		// NOT AlwaysTrueExpr → AlwaysFalseExpr
		// Handles: non-nullable bool NOT IN [true, false] → AlwaysFalse
		if IsAlwaysTrueExpr(child) {
			return newAlwaysFalseExpr()
		}
		// NOT (IS NOT NULL) → IS NULL
		if ne := child.GetNullExpr(); ne != nil {
			if ne.GetOp() == planpb.NullExpr_IsNotNull {
				return newNullExpr(ne.GetColumnInfo(), planpb.NullExpr_IsNull)
			}
			if ne.GetOp() == planpb.NullExpr_IsNull {
				return newNullExpr(ne.GetColumnInfo(), planpb.NullExpr_IsNotNull)
			}
		}
		// NOT (col == val) → col != val
		// Handles: bool NOT IN [true] → != true, bool NOT IN [false] → != false
		if ure := child.GetUnaryRangeExpr(); ure != nil && ure.GetOp() == planpb.OpType_Equal {
			if !canRewriteNotEqual(ure.GetColumnInfo(), ure.GetValue()) {
				return &planpb.Expr{
					Expr: &planpb.Expr_UnaryExpr{
						UnaryExpr: &planpb.UnaryExpr{
							Op:    expr.GetOp(),
							Child: child,
						},
					},
				}
			}
			return newUnaryRangeExpr(ure.GetColumnInfo(), planpb.OpType_NotEqual, ure.GetValue())
		}
	}

	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op:    expr.GetOp(),
				Child: child,
			},
		},
	}
}

func (v *visitor) visitTermExpr(expr *planpb.TermExpr) interface{} {
	sortTermValues(expr)
	if !v.optimizeEnabled {
		return &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: expr}}
	}

	// Optimize bool IN expressions:
	// - in [true, false] → AlwaysTrueExpr for non-nullable fields; nullable fields keep TermExpr
	// - in [true] → == true (uses fast SIMD path instead of slow scalar loop)
	// - in [false] → == false
	if v.optimizeEnabled && effectiveDataType(expr.GetColumnInfo()) == schemapb.DataType_Bool {
		values := expr.GetValues()
		if allBoolVals(values) {
			if boolValuesCoverDomain(values) {
				if !canFoldPredicateToBoolConstant(expr.GetColumnInfo()) {
					return &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: expr}}
				}
				return newAlwaysTrueExpr()
			}
			if len(values) == 1 {
				return newUnaryRangeExpr(expr.GetColumnInfo(), planpb.OpType_Equal, values[0])
			}
		}
	}

	// Single-value IN → == (avoids SIMD setup overhead for trivial case)
	if len(expr.GetValues()) == 1 {
		return newUnaryRangeExpr(expr.GetColumnInfo(), planpb.OpType_Equal, expr.GetValues()[0])
	}

	return &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: expr}}
}

func boolValuesCoverDomain(values []*planpb.GenericValue) bool {
	hasFalse, hasTrue := false, false
	for _, val := range values {
		if val.GetBoolVal() {
			hasTrue = true
		} else {
			hasFalse = true
		}
	}
	return hasTrue && hasFalse
}

// allBoolVals returns true if all values in the slice are BoolVal type.
func allBoolVals(values []*planpb.GenericValue) bool {
	if len(values) == 0 {
		return false
	}
	for _, v := range values {
		if _, ok := v.GetVal().(*planpb.GenericValue_BoolVal); !ok {
			return false
		}
	}
	return true
}

// visitValueExpr converts constant boolean ValueExpr to AlwaysTrueExpr/AlwaysFalseExpr.
// This handles cases like "1==1" which the parser constant-folds into ValueExpr(bool=true),
// normalizing them to the canonical AlwaysTrueExpr/AlwaysFalseExpr representation.
func (v *visitor) visitValueExpr(expr *planpb.ValueExpr, original *planpb.Expr) interface{} {
	if !v.optimizeEnabled {
		return original
	}
	val := expr.GetValue()
	if boolVal, ok := val.GetVal().(*planpb.GenericValue_BoolVal); ok {
		if boolVal.BoolVal {
			return newAlwaysTrueExpr()
		}
		return newAlwaysFalseExpr()
	}
	return original
}

func foldBinary(op planpb.BinaryExpr_BinaryOp, exprs []*planpb.Expr) *planpb.Expr {
	if len(exprs) == 0 {
		return nil
	}

	// Handle AlwaysTrue and AlwaysFalse optimizations (single-pass)
	switch op {
	case planpb.BinaryExpr_LogicalAnd:
		filtered := make([]*planpb.Expr, 0, len(exprs))
		for _, e := range exprs {
			if IsAlwaysFalseExpr(e) {
				// AND: any AlwaysFalse → entire expression is AlwaysFalse
				return newAlwaysFalseExpr()
			}
			if !IsAlwaysTrueExpr(e) {
				// Filter out AlwaysTrue (since AlwaysTrue AND X = X)
				filtered = append(filtered, e)
			}
		}
		exprs = filtered
		// If all were AlwaysTrue, return AlwaysTrue
		if len(exprs) == 0 {
			return newAlwaysTrueExpr()
		}
	case planpb.BinaryExpr_LogicalOr:
		filtered := make([]*planpb.Expr, 0, len(exprs))
		for _, e := range exprs {
			if IsAlwaysTrueExpr(e) {
				// OR: any AlwaysTrue → entire expression is AlwaysTrue
				return newAlwaysTrueExpr()
			}
			if !IsAlwaysFalseExpr(e) {
				// Filter out AlwaysFalse (since AlwaysFalse OR X = X)
				filtered = append(filtered, e)
			}
		}
		exprs = filtered
		// If all were AlwaysFalse, return AlwaysFalse
		if len(exprs) == 0 {
			return newAlwaysFalseExpr()
		}
	}

	return buildBalancedBinary(op, exprs)
}

// buildBalancedBinary preserves every operand and its template flag while
// keeping the tree height O(log N) for protobuf and recursive consumers.
func buildBalancedBinary(op planpb.BinaryExpr_BinaryOp, exprs []*planpb.Expr) *planpb.Expr {
	if len(exprs) == 0 {
		return nil
	}
	for len(exprs) > 1 {
		next := make([]*planpb.Expr, 0, (len(exprs)+1)/2)
		for i := 0; i < len(exprs); i += 2 {
			if i+1 < len(exprs) {
				next = append(next, &planpb.Expr{
					Expr: &planpb.Expr_BinaryExpr{
						BinaryExpr: &planpb.BinaryExpr{
							Left:  exprs[i],
							Right: exprs[i+1],
							Op:    op,
						},
					},
					IsTemplate: exprs[i].GetIsTemplate() || exprs[i+1].GetIsTemplate(),
				})
			} else {
				next = append(next, exprs[i])
			}
		}
		exprs = next
	}
	return exprs[0]
}
