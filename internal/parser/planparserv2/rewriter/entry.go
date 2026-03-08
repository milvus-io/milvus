package rewriter

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func RewriteExpr(e *planpb.Expr) *planpb.Expr {
	optimizeEnabled := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
	return RewriteExprWithConfig(e, optimizeEnabled)
}

func RewriteExprWithConfig(e *planpb.Expr, optimizeEnabled bool) *planpb.Expr {
	if e == nil {
		return nil
	}
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
	left := v.visitExpr(expr.GetLeft()).(*planpb.Expr)
	right := v.visitExpr(expr.GetRight()).(*planpb.Expr)
	switch expr.GetOp() {
	case planpb.BinaryExpr_LogicalOr:
		parts := flattenOr(left, right)
		if v.optimizeEnabled {
			parts = v.combineOrEqualsToIn(parts)
			parts = v.combineOrTextMatchToMerged(parts)
			parts = v.combineOrRangePredicates(parts)
			parts = v.combineOrBinaryRanges(parts)
			parts = v.combineOrInWithNotEqual(parts)
			parts = v.combineOrInWithIn(parts)
			parts = v.combineOrInWithEqual(parts)
		}
		return foldBinary(planpb.BinaryExpr_LogicalOr, parts)
	case planpb.BinaryExpr_LogicalAnd:
		parts := flattenAnd(left, right)
		if v.optimizeEnabled {
			parts = v.combineAndRangePredicates(parts)
			parts = v.combineAndBinaryRanges(parts)
			parts = v.combineAndInWithIn(parts)
			parts = v.combineAndInWithNotEqual(parts)
			parts = v.combineAndInWithRange(parts)
			parts = v.combineAndInWithEqual(parts)
			parts = v.combineAndNotEqualsToNotIn(parts)
		}
		return foldBinary(planpb.BinaryExpr_LogicalAnd, parts)
	default:
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

func (v *visitor) visitUnaryExpr(expr *planpb.UnaryExpr) interface{} {
	child := v.visitExpr(expr.GetChild()).(*planpb.Expr)

	// Optimize double negation: NOT (NOT AlwaysTrue) → AlwaysTrue
	if expr.GetOp() == planpb.UnaryExpr_Not {
		if IsAlwaysFalseExpr(child) {
			return newAlwaysTrueExpr()
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
	return &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: expr}}
}

// visitValueExpr converts constant boolean ValueExpr to AlwaysTrueExpr/AlwaysFalseExpr.
// This handles cases like "1==1" which the parser constant-folds into ValueExpr(bool=true),
// normalizing them to the canonical AlwaysTrueExpr/AlwaysFalseExpr representation.
func (v *visitor) visitValueExpr(expr *planpb.ValueExpr, original *planpb.Expr) interface{} {
	val := expr.GetValue()
	if boolVal, ok := val.GetVal().(*planpb.GenericValue_BoolVal); ok {
		if boolVal.BoolVal {
			return newAlwaysTrueExpr()
		}
		return newAlwaysFalseExpr()
	}
	return original
}

func flattenOr(a, b *planpb.Expr) []*planpb.Expr {
	out := make([]*planpb.Expr, 0, 4)
	collectOr(a, &out)
	collectOr(b, &out)
	return out
}

func collectOr(e *planpb.Expr, out *[]*planpb.Expr) {
	if be := e.GetBinaryExpr(); be != nil && be.GetOp() == planpb.BinaryExpr_LogicalOr {
		collectOr(be.GetLeft(), out)
		collectOr(be.GetRight(), out)
		return
	}
	*out = append(*out, e)
}

func flattenAnd(a, b *planpb.Expr) []*planpb.Expr {
	out := make([]*planpb.Expr, 0, 4)
	collectAnd(a, &out)
	collectAnd(b, &out)
	return out
}

func collectAnd(e *planpb.Expr, out *[]*planpb.Expr) {
	if be := e.GetBinaryExpr(); be != nil && be.GetOp() == planpb.BinaryExpr_LogicalAnd {
		collectAnd(be.GetLeft(), out)
		collectAnd(be.GetRight(), out)
		return
	}
	*out = append(*out, e)
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

	if len(exprs) == 1 {
		return exprs[0]
	}
	cur := exprs[0]
	for i := 1; i < len(exprs); i++ {
		cur = &planpb.Expr{
			Expr: &planpb.Expr_BinaryExpr{
				BinaryExpr: &planpb.BinaryExpr{
					Left:  cur,
					Right: exprs[i],
					Op:    op,
				},
			},
		}
	}
	return cur
}
