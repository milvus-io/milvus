package rewriter

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// RewriteExpr is the public entry to apply expression rewrite rules on a plan expression tree.
func RewriteExpr(e *planpb.Expr) *planpb.Expr {
	if e == nil {
		return nil
	}
	v := &visitor{}
	res := v.visitExpr(e)
	if out, ok := res.(*planpb.Expr); ok && out != nil {
		return out
	}
	return e
}

type visitor struct{}

func (v *visitor) visitExpr(expr *planpb.Expr) interface{} {
	switch real := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		return v.visitBinaryExpr(real.BinaryExpr)
	case *planpb.Expr_UnaryExpr:
		return v.visitUnaryExpr(real.UnaryExpr)
	case *planpb.Expr_TermExpr:
		return v.visitTermExpr(real.TermExpr)
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
		parts = v.combineOrEqualsToIn(parts)
		parts = v.combineOrTextMatchToMerged(parts)
		parts = v.combineOrRangePredicates(parts)
		parts = v.combineOrInWithNotEqual(parts)
		parts = v.combineOrInWithIn(parts)
		parts = v.combineOrInWithEqual(parts)
		return foldBinary(planpb.BinaryExpr_LogicalOr, parts)
	case planpb.BinaryExpr_LogicalAnd:
		parts := flattenAnd(left, right)
		parts = v.combineAndRangePredicates(parts)
		parts = v.combineAndInWithIn(parts)
		parts = v.combineAndInWithNotEqual(parts)
		parts = v.combineAndInWithRange(parts)
		parts = v.combineAndInWithEqual(parts)
		parts = v.combineAndNotEqualsToNotIn(parts)
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


