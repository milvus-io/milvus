package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

func flattenOr(a, b *planpb.Expr) []*planpb.Expr {
	out := make([]*planpb.Expr, 0, 4)
	collectOr(a, &out)
	collectOr(b, &out)
	return out
}

func collectOr(expr *planpb.Expr, out *[]*planpb.Expr) {
	if binary := expr.GetBinaryExpr(); binary != nil && binary.GetOp() == planpb.BinaryExpr_LogicalOr {
		collectOr(binary.GetLeft(), out)
		collectOr(binary.GetRight(), out)
		return
	}
	*out = append(*out, expr)
}

func flattenAnd(a, b *planpb.Expr) []*planpb.Expr {
	out := make([]*planpb.Expr, 0, 4)
	collectAnd(a, &out)
	collectAnd(b, &out)
	return out
}

func collectAnd(expr *planpb.Expr, out *[]*planpb.Expr) {
	if binary := expr.GetBinaryExpr(); binary != nil && binary.GetOp() == planpb.BinaryExpr_LogicalAnd {
		collectAnd(binary.GetLeft(), out)
		collectAnd(binary.GetRight(), out)
		return
	}
	*out = append(*out, expr)
}

func foldBinary(op planpb.BinaryExpr_BinaryOp, expressions []*planpb.Expr) *planpb.Expr {
	if len(expressions) == 0 {
		return nil
	}

	switch op {
	case planpb.BinaryExpr_LogicalAnd:
		filtered := make([]*planpb.Expr, 0, len(expressions))
		for _, expression := range expressions {
			if IsAlwaysFalseExpr(expression) {
				return newAlwaysFalseExpr()
			}
			if !IsAlwaysTrueExpr(expression) {
				filtered = append(filtered, expression)
			}
		}
		expressions = filtered
		if len(expressions) == 0 {
			return newAlwaysTrueExpr()
		}
	case planpb.BinaryExpr_LogicalOr:
		filtered := make([]*planpb.Expr, 0, len(expressions))
		for _, expression := range expressions {
			if IsAlwaysTrueExpr(expression) {
				return newAlwaysTrueExpr()
			}
			if !IsAlwaysFalseExpr(expression) {
				filtered = append(filtered, expression)
			}
		}
		expressions = filtered
		if len(expressions) == 0 {
			return newAlwaysFalseExpr()
		}
	}

	if len(expressions) == 1 {
		return expressions[0]
	}
	current := expressions[0]
	for _, expression := range expressions[1:] {
		current = &planpb.Expr{
			Expr: &planpb.Expr_BinaryExpr{
				BinaryExpr: &planpb.BinaryExpr{
					Left:  current,
					Right: expression,
					Op:    op,
				},
			},
		}
	}
	return current
}
