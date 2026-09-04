package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type simplifyUnaryRule struct{}

func (simplifyUnaryRule) Match(expr *planpb.Expr) bool {
	unary := expr.GetUnaryExpr()
	if unary == nil || unary.GetOp() != planpb.UnaryExpr_Not || IsAlwaysFalseExpr(expr) {
		return false
	}

	child := unary.GetChild()
	if IsAlwaysFalseExpr(child) || IsAlwaysTrueExpr(child) {
		return true
	}
	if nullExpr := child.GetNullExpr(); nullExpr != nil {
		return nullExpr.GetOp() == planpb.NullExpr_IsNotNull || nullExpr.GetOp() == planpb.NullExpr_IsNull
	}
	unaryRange := child.GetUnaryRangeExpr()
	return unaryRange != nil && unaryRange.GetOp() == planpb.OpType_Equal &&
		canRewriteNotEqual(unaryRange.GetColumnInfo(), unaryRange.GetValue())
}

func (simplifyUnaryRule) Apply(expr *planpb.Expr) (*planpb.Expr, bool) {
	unary := expr.GetUnaryExpr()

	child := unary.GetChild()
	if IsAlwaysFalseExpr(child) {
		return newAlwaysTrueExpr(), true
	}
	if IsAlwaysTrueExpr(child) {
		return newAlwaysFalseExpr(), true
	}
	if nullExpr := child.GetNullExpr(); nullExpr != nil {
		switch nullExpr.GetOp() {
		case planpb.NullExpr_IsNotNull:
			return newNullExpr(nullExpr.GetColumnInfo(), planpb.NullExpr_IsNull), true
		case planpb.NullExpr_IsNull:
			return newNullExpr(nullExpr.GetColumnInfo(), planpb.NullExpr_IsNotNull), true
		}
	}
	if unaryRange := child.GetUnaryRangeExpr(); unaryRange != nil && unaryRange.GetOp() == planpb.OpType_Equal {
		return newUnaryRangeExpr(unaryRange.GetColumnInfo(), planpb.OpType_NotEqual, unaryRange.GetValue()), true
	}
	return expr, false
}
