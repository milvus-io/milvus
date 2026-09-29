package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type canonicalizeValueRule struct{}

func (canonicalizeValueRule) Match(expr *planpb.Expr) bool {
	valueExpr := expr.GetValueExpr()
	if valueExpr == nil {
		return false
	}
	_, ok := valueExpr.GetValue().GetVal().(*planpb.GenericValue_BoolVal)
	return ok
}

func (canonicalizeValueRule) Apply(expr *planpb.Expr) (*planpb.Expr, bool) {
	valueExpr := expr.GetValueExpr()
	boolValue := valueExpr.GetValue().GetVal().(*planpb.GenericValue_BoolVal)
	if boolValue.BoolVal {
		return newAlwaysTrueExpr(), true
	}
	return newAlwaysFalseExpr(), true
}
