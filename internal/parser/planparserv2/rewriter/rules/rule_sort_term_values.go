package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type sortTermValuesRule struct{}

func (sortTermValuesRule) Match(expr *planpb.Expr) bool {
	term := expr.GetTermExpr()
	return term != nil && len(term.GetValues()) > 1
}

func (sortTermValuesRule) Apply(expr *planpb.Expr) (*planpb.Expr, bool) {
	return expr, sortTermValues(expr.GetTermExpr())
}
