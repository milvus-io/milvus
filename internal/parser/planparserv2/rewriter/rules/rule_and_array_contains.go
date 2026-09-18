package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andArrayContainsRule struct{}

func (andArrayContainsRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return canCombineArrayContains(part.GetJsonContainsExpr(), planpb.JSONContainsExpr_ContainsAll)
	}) >= 2
}

func (andArrayContainsRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	out := combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAll)
	return logicalReductionResult(parts, out)
}
