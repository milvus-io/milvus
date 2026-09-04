package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orArrayContainsRule struct{}

func (orArrayContainsRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return canCombineArrayContains(part.GetJsonContainsExpr(), planpb.JSONContainsExpr_ContainsAny)
	}) >= 2
}

func (orArrayContainsRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	out := combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAny)
	return logicalReductionResult(parts, out)
}
