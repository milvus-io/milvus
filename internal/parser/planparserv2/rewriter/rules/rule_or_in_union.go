package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orInUnionRule struct{}

func (orInUnionRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return part.GetTermExpr() != nil
	}) >= 2
}

func (orInUnionRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineOrInWithIn(parts))
}
