package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orInNotEqualRule struct{}

func (orInNotEqualRule) match(parts []*planpb.Expr) bool {
	return hasMatchingPart(parts, isMembershipPart) &&
		hasMatchingPart(parts, func(part *planpb.Expr) bool {
			return isUnaryRangePart(part, planpb.OpType_NotEqual)
		})
}

func (orInNotEqualRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineOrInWithNotEqual(parts))
}
