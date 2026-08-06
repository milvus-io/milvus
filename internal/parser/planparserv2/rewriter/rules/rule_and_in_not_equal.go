package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andInNotEqualRule struct{}

func (andInNotEqualRule) match(parts []*planpb.Expr) bool {
	return hasMatchingPart(parts, isMembershipPart) &&
		hasMatchingPart(parts, func(part *planpb.Expr) bool {
			return isUnaryRangePart(part, planpb.OpType_NotEqual)
		})
}

func (andInNotEqualRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineAndInWithNotEqual(parts))
}
