package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andNotEqualsToNotInRule struct{}

func (andNotEqualsToNotInRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return isUnaryRangePart(part, planpb.OpType_NotEqual)
	}) >= 2
}

func (andNotEqualsToNotInRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineAndNotEqualsToNotIn(parts))
}
