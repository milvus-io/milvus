package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orEqualsToInRule struct{}

func (orEqualsToInRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return isUnaryRangePart(part, planpb.OpType_Equal)
	}) >= 2
}

func (orEqualsToInRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineOrEqualsToIn(parts))
}
