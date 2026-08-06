package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orRangeRule struct{}

func (orRangeRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, isRangePart) >= 2
}

func (orRangeRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineOrRangePredicates(parts))
}
