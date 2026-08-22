package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andRangeRule struct{}

func (andRangeRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, isRangePart) >= 2
}

func (andRangeRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineAndRangePredicates(parts))
}
