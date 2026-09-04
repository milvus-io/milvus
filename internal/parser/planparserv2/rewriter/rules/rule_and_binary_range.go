package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andBinaryRangeRule struct{}

func (andBinaryRangeRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return part.GetBinaryRangeExpr() != nil || isRangePart(part)
	}) >= 2
}

func (andBinaryRangeRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineAndBinaryRanges(parts))
}
