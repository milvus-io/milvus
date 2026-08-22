package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orBinaryRangeRule struct{}

func (orBinaryRangeRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		return part.GetBinaryRangeExpr() != nil || isRangePart(part)
	}) >= 2
}

func (orBinaryRangeRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineOrBinaryRanges(parts))
}
