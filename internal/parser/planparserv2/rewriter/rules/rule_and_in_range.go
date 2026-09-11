package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andInRangeRule struct{}

func (andInRangeRule) match(parts []*planpb.Expr) bool {
	return hasMatchingPart(parts, func(part *planpb.Expr) bool {
		return part.GetTermExpr() != nil
	}) && hasMatchingPart(parts, isRangePart)
}

func (andInRangeRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineAndInWithRange(parts))
}
