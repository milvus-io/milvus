package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type andInEqualRule struct{}

func (andInEqualRule) match(parts []*planpb.Expr) bool {
	return hasMatchingPart(parts, func(part *planpb.Expr) bool {
		return part.GetTermExpr() != nil
	}) && hasMatchingPart(parts, func(part *planpb.Expr) bool {
		return isUnaryRangePart(part, planpb.OpType_Equal)
	})
}

func (andInEqualRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineAndInWithEqual(parts))
}
