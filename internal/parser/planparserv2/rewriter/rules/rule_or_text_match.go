package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

type orTextMatchRule struct{}

func (orTextMatchRule) match(parts []*planpb.Expr) bool {
	return countMatchingParts(parts, func(part *planpb.Expr) bool {
		unaryRange := part.GetUnaryRangeExpr()
		return unaryRange != nil && unaryRange.GetOp() == planpb.OpType_TextMatch &&
			unaryRange.GetColumnInfo() != nil && unaryRange.GetValue() != nil &&
			len(unaryRange.GetExtraValues()) == 0
	}) >= 2
}

func (orTextMatchRule) apply(parts []*planpb.Expr) ([]*planpb.Expr, bool) {
	return logicalReductionResult(parts, combineOrTextMatchToMerged(parts))
}
