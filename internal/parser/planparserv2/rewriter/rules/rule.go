package rules

import "github.com/milvus-io/milvus/pkg/v3/proto/planpb"

// Rule rewrites one expression node. Match performs a cheap eligibility check.
// Apply performs the transformation and reports whether it changed the node;
// a changed result must include a non-nil replacement expression.
type Rule interface {
	Match(expr *planpb.Expr) bool
	Apply(expr *planpb.Expr) (*planpb.Expr, bool)
}

type logicalPartsRule interface {
	match(parts []*planpb.Expr) bool
	apply(parts []*planpb.Expr) ([]*planpb.Expr, bool)
}

func countMatchingParts(parts []*planpb.Expr, match func(*planpb.Expr) bool) int {
	count := 0
	for _, part := range parts {
		if match(part) {
			count++
		}
	}
	return count
}

func hasMatchingPart(parts []*planpb.Expr, match func(*planpb.Expr) bool) bool {
	return countMatchingParts(parts, match) > 0
}

func isUnaryRangePart(part *planpb.Expr, op planpb.OpType) bool {
	unaryRange := part.GetUnaryRangeExpr()
	return unaryRange != nil && unaryRange.GetOp() == op &&
		unaryRange.GetColumnInfo() != nil && unaryRange.GetValue() != nil
}

func isRangePart(part *planpb.Expr) bool {
	unaryRange := part.GetUnaryRangeExpr()
	if unaryRange == nil || unaryRange.GetColumnInfo() == nil || unaryRange.GetValue() == nil {
		return false
	}
	switch unaryRange.GetOp() {
	case planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual,
		planpb.OpType_LessThan, planpb.OpType_LessEqual:
		return true
	default:
		return false
	}
}

func isMembershipPart(part *planpb.Expr) bool {
	return part.GetTermExpr() != nil || isUnaryRangePart(part, planpb.OpType_Equal)
}

// logicalReductionResult centralizes the changed contract for legacy logical
// combiners whose real matches consume at least two operands and emit fewer
// operands. Returning the original slice when cardinality is unchanged prevents
// equivalent map-based regrouping from being treated as rewrite progress.
//
// TODO: Preserve first-occurrence order when emitting independent groups in
// term_in.go, range.go, and text_match.go. For now map iteration may still
// reorder operands when a rule performs a real reduction.
func logicalReductionResult(parts, out []*planpb.Expr) ([]*planpb.Expr, bool) {
	if len(out) >= len(parts) {
		return parts, false
	}
	return out, true
}
