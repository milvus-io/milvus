package rules

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type simplifyLogicalRule struct{}

func (simplifyLogicalRule) Match(expr *planpb.Expr) bool {
	binary := expr.GetBinaryExpr()
	return binary != nil && (binary.GetOp() == planpb.BinaryExpr_LogicalOr ||
		binary.GetOp() == planpb.BinaryExpr_LogicalAnd)
}

func (simplifyLogicalRule) Apply(expr *planpb.Expr) (*planpb.Expr, bool) {
	binary := expr.GetBinaryExpr()

	var (
		parts []*planpb.Expr
		rules []logicalPartsRule
	)
	switch binary.GetOp() {
	case planpb.BinaryExpr_LogicalOr:
		parts = flattenOr(binary.GetLeft(), binary.GetRight())
		rules = orLogicalRules
	case planpb.BinaryExpr_LogicalAnd:
		parts = flattenAnd(binary.GetLeft(), binary.GetRight())
		rules = andLogicalRules
	}

	partsChanged := false
	for _, rule := range rules {
		if !rule.match(parts) {
			continue
		}
		next, changed := rule.apply(parts)
		if changed {
			parts = next
			partsChanged = true
		}
	}

	folded := foldBinary(binary.GetOp(), parts)
	if partsChanged || !proto.Equal(expr, folded) {
		return folded, true
	}
	return expr, false
}
