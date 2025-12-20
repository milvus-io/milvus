package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// collectTextMatchLiterals walks the provided expr and collects all OpType_TextMatch literals (grouped by fieldId) into the returned map.
func collectTextMatchLiterals(expr *planpb.Expr) map[int64]string {
	colToLiteral := map[int64]string{}
	var collect func(e *planpb.Expr)
	collect = func(e *planpb.Expr) {
		if e == nil {
			return
		}
		if ue := e.GetUnaryRangeExpr(); ue != nil && ue.GetOp() == planpb.OpType_TextMatch {
			col := ue.GetColumnInfo()
			colToLiteral[col.GetFieldId()] = ue.GetValue().GetStringVal()
			return
		}
		if be := e.GetBinaryExpr(); be != nil && be.GetOp() == planpb.BinaryExpr_LogicalOr {
			collect(be.GetLeft())
			collect(be.GetRight())
			return
		}
	}
	collect(expr)
	return colToLiteral
}

func TestRewrite_TextMatch_OR_Merge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `text_match(VarCharField, "A C") or text_match(VarCharField, "B D")`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "should merge to single text_match")
	require.Equal(t, planpb.OpType_TextMatch, ure.GetOp())
	require.Equal(t, "A C B D", ure.GetValue().GetStringVal())
}

func TestRewrite_TextMatch_OR_DifferentField_NoMerge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `text_match(VarCharField, "A") or text_match(StringField, "B")`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
	require.NotNil(t, be.GetLeft().GetUnaryRangeExpr())
	require.NotNil(t, be.GetRight().GetUnaryRangeExpr())
}

func TestRewrite_TextMatch_OR_MultiFields_Merge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `text_match(VarCharField, "A") or text_match(StringField, "A") or text_match(VarCharField, "B")`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// should be text_match(VarCharField, "A B") or text_match(StringField, "A")
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())

	// collect all text_match literals grouped by column id
	colToLiteral := collectTextMatchLiterals(expr)
	require.Equal(t, 2, len(colToLiteral))
	// one of them must be "A B", and the other is "A"
	hasAB := false
	hasA := false
	for _, lit := range colToLiteral {
		if lit == "A B" {
			hasAB = true
		}
		if lit == "A" {
			hasA = true
		}
	}
	require.True(t, hasAB)
	require.True(t, hasA)
}

func TestRewrite_TextMatch_OR_MoreMultiFields_Merge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `text_match(VarCharField, "A") or (text_match(StringField, "C") or text_match(VarCharField, "B")) or text_match(StringField, "D")`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// should be text_match(VarCharField, "A B") or text_match(StringField, "C D")
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())

	// collect all text_match literals grouped by column id
	colToLiteral := collectTextMatchLiterals(expr)
	require.Equal(t, 2, len(colToLiteral))
	// one of them must be "A B", and the other is "C D"
	hasAB := false
	hasCD := false
	for _, lit := range colToLiteral {
		if lit == "A B" {
			hasAB = true
		}
		if lit == "C D" {
			hasCD = true
		}
	}
	require.True(t, hasAB)
	require.True(t, hasCD)
}

func TestRewrite_TextMatch_OR_WithOption_NoMerge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `text_match(VarCharField, "A", minimum_should_match=1) or text_match(VarCharField, "B")`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}
