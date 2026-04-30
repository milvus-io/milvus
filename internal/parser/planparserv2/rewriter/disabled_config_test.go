package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestRewrite_Disabled_KeepsIntInAsTermExpr(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{
					FieldId:  101,
					DataType: schemapb.DataType_Int64,
				},
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_Int64Val{Int64Val: 3}},
					{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
					{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	term := result.GetTermExpr()
	require.NotNil(t, term, "optimize=false should keep IN as TermExpr")
	require.Len(t, term.GetValues(), 2)
	require.Equal(t, int64(1), term.GetValues()[0].GetInt64Val())
	require.Equal(t, int64(3), term.GetValues()[1].GetInt64Val())
	require.Nil(t, result.GetUnaryRangeExpr(), "optimize=false should not rewrite IN to ==")
}

func TestRewrite_Disabled_KeepsSingleValueIntInAsTermExpr(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{
					FieldId:  101,
					DataType: schemapb.DataType_Int64,
				},
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	term := result.GetTermExpr()
	require.NotNil(t, term, "optimize=false should keep single-value IN as TermExpr")
	require.Len(t, term.GetValues(), 1)
	require.Equal(t, int64(5), term.GetValues()[0].GetInt64Val())
	require.Nil(t, result.GetUnaryRangeExpr(), "optimize=false should not rewrite single-value IN to ==")
}

func TestRewrite_Disabled_KeepsIntNotInAsUnaryTermExpr(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op: planpb.UnaryExpr_Not,
				Child: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:  101,
								DataType: schemapb.DataType_Int64,
							},
							Values: []*planpb.GenericValue{
								{Val: &planpb.GenericValue_Int64Val{Int64Val: 3}},
								{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
								{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
							},
						},
					},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	unary := result.GetUnaryExpr()
	require.NotNil(t, unary, "optimize=false should keep NOT IN as UnaryExpr")
	require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
	term := unary.GetChild().GetTermExpr()
	require.NotNil(t, term, "optimize=false should keep NOT IN child as TermExpr")
	require.Len(t, term.GetValues(), 2)
	require.Equal(t, int64(1), term.GetValues()[0].GetInt64Val())
	require.Equal(t, int64(3), term.GetValues()[1].GetInt64Val())
	require.Nil(t, result.GetUnaryRangeExpr(), "optimize=false should not rewrite NOT IN to !=")
}

func TestRewrite_Disabled_KeepsSingleValueIntNotInAsUnaryTermExpr(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op: planpb.UnaryExpr_Not,
				Child: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:  101,
								DataType: schemapb.DataType_Int64,
							},
							Values: []*planpb.GenericValue{
								{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
							},
						},
					},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	unary := result.GetUnaryExpr()
	require.NotNil(t, unary, "optimize=false should keep single-value NOT IN as UnaryExpr")
	require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
	term := unary.GetChild().GetTermExpr()
	require.NotNil(t, term, "optimize=false should keep single-value NOT IN child as TermExpr")
	require.Len(t, term.GetValues(), 1)
	require.Equal(t, int64(5), term.GetValues()[0].GetInt64Val())
	require.Nil(t, result.GetUnaryRangeExpr(), "optimize=false should not rewrite single-value NOT IN to !=")
}

func TestRewrite_Disabled_KeepsBoolInAsTermExpr(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{
					FieldId:  105,
					DataType: schemapb.DataType_Bool,
				},
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
					{Val: &planpb.GenericValue_BoolVal{BoolVal: false}},
					{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	term := result.GetTermExpr()
	require.NotNil(t, term, "optimize=false should keep bool IN as TermExpr")
	require.Len(t, term.GetValues(), 2)
	require.False(t, term.GetValues()[0].GetBoolVal())
	require.True(t, term.GetValues()[1].GetBoolVal())
	require.False(t, rewriter.IsAlwaysTrueExpr(result), "optimize=false should not fold bool IN to AlwaysTrueExpr")
}

func TestRewrite_Disabled_KeepsValueExprInsideLogicalBinary(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op: planpb.BinaryExpr_LogicalAnd,
				Left: &planpb.Expr{
					Expr: &planpb.Expr_UnaryRangeExpr{
						UnaryRangeExpr: &planpb.UnaryRangeExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:  101,
								DataType: schemapb.DataType_Int64,
							},
							Op:    planpb.OpType_GreaterThan,
							Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
						},
					},
				},
				Right: &planpb.Expr{
					Expr: &planpb.Expr_ValueExpr{
						ValueExpr: &planpb.ValueExpr{
							Value: &planpb.GenericValue{
								Val: &planpb.GenericValue_BoolVal{BoolVal: true},
							},
						},
					},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	binary := result.GetBinaryExpr()
	require.NotNil(t, binary, "optimize=false should keep logical binary shape")
	require.NotNil(t, binary.GetRight().GetValueExpr(), "optimize=false should keep ValueExpr child unchanged")
	require.False(t, rewriter.IsAlwaysTrueExpr(result), "optimize=false should not short-circuit logical binary")
}
