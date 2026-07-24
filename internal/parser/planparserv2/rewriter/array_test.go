package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestRewriteEmptyArrayComparisonWhenOptimizationDisabled(t *testing.T) {
	for _, op := range []planpb.OpType{planpb.OpType_Equal, planpb.OpType_NotEqual} {
		t.Run(op.String(), func(t *testing.T) {
			columnInfo := &planpb.ColumnInfo{
				FieldId:     101,
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				Nullable:    true,
			}
			input := &planpb.Expr{
				Expr: &planpb.Expr_UnaryRangeExpr{
					UnaryRangeExpr: &planpb.UnaryRangeExpr{
						ColumnInfo: columnInfo,
						Op:         op,
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_ArrayVal{
								ArrayVal: &planpb.Array{SameType: true},
							},
						},
					},
				},
			}

			result := rewriter.RewriteExprWithConfig(input, false)
			arrayLength := result.GetBinaryArithOpEvalRangeExpr()
			require.NotNil(t, arrayLength)
			require.Equal(t, planpb.ArithOpType_ArrayLength, arrayLength.GetArithOp())
			require.Equal(t, op, arrayLength.GetOp())
			require.Equal(t, int64(0), arrayLength.GetValue().GetInt64Val())
			require.True(t, arrayLength.GetColumnInfo().GetNullable())
		})
	}
}

func TestRewriteEmptyArrayComparisonOnlyForWholeArrayColumns(t *testing.T) {
	testcases := []struct {
		name       string
		columnInfo *planpb.ColumnInfo
	}{
		{
			name: "array element",
			columnInfo: &planpb.ColumnInfo{
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				NestedPath:  []string{"0"},
			},
		},
		{
			name: "element level",
			columnInfo: &planpb.ColumnInfo{
				DataType:       schemapb.DataType_Array,
				ElementType:    schemapb.DataType_Int64,
				IsElementLevel: true,
			},
		},
		{
			name: "json path",
			columnInfo: &planpb.ColumnInfo{
				DataType:   schemapb.DataType_JSON,
				NestedPath: []string{"array"},
			},
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			input := &planpb.Expr{
				Expr: &planpb.Expr_UnaryRangeExpr{
					UnaryRangeExpr: &planpb.UnaryRangeExpr{
						ColumnInfo: testcase.columnInfo,
						Op:         planpb.OpType_Equal,
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{}},
						},
					},
				},
			}

			result := rewriter.RewriteExprWithConfig(input, false)
			require.NotNil(t, result.GetUnaryRangeExpr())
			require.Nil(t, result.GetBinaryArithOpEvalRangeExpr())
		})
	}
}

func TestRewriteNonEmptyArrayComparisonUnchanged(t *testing.T) {
	input := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
				},
				Op: planpb.OpType_Equal,
				Value: &planpb.GenericValue{
					Val: &planpb.GenericValue_ArrayVal{
						ArrayVal: &planpb.Array{
							Array: []*planpb.GenericValue{
								{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
							},
							SameType:    true,
							ElementType: schemapb.DataType_Int64,
						},
					},
				},
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	require.NotNil(t, result.GetUnaryRangeExpr())
	require.Nil(t, result.GetBinaryArithOpEvalRangeExpr())
}
