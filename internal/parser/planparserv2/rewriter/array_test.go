package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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

func TestRewriteWholeArrayMembershipToEqualityBranches(t *testing.T) {
	for _, optimizeEnabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "optimization disabled", true: "optimization enabled"}[optimizeEnabled], func(t *testing.T) {
			columnInfo := &planpb.ColumnInfo{
				FieldId:     101,
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				Nullable:    true,
			}
			input := &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: columnInfo,
						Values: []*planpb.GenericValue{
							newArrayLiteral(),
							newArrayLiteral(1, 2),
						},
					},
				},
			}

			result := rewriter.RewriteExprWithConfig(input, optimizeEnabled)
			require.Nil(t, findTermExpr(result))

			var emptyArrayLengths int
			var nonEmptyEqualities int
			walkArrayMembershipExpr(result, func(current *planpb.Expr) {
				if arrayLength := current.GetBinaryArithOpEvalRangeExpr(); arrayLength != nil &&
					arrayLength.GetArithOp() == planpb.ArithOpType_ArrayLength &&
					arrayLength.GetOp() == planpb.OpType_Equal &&
					arrayLength.GetValue().GetInt64Val() == 0 {
					emptyArrayLengths++
				}
				if equality := current.GetUnaryRangeExpr(); equality != nil &&
					equality.GetOp() == planpb.OpType_Equal &&
					len(equality.GetValue().GetArrayVal().GetArray()) > 0 {
					nonEmptyEqualities++
				}
			})
			require.Equal(t, 1, emptyArrayLengths)
			require.Equal(t, 1, nonEmptyEqualities)
		})
	}
}

func TestRewriteWholeArrayNotInKeepsOuterNot(t *testing.T) {
	for _, optimizeEnabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "optimization disabled", true: "optimization enabled"}[optimizeEnabled], func(t *testing.T) {
			columnInfo := &planpb.ColumnInfo{
				FieldId:     101,
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				Nullable:    true,
			}
			term := &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: columnInfo,
						Values: []*planpb.GenericValue{
							newArrayLiteral(1, 2),
							newArrayLiteral(3, 4),
						},
					},
				},
			}
			input := &planpb.Expr{
				Expr: &planpb.Expr_UnaryExpr{
					UnaryExpr: &planpb.UnaryExpr{
						Op:    planpb.UnaryExpr_Not,
						Child: term,
					},
				},
			}

			result := rewriter.RewriteExprWithConfig(input, optimizeEnabled)
			unary := result.GetUnaryExpr()
			require.NotNil(t, unary)
			require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
			require.NotNil(t, unary.GetChild().GetBinaryExpr())
			require.Nil(t, findTermExpr(result))

			var equalities int
			walkArrayMembershipExpr(unary.GetChild(), func(current *planpb.Expr) {
				if equality := current.GetUnaryRangeExpr(); equality != nil &&
					equality.GetOp() == planpb.OpType_Equal &&
					equality.GetValue().GetArrayVal() != nil {
					equalities++
				}
			})
			require.Equal(t, 2, equalities)
		})
	}
}

func newArrayLiteral(values ...int64) *planpb.GenericValue {
	elements := make([]*planpb.GenericValue, 0, len(values))
	for _, value := range values {
		elements = append(elements, &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{Int64Val: value},
		})
	}
	return &planpb.GenericValue{
		Val: &planpb.GenericValue_ArrayVal{
			ArrayVal: &planpb.Array{
				Array:       elements,
				SameType:    true,
				ElementType: schemapb.DataType_Int64,
			},
		},
	}
}

func walkArrayMembershipExpr(expr *planpb.Expr, visit func(*planpb.Expr)) {
	if expr == nil {
		return
	}
	visit(expr)
	if binary := expr.GetBinaryExpr(); binary != nil {
		walkArrayMembershipExpr(binary.GetLeft(), visit)
		walkArrayMembershipExpr(binary.GetRight(), visit)
	}
	if unary := expr.GetUnaryExpr(); unary != nil {
		walkArrayMembershipExpr(unary.GetChild(), visit)
	}
}
