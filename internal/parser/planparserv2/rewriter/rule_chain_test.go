package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type ruleChainCase struct {
	name      string
	build     func() *planpb.Expr
	wantOp    planpb.OpType
	wantValue int64
}

func ruleChainCases() []ruleChainCase {
	return []ruleChainCase{
		{
			name: "IN intersection becomes equal",
			build: func() *planpb.Expr {
				column := edgeTestRangeColumn(schemapb.DataType_Int64)
				return edgeTestLogical(
					planpb.BinaryExpr_LogicalAnd,
					edgeTestTerm(column, edgeTestIntValue(1), edgeTestIntValue(2)),
					edgeTestTerm(column, edgeTestIntValue(2), edgeTestIntValue(3)),
				)
			},
			wantOp:    planpb.OpType_Equal,
			wantValue: 2,
		},
		{
			name: "IN with not equal becomes equal",
			build: func() *planpb.Expr {
				column := edgeTestRangeColumn(schemapb.DataType_Int64)
				return edgeTestLogical(
					planpb.BinaryExpr_LogicalAnd,
					edgeTestTerm(column, edgeTestIntValue(1), edgeTestIntValue(2)),
					edgeTestUnaryRange(column, planpb.OpType_NotEqual, edgeTestIntValue(1)),
				)
			},
			wantOp:    planpb.OpType_Equal,
			wantValue: 2,
		},
		{
			name: "IN with range becomes equal",
			build: func() *planpb.Expr {
				column := edgeTestRangeColumn(schemapb.DataType_Int64)
				return edgeTestLogical(
					planpb.BinaryExpr_LogicalAnd,
					edgeTestTerm(column, edgeTestIntValue(1), edgeTestIntValue(2), edgeTestIntValue(3)),
					edgeTestUnaryRange(column, planpb.OpType_GreaterThan, edgeTestIntValue(2)),
				)
			},
			wantOp:    planpb.OpType_Equal,
			wantValue: 3,
		},
		{
			name: "duplicate equals OR becomes equal",
			build: func() *planpb.Expr {
				column := edgeTestRangeColumn(schemapb.DataType_Int64)
				return edgeTestLogical(
					planpb.BinaryExpr_LogicalOr,
					edgeTestUnaryRange(column, planpb.OpType_Equal, edgeTestIntValue(1)),
					edgeTestUnaryRange(column, planpb.OpType_Equal, edgeTestIntValue(1)),
				)
			},
			wantOp:    planpb.OpType_Equal,
			wantValue: 1,
		},
		{
			name: "duplicate not equals AND becomes not equal",
			build: func() *planpb.Expr {
				column := edgeTestRangeColumn(schemapb.DataType_Int64)
				return edgeTestLogical(
					planpb.BinaryExpr_LogicalAnd,
					edgeTestUnaryRange(column, planpb.OpType_NotEqual, edgeTestIntValue(1)),
					edgeTestUnaryRange(column, planpb.OpType_NotEqual, edgeTestIntValue(1)),
				)
			},
			wantOp:    planpb.OpType_NotEqual,
			wantValue: 1,
		},
	}
}

func TestRewriteRuleChains(t *testing.T) {
	for _, test := range ruleChainCases() {
		t.Run(test.name, func(t *testing.T) {
			result := rewriter.RewriteExprWithConfig(test.build(), true)

			unaryRange := result.GetUnaryRangeExpr()
			require.NotNil(t, unaryRange, "rewrite should stabilize at a unary range expression: %s", result)
			require.Equal(t, test.wantOp, unaryRange.GetOp())
			require.Equal(t, test.wantValue, unaryRange.GetValue().GetInt64Val())
		})
	}
}

func TestRewriteStableResultIsIdempotent(t *testing.T) {
	for _, test := range ruleChainCases() {
		t.Run(test.name, func(t *testing.T) {
			result := rewriter.RewriteExprWithConfig(test.build(), true)
			stableSnapshot := proto.Clone(result).(*planpb.Expr)

			rewritten := rewriter.RewriteExprWithConfig(result, true)

			require.True(t, proto.Equal(stableSnapshot, rewritten),
				"rewriting a stable expression should not change its protobuf structure")
		})
	}
}
