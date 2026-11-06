package rewriter_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func buildSchemaHelperForDedupTest(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "category", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "name", DataType: schemapb.DataType_VarChar},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "dedup_test",
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func countASTNodes(e *planpb.Expr) (andCount, orCount, unaryRangeCount, binaryRangeCount, termCount int) {
	if e == nil {
		return
	}
	switch real := e.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		if real.BinaryExpr.GetOp() == planpb.BinaryExpr_LogicalAnd {
			andCount++
		} else if real.BinaryExpr.GetOp() == planpb.BinaryExpr_LogicalOr {
			orCount++
		}
		lAnd, lOr, lUr, lBr, lTerm := countASTNodes(real.BinaryExpr.GetLeft())
		rAnd, rOr, rUr, rBr, rTerm := countASTNodes(real.BinaryExpr.GetRight())
		return andCount + lAnd + rAnd, orCount + lOr + rOr, lUr + rUr, lBr + rBr, lTerm + rTerm
	case *planpb.Expr_UnaryRangeExpr:
		return 0, 0, 1, 0, 0
	case *planpb.Expr_BinaryRangeExpr:
		return 0, 0, 0, 1, 0
	case *planpb.Expr_TermExpr:
		return 0, 0, 0, 0, 1
	default:
		return 0, 0, 0, 0, 0
	}
}

func TestDeduplicate_AND_RepeatedUnaryRange(t *testing.T) {
	helper := buildSchemaHelperForDedupTest(t)
	// Repeat category == 1 128 times
	parts := make([]string, 128)
	for i := range parts {
		parts[i] = "category == 1"
	}
	filterStr := strings.Join(parts, " and ")

	expr, err := parser.ParseExpr(helper, filterStr, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	andCount, _, urCount, _, _ := countASTNodes(expr)
	// Should be collapsed to a single UnaryRangeExpr with 0 LogicalAnd nodes
	require.Equal(t, 0, andCount, "all duplicate AND nodes should be eliminated")
	require.Equal(t, 1, urCount, "should fold to exactly one unary range expr")
	require.NotNil(t, expr.GetUnaryRangeExpr())
	require.Equal(t, int64(1), expr.GetUnaryRangeExpr().GetValue().GetInt64Val())
}

func TestDeduplicate_AND_RepeatedBinaryRange(t *testing.T) {
	helper := buildSchemaHelperForDedupTest(t)
	// Repeat 1 <= age <= 1000 64 times
	parts := make([]string, 64)
	for i := range parts {
		parts[i] = "1 <= age <= 1000"
	}
	filterStr := strings.Join(parts, " and ")

	expr, err := parser.ParseExpr(helper, filterStr, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	andCount, _, _, brCount, _ := countASTNodes(expr)
	require.Equal(t, 0, andCount, "all duplicate AND nodes should be eliminated")
	require.Equal(t, 1, brCount, "should fold to exactly one binary range expr")
	require.NotNil(t, expr.GetBinaryRangeExpr())
}

func TestDeduplicate_OR_RepeatedOperands(t *testing.T) {
	helper := buildSchemaHelperForDedupTest(t)
	// category == 1 or category == 1 or category == 1
	filterStr := "category == 1 or category == 1 or category == 1"

	expr, err := parser.ParseExpr(helper, filterStr, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	_, orCount, urCount, _, _ := countASTNodes(expr)
	require.Equal(t, 0, orCount, "duplicate OR nodes should be eliminated via A || A == A")
	require.Equal(t, 1, urCount, "should fold to single unary range expr")
	require.NotNil(t, expr.GetUnaryRangeExpr())
}

func TestDeduplicate_AND_MixedDifferentPredicates(t *testing.T) {
	helper := buildSchemaHelperForDedupTest(t)
	// category == 1 and age > 10 and category == 1 and age > 10
	filterStr := "category == 1 and age > 10 and category == 1 and age > 10"

	expr, err := parser.ParseExpr(helper, filterStr, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	andCount, _, urCount, _, _ := countASTNodes(expr)
	require.Equal(t, 1, andCount, "should have exactly one AND connecting the two distinct predicates")
	require.Equal(t, 2, urCount, "should retain exactly two distinct unary range exprs")
}
