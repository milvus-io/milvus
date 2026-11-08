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

func TestDeduplicate_WithCommonFactorExtraction(t *testing.T) {
	helper := buildSchemaHelperForDedupTest(t)

	t.Run("or_of_identical_and_branches_128x", func(t *testing.T) {
		// (A AND B) OR (A AND B) repeated 128 times (exceeds maxCommonFactorBranches = 64)
		branches := make([]string, 128)
		for i := range branches {
			branches[i] = "(category == 1 and age <= 500)"
		}
		expr, err := parser.ParseExpr(helper, strings.Join(branches, " or "), nil)
		require.NoError(t, err)
		require.NotNil(t, expr)

		andCount, orCount, urCount, _, _ := countASTNodes(expr)
		require.Equal(t, 1, andCount, "should collapse to a single AND node (A AND B)")
		require.Equal(t, 0, orCount, "all duplicate OR branches should be eliminated")
		require.Equal(t, 2, urCount, "should retain exactly two leaf predicates")
	})

	t.Run("or_common_factor_after_deduplication_128_branches", func(t *testing.T) {
		// 64x (A AND B) OR 64x (A AND C) = 128 branches (> maxCommonFactorBranches = 64).
		// Deduplication reduces 128 branches to 2 branches: (A AND B) OR (A AND C),
		// allowing common-factor extraction to produce A AND (B OR C).
		branches := make([]string, 0, 128)
		for i := 0; i < 64; i++ {
			branches = append(branches, "(category == 1 and age < 100)")
			branches = append(branches, "(category == 1 and name == 'alice')")
		}
		expr, err := parser.ParseExpr(helper, strings.Join(branches, " or "), nil)
		require.NoError(t, err)
		require.NotNil(t, expr)

		andCount, orCount, urCount, _, _ := countASTNodes(expr)
		require.Equal(t, 1, andCount, "common factor should be extracted into a single top-level AND")
		require.Equal(t, 1, orCount, "residual predicates should form a single OR")
		require.Equal(t, 3, urCount, "should contain A, B, and C")
	})

	t.Run("and_common_factor_after_deduplication_128_clauses", func(t *testing.T) {
		// 64x (A OR B) AND 64x (A OR C) = 128 clauses (> maxCommonFactorBranches = 64).
		// Deduplication reduces 128 clauses to 2 clauses: (A OR B) AND (A OR C),
		// allowing CNF common-factor extraction to produce A OR (B AND C).
		clauses := make([]string, 0, 128)
		for i := 0; i < 64; i++ {
			clauses = append(clauses, "(category == 1 or age < 100)")
			clauses = append(clauses, "(category == 1 or name == 'alice')")
		}
		expr, err := parser.ParseExpr(helper, strings.Join(clauses, " and "), nil)
		require.NoError(t, err)
		require.NotNil(t, expr)

		andCount, orCount, urCount, _, _ := countASTNodes(expr)
		require.Equal(t, 1, orCount, "common factor should be extracted into a single top-level OR")
		require.Equal(t, 1, andCount, "residual predicates should form a single AND")
		require.Equal(t, 3, urCount, "should contain A, B, and C")
	})
}
