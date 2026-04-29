package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ============================================================
// 1. Contradiction Detection: Equal vs Equal
// ============================================================

func TestRewrite_Contradiction_EqualEqual_Int64(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_EqualEqual_String(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" and VarCharField == "b"`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_EqualEqual_SameValue_Dedup(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a == 5 AND a == 5 → a == 5
	expr, err := parser.ParseExpr(helper, `Int64Field == 5 and Int64Field == 5`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_Contradiction_EqualEqual_ThreeValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2 and Int64Field == 3`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_EqualEqual_DifferentColumns_NoOptimize(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Different columns → no contradiction
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and VarCharField == "a"`, nil)
	require.NoError(t, err)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "different columns should not trigger contradiction")
}

func TestRewrite_Contradiction_EqualEqual_WithOtherConditions(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a == 1 AND a == 2 AND b > 10 → false (contradiction on a kills entire AND)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2 and FloatField > 10`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// ============================================================
// 2. Contradiction Detection: Range vs Equal
// ============================================================

func TestRewrite_Contradiction_RangeEqual_LowerBound(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a > 10 AND a == 5 → false
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 5`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_RangeEqual_UpperBound(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a < 10 AND a == 20 → false
	expr, err := parser.ParseExpr(helper, `Int64Field < 10 and Int64Field == 20`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_RangeEqual_ExclusiveBoundary(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a > 10 AND a == 10 → false (exclusive >)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 10`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_RangeEqual_InclusiveBoundary(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a >= 10 AND a == 10 → a == 10
	expr, err := parser.ParseExpr(helper, `Int64Field >= 10 and Int64Field == 10`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

func TestRewrite_Contradiction_RangeEqual_InRange(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a > 10 AND a == 15 → a == 15
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 15`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(15), ure.GetValue().GetInt64Val())
}

func TestRewrite_Contradiction_BinaryRangeEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < a < 50) AND a == 30 → a == 30
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and Int64Field == 30`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(30), ure.GetValue().GetInt64Val())
}

func TestRewrite_Contradiction_BinaryRangeEqual_Outside(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < a < 50) AND a == 5 → false
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and Int64Field == 5`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_RangeEqual_Float(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// FloatField > 10.5 AND FloatField == 5.0 → false
	expr, err := parser.ParseExpr(helper, `FloatField > 10.5 and FloatField == 5.0`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Contradiction_RangeEqual_String(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// VarCharField > "m" AND VarCharField == "abc" → false
	expr, err := parser.ParseExpr(helper, `VarCharField > "m" and VarCharField == "abc"`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// ============================================================
// 3. Tautology Detection
// ============================================================

func buildSchemaHelperNonNullableT(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "Int64Field", DataType: schemapb.DataType_Int64, Nullable: false},
		{FieldID: 102, Name: "VarCharField", DataType: schemapb.DataType_VarChar, Nullable: false},
		{FieldID: 103, Name: "FloatField", DataType: schemapb.DataType_Double, Nullable: false},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "rewrite_non_nullable_test",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func buildSchemaHelperNullableT(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "Int64Field", DataType: schemapb.DataType_Int64, Nullable: true},
		{FieldID: 102, Name: "VarCharField", DataType: schemapb.DataType_VarChar, Nullable: true},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "rewrite_nullable_test",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func TestRewrite_Tautology_GT_LE_NonNullable(t *testing.T) {
	helper := buildSchemaHelperNonNullableT(t)
	// a > 10 OR a <= 10 → true (non-nullable)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field <= 10`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

func TestRewrite_Tautology_GE_LT_NonNullable(t *testing.T) {
	helper := buildSchemaHelperNonNullableT(t)
	// a >= 10 OR a < 10 → true
	expr, err := parser.ParseExpr(helper, `Int64Field >= 10 or Int64Field < 10`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

func TestRewrite_Tautology_String_NonNullable(t *testing.T) {
	helper := buildSchemaHelperNonNullableT(t)
	// VarCharField >= "m" OR VarCharField < "m" → true
	expr, err := parser.ParseExpr(helper, `VarCharField >= "m" or VarCharField < "m"`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

func TestRewrite_Tautology_Float_NonNullable(t *testing.T) {
	helper := buildSchemaHelperNonNullableT(t)
	expr, err := parser.ParseExpr(helper, `FloatField > 3.14 or FloatField <= 3.14`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

func TestRewrite_Tautology_Nullable_NoOptimize(t *testing.T) {
	helper := buildSchemaHelperNullableT(t)
	// a > 10 OR a <= 10 → NOT tautology for nullable (NULL values are excluded)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field <= 10`, nil)
	require.NoError(t, err)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr), "nullable columns should not produce tautology")
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as OR for nullable column")
}

func TestRewrite_Tautology_NotComplement_NoOptimize(t *testing.T) {
	helper := buildSchemaHelperNonNullableT(t)
	// a > 10 OR a < 10 → NOT a tautology (missing == 10)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field < 10`, nil)
	require.NoError(t, err)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr))
}

func TestRewrite_Tautology_DifferentValues_NoOptimize(t *testing.T) {
	helper := buildSchemaHelperNonNullableT(t)
	// a > 10 OR a <= 20 → NOT a tautology (different values)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field <= 20`, nil)
	require.NoError(t, err)
	// This isn't a complementary pair (different values), not optimized by tautology detection
	// (although it does cover everything, detecting this is more complex)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr))
}

// ============================================================
// 4. Absorption Law
// ============================================================

func TestRewrite_Absorption_OR_Simple(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (a > 10) OR ((a > 10) AND (b > 20)) → (a > 10)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or (Int64Field > 10 and FloatField > 20)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "absorption should simplify to just a > 10")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

func TestRewrite_Absorption_OR_Reversed(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// ((a > 10) AND (b > 20)) OR (a > 10) → (a > 10)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and FloatField > 20) or Int64Field > 10`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "absorption should simplify to just a > 10")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
}

func TestRewrite_Absorption_AND_Simple(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (a > 10) AND ((a > 10) OR (b > 20)) → (a > 10)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and (Int64Field > 10 or FloatField > 20)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "absorption should simplify to just a > 10")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
}

func TestRewrite_Absorption_NoMatch_NoOptimize(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (a > 10) OR ((a > 20) AND (b > 30)) → no absorption (a > 10 != a > 20)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or (Int64Field > 20 and FloatField > 30)`, nil)
	require.NoError(t, err)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "no absorption should happen")
}

func TestRewrite_Absorption_OR_Equality(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (a == 5) OR ((a == 5) AND (b < 10)) → (a == 5)
	expr, err := parser.ParseExpr(helper, `Int64Field == 5 or (Int64Field == 5 and FloatField < 10)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

// ============================================================
// 5. OR Multi-Interval Merge (3+ intervals)
// ============================================================

func TestRewrite_OR_ThreeOverlapping_Int(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 20) OR (15 < x < 25) OR (22 < x < 30) → (10 < x < 30)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 20) or (Int64Field > 15 and Int64Field < 25) or (Int64Field > 22 and Int64Field < 30)`, nil)
	require.NoError(t, err)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(30), bre.GetUpperValue().GetInt64Val())
}

func TestRewrite_OR_FourAdjacentInclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x <= 20) OR (20 < x <= 30) OR (30 < x <= 40) OR (40 < x <= 50) → (10 < x <= 50)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field <= 20) or (Int64Field > 20 and Int64Field <= 30) or (Int64Field > 30 and Int64Field <= 40) or (Int64Field > 40 and Int64Field <= 50)`, nil)
	require.NoError(t, err)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, false, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
}

func TestRewrite_OR_ThreeContained(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 100) OR (20 < x < 50) OR (30 < x < 40) → (10 < x < 100)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 100) or (Int64Field > 20 and Int64Field < 50) or (Int64Field > 30 and Int64Field < 40)`, nil)
	require.NoError(t, err)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(100), bre.GetUpperValue().GetInt64Val())
}

func TestRewrite_OR_ThreeDisjoint_NoMerge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (1 < x < 5) OR (10 < x < 15) OR (20 < x < 25) → remains as OR
	expr, err := parser.ParseExpr(helper, `(Int64Field > 1 and Int64Field < 5) or (Int64Field > 10 and Int64Field < 15) or (Int64Field > 20 and Int64Field < 25)`, nil)
	require.NoError(t, err)
	// Should not merge to single range (disjoint)
	bre := expr.GetBinaryRangeExpr()
	require.Nil(t, bre, "disjoint intervals should NOT merge to single range")
}

// ============================================================
// 6. OR Bounded + Unbounded Merge
// ============================================================

func TestRewrite_OR_UnboundedLower_Bounded_Overlapping(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x > 10) OR (5 < x < 15) → (x > 5)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or (Int64Field > 5 and Int64Field < 15)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_OR_UnboundedUpper_Bounded_Overlapping(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x < 20) OR (10 < x < 30) → (x < 30)
	expr, err := parser.ParseExpr(helper, `Int64Field < 20 or (Int64Field > 10 and Int64Field < 30)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessThan, ure.GetOp())
	require.Equal(t, int64(30), ure.GetValue().GetInt64Val())
}

func TestRewrite_OR_UnboundedLower_Bounded_NotOverlapping(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x > 100) OR (5 < x < 10) → no merge (disjoint)
	expr, err := parser.ParseExpr(helper, `Int64Field > 100 or (Int64Field > 5 and Int64Field < 10)`, nil)
	require.NoError(t, err)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "disjoint unbounded + bounded should not merge")
}

func TestRewrite_OR_UnboundedLower_MultipleBounded(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x > 20) OR (5 < x < 10) OR (8 < x < 25) → (x > 5)
	expr, err := parser.ParseExpr(helper, `Int64Field > 20 or (Int64Field > 5 and Int64Field < 10) or (Int64Field > 8 and Int64Field < 25)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_OR_UnboundedUpper_Inclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x <= 20) OR (15 <= x <= 30) → (x <= 30)
	expr, err := parser.ParseExpr(helper, `Int64Field <= 20 or (Int64Field >= 15 and Int64Field <= 30)`, nil)
	require.NoError(t, err)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessEqual, ure.GetOp())
	require.Equal(t, int64(30), ure.GetValue().GetInt64Val())
}

// ============================================================
// NULL-awareness: Direct protobuf construction tests
// ============================================================

func TestRewrite_Direct_Contradiction_EqualEqual(t *testing.T) {
	col := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64}
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: left, Right: right, Op: planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, true)
	require.True(t, rewriter.IsAlwaysFalseExpr(result))
}

func TestRewrite_Direct_Tautology_NonNullable(t *testing.T) {
	col := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64, Nullable: false}
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_LessEqual,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: left, Right: right, Op: planpb.BinaryExpr_LogicalOr,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, true)
	require.True(t, rewriter.IsAlwaysTrueExpr(result))
}

func TestRewrite_Direct_Tautology_Nullable_NotOptimized(t *testing.T) {
	col := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64, Nullable: true}
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_LessEqual,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: left, Right: right, Op: planpb.BinaryExpr_LogicalOr,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, true)
	require.False(t, rewriter.IsAlwaysTrueExpr(result), "nullable column should not be tautology")
}

func TestRewrite_Direct_Absorption_OR(t *testing.T) {
	colA := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64}
	colB := &planpb.ColumnInfo{FieldId: 104, DataType: schemapb.DataType_Double}
	// P = (a > 10)
	p := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: colA,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	// Q = (b > 20)
	q := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: colB,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 20}},
			},
		},
	}
	// P AND Q
	pAndQ := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: p, Right: q, Op: planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
	// P2 = same as P (a > 10) — must construct separately since pointers differ
	p2 := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: colA,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	// P2 OR (P AND Q) → P2
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: p2, Right: pAndQ, Op: planpb.BinaryExpr_LogicalOr,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, true)
	ure := result.GetUnaryRangeExpr()
	require.NotNil(t, ure, "absorption should produce single range")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

func TestRewrite_Direct_Absorption_AND(t *testing.T) {
	colA := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64}
	colB := &planpb.ColumnInfo{FieldId: 104, DataType: schemapb.DataType_Double}
	// P = (a > 10), Q = (b > 20)
	p := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: colA,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	q := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: colB,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 20}},
			},
		},
	}
	// P OR Q
	pOrQ := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: p, Right: q, Op: planpb.BinaryExpr_LogicalOr,
			},
		},
	}
	p2 := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: colA,
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	// P2 AND (P OR Q) → P2
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: p2, Right: pOrQ, Op: planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, true)
	ure := result.GetUnaryRangeExpr()
	require.NotNil(t, ure, "AND absorption should produce single range")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// ============================================================
// Optimization disabled: verify no optimization happens
// ============================================================

func TestRewrite_OptimizationDisabled_NoContradiction(t *testing.T) {
	col := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64}
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left: left, Right: right, Op: planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, false)
	require.False(t, rewriter.IsAlwaysFalseExpr(result), "optimization disabled should not detect contradiction")
}
