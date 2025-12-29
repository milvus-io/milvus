package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// Test BinaryRangeExpr AND BinaryRangeExpr - intersection
func TestRewrite_BinaryRange_AND_BinaryRange_Intersection(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 50) AND (20 < x < 40) → (20 < x < 40)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and (Int64Field > 20 and Int64Field < 40)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre, "should merge to single binary range")
	require.Equal(t, false, bre.GetLowerInclusive())
	require.Equal(t, false, bre.GetUpperInclusive())
	require.Equal(t, int64(20), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(40), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr AND BinaryRangeExpr - tighter lower
func TestRewrite_BinaryRange_AND_BinaryRange_TighterLower(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 50) AND (5 < x < 40) → (10 < x < 40)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and (Int64Field > 5 and Int64Field < 40)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(40), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr AND BinaryRangeExpr - tighter upper
func TestRewrite_BinaryRange_AND_BinaryRange_TighterUpper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 50) AND (15 < x < 60) → (15 < x < 50)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and (Int64Field > 15 and Int64Field < 60)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(15), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr AND BinaryRangeExpr - empty intersection
func TestRewrite_BinaryRange_AND_BinaryRange_EmptyIntersection(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 20) AND (30 < x < 40) → false
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 20) and (Int64Field > 30 and Int64Field < 40)`, nil)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test BinaryRangeExpr AND BinaryRangeExpr - equal bounds, both inclusive
func TestRewrite_BinaryRange_AND_BinaryRange_EqualBounds_BothInclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 <= x <= 10) AND (10 <= x <= 20) → (x == 10) which is (10 <= x <= 10)
	expr, err := parser.ParseExpr(helper, `(Int64Field >= 10 and Int64Field <= 10) and (Int64Field >= 10 and Int64Field <= 20)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, true, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(10), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr AND BinaryRangeExpr - equal bounds, one exclusive → false
func TestRewrite_BinaryRange_AND_BinaryRange_EqualBounds_Exclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x <= 20) AND (5 <= x < 10) → false (bounds meet at 10 but exclusive)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field <= 20) and (Int64Field >= 5 and Int64Field < 10)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test BinaryRangeExpr AND UnaryRangeExpr - tighten lower bound
func TestRewrite_BinaryRange_AND_UnaryRange_TightenLower(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 50) AND (x > 30) → (30 < x < 50)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and Int64Field > 30`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(30), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr AND UnaryRangeExpr - tighten upper bound
func TestRewrite_BinaryRange_AND_UnaryRange_TightenUpper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 50) AND (x < 25) → (10 < x < 25)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and Int64Field < 25`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(25), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr AND UnaryRangeExpr - weaker bound (no change)
func TestRewrite_BinaryRange_AND_UnaryRange_WeakerBound(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (20 < x < 30) AND (x > 10) → (20 < x < 30) (10 is weaker than 20)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 20 and Int64Field < 30) and Int64Field > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(20), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(30), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr OR BinaryRangeExpr - overlapping → union
func TestRewrite_BinaryRange_OR_BinaryRange_Overlapping(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 25) OR (20 < x < 40) → (10 < x < 40)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 25) or (Int64Field > 20 and Int64Field < 40)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre, "overlapping intervals should merge")
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(40), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr OR BinaryRangeExpr - adjacent (inclusive) → union
func TestRewrite_BinaryRange_OR_BinaryRange_Adjacent(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x <= 20) OR (20 <= x < 30) → (10 < x < 30)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field <= 20) or (Int64Field >= 20 and Int64Field < 30)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre, "adjacent intervals should merge")
	require.Equal(t, false, bre.GetLowerInclusive())
	require.Equal(t, false, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(30), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRangeExpr OR BinaryRangeExpr - disjoint (no merge)
func TestRewrite_BinaryRange_OR_BinaryRange_Disjoint(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 20) OR (30 < x < 40) → remains as OR
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 20) or (Int64Field > 30 and Int64Field < 40)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// Should remain as BinaryExpr OR since intervals are disjoint
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "disjoint intervals should not merge")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

// Test BinaryRangeExpr OR BinaryRangeExpr - adjacent but both exclusive (no merge)
func TestRewrite_BinaryRange_OR_BinaryRange_AdjacentExclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 20) OR (20 < x < 30) → remains as OR (gap at 20)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 20) or (Int64Field > 20 and Int64Field < 30)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "exclusive adjacent intervals should not merge")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

// Test BinaryRangeExpr OR BinaryRangeExpr - prefer inclusive when merging
func TestRewrite_BinaryRange_OR_BinaryRange_PreferInclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 <= x < 25) OR (15 <= x <= 30) → (10 <= x <= 30)
	expr, err := parser.ParseExpr(helper, `(Int64Field >= 10 and Int64Field < 25) or (Int64Field >= 15 and Int64Field <= 30)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	// Lower should be 10 (from first), upper should be 30 (from second)
	// Both should prefer inclusive where available
	require.Equal(t, true, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(30), bre.GetUpperValue().GetInt64Val())
}

// Test with Float fields
func TestRewrite_BinaryRange_AND_Float(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (1.0 < x < 5.0) AND (2.0 < x < 4.0) → (2.0 < x < 4.0)
	expr, err := parser.ParseExpr(helper, `(FloatField > 1.0 and FloatField < 5.0) and (FloatField > 2.0 and FloatField < 4.0)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.InDelta(t, 2.0, bre.GetLowerValue().GetFloatVal(), 1e-9)
	require.InDelta(t, 4.0, bre.GetUpperValue().GetFloatVal(), 1e-9)
}

// Test with VarChar fields
func TestRewrite_BinaryRange_OR_VarChar_Overlapping(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// ("b" < x < "m") OR ("j" < x < "z") → ("b" < x < "z")
	expr, err := parser.ParseExpr(helper, `(VarCharField > "b" and VarCharField < "m") or (VarCharField > "j" and VarCharField < "z")`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, "b", bre.GetLowerValue().GetStringVal())
	require.Equal(t, "z", bre.GetUpperValue().GetStringVal())
}

// Test with JSON fields
func TestRewrite_BinaryRange_AND_JSON(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// (10 < json["price"] < 100) AND (20 < json["price"] < 80) → (20 < json["price"] < 80)
	expr, err := parser.ParseExpr(helper, `(JSONField["price"] > 10 and JSONField["price"] < 100) and (JSONField["price"] > 20 and JSONField["price"] < 80)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(20), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(80), bre.GetUpperValue().GetInt64Val())
}

// Test with JSON fields - OR overlapping
func TestRewrite_BinaryRange_OR_JSON_Overlapping(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// (1.0 < json["score"] < 3.0) OR (2.5 < json["score"] < 5.0) → (1.0 < json["score"] < 5.0)
	expr, err := parser.ParseExpr(helper, `(JSONField["score"] > 1.0 and JSONField["score"] < 3.0) or (JSONField["score"] > 2.5 and JSONField["score"] < 5.0)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.InDelta(t, 1.0, bre.GetLowerValue().GetFloatVal(), 1e-9)
	require.InDelta(t, 5.0, bre.GetUpperValue().GetFloatVal(), 1e-9)
}

// Test mixing BinaryRange with multiple UnaryRanges
func TestRewrite_BinaryRange_AND_MultipleUnary(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 100) AND (x > 20) AND (x < 80) → (20 < x < 80)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 100) and Int64Field > 20 and Int64Field < 80`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(20), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(80), bre.GetUpperValue().GetInt64Val())
}

// Test three BinaryRanges with AND
func TestRewrite_BinaryRange_AND_ThreeBinaryRanges(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (5 < x < 100) AND (10 < x < 90) AND (15 < x < 80) → (15 < x < 80)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 5 and Int64Field < 100) and (Int64Field > 10 and Int64Field < 90) and (Int64Field > 15 and Int64Field < 80)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, int64(15), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(80), bre.GetUpperValue().GetInt64Val())
}

// Test BinaryRange on different columns should not merge
func TestRewrite_BinaryRange_AND_DifferentColumns(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < Int64Field < 50) AND (20 < FloatField < 40) → both remain
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 50) and (FloatField > 20 and FloatField < 40)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "different columns should not merge")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// Test BinaryRange with JSON different paths should not merge
func TestRewrite_BinaryRange_AND_JSON_DifferentPaths(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// (10 < json["a"] < 50) AND (20 < json["b"] < 40) → both remain
	expr, err := parser.ParseExpr(helper, `(JSONField["a"] > 10 and JSONField["a"] < 50) and (JSONField["b"] > 20 and JSONField["b"] < 40)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "different JSON paths should not merge")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// Test BinaryRangeExpr OR with 3 overlapping intervals
// NOTE: Current implementation limitation - only merges 2 intervals at a time
func TestRewrite_BinaryRange_OR_ThreeOverlapping_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 20) OR (15 < x < 25) OR (22 < x < 30)
	// Ideally should merge to (10 < x < 30)
	// Currently: may only partially merge due to limitation
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 20) or (Int64Field > 15 and Int64Field < 25) or (Int64Field > 22 and Int64Field < 30)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Due to current limitation, result may vary depending on tree structure
	// This test documents the current behavior rather than ideal behavior
	// When enhancement is implemented, this test should be updated to verify (10 < x < 30)

	// For now, just verify it doesn't crash and produces valid output
	require.NotNil(t, expr)
	// Could be BinaryRangeExpr (if some merged) or BinaryExpr OR (if not merged)
	// We document that 3+ intervals are NOT fully optimized yet
}

// Test BinaryRangeExpr OR with 3 fully overlapping intervals
func TestRewrite_BinaryRange_OR_ThreeFullyOverlapping(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x < 30) OR (12 < x < 28) OR (15 < x < 25)
	// The second and third are fully contained in the first
	// Ideally should merge to (10 < x < 30)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field < 30) or (Int64Field > 12 and Int64Field < 28) or (Int64Field > 15 and Int64Field < 25)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: may not fully optimize
	// This test documents that 3+ interval merging is not yet complete
	require.NotNil(t, expr)
}

// Test BinaryRangeExpr OR with 4 adjacent intervals
func TestRewrite_BinaryRange_OR_FourAdjacent_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (10 < x <= 20) OR (20 < x <= 30) OR (30 < x <= 40) OR (40 < x <= 50)
	// Ideally should merge to (10 < x <= 50)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10 and Int64Field <= 20) or (Int64Field > 20 and Int64Field <= 30) or (Int64Field > 30 and Int64Field <= 40) or (Int64Field > 40 and Int64Field <= 50)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: only pairs of adjacent intervals may merge
	// Full chain merging not implemented
	require.NotNil(t, expr)
}

// Test OR with unbounded lower + bounded interval
// NOTE: Current implementation limitation - unbounded intervals not merged with bounded
func TestRewrite_BinaryRange_OR_UnboundedLower_Bounded_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x > 10) OR (5 < x < 15)
	// Ideally should merge to (x > 5)
	// Currently: both predicates remain separate
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or (Int64Field > 5 and Int64Field < 15)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: unbounded + bounded intervals not optimized
	// Result should be a BinaryExpr OR with both predicates
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as OR (not merged)")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

// Test OR with unbounded upper + bounded interval
func TestRewrite_BinaryRange_OR_UnboundedUpper_Bounded_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x < 20) OR (10 < x < 30)
	// Ideally should merge to (x < 30)
	// Currently: both predicates remain separate
	expr, err := parser.ParseExpr(helper, `Int64Field < 20 or (Int64Field > 10 and Int64Field < 30)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: unbounded + bounded intervals not optimized
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as OR (not merged)")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

// Test OR with unbounded lower + unbounded upper
func TestRewrite_BinaryRange_OR_UnboundedBoth_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x > 10) OR (x < 20)
	// This covers most values (gap only between 10 and 20 if both exclusive)
	// Currently: both predicates remain separate
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field < 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: unbounded intervals in OR not merged
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as OR")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

// Test OR with multiple unbounded lower bounds - these DO get optimized (weakening)
func TestRewrite_BinaryRange_OR_MultipleUnboundedLower(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (x > 10) OR (x > 20) → (x > 10) [weaker bound]
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// This SHOULD be optimized (weakening works for same direction)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "should merge to single unary range")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}
