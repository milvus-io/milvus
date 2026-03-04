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

func TestRewrite_Range_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_AND_Strengthen_Upper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field < 50 and Int64Field < 60`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessThan, ure.GetOp())
	require.Equal(t, int64(50), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_AND_EquivalentBounds(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a ≥ x AND a > x → a > x
	expr, err := parser.ParseExpr(helper, `Int64Field >= 10 and Int64Field > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
	// a ≤ y AND a < y → a < y
	expr, err = parser.ParseExpr(helper, `Int64Field <= 10 and Int64Field < 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure = expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_OR_Weaken(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_OR_Weaken_Upper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field < 10 or Int64Field < 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessThan, ure.GetOp())
	require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_OR_EquivalentBounds(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// a ≥ x OR a > x → a ≥ x
	expr, err := parser.ParseExpr(helper, `Int64Field >= 10 or Int64Field > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterEqual, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
	// a ≤ y OR a < y → a ≤ y
	expr, err = parser.ParseExpr(helper, `Int64Field <= 10 or Int64Field < 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure = expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessEqual, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_AND_ToBinaryRange(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field < 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, false, bre.GetLowerInclusive())
	require.Equal(t, false, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
}

func TestRewrite_Range_AND_ToBinaryRange_Inclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field >= 10 and Int64Field <= 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, true, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
}

func TestRewrite_Range_OR_MixedDirection_NoMerge(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 or Int64Field < 5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
	require.NotNil(t, be.GetLeft().GetUnaryRangeExpr())
	require.NotNil(t, be.GetRight().GetUnaryRangeExpr())
}

// Edge cases for Float/Double columns: allow mixing int and float literals.
func TestRewrite_Range_AND_Strengthen_Float_Mixed(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `FloatField > 10 and FloatField > 15.0`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.InDelta(t, 15.0, ure.GetValue().GetFloatVal(), 1e-9)
}

func TestRewrite_Range_AND_ToBinaryRange_Float_Mixed(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `FloatField > 10 and FloatField < 20.5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.False(t, bre.GetLowerInclusive())
	require.False(t, bre.GetUpperInclusive())
	// lower may be encoded as int or float; assert either encoding equals 10
	lv := bre.GetLowerValue()
	switch lv.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		require.Equal(t, int64(10), lv.GetInt64Val())
	case *planpb.GenericValue_FloatVal:
		require.InDelta(t, 10.0, lv.GetFloatVal(), 1e-9)
	default:
		t.Fatalf("unexpected lower value type")
	}
	// upper is float literal 20.5
	require.InDelta(t, 20.5, bre.GetUpperValue().GetFloatVal(), 1e-9)
}

func TestRewrite_Range_OR_Weaken_Float_Mixed(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `FloatField > 10 or FloatField > 20.5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	// weakest lower is 10; value may be encoded as int or float
	switch ure.GetValue().GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
	case *planpb.GenericValue_FloatVal:
		require.InDelta(t, 10.0, ure.GetValue().GetFloatVal(), 1e-9)
	default:
		t.Fatalf("unexpected value type")
	}
}

func TestRewrite_Range_AND_NoMerge_Int_WithFloatLiteral(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	_, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field > 15.0`, nil)
	// keeping this test so that we know the parser will not accept this expression, so we
	// don't need to optimize it.
	require.Error(t, err)
}

func TestRewrite_Range_Tie_Inclusive_Float(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// OR: >=10 or >10 -> >=10 (weaken prefers inclusive)
	expr, err := parser.ParseExpr(helper, `FloatField >= 10 or FloatField > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterEqual, ure.GetOp())
	// AND: >=10 and >10 -> >10 (tighten prefers strict)
	expr, err = parser.ParseExpr(helper, `FloatField >= 10 and FloatField > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure = expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
}

// VarChar range optimization tests
func TestRewrite_Range_VarChar_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField > "a" and VarCharField > "b"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, "b", ure.GetValue().GetStringVal())
}

func TestRewrite_Range_VarChar_OR_Weaken_Upper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField < "m" or VarCharField < "z"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessThan, ure.GetOp())
	require.Equal(t, "z", ure.GetValue().GetStringVal())
}

// Array fields: ensure parser rejects direct range comparison on arrays for different element types.
// If in the future parser supports range on arrays, these tests can be updated accordingly.
func buildSchemaHelperWithArraysT(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 201, Name: "ArrayInt", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{FieldID: 202, Name: "ArrayFloat", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double},
		{FieldID: 203, Name: "ArrayVarchar", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "rewrite_array_test",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func TestRewrite_Range_Array_Int_NotSupported(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	_, err := parser.ParseExpr(helper, `ArrayInt > 10`, nil)
	require.Error(t, err)
}

func TestRewrite_Range_Array_Float_NotSupported(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	_, err := parser.ParseExpr(helper, `ArrayFloat > 10.5`, nil)
	require.Error(t, err)
}

func TestRewrite_Range_Array_VarChar_NotSupported(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	_, err := parser.ParseExpr(helper, `ArrayVarchar > "a"`, nil)
	require.Error(t, err)
}

// Array index access optimizations
func TestRewrite_Range_ArrayInt_Index_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	expr, err := parser.ParseExpr(helper, `ArrayInt[0] > 10 and ArrayInt[0] > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
}

func TestRewrite_Range_ArrayFloat_Index_OR_Weaken_Mixed(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	expr, err := parser.ParseExpr(helper, `ArrayFloat[0] > 10 or ArrayFloat[0] > 20.5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	switch ure.GetValue().GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
	case *planpb.GenericValue_FloatVal:
		require.InDelta(t, 10.0, ure.GetValue().GetFloatVal(), 1e-9)
	default:
		t.Fatalf("unexpected value type")
	}
}

func TestRewrite_Range_ArrayVarChar_Index_ToBinaryRange(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	expr, err := parser.ParseExpr(helper, `ArrayVarchar[0] > "a" and ArrayVarchar[0] < "m"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.False(t, bre.GetLowerInclusive())
	require.False(t, bre.GetUpperInclusive())
	require.Equal(t, "a", bre.GetLowerValue().GetStringVal())
	require.Equal(t, "m", bre.GetUpperValue().GetStringVal())
}

// helper to flatten AND tree into list of exprs
func collectAndExprs(e *planpb.Expr, out *[]*planpb.Expr) {
	if be := e.GetBinaryExpr(); be != nil && be.GetOp() == planpb.BinaryExpr_LogicalAnd {
		collectAndExprs(be.GetLeft(), out)
		collectAndExprs(be.GetRight(), out)
		return
	}
	*out = append(*out, e)
}

func TestRewrite_Range_Array_Index_Different_NoMerge(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	expr, err := parser.ParseExpr(helper, `ArrayInt[0] > 10 and ArrayInt[1] > 20 and ArrayInt[0] < 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
	parts := []*planpb.Expr{}
	collectAndExprs(expr, &parts)
	// expect exactly two parts after rewrite: interval on index 0, and lower bound on index 1
	require.Equal(t, 2, len(parts))
	var seenInterval, seenLower bool
	for _, p := range parts {
		if bre := p.GetBinaryRangeExpr(); bre != nil {
			seenInterval = true
			// 10 < ArrayInt[0] < 20
			require.False(t, bre.GetLowerInclusive())
			require.False(t, bre.GetUpperInclusive())
			require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
			require.Equal(t, int64(20), bre.GetUpperValue().GetInt64Val())
			continue
		}
		if ure := p.GetUnaryRangeExpr(); ure != nil {
			seenLower = true
			require.True(t, ure.GetOp() == planpb.OpType_GreaterThan || ure.GetOp() == planpb.OpType_GreaterEqual)
			// bound value 20 on index 1 lower side
			require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
			continue
		}
		// should not reach here: only BinaryRangeExpr and UnaryRangeExpr expected
		t.Fatalf("unexpected expr kind in AND parts")
	}
	require.True(t, seenInterval)
	require.True(t, seenLower)
}

// Test invalid BinaryRangeExpr: lower > upper → false
func TestRewrite_Range_AND_InvalidRange_LowerGreaterThanUpper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field > 100 AND Int64Field < 50 → false (impossible range)
	expr, err := parser.ParseExpr(helper, `Int64Field > 100 and Int64Field < 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test invalid BinaryRangeExpr: lower == upper with exclusive bounds → false
func TestRewrite_Range_AND_InvalidRange_EqualBoundsExclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field > 50 AND Int64Field < 50 → false (exclusive on equal bounds)
	expr, err := parser.ParseExpr(helper, `Int64Field > 50 and Int64Field < 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test invalid BinaryRangeExpr: lower == upper with one exclusive → false
func TestRewrite_Range_AND_InvalidRange_EqualBoundsOneExclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field >= 50 AND Int64Field < 50 → false (one exclusive on equal bounds)
	expr, err := parser.ParseExpr(helper, `Int64Field >= 50 and Int64Field < 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test valid BinaryRangeExpr: lower == upper with both inclusive → valid
func TestRewrite_Range_AND_ValidRange_EqualBoundsBothInclusive(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field >= 50 AND Int64Field <= 50 → (50 <= x <= 50), which is valid (x == 50)
	expr, err := parser.ParseExpr(helper, `Int64Field >= 50 and Int64Field <= 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre, "should create valid binary range for x == 50")
	require.Equal(t, true, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.Equal(t, int64(50), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
}

// Test invalid BinaryRangeExpr with float: lower > upper → false
func TestRewrite_Range_AND_InvalidRange_Float_LowerGreaterThanUpper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// FloatField > 99.9 AND FloatField < 10.5 → false
	expr, err := parser.ParseExpr(helper, `FloatField > 99.9 and FloatField < 10.5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test invalid BinaryRangeExpr with string: lower > upper → false
func TestRewrite_Range_AND_InvalidRange_String_LowerGreaterThanUpper(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// VarCharField > "zebra" AND VarCharField < "apple" → false
	expr, err := parser.ParseExpr(helper, `VarCharField > "zebra" and VarCharField < "apple"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test AlwaysFalse propagation through nested AND expressions
func TestRewrite_AlwaysFalse_Propagation_DeepNesting(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Deep nesting: (Int64Field > 10) AND ((Int64Field > 20) AND (Int64Field > 100 AND Int64Field < 50))
	// The innermost (Int64Field > 100 AND Int64Field < 50) should become AlwaysFalse
	// This AlwaysFalse should propagate up through all ANDs, making the entire expression AlwaysFalse
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10) and ((Int64Field > 20) and (Int64Field > 100 and Int64Field < 50))`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// Should propagate to become AlwaysFalse at top level
	require.True(t, rewriter.IsAlwaysFalseExpr(expr), "AlwaysFalse should propagate to top level")
}

// Test AlwaysFalse elimination in OR expressions
func TestRewrite_AlwaysFalse_Elimination_InOR(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (Int64Field > 10) OR ((Int64Field > 20) OR (Int64Field > 100 AND Int64Field < 50))
	// The innermost becomes AlwaysFalse, should be eliminated from OR
	// Result should be: Int64Field > 10 OR Int64Field > 20 → Int64Field > 10 (weaker bound)
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10) or ((Int64Field > 20) or (Int64Field > 100 and Int64Field < 50))`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// Should simplify to single range condition: Int64Field > 10
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "AlwaysFalse should be eliminated, leaving simplified range")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// Test complex double negation: NOT NOT AlwaysTrue → AlwaysTrue
func TestRewrite_DoubleNegation_ToAlwaysTrue(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `not (Int64Field > 100 and Int64Field < 50)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

// Test complex nested double negation with multiple layers
func TestRewrite_ComplexDoubleNegation_MultiLayer(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `not ((Int64Field > 100 and Int64Field < 50) or (FloatField > 99.9 and FloatField < 10.5))`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

// Test AlwaysTrue in AND with normal conditions gets eliminated
func TestRewrite_AlwaysTrue_Elimination_InAND(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// (Int64Field > 10) AND NOT(Int64Field > 100 AND Int64Field < 50)
	// The second part becomes AlwaysTrue, should be eliminated from AND
	// Result should be just: Int64Field > 10
	expr, err := parser.ParseExpr(helper, `(Int64Field > 10) and not (Int64Field > 100 and Int64Field < 50)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// Test constant-folded ValueExpr(bool=true) is converted to AlwaysTrueExpr
func TestRewrite_ConstantTrue_EqualInts(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// "1==1" is constant-folded by parser to ValueExpr(bool=true),
	// rewriter should convert it to AlwaysTrueExpr
	expr, err := parser.ParseExpr(helper, `1==1`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

// Test constant-folded ValueExpr(bool=true) from greater-than comparison
func TestRewrite_ConstantTrue_GreaterThan(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `1>0`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

// Test constant-folded ValueExpr(bool=true) from not-equal comparison
func TestRewrite_ConstantTrue_NotEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `2!=3`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr))
}

// Test constant-folded ValueExpr(bool=false) is converted to AlwaysFalseExpr
func TestRewrite_ConstantFalse_EqualInts(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// "1==2" is constant-folded by parser to ValueExpr(bool=false),
	// rewriter should convert it to AlwaysFalseExpr (NOT AlwaysTrueExpr)
	expr, err := parser.ParseExpr(helper, `1==2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test constant-folded ValueExpr(bool=false) from greater-than comparison
func TestRewrite_ConstantFalse_GreaterThan(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `1>2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

// Test rewriter directly with ValueExpr(bool=true) in AND — bypassing parser validation
func TestRewrite_Direct_ValueExprTrue_InAND(t *testing.T) {
	// Construct: (Int64Field > 10) AND ValueExpr(true)
	// This simulates what would happen if the parser allowed it
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  left,
				Right: right,
				Op:    planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, false)
	// ValueExpr(true) → AlwaysTrueExpr, then eliminated from AND → just Int64Field > 10
	ure := result.GetUnaryRangeExpr()
	require.NotNil(t, ure, "constant true should be eliminated from AND, leaving range expr")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// Test rewriter directly with ValueExpr(bool=false) in AND — short-circuits to AlwaysFalse
func TestRewrite_Direct_ValueExprFalse_InAND(t *testing.T) {
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: false}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  left,
				Right: right,
				Op:    planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, false)
	// ValueExpr(false) → AlwaysFalseExpr, then AND short-circuits → AlwaysFalse
	require.True(t, rewriter.IsAlwaysFalseExpr(result))
}

// Test rewriter directly with ValueExpr(bool=true) in OR — short-circuits to AlwaysTrue
func TestRewrite_Direct_ValueExprTrue_InOR(t *testing.T) {
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  left,
				Right: right,
				Op:    planpb.BinaryExpr_LogicalOr,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, false)
	// ValueExpr(true) → AlwaysTrueExpr, then OR short-circuits → AlwaysTrue
	require.True(t, rewriter.IsAlwaysTrueExpr(result))
}

// Test rewriter directly with ValueExpr(bool=false) in OR — eliminated
func TestRewrite_Direct_ValueExprFalse_InOR(t *testing.T) {
	left := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		},
	}
	right := &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: false}},
			},
		},
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  left,
				Right: right,
				Op:    planpb.BinaryExpr_LogicalOr,
			},
		},
	}
	result := rewriter.RewriteExprWithConfig(input, false)
	// ValueExpr(false) → AlwaysFalseExpr, then eliminated from OR → just Int64Field > 10
	ure := result.GetUnaryRangeExpr()
	require.NotNil(t, ure, "constant false should be eliminated from OR, leaving range expr")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}
