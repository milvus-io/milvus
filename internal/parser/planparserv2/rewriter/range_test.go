package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
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