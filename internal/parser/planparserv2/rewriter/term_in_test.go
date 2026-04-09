package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func buildSchemaHelperForRewriteT(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "Int64Field", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "VarCharField", DataType: schemapb.DataType_VarChar},
		{FieldID: 103, Name: "StringField", DataType: schemapb.DataType_String},
		{FieldID: 104, Name: "FloatField", DataType: schemapb.DataType_Double},
		{FieldID: 105, Name: "BoolField", DataType: schemapb.DataType_Bool},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "rewrite_test",
		AutoID: false,
		Fields: fields,
	}
	// enable text_match on string-like fields
	for _, f := range schema.Fields {
		if typeutil.IsStringType(f.DataType) {
			f.TypeParams = append(f.TypeParams, &commonpb.KeyValuePair{
				Key:   "enable_match",
				Value: "True",
			})
		}
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func buildSchemaHelperForRewriteNullableT(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "Int64Field", DataType: schemapb.DataType_Int64},
		{FieldID: 106, Name: "NullableBoolField", DataType: schemapb.DataType_Bool, Nullable: true},
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

// --- OR-equals merge tests (all merge to IN, SIMD-optimized) ---

func TestRewrite_OREquals_ToIN_VarChar_AboveThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// 3 varchar OR-equals should merge to IN
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" or VarCharField == "b" or VarCharField == "c"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "3 varchar OR-equals should merge to IN (threshold=3)")
	require.Equal(t, 3, len(term.GetValues()))
}

func TestRewrite_OREquals_Merged_VarChar_TwoValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// All OR-equals always merge to IN (SIMD-optimized)
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" or VarCharField == "b"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "2 varchar OR-equals should merge to IN")
	require.Equal(t, 2, len(term.GetValues()))
}

func TestRewrite_OREquals_Merged_Int_TwoValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// All OR-equals always merge to IN (SIMD-optimized)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 or Int64Field == 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "2 int OR-equals should merge to IN")
	require.Equal(t, 2, len(term.GetValues()))
}

func TestRewrite_OREquals_Merged_Int_TenValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 or Int64Field == 2 or Int64Field == 3 or Int64Field == 4 or Int64Field == 5 or Int64Field == 6 or Int64Field == 7 or Int64Field == 8 or Int64Field == 9 or Int64Field == 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "10 int OR-equals should merge to IN")
	require.Equal(t, 10, len(term.GetValues()))
}

// --- IN kept tests (no splitting, SIMD-optimized) ---

func TestRewrite_InKept_Int_SmallCount(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// All IN expressions stay as IN (SIMD-optimized)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "int in with 3 values should stay as IN")
	require.Equal(t, 3, len(term.GetValues()))
}

func TestRewrite_InSingle_Int_BecomesEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Single-value IN → == (avoids SIMD overhead)
	expr, err := parser.ParseExpr(helper, `Int64Field in [5]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "in [single] should become ==")
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_InKept_Int_TenValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "int in with 10 values should stay as IN")
	require.Equal(t, 10, len(term.GetValues()))
}

// --- NOT IN kept tests (no splitting, SIMD-optimized) ---

func TestRewrite_NotInKept_Int_TwoValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// All NOT IN stay as NOT(IN) (SIMD-optimized)
	expr, err := parser.ParseExpr(helper, `Int64Field not in [4,3]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	unary := expr.GetUnaryExpr()
	require.NotNil(t, unary, "not in should stay as NOT(IN)")
	require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
	term := unary.GetChild().GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
}

func TestRewrite_NotInSingle_Int_BecomesNotEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Single-value NOT IN → != (avoids SIMD overhead)
	expr, err := parser.ParseExpr(helper, `Int64Field not in [5]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "not in [single] should become !=")
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_NotInKept_Float_TwoValues(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Float NOT IN also stays as NOT(IN)
	expr, err := parser.ParseExpr(helper, `FloatField not in [4.0,3.0]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	unary := expr.GetUnaryExpr()
	require.NotNil(t, unary, "float not in should stay as NOT(IN)")
	require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
	term := unary.GetChild().GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
}

// --- sort/dedup tests ---

func TestRewrite_Term_SortAndDedup_String(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// dedup + sort: 5 values with dups → 3 unique sorted
	expr, err := parser.ParseExpr(helper, `VarCharField in ["c","b","a","b","a"]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 3, len(term.GetValues()))
	require.Equal(t, "a", term.GetValues()[0].GetStringVal())
	require.Equal(t, "b", term.GetValues()[1].GetStringVal())
	require.Equal(t, "c", term.GetValues()[2].GetStringVal())
}

func TestRewrite_Term_SortAndDedup_Int(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// dedup + sort: 11 values with one dup → 10 unique sorted
	expr, err := parser.ParseExpr(helper, `Int64Field in [10,9,4,6,6,7,1,2,3,5,8]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 10, len(term.GetValues()))
}

// Bool IN — no special rewriting, handled by execution layer.
// Single-value IN still folds to == via the generic single-value optimization.

func TestRewrite_BoolIn_BothValues_StaysAsIn(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// BoolField in [true, false] covers all possible bool values → AlwaysTrueExpr
	expr, err := parser.ParseExpr(helper, `BoolField in [true,false,false,true]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr),
		"bool IN [true, false] should be rewritten to AlwaysTrueExpr")
}

func TestRewrite_Bool_In_SingleTrue_ToEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `BoolField in [true]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "bool IN [true] should be rewritten to == true")
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, true, ure.GetValue().GetBoolVal())
}

func TestRewrite_Bool_In_SingleFalse_ToEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `BoolField in [false]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "bool IN [false] should be rewritten to == false")
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, false, ure.GetValue().GetBoolVal())
}

func TestRewrite_Bool_In_DedupedSingleTrue_ToEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// After dedup, [true, true] becomes [true] → == true
	expr, err := parser.ParseExpr(helper, `BoolField in [true, true]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "bool IN [true, true] should dedup then rewrite to == true")
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, true, ure.GetValue().GetBoolVal())
}

func TestRewrite_Bool_In_TrueFalse_Nullable_ToIsNotNull(t *testing.T) {
	// For nullable bool, in [true, false] should become IS NOT NULL (not AlwaysTrueExpr)
	// because null values should not match.
	helper := buildSchemaHelperForRewriteNullableT(t)
	expr, err := parser.ParseExpr(helper, `NullableBoolField in [true,false]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	nullExpr := expr.GetNullExpr()
	require.NotNil(t, nullExpr, "nullable bool IN [true, false] should be rewritten to IS NOT NULL")
	require.Equal(t, planpb.NullExpr_IsNotNull, nullExpr.GetOp())
}

func TestRewrite_Bool_In_SingleTrue_Nullable_ToEqual(t *testing.T) {
	// For nullable bool, in [true] should still become == true
	helper := buildSchemaHelperForRewriteNullableT(t)
	expr, err := parser.ParseExpr(helper, `NullableBoolField in [true]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "nullable bool IN [true] should be rewritten to == true")
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, true, ure.GetValue().GetBoolVal())
}

func TestRewrite_Bool_NotIn_TrueFalse_ToAlwaysFalse(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// not in [true, false] on non-nullable → nothing can match → AlwaysFalse
	expr, err := parser.ParseExpr(helper, `BoolField not in [true,false]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr),
		"bool NOT IN [true, false] should be rewritten to AlwaysFalseExpr")
}

func TestRewrite_Bool_NotIn_TrueFalse_Nullable_ToIsNull(t *testing.T) {
	helper := buildSchemaHelperForRewriteNullableT(t)
	// not in [true, false] on nullable → only NULL matches → IS NULL
	expr, err := parser.ParseExpr(helper, `NullableBoolField not in [true,false]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	nullExpr := expr.GetNullExpr()
	require.NotNil(t, nullExpr, "nullable bool NOT IN [true, false] should be rewritten to IS NULL")
	require.Equal(t, planpb.NullExpr_IsNull, nullExpr.GetOp())
}

func TestRewrite_Bool_NotIn_SingleTrue_ToNotEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// not in [true] → != true
	expr, err := parser.ParseExpr(helper, `BoolField not in [true]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "bool NOT IN [true] should be rewritten to != true")
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, true, ure.GetValue().GetBoolVal())
}

func TestRewrite_Bool_NotIn_SingleFalse_ToNotEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// not in [false] → != false
	expr, err := parser.ParseExpr(helper, `BoolField not in [false]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "bool NOT IN [false] should be rewritten to != false")
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, false, ure.GetValue().GetBoolVal())
}

func TestRewrite_Flatten_Then_OR_ToIN(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// nested OR-equals should flatten and merge to IN
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" or (VarCharField == "b" or VarCharField == "c") or VarCharField == "d"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "nested OR-equals should flatten and merge to IN")
	require.Equal(t, 4, len(term.GetValues()))
	got := []string{
		term.GetValues()[0].GetStringVal(),
		term.GetValues()[1].GetStringVal(),
		term.GetValues()[2].GetStringVal(),
		term.GetValues()[3].GetStringVal(),
	}
	require.ElementsMatch(t, []string{"a", "b", "c", "d"}, got)
}

// --- combine tests ---

func TestRewrite_And_In_And_Equal_VInSet_ReducesToEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field == 3`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(3), ure.GetValue().GetInt64Val())
}

func TestRewrite_And_In_And_Equal_VNotInSet_False(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field == 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Or_In_Or_Equal_Union(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] or Int64Field == 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 11, len(term.GetValues()))
}

func TestRewrite_And_In_With_Range_Filter(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field > 8`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
	require.Equal(t, int64(9), term.GetValues()[0].GetInt64Val())
	require.Equal(t, int64(10), term.GetValues()[1].GetInt64Val())
}

func TestRewrite_Or_In_Union(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] or Int64Field in [10,11,12,13,14,15,16,17,18,19]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 19, len(term.GetValues()))
}

func TestRewrite_And_In_Intersection(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field in [5,6,7,8,9,10,11,12,13,14]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 6, len(term.GetValues()))
}

func TestRewrite_And_In_Intersection_Empty_ToFalse(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field in [11,12,13,14,15,16,17,18,19,20]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_And_In_And_NotEqual_Remove(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field != 5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 9, len(term.GetValues()))
}

func TestRewrite_And_In_And_NotEqual_AllRemoved_ToFalse(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// in [10 values] and != each of them → false
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] and Int64Field != 1 and Int64Field != 2 and Int64Field != 3 and Int64Field != 4 and Int64Field != 5 and Int64Field != 6 and Int64Field != 7 and Int64Field != 8 and Int64Field != 9 and Int64Field != 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Or_In_Or_NotEqual_VInSet_ToTrue(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] or Int64Field != 5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr),
		"OR(IN, !=) tautology should be rewritten to AlwaysTrueExpr")
}

func TestRewrite_Or_In_Or_NotEqual_VarChar_Tautology(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField in ["", "a", "b"] or VarCharField != ""`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysTrueExpr(expr),
		"OR(IN, !=) tautology with VarChar should be rewritten to AlwaysTrueExpr")
}

func TestRewrite_Or_In_Or_NotEqual_VNotInSet_ToNotEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10] or Int64Field != 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
}

// Test contradictory equals: (a == 1) AND (a == 2) → false
// NOTE: This is a known limitation - currently NOT optimized
func TestRewrite_And_Equal_And_Equal_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

func TestRewrite_And_Equal_ThreeWay_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2 and Int64Field == 3`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND chain (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

func TestRewrite_And_Range_And_Equal_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	_ = expr
}

func TestRewrite_And_Range_And_Equal_NonContradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 15`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

func TestRewrite_And_Equal_String_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField == "apple" and VarCharField == "banana"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

func buildSchemaWithTimestamptz(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 102, Name: "ts", DataType: schemapb.DataType_Timestamptz},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "timestamptz_test",
		AutoID: false,
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// findTermExpr recursively checks if any node in the plan tree is a TermExpr.
func findTermExpr(expr *planpb.Expr) *planpb.TermExpr {
	if expr == nil {
		return nil
	}
	if te := expr.GetTermExpr(); te != nil {
		return te
	}
	if be := expr.GetBinaryExpr(); be != nil {
		if found := findTermExpr(be.GetLeft()); found != nil {
			return found
		}
		return findTermExpr(be.GetRight())
	}
	if ue := expr.GetUnaryExpr(); ue != nil {
		return findTermExpr(ue.GetChild())
	}
	return nil
}

// TestTimestamptz_NotEqual_InAndContext verifies that a single != on a
// Timestamptz field inside an AND expression stays as UnaryRangeExpr.
// combineAndNotEqualsToNotIn requires 2+ values to merge into NOT(IN),
// so a single != is left as-is.
func TestTimestamptz_NotEqual_InAndContext(t *testing.T) {
	helper := buildSchemaWithTimestamptz(t)

	// This is the exact pattern from the failing e2e test:
	// pk_range AND ts != ISO '...'
	expr, err := parser.ParseExpr(helper,
		`id >= 30 and id <= 35 and ts != ISO '9999-12-31T23:46:05Z'`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Single != should NOT be merged into TermExpr.
	te := findTermExpr(expr)
	require.Nil(t, te,
		"single Timestamptz != should remain as UnaryRangeExpr, not be merged into TermExpr")
}

// TestTimestamptz_NotEqual_Standalone verifies a standalone != on Timestamptz
// stays as UnaryRangeExpr (no AND context means combineAndNotEqualsToNotIn
// is not triggered).
func TestTimestamptz_NotEqual_Standalone(t *testing.T) {
	helper := buildSchemaWithTimestamptz(t)

	expr, err := parser.ParseExpr(helper,
		`ts != ISO '2025-01-01T00:00:00Z'`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "standalone != should be UnaryRangeExpr")
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, schemapb.DataType_Timestamptz, ure.GetColumnInfo().GetDataType())
}

// TestTimestamptz_MultipleNotEquals_BecomesTermExpr verifies that multiple
// != on the same Timestamptz field in an AND context are merged into a
// single NOT(TermExpr). This confirms that C++ TermExpr.cpp must support
// TIMESTAMPTZ (which we added in this PR).
func TestTimestamptz_MultipleNotEquals_BecomesTermExpr(t *testing.T) {
	helper := buildSchemaWithTimestamptz(t)

	// Use 2 != on ts (same field) in an AND.
	// Both != should be merged into NOT(IN [v1, v2]).
	expr, err := parser.ParseExpr(helper,
		`ts != ISO '2025-01-01T00:00:00Z' and ts != ISO '2025-06-01T00:00:00Z'`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Should be rewritten to NOT(TermExpr) — which requires C++ TIMESTAMPTZ support.
	te := findTermExpr(expr)
	require.NotNil(t, te,
		"Timestamptz != should be merged into NOT(TermExpr); "+
			"C++ TermExpr.cpp TIMESTAMPTZ case is required for this to work")
	require.Equal(t, schemapb.DataType_Timestamptz, te.GetColumnInfo().GetDataType())
	require.Len(t, te.GetValues(), 2)
}

// TestTimestamptz_InExpr_ProducesTermExpr verifies that an explicit IN
// expression on a Timestamptz field produces a TermExpr. Before the C++
// TIMESTAMPTZ fix, this would have crashed at query time even on master —
// it was just never tested because no e2e test used IN on Timestamptz
// and the parser doesn't support `IN [ISO '...']` syntax directly.
// Instead we test via multiple == OR that get merged into IN by the rewriter.
func TestTimestamptz_InExpr_ProducesTermExpr(t *testing.T) {
	helper := buildSchemaWithTimestamptz(t)

	// 2 == on the same Timestamptz field in OR -> merged to IN by combineOrEqualsToIn
	expr, err := parser.ParseExpr(helper,
		`ts == ISO '2025-01-01T00:00:00Z' or ts == ISO '2025-06-01T00:00:00Z'`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	te := findTermExpr(expr)
	require.NotNil(t, te,
		"OR of Timestamptz == should be merged into TermExpr (IN)")
	require.Equal(t, schemapb.DataType_Timestamptz, te.GetColumnInfo().GetDataType())
	require.Len(t, te.GetValues(), 2,
		"IN should contain both timestamp values")
}

// TestGeometryAndText_BlockedAtParser verifies that Geometry and Text fields
// cannot be used in term/comparison expressions. These types have no case in
// C++ TermExpr.cpp or UnaryExpr.cpp, so the parser must reject them.
func TestGeometryAndText_BlockedAtParser(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 102, Name: "geo", DataType: schemapb.DataType_Geometry},
		{FieldID: 103, Name: "txt", DataType: schemapb.DataType_Text},
	}
	schema := &schemapb.CollectionSchema{Name: "block_test", Fields: fields}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	// Geometry: == and IN should be rejected at parser level
	_, err = parser.ParseExpr(helper, `geo == "POINT(1 2)"`, nil)
	require.Error(t, err, "Geometry == should be rejected by parser")

	_, err = parser.ParseExpr(helper, `geo in ["POINT(1 2)", "POINT(3 4)"]`, nil)
	require.Error(t, err, "Geometry IN should be rejected by parser")

	// Text: any filter expression should be rejected at parser level
	_, err = parser.ParseExpr(helper, `txt == "hello"`, nil)
	require.Error(t, err, "Text == should be rejected by parser")

	_, err = parser.ParseExpr(helper, `txt in ["hello", "world"]`, nil)
	require.Error(t, err, "Text IN should be rejected by parser")

	_, err = parser.ParseExpr(helper, `txt != "hello"`, nil)
	require.Error(t, err, "Text != should be rejected by parser")
}
