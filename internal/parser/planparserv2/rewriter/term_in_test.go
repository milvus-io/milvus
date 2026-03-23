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

// --- shouldUseInExpr threshold tests ---

func TestRewrite_OREquals_ToIN_VarChar_AboveThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// varchar threshold is 3, so 3 values should produce IN
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" or VarCharField == "b" or VarCharField == "c"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "3 varchar OR-equals should merge to IN (threshold=3)")
	require.Equal(t, 3, len(term.GetValues()))
}

func TestRewrite_OREquals_NotMerged_VarChar_BelowThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// varchar threshold is 3, so 2 values should NOT produce IN
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" or VarCharField == "b"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Nil(t, expr.GetTermExpr(), "2 varchar OR-equals should not merge to IN")
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

func TestRewrite_OREquals_NotMerged_Int_BelowThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// int threshold is 10, so 2 values should NOT merge
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 or Int64Field == 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Nil(t, expr.GetTermExpr(), "numeric OR-equals should not merge to IN under threshold")
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
	require.NotNil(t, be.GetLeft().GetUnaryRangeExpr())
	require.NotNil(t, be.GetRight().GetUnaryRangeExpr())
	require.Equal(t, planpb.OpType_Equal, be.GetLeft().GetUnaryRangeExpr().GetOp())
	require.Equal(t, planpb.OpType_Equal, be.GetRight().GetUnaryRangeExpr().GetOp())
}

func TestRewrite_OREquals_Merged_Int_AtThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// int threshold is 10, so exactly 10 values should merge
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 or Int64Field == 2 or Int64Field == 3 or Int64Field == 4 or Int64Field == 5 or Int64Field == 6 or Int64Field == 7 or Int64Field == 8 or Int64Field == 9 or Int64Field == 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "10 int OR-equals should merge to IN (threshold=10)")
	require.Equal(t, 10, len(term.GetValues()))
}

// --- in → == or split tests ---

func TestRewrite_InSplit_Int_BelowThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// int in [1,2,3] → 3 values < 10 → split to == or
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Nil(t, expr.GetTermExpr(), "int in with 3 values should be split to == or")
	// Should be an OR tree
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

func TestRewrite_InSplit_Int_SingleValue(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// int in [5] → split to == 5
	expr, err := parser.ParseExpr(helper, `Int64Field in [5]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "in [single] should become ==")
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_InKept_Int_AboveThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3,4,5,6,7,8,9,10]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "int in with 10 values should stay as IN")
	require.Equal(t, 10, len(term.GetValues()))
}

// --- not in split tests ---

func TestRewrite_NotInSplit_Int_BelowThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// not in [3,4] → 2 values < 10 → split to != 3 AND != 4
	expr, err := parser.ParseExpr(helper, `Int64Field not in [4,3]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// Should be AND tree of !=
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "not in with 2 int values should split to != AND !=")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
	require.Equal(t, planpb.OpType_NotEqual, be.GetLeft().GetUnaryRangeExpr().GetOp())
	require.Equal(t, planpb.OpType_NotEqual, be.GetRight().GetUnaryRangeExpr().GetOp())
}

func TestRewrite_NotInSplit_Int_SingleValue(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field not in [5]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "not in [single] should become !=")
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, int64(5), ure.GetValue().GetInt64Val())
}

func TestRewrite_NotInSplit_Float_BelowThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// float threshold is 15, so 2 values → split
	expr, err := parser.ParseExpr(helper, `FloatField not in [4.0,3.0]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "not in with 2 float values should split to != AND !=")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// --- sort/dedup tests (use values above threshold) ---

func TestRewrite_Term_SortAndDedup_String(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// varchar threshold is 3, use 3+ unique values
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
	// int threshold is 10, use 10+ unique values
	expr, err := parser.ParseExpr(helper, `Int64Field in [10,9,4,6,6,7,1,2,3,5,8]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 10, len(term.GetValues()))
}

func TestRewrite_In_SortAndDedup_Bool(t *testing.T) {
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

func TestRewrite_Flatten_Then_OR_ToIN(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// varchar threshold is 3, so 4 values should merge
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

// --- combine tests (use values above threshold to keep IN form) ---

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
