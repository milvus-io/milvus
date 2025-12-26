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

func TestRewrite_OREquals_ToIN_NonNumeric(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField == "a" or VarCharField == "b"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "expected OR-equals to be rewritten to TermExpr(IN ...)")
	require.Equal(t, 2, len(term.GetValues()))
	require.Equal(t, "a", term.GetValues()[0].GetStringVal())
	require.Equal(t, "b", term.GetValues()[1].GetStringVal())
}

func TestRewrite_OREquals_NotMerged_OnNumericBelowThreshold(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
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

func TestRewrite_Term_SortAndDedup_String(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `VarCharField in ["b","a","b","a"]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
	require.Equal(t, "a", term.GetValues()[0].GetStringVal())
	require.Equal(t, "b", term.GetValues()[1].GetStringVal())
}

func TestRewrite_Term_SortAndDedup_Int(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [9,4,6,6,7]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 4, len(term.GetValues()))
	got := []int64{
		term.GetValues()[0].GetInt64Val(),
		term.GetValues()[1].GetInt64Val(),
		term.GetValues()[2].GetInt64Val(),
		term.GetValues()[3].GetInt64Val(),
	}
	require.ElementsMatch(t, []int64{4, 6, 7, 9}, got)
}

func TestRewrite_NotIn_SortAndDedup_Int(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field not in [4,4,3]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	un := expr.GetUnaryExpr()
	require.NotNil(t, un)
	term := un.GetChild().GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
	require.Equal(t, int64(3), term.GetValues()[0].GetInt64Val())
	require.Equal(t, int64(4), term.GetValues()[1].GetInt64Val())
}

func TestRewrite_NotIn_SortAndDedup_Float(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `FloatField not in [4.0,4,3.0]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	un := expr.GetUnaryExpr()
	require.NotNil(t, un)
	term := un.GetChild().GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
	require.Equal(t, 3.0, term.GetValues()[0].GetFloatVal())
	require.Equal(t, 4.0, term.GetValues()[1].GetFloatVal())
}

func TestRewrite_In_SortAndDedup_Bool(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `BoolField in [true,false,false,true]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 2, len(term.GetValues()))
	require.Equal(t, false, term.GetValues()[0].GetBoolVal())
	require.Equal(t, true, term.GetValues()[1].GetBoolVal())
}

func TestRewrite_Flatten_Then_OR_ToIN(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
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

func TestRewrite_And_In_And_Equal_VInSet_ReducesToEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,3,5] and Int64Field == 3`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_Equal, ure.GetOp())
	require.Equal(t, int64(3), ure.GetValue().GetInt64Val())
}

func TestRewrite_And_In_And_Equal_VNotInSet_False(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,3,5] and Int64Field == 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Or_In_Or_Equal_Union(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,3] or Int64Field == 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, []int64{1, 2, 3}, []int64{
		term.GetValues()[0].GetInt64Val(),
		term.GetValues()[1].GetInt64Val(),
		term.GetValues()[2].GetInt64Val(),
	})
}

func TestRewrite_And_In_With_Range_Filter(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,3,5] and Int64Field > 3`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, 1, len(term.GetValues()))
	require.Equal(t, int64(5), term.GetValues()[0].GetInt64Val())
}

func TestRewrite_Or_In_Union(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,3] or Int64Field in [3,4]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, []int64{1, 3, 4}, []int64{
		term.GetValues()[0].GetInt64Val(),
		term.GetValues()[1].GetInt64Val(),
		term.GetValues()[2].GetInt64Val(),
	})
}

func TestRewrite_And_In_Intersection(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3] and Int64Field in [2,3,4]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, []int64{2, 3}, []int64{
		term.GetValues()[0].GetInt64Val(),
		term.GetValues()[1].GetInt64Val(),
	})
}

func TestRewrite_And_In_Intersection_Empty_ToFalse(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1] and Int64Field in [2]`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_And_In_And_NotEqual_Remove(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2,3] and Int64Field != 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	term := expr.GetTermExpr()
	require.NotNil(t, term)
	require.Equal(t, []int64{1, 3}, []int64{
		term.GetValues()[0].GetInt64Val(),
		term.GetValues()[1].GetInt64Val(),
	})
}

func TestRewrite_And_In_And_NotEqual_AllRemoved_ToFalse(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [2] and Int64Field != 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestRewrite_Or_In_Or_NotEqual_VInSet_ToTrue(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1,2] or Int64Field != 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	val := expr.GetValueExpr()
	require.NotNil(t, val)
	require.Equal(t, true, val.GetValue().GetBoolVal())
}

func TestRewrite_Or_In_Or_NotEqual_VNotInSet_ToNotEqual(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	expr, err := parser.ParseExpr(helper, `Int64Field in [1] or Int64Field != 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_NotEqual, ure.GetOp())
	require.Equal(t, int64(2), ure.GetValue().GetInt64Val())
}

// Test contradictory equals: (a == 1) AND (a == 2) → false
// NOTE: This is a known limitation - currently NOT optimized
func TestRewrite_And_Equal_And_Equal_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field == 1 AND Int64Field == 2 → false (contradiction)
	// Currently NOT optimized because equals don't convert to IN in AND context
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: NOT optimized to false
	// Remains as BinaryExpr AND with two equal predicates
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// Test contradictory equals with three values
func TestRewrite_And_Equal_ThreeWay_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field == 1 AND Int64Field == 2 AND Int64Field == 3 → false
	// Currently NOT optimized
	expr, err := parser.ParseExpr(helper, `Int64Field == 1 and Int64Field == 2 and Int64Field == 3`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: NOT optimized to false
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND chain (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// Test range + contradictory equal: (a > 10) AND (a == 5) → false
// NOTE: Current limitation - NOT optimized
func TestRewrite_And_Range_And_Equal_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field > 10 AND Int64Field == 5 → false (5 is not > 10)
	// This requires combining range and equality checks, which is partially implemented
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current behavior: may not optimize to false
	// If it does optimize via IN+range filtering, it would become false
	// This test documents current limitation
	// When fully optimized, should be constant false
	_ = expr // Test documents that this case exists
}

// Test non-contradictory range + equal: (a > 10) AND (a == 15) → stays as is
// NOTE: This requires Equal to be in IN form first, which happens via combineAndInWithEqual
// But Equal alone doesn't convert to IN in AND context, so this optimization doesn't happen
func TestRewrite_And_Range_And_Equal_NonContradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// Int64Field > 10 AND Int64Field == 15 → should simplify to Int64Field == 15
	// But currently NOT optimized without IN involved
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field == 15`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: remains as AND with range and equal
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// Test string contradictory equals - current limitation
func TestRewrite_And_Equal_String_Contradiction_CurrentLimitation(t *testing.T) {
	helper := buildSchemaHelperForRewriteT(t)
	// VarCharField == "apple" AND VarCharField == "banana" → false
	// Currently NOT optimized
	expr, err := parser.ParseExpr(helper, `VarCharField == "apple" and VarCharField == "banana"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// Current limitation: NOT optimized to false
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as AND (not optimized)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}
