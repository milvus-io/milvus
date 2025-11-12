package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func buildSchemaHelperWithJSON(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "Int64Field", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "JSONField", DataType: schemapb.DataType_JSON},
		{FieldID: 103, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
	}
	schema := &schemapb.CollectionSchema{
		Name:               "rewrite_json_test",
		AutoID:             false,
		Fields:             fields,
		EnableDynamicField: true,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// Test JSON field with int comparison - AND tightening
func TestRewrite_JSON_Int_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["price"] > 10 and JSONField["price"] > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "should merge to single unary range")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
	// Verify column info
	require.Equal(t, schemapb.DataType_JSON, ure.GetColumnInfo().GetDataType())
	require.Equal(t, []string{"price"}, ure.GetColumnInfo().GetNestedPath())
}

// Test JSON field with int comparison - AND to BinaryRange
func TestRewrite_JSON_Int_AND_ToBinaryRange(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["price"] > 10 and JSONField["price"] < 50`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre, "should create binary range")
	require.Equal(t, false, bre.GetLowerInclusive())
	require.Equal(t, false, bre.GetUpperInclusive())
	require.Equal(t, int64(10), bre.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(50), bre.GetUpperValue().GetInt64Val())
	// Verify column info
	require.Equal(t, schemapb.DataType_JSON, bre.GetColumnInfo().GetDataType())
	require.Equal(t, []string{"price"}, bre.GetColumnInfo().GetNestedPath())
}

// Test JSON field with float comparison - AND tightening with mixed int/float
func TestRewrite_JSON_Float_AND_Strengthen_Mixed(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["score"] > 10 and JSONField["score"] > 15.5`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.InDelta(t, 15.5, ure.GetValue().GetFloatVal(), 1e-9)
}

// Test JSON field with string comparison - AND tightening
func TestRewrite_JSON_String_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["name"] > "alice" and JSONField["name"] > "bob"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, "bob", ure.GetValue().GetStringVal())
}

// Test JSON field with string comparison - AND to BinaryRange
func TestRewrite_JSON_String_AND_ToBinaryRange(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["name"] >= "alice" and JSONField["name"] <= "zebra"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, true, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.Equal(t, "alice", bre.GetLowerValue().GetStringVal())
	require.Equal(t, "zebra", bre.GetUpperValue().GetStringVal())
}

// Test JSON field with OR - weakening lower bounds
func TestRewrite_JSON_Int_OR_Weaken(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["age"] > 10 or JSONField["age"] > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// Test JSON field with OR - weakening upper bounds
func TestRewrite_JSON_String_OR_Weaken_Upper(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["category"] < "electronics" or JSONField["category"] < "sports"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_LessThan, ure.GetOp())
	require.Equal(t, "sports", ure.GetValue().GetStringVal())
}

// Test JSON field with numeric types (int and float) - SHOULD merge
func TestRewrite_JSON_NumericTypes_Merge(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// JSONField["value"] > 10.5 (float) and JSONField["value"] > 20 (int)
	// Numeric types should merge - both treated as Double
	expr, err := parser.ParseExpr(helper, `JSONField["value"] > 10.5 and JSONField["value"] > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure, "numeric types should merge to single predicate")
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	// Should pick the stronger bound (20)
	switch ure.GetValue().GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
	case *planpb.GenericValue_FloatVal:
		require.InDelta(t, 20.0, ure.GetValue().GetFloatVal(), 1e-9)
	default:
		t.Fatalf("unexpected value type")
	}
}

// Test JSON field with mixed type categories - should NOT merge
// Note: Numeric types (int and float) ARE compatible, but numeric and string are not
func TestRewrite_JSON_MixedTypes_NoMerge(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// JSONField["value"] > 10 (numeric) and JSONField["value"] > "hello" (string)
	// These should remain separate as they have different type categories
	expr, err := parser.ParseExpr(helper, `JSONField["value"] > 10 and JSONField["value"] > "hello"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as binary expr (AND)")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
	// Both sides should be UnaryRangeExpr
	require.NotNil(t, be.GetLeft().GetUnaryRangeExpr())
	require.NotNil(t, be.GetRight().GetUnaryRangeExpr())
}

// Test JSON field with mixed type categories in OR - should NOT merge
func TestRewrite_JSON_MixedTypes_OR_NoMerge(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["data"] > 100 or JSONField["data"] > "text"`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "should remain as binary expr (OR)")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, be.GetOp())
}

// Test dynamic field ($meta) - same as JSON field
func TestRewrite_DynamicField_Int_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// $meta is the dynamic field
	expr, err := parser.ParseExpr(helper, `$meta["count"] > 5 and $meta["count"] > 15`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(15), ure.GetValue().GetInt64Val())
}

// Test dynamic field - AND to BinaryRange
func TestRewrite_DynamicField_Float_AND_ToBinaryRange(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `$meta["rating"] >= 1.0 and $meta["rating"] <= 5.0`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	bre := expr.GetBinaryRangeExpr()
	require.NotNil(t, bre)
	require.Equal(t, true, bre.GetLowerInclusive())
	require.Equal(t, true, bre.GetUpperInclusive())
	require.InDelta(t, 1.0, bre.GetLowerValue().GetFloatVal(), 1e-9)
	require.InDelta(t, 5.0, bre.GetUpperValue().GetFloatVal(), 1e-9)
}

// Test different nested paths - should NOT merge
func TestRewrite_JSON_DifferentPaths_NoMerge(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["a"] > 10 and JSONField["b"] > 20`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	be := expr.GetBinaryExpr()
	require.NotNil(t, be, "different paths should not merge")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())
}

// Test nested JSON path
func TestRewrite_JSON_NestedPath_AND_Strengthen(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `JSONField["user"]["age"] > 18 and JSONField["user"]["age"] > 21`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(21), ure.GetValue().GetInt64Val())
	require.Equal(t, []string{"user", "age"}, ure.GetColumnInfo().GetNestedPath())
}

// Test inclusive vs exclusive bounds
func TestRewrite_JSON_AND_EquivalentBounds(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// JSONField["x"] >= 10 AND JSONField["x"] > 10 → JSONField["x"] > 10 (exclusive is stronger)
	expr, err := parser.ParseExpr(helper, `JSONField["x"] >= 10 and JSONField["x"] > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterThan, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// Test OR with inclusive bounds preference
func TestRewrite_JSON_OR_EquivalentBounds(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	// JSONField["x"] >= 10 OR JSONField["x"] > 10 → JSONField["x"] >= 10 (inclusive is weaker)
	expr, err := parser.ParseExpr(helper, `JSONField["x"] >= 10 or JSONField["x"] > 10`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	ure := expr.GetUnaryRangeExpr()
	require.NotNil(t, ure)
	require.Equal(t, planpb.OpType_GreaterEqual, ure.GetOp())
	require.Equal(t, int64(10), ure.GetValue().GetInt64Val())
}

// Test that scalar field and JSON field don't interfere
func TestRewrite_JSON_And_Scalar_Independent(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper, `Int64Field > 10 and Int64Field > 20 and JSONField["price"] > 5 and JSONField["price"] > 15`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	// Should result in AND of two optimized predicates
	be := expr.GetBinaryExpr()
	require.NotNil(t, be)
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, be.GetOp())

	// Collect all predicates
	var predicates []*planpb.Expr
	var collect func(*planpb.Expr)
	collect = func(e *planpb.Expr) {
		if be := e.GetBinaryExpr(); be != nil && be.GetOp() == planpb.BinaryExpr_LogicalAnd {
			collect(be.GetLeft())
			collect(be.GetRight())
		} else {
			predicates = append(predicates, e)
		}
	}
	collect(expr)

	require.Equal(t, 2, len(predicates), "should have two optimized predicates")

	// Check that both are optimized to the stronger bounds
	var hasInt64, hasJSON bool
	for _, p := range predicates {
		if ure := p.GetUnaryRangeExpr(); ure != nil {
			col := ure.GetColumnInfo()
			if col.GetDataType() == schemapb.DataType_Int64 {
				hasInt64 = true
				require.Equal(t, int64(20), ure.GetValue().GetInt64Val())
			} else if col.GetDataType() == schemapb.DataType_JSON {
				hasJSON = true
				require.Equal(t, int64(15), ure.GetValue().GetInt64Val())
			}
		}
	}
	require.True(t, hasInt64, "should have optimized Int64Field predicate")
	require.True(t, hasJSON, "should have optimized JSONField predicate")
}
