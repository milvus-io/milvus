package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func buildSchemaHelperForPhase2Test(t *testing.T) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "category", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "int8_field", DataType: schemapb.DataType_Int8},
		{FieldID: 104, Name: "int16_field", DataType: schemapb.DataType_Int16},
		{FieldID: 105, Name: "int32_field", DataType: schemapb.DataType_Int32},
		{FieldID: 106, Name: "status", DataType: schemapb.DataType_Int64},
	}
	schema := &schemapb.CollectionSchema{
		Name:   "phase2_test",
		Fields: fields,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func TestRewrite_IntegerFullDomainRange_Int8(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// int8 >= -128 and int8 <= 127 is full domain tautology for non-nullable int8.
	// When combined with category == 1, the int8 range should be folded to AlwaysTrue and dropped.
	expr, err := parser.ParseExpr(helper, "category == 1 and (int8_field >= -128 and int8_field <= 127)", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	// The result should be purely category == 1 with NO binary expr or and nodes
	require.Nil(t, expr.GetBinaryExpr(), "AND node should be eliminated when right operand is full domain")
	ur := expr.GetUnaryRangeExpr()
	require.NotNil(t, ur, "should fold to single UnaryRangeExpr")
	require.Equal(t, int64(101), ur.GetColumnInfo().GetFieldId())
	require.Equal(t, int64(1), ur.GetValue().GetInt64Val())
}

func TestRewrite_IntegerFullDomainRange_Int16(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	expr, err := parser.ParseExpr(helper, "category == 1 and (int16_field >= -32768 and int16_field <= 32767)", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	require.Nil(t, expr.GetBinaryExpr())
	ur := expr.GetUnaryRangeExpr()
	require.NotNil(t, ur)
	require.Equal(t, int64(101), ur.GetColumnInfo().GetFieldId())
}

func TestRewrite_IntegerFullDomainRange_Int32(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	expr, err := parser.ParseExpr(helper, "category == 1 and (int32_field >= -2147483648 and int32_field <= 2147483647)", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	require.Nil(t, expr.GetBinaryExpr())
	ur := expr.GetUnaryRangeExpr()
	require.NotNil(t, ur)
	require.Equal(t, int64(101), ur.GetColumnInfo().GetFieldId())
}

func TestRewrite_IntegerDomainContradiction(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// int8 > 127 is impossible -> AlwaysFalse
	expr, err := parser.ParseExpr(helper, "category == 1 and int8_field > 127", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr), "should fold to AlwaysFalse")

	// int8 < -128 is impossible -> AlwaysFalse
	expr2, err2 := parser.ParseExpr(helper, "category == 1 and int8_field < -128", nil)
	require.NoError(t, err2)
	require.NotNil(t, expr2)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr2), "should fold to AlwaysFalse")
}

func TestRewrite_SelectivityOrdering_EqualityBeforeRange(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// Input has wide range first, equality second: (10 <= age <= 100) and category == 1
	expr, err := parser.ParseExpr(helper, "(10 <= age <= 100) and category == 1", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	binary := expr.GetBinaryExpr()
	require.NotNil(t, binary, "should produce BinaryExpr for two different conditions")

	// Left operand MUST be category == 1 (higher selectivity)
	leftUR := binary.GetLeft().GetUnaryRangeExpr()
	require.NotNil(t, leftUR, "Left should be equality expression")
	require.Equal(t, int64(101), leftUR.GetColumnInfo().GetFieldId())

	// Right operand should be the range expression
	rightBR := binary.GetRight().GetBinaryRangeExpr()
	require.NotNil(t, rightBR, "Right should be range expression")
	require.Equal(t, int64(102), rightBR.GetColumnInfo().GetFieldId())
}

func TestRewrite_SelectivityOrdering_EqualityBeforeNegation(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// Input has negation first, equality second: status != 5 and category == 1
	expr, err := parser.ParseExpr(helper, "status != 5 and category == 1", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	binary := expr.GetBinaryExpr()
	require.NotNil(t, binary)

	// Left operand MUST be category == 1 (equality has higher selectivity than negation)
	leftUR := binary.GetLeft().GetUnaryRangeExpr()
	require.NotNil(t, leftUR)
	require.Equal(t, int64(101), leftUR.GetColumnInfo().GetFieldId())

	// Right operand should be status != 5
	rightUR := binary.GetRight().GetUnaryRangeExpr()
	require.NotNil(t, rightUR)
	require.Equal(t, int64(106), rightUR.GetColumnInfo().GetFieldId())
}

func TestRewrite_LiteralAndTrueIdentity_Elimination(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// Case 1: Simple F and true
	expr1, err1 := parser.ParseExpr(helper, "category == 1 and true", nil)
	require.NoError(t, err1)
	require.Nil(t, expr1.GetBinaryExpr(), "binary AND node must be eliminated for literal true")
	ur1 := expr1.GetUnaryRangeExpr()
	require.NotNil(t, ur1)
	require.Equal(t, int64(101), ur1.GetColumnInfo().GetFieldId())

	// Case 2: 128x literal 'and true' chain: F and true and true ...
	clause := "category == 1"
	for i := 0; i < 128; i++ {
		clause += " and true"
	}
	expr128, err128 := parser.ParseExpr(helper, clause, nil)
	require.NoError(t, err128)
	require.Nil(t, expr128.GetBinaryExpr(), "128x literal true must all be eliminated leaving only root predicate")
	ur128 := expr128.GetUnaryRangeExpr()
	require.NotNil(t, ur128)
	require.Equal(t, int64(101), ur128.GetColumnInfo().GetFieldId())
	require.Equal(t, int64(1), ur128.GetValue().GetInt64Val())
}
