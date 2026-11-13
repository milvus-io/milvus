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
		{FieldID: 107, Name: "nullable_int8_field", DataType: schemapb.DataType_Int8, Nullable: true},
		{FieldID: 108, Name: "nullable_int16_field", DataType: schemapb.DataType_Int16, Nullable: true},
		{FieldID: 109, Name: "nullable_int32_field", DataType: schemapb.DataType_Int32, Nullable: true},
		{FieldID: 110, Name: "nullable_int64_field", DataType: schemapb.DataType_Int64, Nullable: true},
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

func TestRewrite_OperandSourceOrderPreserved(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// Input has range first, equality second: (10 <= age <= 100) and category == 1
	// Preserving operand source order guarantees AST stability and caller contract expectations.
	expr, err := parser.ParseExpr(helper, "(10 <= age <= 100) and category == 1", nil)
	require.NoError(t, err)
	require.NotNil(t, expr)

	binary := expr.GetBinaryExpr()
	require.NotNil(t, binary, "should produce BinaryExpr for two different conditions")

	// Left operand should be the range expression, preserving source order
	leftBR := binary.GetLeft().GetBinaryRangeExpr()
	require.NotNil(t, leftBR, "Left should be range expression preserving source order")
	require.Equal(t, int64(102), leftBR.GetColumnInfo().GetFieldId())

	// Right operand should be category == 1
	rightUR := binary.GetRight().GetUnaryRangeExpr()
	require.NotNil(t, rightUR, "Right should be equality expression")
	require.Equal(t, int64(101), rightUR.GetColumnInfo().GetFieldId())
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

func TestRewrite_NullableIntegerDomainPreservation(t *testing.T) {
	helper := buildSchemaHelperForPhase2Test(t)

	// 1. Impossible upper bound: nullable_int8_field > 127
	// Must NOT fold to AlwaysFalseExpr, because NULL > 127 evaluates to UNKNOWN.
	expr1, err1 := parser.ParseExpr(helper, "nullable_int8_field > 127", nil)
	require.NoError(t, err1)
	require.NotNil(t, expr1)
	require.False(t, rewriter.IsAlwaysFalseExpr(expr1), "nullable > 127 must not fold to AlwaysFalse")
	require.NotNil(t, expr1.GetUnaryRangeExpr(), "predicate structure should be preserved")

	// 2. Impossible lower bound: nullable_int8_field < -128
	// Must NOT fold to AlwaysFalseExpr.
	expr2, err2 := parser.ParseExpr(helper, "nullable_int8_field < -128", nil)
	require.NoError(t, err2)
	require.NotNil(t, expr2)
	require.False(t, rewriter.IsAlwaysFalseExpr(expr2), "nullable < -128 must not fold to AlwaysFalse")
	require.NotNil(t, expr2.GetUnaryRangeExpr())

	// 3. NOT on impossible bound: not (nullable_int8_field > 127)
	// Under 3VL, NOT(UNKNOWN) = UNKNOWN. It must NOT fold to AlwaysTrueExpr.
	expr3, err3 := parser.ParseExpr(helper, "not (nullable_int8_field > 127)", nil)
	require.NoError(t, err3)
	require.NotNil(t, expr3)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr3), "not (nullable > 127) must not fold to AlwaysTrue")
	ue3 := expr3.GetUnaryExpr()
	require.NotNil(t, ue3, "NOT expression must be retained")

	// 4. Full domain range: nullable_int8_field >= -128 and nullable_int8_field <= 127
	// Must NOT fold to AlwaysTrueExpr, because NULL is neither >= -128 nor <= 127.
	expr4, err4 := parser.ParseExpr(helper, "nullable_int8_field >= -128 and nullable_int8_field <= 127", nil)
	require.NoError(t, err4)
	require.NotNil(t, expr4)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr4), "nullable full domain must not fold to AlwaysTrue")
	require.NotNil(t, expr4.GetBinaryRangeExpr(), "full-domain range should remain BinaryRangeExpr")

	// 5. Test with Int16, Int32, Int64 nullable types
	// Int16 impossible bound & NOT & full-domain
	expr16, err16 := parser.ParseExpr(helper, "nullable_int16_field > 32767", nil)
	require.NoError(t, err16)
	require.False(t, rewriter.IsAlwaysFalseExpr(expr16), "nullable Int16 > 32767 must not fold to AlwaysFalse")
	expr16Not, err16Not := parser.ParseExpr(helper, "not (nullable_int16_field > 32767)", nil)
	require.NoError(t, err16Not)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr16Not), "not (nullable Int16 > 32767) must not fold to AlwaysTrue")
	expr16Full, err16Full := parser.ParseExpr(helper, "nullable_int16_field >= -32768 and nullable_int16_field <= 32767", nil)
	require.NoError(t, err16Full)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr16Full), "nullable Int16 full domain must not fold to AlwaysTrue")

	// Int32 impossible bound & NOT & full-domain
	expr32, err32 := parser.ParseExpr(helper, "nullable_int32_field > 2147483647", nil)
	require.NoError(t, err32)
	require.False(t, rewriter.IsAlwaysFalseExpr(expr32), "nullable Int32 > 2147483647 must not fold to AlwaysFalse")
	expr32Not, err32Not := parser.ParseExpr(helper, "not (nullable_int32_field > 2147483647)", nil)
	require.NoError(t, err32Not)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr32Not), "not (nullable Int32 > 2147483647) must not fold to AlwaysTrue")
	expr32Full, err32Full := parser.ParseExpr(helper, "nullable_int32_field >= -2147483648 and nullable_int32_field <= 2147483647", nil)
	require.NoError(t, err32Full)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr32Full), "nullable Int32 full domain must not fold to AlwaysTrue")

	// Int64 impossible bound & NOT & full-domain
	// 9223372036854775807 is MaxInt64; > MaxInt64 is impossible in the Int64 domain.
	expr64, err64 := parser.ParseExpr(helper, "nullable_int64_field > 9223372036854775807", nil)
	require.NoError(t, err64)
	require.NotNil(t, expr64)
	require.False(t, rewriter.IsAlwaysFalseExpr(expr64), "nullable Int64 > MaxInt64 must not fold to AlwaysFalse")

	expr64Not, err64Not := parser.ParseExpr(helper, "not (nullable_int64_field > 9223372036854775807)", nil)
	require.NoError(t, err64Not)
	require.NotNil(t, expr64Not)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr64Not), "not (nullable Int64 > MaxInt64) must not fold to AlwaysTrue")

	expr64Full, err64Full := parser.ParseExpr(helper, "nullable_int64_field >= -9223372036854775808 and nullable_int64_field <= 9223372036854775807", nil)
	require.NoError(t, err64Full)
	require.NotNil(t, expr64Full)
	require.False(t, rewriter.IsAlwaysTrueExpr(expr64Full), "nullable Int64 full domain must not fold to AlwaysTrue")
}
