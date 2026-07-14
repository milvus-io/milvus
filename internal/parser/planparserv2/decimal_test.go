package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// DecimalField in the shared test schema is Decimal(18, 4) (see newTestSchema),
// so "19.99" must always encode to unscaled 199900.

func TestDecimalEquality(t *testing.T) {
	helper := newTestSchemaHelper(t)

	expr, err := ParseExpr(helper, `DecimalField == 19.99`, nil)
	require.NoError(t, err)
	unary := expr.GetUnaryRangeExpr()
	require.NotNil(t, unary)
	assert.Equal(t, schemapb.DataType_Decimal, unary.GetColumnInfo().GetDataType())
	assert.EqualValues(t, 199900, unary.GetValue().GetInt64Val())

	// Reversed operand order must produce the same exact unscaled value.
	expr, err = ParseExpr(helper, `19.99 == DecimalField`, nil)
	require.NoError(t, err)
	unary = expr.GetUnaryRangeExpr()
	require.NotNil(t, unary)
	assert.EqualValues(t, 199900, unary.GetValue().GetInt64Val())

	// Whole-number literal against a scale=4 field must still be scaled, not passed through raw.
	expr, err = ParseExpr(helper, `DecimalField == 20`, nil)
	require.NoError(t, err)
	unary = expr.GetUnaryRangeExpr()
	require.NotNil(t, unary)
	assert.EqualValues(t, 200000, unary.GetValue().GetInt64Val())

	// Negative literal.
	expr, err = ParseExpr(helper, `DecimalField == -19.99`, nil)
	require.NoError(t, err)
	unary = expr.GetUnaryRangeExpr()
	require.NotNil(t, unary)
	assert.EqualValues(t, -199900, unary.GetValue().GetInt64Val())
}

func TestDecimalRelational(t *testing.T) {
	helper := newTestSchemaHelper(t)

	expr, err := ParseExpr(helper, `DecimalField > 10.5`, nil)
	require.NoError(t, err)
	unary := expr.GetUnaryRangeExpr()
	require.NotNil(t, unary)
	assert.Equal(t, planpb.OpType_GreaterThan, unary.GetOp())
	assert.EqualValues(t, 105000, unary.GetValue().GetInt64Val())

	// Literal on the left must reverse the operator and still scale exactly.
	expr, err = ParseExpr(helper, `10.5 < DecimalField`, nil)
	require.NoError(t, err)
	unary = expr.GetUnaryRangeExpr()
	require.NotNil(t, unary)
	assert.EqualValues(t, 105000, unary.GetValue().GetInt64Val())
}

func TestDecimalRange(t *testing.T) {
	helper := newTestSchemaHelper(t)

	expr, err := ParseExpr(helper, `10 < DecimalField < 100.25`, nil)
	require.NoError(t, err)
	rangeExpr := expr.GetBinaryRangeExpr()
	require.NotNil(t, rangeExpr)
	assert.EqualValues(t, 100000, rangeExpr.GetLowerValue().GetInt64Val())
	assert.EqualValues(t, 1002500, rangeExpr.GetUpperValue().GetInt64Val())

	// Reverse-range form: "100.25 > DecimalField > 10"
	expr, err = ParseExpr(helper, `100.25 > DecimalField > 10`, nil)
	require.NoError(t, err)
	rangeExpr = expr.GetBinaryRangeExpr()
	require.NotNil(t, rangeExpr)
	assert.EqualValues(t, 100000, rangeExpr.GetLowerValue().GetInt64Val())
	assert.EqualValues(t, 1002500, rangeExpr.GetUpperValue().GetInt64Val())
}

func TestDecimalTermExpr(t *testing.T) {
	helper := newTestSchemaHelper(t)

	expr, err := ParseExpr(helper, `DecimalField in [19.99, 29.99, 30]`, nil)
	require.NoError(t, err)
	termExpr := expr.GetTermExpr()
	require.NotNil(t, termExpr)
	require.Len(t, termExpr.GetValues(), 3)
	assert.EqualValues(t, 199900, termExpr.GetValues()[0].GetInt64Val())
	assert.EqualValues(t, 299900, termExpr.GetValues()[1].GetInt64Val())
	assert.EqualValues(t, 300000, termExpr.GetValues()[2].GetInt64Val())
}

func TestDecimalInvalidLiterals(t *testing.T) {
	helper := newTestSchemaHelper(t)

	// Exceeds the field's scale (4).
	assertInvalidExpr(t, helper, `DecimalField == 1.123456`)
	// Malformed decimal syntax (not a plain digit/dot literal).
	assertInvalidExpr(t, helper, `DecimalField == 1e5`)
	// Range bound exceeding scale.
	assertInvalidExpr(t, helper, `1.123456 < DecimalField < 100`)
	// IN-list element exceeding scale.
	assertInvalidExpr(t, helper, `DecimalField in [1.123456, 2]`)
}

func TestDecimalValidExpr(t *testing.T) {
	helper := newTestSchemaHelper(t)
	exprStrs := []string{
		`DecimalField == 19.99`,
		`DecimalField != 19.99`,
		`DecimalField > 10.5`,
		`DecimalField >= 10.5`,
		`DecimalField < 10.5`,
		`DecimalField <= 10.5`,
		`10 < DecimalField < 100`,
		`DecimalField in [19.99, 29.99]`,
		`DecimalField not in [19.99, 29.99]`,
		`DecimalField is null`,
		`DecimalField is not null`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}
