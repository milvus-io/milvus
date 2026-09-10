package rewriter_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func edgeTestRangeColumn(dataType schemapb.DataType) *planpb.ColumnInfo {
	column := &planpb.ColumnInfo{
		FieldId:  101,
		DataType: dataType,
	}
	if dataType == schemapb.DataType_JSON {
		column.NestedPath = []string{"value"}
	}
	return column
}

func edgeTestIntValue(value int64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: value}}
}

func edgeTestFloatValue(value float64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: value}}
}

func edgeTestStringValue(value string) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: value}}
}

func edgeTestBoolValue(value bool) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: value}}
}

func edgeTestUnaryRange(column *planpb.ColumnInfo, op planpb.OpType, value *planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: column,
				Op:         op,
				Value:      value,
			},
		},
	}
}

func edgeTestBinaryRange(column *planpb.ColumnInfo, lower, upper *planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:     column,
				LowerInclusive: true,
				UpperInclusive: true,
				LowerValue:     lower,
				UpperValue:     upper,
			},
		},
	}
}

func edgeTestTerm(column *planpb.ColumnInfo, values ...*planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: column,
				Values:     values,
			},
		},
	}
}

func edgeTestLogical(op planpb.BinaryExpr_BinaryOp, left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    op,
				Left:  left,
				Right: right,
			},
		},
	}
}

func edgeTestCountBinaryRanges(expr *planpb.Expr) int {
	if expr == nil {
		return 0
	}
	if expr.GetBinaryRangeExpr() != nil {
		return 1
	}
	if binary := expr.GetBinaryExpr(); binary != nil {
		return edgeTestCountBinaryRanges(binary.GetLeft()) + edgeTestCountBinaryRanges(binary.GetRight())
	}
	return 0
}

func edgeTestCountUnaryRanges(expr *planpb.Expr) int {
	if expr == nil {
		return 0
	}
	if expr.GetUnaryRangeExpr() != nil {
		return 1
	}
	if binary := expr.GetBinaryExpr(); binary != nil {
		return edgeTestCountUnaryRanges(binary.GetLeft()) + edgeTestCountUnaryRanges(binary.GetRight())
	}
	return 0
}

func TestRewrite_JSONBinaryRangeMixedBoundTypesSkipMerge(t *testing.T) {
	logicalOps := []struct {
		name string
		op   planpb.BinaryExpr_BinaryOp
	}{
		{name: "and", op: planpb.BinaryExpr_LogicalAnd},
		{name: "or", op: planpb.BinaryExpr_LogicalOr},
	}
	unsupportedUpperBounds := []struct {
		name  string
		value *planpb.GenericValue
	}{
		{name: "string", value: edgeTestStringValue("z")},
		{name: "bool", value: edgeTestBoolValue(true)},
	}

	for _, logicalOp := range logicalOps {
		for _, unsupportedUpper := range unsupportedUpperBounds {
			for _, mixedFirst := range []bool{true, false} {
				order := "numeric_first"
				if mixedFirst {
					order = "mixed_first"
				}
				t.Run(logicalOp.name+"_"+unsupportedUpper.name+"_"+order, func(t *testing.T) {
					column := edgeTestRangeColumn(schemapb.DataType_JSON)
					mixed := edgeTestBinaryRange(column, edgeTestIntValue(1), unsupportedUpper.value)
					numeric := edgeTestBinaryRange(column, edgeTestIntValue(2), edgeTestIntValue(4))
					left, right := numeric, mixed
					if mixedFirst {
						left, right = mixed, numeric
					}

					result := rewriter.RewriteExprWithConfig(edgeTestLogical(logicalOp.op, left, right), true)

					binary := result.GetBinaryExpr()
					require.NotNil(t, binary)
					require.Equal(t, logicalOp.op, binary.GetOp())
					require.Equal(t, 2, edgeTestCountBinaryRanges(result))
				})
			}
		}
	}
}

func TestRewrite_JSONBinaryRangeMixedNumericBoundsMerge(t *testing.T) {
	t.Run("and", func(t *testing.T) {
		column := edgeTestRangeColumn(schemapb.DataType_JSON)
		left := edgeTestBinaryRange(column, edgeTestIntValue(1), edgeTestFloatValue(5.5))
		right := edgeTestBinaryRange(column, edgeTestFloatValue(2.5), edgeTestIntValue(4))

		result := rewriter.RewriteExprWithConfig(edgeTestLogical(planpb.BinaryExpr_LogicalAnd, left, right), true)

		binaryRange := result.GetBinaryRangeExpr()
		require.NotNil(t, binaryRange)
		require.InDelta(t, 2.5, binaryRange.GetLowerValue().GetFloatVal(), 1e-9)
		require.Equal(t, int64(4), binaryRange.GetUpperValue().GetInt64Val())
	})

	t.Run("or", func(t *testing.T) {
		column := edgeTestRangeColumn(schemapb.DataType_JSON)
		left := edgeTestBinaryRange(column, edgeTestIntValue(1), edgeTestFloatValue(3.5))
		right := edgeTestBinaryRange(column, edgeTestFloatValue(2.5), edgeTestIntValue(5))

		result := rewriter.RewriteExprWithConfig(edgeTestLogical(planpb.BinaryExpr_LogicalOr, left, right), true)

		binaryRange := result.GetBinaryRangeExpr()
		require.NotNil(t, binaryRange)
		require.Equal(t, int64(1), binaryRange.GetLowerValue().GetInt64Val())
		require.Equal(t, int64(5), binaryRange.GetUpperValue().GetInt64Val())
	})
}

func TestRewrite_NaNUnaryRangeSkipsMerge(t *testing.T) {
	columnTypes := []struct {
		name     string
		dataType schemapb.DataType
	}{
		{name: "json", dataType: schemapb.DataType_JSON},
		{name: "double", dataType: schemapb.DataType_Double},
	}
	logicalOps := []struct {
		name string
		op   planpb.BinaryExpr_BinaryOp
	}{
		{name: "and", op: planpb.BinaryExpr_LogicalAnd},
		{name: "or", op: planpb.BinaryExpr_LogicalOr},
	}

	for _, columnType := range columnTypes {
		for _, logicalOp := range logicalOps {
			for _, nanFirst := range []bool{true, false} {
				order := "finite_first"
				if nanFirst {
					order = "nan_first"
				}
				t.Run(columnType.name+"_"+logicalOp.name+"_"+order, func(t *testing.T) {
					column := edgeTestRangeColumn(columnType.dataType)
					nanRange := edgeTestUnaryRange(column, planpb.OpType_GreaterEqual, edgeTestFloatValue(math.NaN()))
					finiteRange := edgeTestUnaryRange(column, planpb.OpType_GreaterEqual, edgeTestFloatValue(1))
					left, right := finiteRange, nanRange
					if nanFirst {
						left, right = nanRange, finiteRange
					}

					result := rewriter.RewriteExprWithConfig(edgeTestLogical(logicalOp.op, left, right), true)

					binary := result.GetBinaryExpr()
					require.NotNil(t, binary)
					require.Equal(t, logicalOp.op, binary.GetOp())
					require.Equal(t, 2, edgeTestCountUnaryRanges(result))
				})
			}
		}
	}
}

func TestRewrite_NaNBinaryRangeSkipsMerge(t *testing.T) {
	columnTypes := []struct {
		name     string
		dataType schemapb.DataType
	}{
		{name: "json", dataType: schemapb.DataType_JSON},
		{name: "double", dataType: schemapb.DataType_Double},
	}
	logicalOps := []struct {
		name string
		op   planpb.BinaryExpr_BinaryOp
	}{
		{name: "and", op: planpb.BinaryExpr_LogicalAnd},
		{name: "or", op: planpb.BinaryExpr_LogicalOr},
	}

	for _, columnType := range columnTypes {
		for _, logicalOp := range logicalOps {
			for _, nanFirst := range []bool{true, false} {
				order := "finite_first"
				if nanFirst {
					order = "nan_first"
				}
				t.Run(columnType.name+"_"+logicalOp.name+"_"+order, func(t *testing.T) {
					column := edgeTestRangeColumn(columnType.dataType)
					nanRange := edgeTestBinaryRange(column, edgeTestFloatValue(1), edgeTestFloatValue(math.NaN()))
					finiteRange := edgeTestBinaryRange(column, edgeTestFloatValue(2), edgeTestFloatValue(4))
					left, right := finiteRange, nanRange
					if nanFirst {
						left, right = nanRange, finiteRange
					}

					result := rewriter.RewriteExprWithConfig(edgeTestLogical(logicalOp.op, left, right), true)

					binary := result.GetBinaryExpr()
					require.NotNil(t, binary)
					require.Equal(t, logicalOp.op, binary.GetOp())
					require.Equal(t, 2, edgeTestCountBinaryRanges(result))
				})
			}
		}
	}
}

func TestRewrite_NaNInWithRangeSkipsOptimization(t *testing.T) {
	testcases := []struct {
		name      string
		dataType  schemapb.DataType
		nanInTerm bool
	}{
		{name: "json_range", dataType: schemapb.DataType_JSON},
		{name: "json_term", dataType: schemapb.DataType_JSON, nanInTerm: true},
		{name: "double_range", dataType: schemapb.DataType_Double},
		{name: "double_term", dataType: schemapb.DataType_Double, nanInTerm: true},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			column := edgeTestRangeColumn(testcase.dataType)
			termValues := []*planpb.GenericValue{edgeTestFloatValue(1), edgeTestFloatValue(2)}
			rangeValue := edgeTestFloatValue(math.NaN())
			if testcase.nanInTerm {
				termValues[0] = edgeTestFloatValue(math.NaN())
				rangeValue = edgeTestFloatValue(1)
			}
			term := edgeTestTerm(column, termValues...)
			rangeExpr := edgeTestUnaryRange(column, planpb.OpType_GreaterEqual, rangeValue)

			result := rewriter.RewriteExprWithConfig(edgeTestLogical(planpb.BinaryExpr_LogicalAnd, term, rangeExpr), true)

			binary := result.GetBinaryExpr()
			require.NotNil(t, binary)
			require.Equal(t, planpb.BinaryExpr_LogicalAnd, binary.GetOp())
			require.NotNil(t, findTermExpr(result))
			require.NotNil(t, findUnaryRangeExpr(result, planpb.OpType_GreaterEqual))
		})
	}
}
