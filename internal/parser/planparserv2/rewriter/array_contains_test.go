package rewriter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestCombineArrayContainsPreservesOrderAndColumnGroups(t *testing.T) {
	columnA := arrayContainsTestColumn(201, schemapb.DataType_Int64)
	columnB := arrayContainsTestColumn(202, schemapb.DataType_Int64)
	before := arrayContainsTestMarker(10)
	middle := arrayContainsTestMarker(20)

	parts := []*planpb.Expr{
		before,
		arrayContainsTestExpr(columnA, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(3)),
		middle,
		arrayContainsTestExpr(columnA, planpb.JSONContainsExpr_ContainsAny,
			arrayContainsTestInt(1), arrayContainsTestInt(2)),
		arrayContainsTestExpr(columnB, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(7)),
		arrayContainsTestExpr(columnA, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(4)),
		arrayContainsTestExpr(columnB, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(8)),
	}

	result := combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAny)
	require.Len(t, result, 4)
	require.Same(t, before, result[0])
	require.Same(t, middle, result[2])

	mergedA := result[1].GetJsonContainsExpr()
	require.NotNil(t, mergedA)
	require.Same(t, columnA, mergedA.GetColumnInfo())
	require.Equal(t, planpb.JSONContainsExpr_ContainsAny, mergedA.GetOp())
	require.Equal(t, []int64{3, 1, 2, 4}, arrayContainsTestIntValues(mergedA.GetElements()))
	require.True(t, mergedA.GetElementsSameType())

	mergedB := result[3].GetJsonContainsExpr()
	require.NotNil(t, mergedB)
	require.Same(t, columnB, mergedB.GetColumnInfo())
	require.Equal(t, []int64{7, 8}, arrayContainsTestIntValues(mergedB.GetElements()))
}

func TestCombineArrayContainsSelectsCompatibleOperators(t *testing.T) {
	testcases := []struct {
		name       string
		targetOp   planpb.JSONContainsExpr_JSONOp
		compatible planpb.JSONContainsExpr_JSONOp
		opposite   planpb.JSONContainsExpr_JSONOp
	}{
		{
			name:       "or absorbs contains any",
			targetOp:   planpb.JSONContainsExpr_ContainsAny,
			compatible: planpb.JSONContainsExpr_ContainsAny,
			opposite:   planpb.JSONContainsExpr_ContainsAll,
		},
		{
			name:       "and absorbs contains all",
			targetOp:   planpb.JSONContainsExpr_ContainsAll,
			compatible: planpb.JSONContainsExpr_ContainsAll,
			opposite:   planpb.JSONContainsExpr_ContainsAny,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			column := arrayContainsTestColumn(201, schemapb.DataType_Int64)
			opposite := arrayContainsTestExpr(column, testcase.opposite,
				arrayContainsTestInt(8), arrayContainsTestInt(9))
			parts := []*planpb.Expr{
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1)),
				opposite,
				arrayContainsTestExpr(column, testcase.compatible,
					arrayContainsTestInt(2), arrayContainsTestInt(3)),
			}

			result := combineArrayContains(parts, testcase.targetOp)
			require.Len(t, result, 2)
			merged := result[0].GetJsonContainsExpr()
			require.NotNil(t, merged)
			require.Equal(t, testcase.targetOp, merged.GetOp())
			require.Equal(t, []int64{1, 2, 3}, arrayContainsTestIntValues(merged.GetElements()))
			require.Same(t, opposite, result[1])
		})
	}
}

func TestCombineArrayContainsSkipsInvalidValues(t *testing.T) {
	testcases := []struct {
		name  string
		build func(*planpb.ColumnInfo) *planpb.Expr
	}{
		{
			name: "contains without element",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains)
			},
		},
		{
			name: "contains with multiple elements",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains,
					arrayContainsTestFloat(3), arrayContainsTestFloat(4))
			},
		},
		{
			name: "nil element",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, nil)
			},
		},
		{
			name: "unknown element",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, &planpb.GenericValue{})
			},
		},
		{
			name: "array element",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, &planpb.GenericValue{
					Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{}},
				})
			},
		},
		{
			name: "nan element",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains,
					arrayContainsTestFloat(math.NaN()))
			},
		},
		{
			name: "contains any with an invalid element",
			build: func(column *planpb.ColumnInfo) *planpb.Expr {
				return arrayContainsTestExpr(column, planpb.JSONContainsExpr_ContainsAny,
					arrayContainsTestFloat(3), nil)
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			column := arrayContainsTestColumn(201, schemapb.DataType_Double)
			invalid := testcase.build(column)
			parts := []*planpb.Expr{
				invalid,
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestFloat(1)),
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestFloat(2)),
			}

			result := combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAny)
			require.Len(t, result, 2)
			require.Same(t, invalid, result[0])
			merged := result[1].GetJsonContainsExpr()
			require.NotNil(t, merged)
			require.Equal(t, planpb.JSONContainsExpr_ContainsAny, merged.GetOp())
			require.Equal(t, []float64{1, 2}, arrayContainsTestFloatValues(merged.GetElements()))
		})
	}
}

func TestCombineArrayContainsRecomputesTypeAndClearsTemplateMetadata(t *testing.T) {
	column := arrayContainsTestColumn(201, schemapb.DataType_None)
	first := arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1))
	first.IsTemplate = true
	first.GetJsonContainsExpr().TemplateVariableName = "first"
	second := arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestString("two"))
	second.IsTemplate = true
	second.GetJsonContainsExpr().TemplateVariableName = "second"

	result := combineArrayContains([]*planpb.Expr{first, second}, planpb.JSONContainsExpr_ContainsAny)
	require.Len(t, result, 1)
	merged := result[0].GetJsonContainsExpr()
	require.NotNil(t, merged)
	require.False(t, merged.GetElementsSameType())
	require.Empty(t, merged.GetTemplateVariableName())
	require.False(t, result[0].GetIsTemplate())

	require.Equal(t, "first", first.GetJsonContainsExpr().GetTemplateVariableName())
	require.Equal(t, "second", second.GetJsonContainsExpr().GetTemplateVariableName())
}

func TestCombineArrayContainsDeduplicatesElementsPreservingOrder(t *testing.T) {
	testcases := []struct {
		name       string
		targetOp   planpb.JSONContainsExpr_JSONOp
		combinedOp planpb.JSONContainsExpr_JSONOp
	}{
		{
			name:       "contains any",
			targetOp:   planpb.JSONContainsExpr_ContainsAny,
			combinedOp: planpb.JSONContainsExpr_ContainsAny,
		},
		{
			name:       "contains all",
			targetOp:   planpb.JSONContainsExpr_ContainsAll,
			combinedOp: planpb.JSONContainsExpr_ContainsAll,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			column := arrayContainsTestColumn(201, schemapb.DataType_Int64)
			parts := []*planpb.Expr{
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(2)),
				arrayContainsTestExpr(column, testcase.combinedOp,
					arrayContainsTestInt(2), arrayContainsTestInt(1)),
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1)),
			}

			result := combineArrayContains(parts, testcase.targetOp)
			require.Len(t, result, 1)
			merged := result[0].GetJsonContainsExpr()
			require.NotNil(t, merged)
			require.Equal(t, []int64{2, 1}, arrayContainsTestIntValues(merged.GetElements()))
		})
	}
}

func TestCombineArrayContainsDeduplicatesAfterNumericCast(t *testing.T) {
	testcases := []struct {
		name           string
		elementType    schemapb.DataType
		first          *planpb.GenericValue
		equivalent     *planpb.GenericValue
		distinct       *planpb.GenericValue
		distinctSecond *planpb.GenericValue
	}{
		{
			name:           "float32",
			elementType:    schemapb.DataType_Float,
			first:          arrayContainsTestInt(16_777_217),
			equivalent:     arrayContainsTestFloat(16_777_216),
			distinct:       arrayContainsTestFloat(16_777_218),
			distinctSecond: arrayContainsTestInt(16_777_218),
		},
		{
			name:           "float64",
			elementType:    schemapb.DataType_Double,
			first:          arrayContainsTestInt(9_007_199_254_740_993),
			equivalent:     arrayContainsTestFloat(9_007_199_254_740_992),
			distinct:       arrayContainsTestFloat(9_007_199_254_740_994),
			distinctSecond: arrayContainsTestInt(9_007_199_254_740_994),
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			column := arrayContainsTestColumn(201, testcase.elementType)
			parts := []*planpb.Expr{
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, testcase.first),
				arrayContainsTestExpr(column, planpb.JSONContainsExpr_ContainsAny,
					testcase.equivalent, testcase.distinct, testcase.distinctSecond),
			}

			result := combineArrayContains(parts, planpb.JSONContainsExpr_ContainsAny)
			require.Len(t, result, 1)
			elements := result[0].GetJsonContainsExpr().GetElements()
			require.Len(t, elements, 2)
			require.Same(t, testcase.first, elements[0])
			require.Same(t, testcase.distinct, elements[1])
		})
	}
}

func TestCombineArrayContainsRequiresAtLeastTwoSources(t *testing.T) {
	column := arrayContainsTestColumn(201, schemapb.DataType_Int64)
	single := arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1))

	result := combineArrayContains([]*planpb.Expr{single}, planpb.JSONContainsExpr_ContainsAny)
	require.Len(t, result, 1)
	require.Same(t, single, result[0])
}

func arrayContainsTestColumn(fieldID int64, elementType schemapb.DataType) *planpb.ColumnInfo {
	return &planpb.ColumnInfo{
		FieldId:     fieldID,
		DataType:    schemapb.DataType_Array,
		ElementType: elementType,
	}
}

func arrayContainsTestExpr(
	column *planpb.ColumnInfo,
	op planpb.JSONContainsExpr_JSONOp,
	elements ...*planpb.GenericValue,
) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:       column,
				Elements:         elements,
				Op:               op,
				ElementsSameType: true,
			},
		},
	}
}

func arrayContainsTestMarker(value int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{Value: arrayContainsTestInt(value)},
		},
	}
}

func arrayContainsTestInt(value int64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: value}}
}

func arrayContainsTestFloat(value float64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: value}}
}

func arrayContainsTestString(value string) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: value}}
}

func arrayContainsTestIntValues(values []*planpb.GenericValue) []int64 {
	result := make([]int64, 0, len(values))
	for _, value := range values {
		result = append(result, value.GetInt64Val())
	}
	return result
}

func arrayContainsTestFloatValues(values []*planpb.GenericValue) []float64 {
	result := make([]float64, 0, len(values))
	for _, value := range values {
		result = append(result, value.GetFloatVal())
	}
	return result
}
