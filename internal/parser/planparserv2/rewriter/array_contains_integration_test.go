package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestRewriteArrayContainsChains(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	testcases := []struct {
		name     string
		expr     string
		expected planpb.JSONContainsExpr_JSONOp
		values   []int64
	}{
		{
			name:     "or two contains",
			expr:     `array_contains(ArrayInt, 1) or array_contains(ArrayInt, 2)`,
			expected: planpb.JSONContainsExpr_ContainsAny,
			values:   []int64{1, 2},
		},
		{
			name: "or balanced tree",
			expr: `(array_contains(ArrayInt, 4) or array_contains(ArrayInt, 1)) or ` +
				`(array_contains(ArrayInt, 3) or array_contains(ArrayInt, 2))`,
			expected: planpb.JSONContainsExpr_ContainsAny,
			values:   []int64{4, 1, 3, 2},
		},
		{
			name: "or absorbs contains any",
			expr: `array_contains(ArrayInt, 3) or ` +
				`(array_contains_any(ArrayInt, [1, 2]) or array_contains(ArrayInt, 4))`,
			expected: planpb.JSONContainsExpr_ContainsAny,
			values:   []int64{3, 1, 2, 4},
		},
		{
			name: "or combines contains any",
			expr: `array_contains_any(ArrayInt, [4, 1]) or ` +
				`array_contains_any(ArrayInt, [3, 2])`,
			expected: planpb.JSONContainsExpr_ContainsAny,
			values:   []int64{4, 1, 3, 2},
		},
		{
			name: "or deduplicates preserving first order",
			expr: `array_contains(ArrayInt, 2) or ` +
				`array_contains_any(ArrayInt, [2, 1, 1]) or array_contains(ArrayInt, 1)`,
			expected: planpb.JSONContainsExpr_ContainsAny,
			values:   []int64{2, 1},
		},
		{
			name:     "and two contains",
			expr:     `array_contains(ArrayInt, 1) and array_contains(ArrayInt, 2)`,
			expected: planpb.JSONContainsExpr_ContainsAll,
			values:   []int64{1, 2},
		},
		{
			name: "and balanced tree",
			expr: `(array_contains(ArrayInt, 4) and array_contains(ArrayInt, 1)) and ` +
				`(array_contains(ArrayInt, 3) and array_contains(ArrayInt, 2))`,
			expected: planpb.JSONContainsExpr_ContainsAll,
			values:   []int64{4, 1, 3, 2},
		},
		{
			name: "and absorbs contains all",
			expr: `array_contains(ArrayInt, 3) and ` +
				`(array_contains_all(ArrayInt, [1, 2]) and array_contains(ArrayInt, 4))`,
			expected: planpb.JSONContainsExpr_ContainsAll,
			values:   []int64{3, 1, 2, 4},
		},
		{
			name: "and combines contains all",
			expr: `array_contains_all(ArrayInt, [4, 1]) and ` +
				`array_contains_all(ArrayInt, [3, 2])`,
			expected: planpb.JSONContainsExpr_ContainsAll,
			values:   []int64{4, 1, 3, 2},
		},
		{
			name:     "json function name on physical array",
			expr:     `json_contains(ArrayInt, 5) or array_contains(ArrayInt, 6)`,
			expected: planpb.JSONContainsExpr_ContainsAny,
			values:   []int64{5, 6},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(helper, testcase.expr, nil)
			require.NoError(t, err)
			contains := expr.GetJsonContainsExpr()
			require.NotNil(t, contains)
			require.Equal(t, schemapb.DataType_Array, contains.GetColumnInfo().GetDataType())
			require.Equal(t, testcase.expected, contains.GetOp())
			require.Equal(t, testcase.values, arrayContainsIntegrationIntValues(contains.GetElements()))
			require.True(t, contains.GetElementsSameType())
		})
	}
}

func TestRewriteArrayContainsKeepsColumnsAndOtherPredicatesSeparate(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	expr, err := parser.ParseExpr(helper,
		`ArrayInt[0] > 10 or array_contains(ArrayInt, 3) or `+
			`array_contains(ArrayFloat, 1.5) or array_contains(ArrayInt, 1) or `+
			`array_contains(ArrayFloat, 2.5)`, nil)
	require.NoError(t, err)

	containsByField := make(map[int64]*planpb.JSONContainsExpr)
	var containsCount int
	var rangeCount int
	walkExpr(expr, func(current *planpb.Expr) {
		if contains := current.GetJsonContainsExpr(); contains != nil {
			containsCount++
			containsByField[contains.GetColumnInfo().GetFieldId()] = contains
		}
		if current.GetUnaryRangeExpr() != nil {
			rangeCount++
		}
	})

	require.Len(t, containsByField, 2)
	require.Equal(t, 2, containsCount)
	require.Equal(t, 1, rangeCount)
	require.Equal(t, planpb.JSONContainsExpr_ContainsAny, containsByField[201].GetOp())
	require.Equal(t, []int64{3, 1}, arrayContainsIntegrationIntValues(containsByField[201].GetElements()))
	require.Equal(t, planpb.JSONContainsExpr_ContainsAny, containsByField[202].GetOp())
	require.Equal(t, []float64{1.5, 2.5}, arrayContainsIntegrationFloatValues(containsByField[202].GetElements()))
}

func TestRewriteArrayContainsColumnVariants(t *testing.T) {
	t.Run("nullable array", func(t *testing.T) {
		helper := buildSchemaHelperWithArraysT(t)
		expr, err := parser.ParseExpr(helper,
			`array_contains(NullableArrayInt, 1) and array_contains_all(NullableArrayInt, [2, 3])`, nil)
		require.NoError(t, err)

		contains := expr.GetJsonContainsExpr()
		require.NotNil(t, contains)
		require.Equal(t, planpb.JSONContainsExpr_ContainsAll, contains.GetOp())
		require.Equal(t, []int64{1, 2, 3}, arrayContainsIntegrationIntValues(contains.GetElements()))
		require.True(t, contains.GetColumnInfo().GetNullable())
	})

	t.Run("struct array subfield", func(t *testing.T) {
		helper := buildSchemaHelperWithStructArrayT(t)
		expr, err := parser.ParseExpr(helper,
			`array_contains(struct_array[sub_int], 4) and array_contains(struct_array[sub_int], 5)`, nil)
		require.NoError(t, err)

		contains := expr.GetJsonContainsExpr()
		require.NotNil(t, contains)
		require.Equal(t, planpb.JSONContainsExpr_ContainsAll, contains.GetOp())
		require.Equal(t, []int64{4, 5}, arrayContainsIntegrationIntValues(contains.GetElements()))
		require.Equal(t, schemapb.DataType_Array, contains.GetColumnInfo().GetDataType())
		require.Equal(t, int64(303), contains.GetColumnInfo().GetFieldId())
	})
}

func TestRewriteArrayContainsFilledTemplates(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	templateValues := map[string]*schemapb.TemplateValue{
		"first":  {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 9}},
		"second": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 7}},
	}
	expr, err := parser.ParseExpr(helper,
		`array_contains(ArrayInt, {first}) or json_contains(ArrayInt, {second})`, templateValues)
	require.NoError(t, err)

	contains := expr.GetJsonContainsExpr()
	require.NotNil(t, contains)
	require.Equal(t, planpb.JSONContainsExpr_ContainsAny, contains.GetOp())
	require.Equal(t, []int64{9, 7}, arrayContainsIntegrationIntValues(contains.GetElements()))
	require.Empty(t, contains.GetTemplateVariableName())
	require.False(t, expr.GetIsTemplate())
}

func TestRewriteArrayContainsDoesNotMergeJSONColumns(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper,
		`json_contains(JSONField["items"], 1) or json_contains(JSONField["items"], 2)`, nil)
	require.NoError(t, err)
	require.NotNil(t, expr.GetBinaryExpr())

	var containsCount int
	var containsAnyCount int
	walkExpr(expr, func(current *planpb.Expr) {
		contains := current.GetJsonContainsExpr()
		if contains == nil {
			return
		}
		require.Equal(t, schemapb.DataType_JSON, contains.GetColumnInfo().GetDataType())
		switch contains.GetOp() {
		case planpb.JSONContainsExpr_Contains:
			containsCount++
		case planpb.JSONContainsExpr_ContainsAny:
			containsAnyCount++
		}
	})
	require.Equal(t, 2, containsCount)
	require.Zero(t, containsAnyCount)
}

func TestRewriteArrayContainsDisabledKeepsOriginalTree(t *testing.T) {
	column := &planpb.ColumnInfo{
		FieldId:     201,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	}
	input := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  arrayContainsIntegrationExpr(column, 1),
				Right: arrayContainsIntegrationExpr(column, 2),
				Op:    planpb.BinaryExpr_LogicalOr,
			},
		},
	}

	result := rewriter.RewriteExprWithConfig(input, false)
	require.NotNil(t, result.GetBinaryExpr())
	var containsCount int
	var containsAnyCount int
	walkExpr(result, func(current *planpb.Expr) {
		contains := current.GetJsonContainsExpr()
		if contains == nil {
			return
		}
		if contains.GetOp() == planpb.JSONContainsExpr_Contains {
			containsCount++
		}
		if contains.GetOp() == planpb.JSONContainsExpr_ContainsAny {
			containsAnyCount++
		}
	})
	require.Equal(t, 2, containsCount)
	require.Zero(t, containsAnyCount)
}

func arrayContainsIntegrationExpr(column *planpb.ColumnInfo, value int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:       column,
				Elements:         []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: value}}},
				Op:               planpb.JSONContainsExpr_Contains,
				ElementsSameType: true,
			},
		},
	}
}

func arrayContainsIntegrationIntValues(values []*planpb.GenericValue) []int64 {
	result := make([]int64, 0, len(values))
	for _, value := range values {
		result = append(result, value.GetInt64Val())
	}
	return result
}

func arrayContainsIntegrationFloatValues(values []*planpb.GenericValue) []float64 {
	result := make([]float64, 0, len(values))
	for _, value := range values {
		result = append(result, value.GetFloatVal())
	}
	return result
}
