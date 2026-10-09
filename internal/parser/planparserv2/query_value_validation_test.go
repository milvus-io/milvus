package planparserv2

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryNaNAccepted(t *testing.T) {
	schema := newTestSchema(true)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: 2000, Name: "FloatArray", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double,
	})
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	nan := &schemapb.TemplateValue{Val: &schemapb.TemplateValue_FloatVal{FloatVal: math.NaN()}}
	array := &schemapb.TemplateArrayValue{Data: &schemapb.TemplateArrayValue_DoubleData{
		DoubleData: &schemapb.DoubleArray{Data: []float64{1.5, math.NaN()}},
	}}
	nans := &schemapb.TemplateValue{Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: array}}
	nested := &schemapb.TemplateValue{Val: &schemapb.TemplateValue_ArrayVal{
		ArrayVal: &schemapb.TemplateArrayValue{Data: &schemapb.TemplateArrayValue_ArrayData{
			ArrayData: &schemapb.TemplateArrayValueArray{Data: []*schemapb.TemplateArrayValue{array}},
		}},
	}}
	for _, tc := range []struct {
		expression string
		value      *schemapb.TemplateValue
	}{
		{"FloatField == {value}", nan},
		{"DoubleField != {value}", nan},
		{"DoubleField in {value}", nans},
		{"DoubleField not in {value}", nans},
		{"0.0 <= DoubleField < {value}", nan},
		{"DoubleField + {value} > 1.0", nan},
		{`JSONField["n"] > {value}`, nan},
		{`JSONField["n"] in {value}`, nans},
		{`JSONField["n"] == {value}`, nested},
		{`json_contains(JSONField["a"], {value})`, nan},
		{`json_contains_any(JSONField["a"], {value})`, nans},
		{`json_contains_all(JSONField["a"], {value})`, nested},
		{"array_contains(FloatArray, {value})", nan},
		{"array_contains_any(FloatArray, {value})", nans},
		{"array_contains_all(FloatArray, {value})", nans},
		{"true or DoubleField == {value}", nan},
		{"false and DoubleField == {value}", nan},
		{"DoubleField > (-1.0)**0.5", nil},
		{"((-1.0)**0.5) == 0.0", nil},
		{"DoubleField > (2.0**1024 - 2.0**1024)", nil},
		{"DoubleField > (0.0 * 2.0**1024)", nil},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			var values map[string]*schemapb.TemplateValue
			if tc.value != nil {
				values = map[string]*schemapb.TemplateValue{"value": tc.value}
			}
			parsed, err := ParseExpr(helper, tc.expression, values)
			require.NoError(t, err)
			require.NotNil(t, parsed)
		})
	}
	for _, createPlan := range []func() error{
		func() error {
			_, err := CreateRetrievePlan(helper, "DoubleField == {value}", map[string]*schemapb.TemplateValue{"value": nan})
			return err
		},
		func() error {
			_, err := CreateSearchPlan(helper, "DoubleField == {value}", "FloatVectorField", nil, map[string]*schemapb.TemplateValue{"value": nan}, nil)
			return err
		},
	} {
		require.NoError(t, createPlan())
	}

	for _, tc := range []struct {
		expression string
		value      *schemapb.TemplateValue
	}{
		{"DoubleField == {value}", &schemapb.TemplateValue{Val: &schemapb.TemplateValue_FloatVal{FloatVal: 1.5}}},
		{"DoubleField < {value}", &schemapb.TemplateValue{Val: &schemapb.TemplateValue_FloatVal{FloatVal: math.Inf(1)}}},
		{"DoubleField > {value}", &schemapb.TemplateValue{Val: &schemapb.TemplateValue_FloatVal{FloatVal: math.Inf(-1)}}},
		{"VarCharField == {value}", &schemapb.TemplateValue{Val: &schemapb.TemplateValue_StringVal{StringVal: "NaN"}}},
		{`JSONField["n"] == "NaN"`, nil},
		{"DoubleField > (-1.0)**2", nil},
	} {
		var values map[string]*schemapb.TemplateValue
		if tc.value != nil {
			values = map[string]*schemapb.TemplateValue{"value": tc.value}
		}
		_, err := ParseExpr(helper, tc.expression, values)
		require.NoError(t, err, tc.expression)
	}

	for _, exprStr := range []string{"DoubleField == {value}", `JSONField["n"] == {value}`} {
		expr, err := ParseExprTemplate(helper, exprStr, nil)
		require.NoError(t, err)
		err = FillExpressionValue(expr, map[string]*planpb.GenericValue{"value": NewFloat(math.NaN())})
		require.NoError(t, err)
	}
	finite, err := ParseExpr(helper, "DoubleField == 1.5", nil)
	require.NoError(t, err)
	finite.GetUnaryRangeExpr().Value = NewFloat(math.NaN())
	require.NoError(t, FillExpressionValue(finite, nil))
}

func TestNaNConstantComparisonOrder(t *testing.T) {
	nan := NewFloat(math.NaN())
	otherNaN := NewFloat(math.Float64frombits(0xfff8000000000001))
	infinity := NewFloat(math.Inf(1))
	for _, tc := range []struct {
		actual *ExprWithType
		want   bool
	}{
		{Equal(nan, otherNaN), true},
		{NotEqual(nan, otherNaN), false},
		{Less(nan, otherNaN), false},
		{LessEqual(nan, otherNaN), true},
		{Greater(nan, infinity), true},
		{GreaterEqual(nan, infinity), true},
		{Less(infinity, nan), true},
		{LessEqual(infinity, nan), true},
		{Greater(NewInt(3), nan), false},
		{Less(NewInt(3), nan), true},
	} {
		require.NotNil(t, tc.actual)
		require.Equal(t, tc.want, getGenericValue(tc.actual).GetBoolVal())
	}
	helper, err := typeutil.CreateSchemaHelper(newTestSchema(true))
	require.NoError(t, err)
	for expr, want := range map[string]bool{
		"((-1.0)**0.5) == ((-1.0)**0.5)": true,
		"((-1.0)**0.5) > (2.0**1024)":    true,
		"((-1.0)**0.5) != ((-1.0)**0.5)": false,
		"((-1.0)**0.5) < 3":              false,
	} {
		parsed, err := ParseExpr(helper, expr, nil)
		require.NoError(t, err, expr)
		require.Equal(t, want, parsed.GetAlwaysTrueExpr() != nil, expr)
	}
}
