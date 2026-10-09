package planparserv2

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func nonFiniteArrayQuerySchema(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()
	schema := newTestSchema(true)
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{FieldID: 12000, Name: "FloatArray", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
		&schemapb.FieldSchema{FieldID: 12001, Name: "DoubleArray", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double})
	for i, leaf := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		name := "nested_int"
		if leaf == schemapb.DataType_Double {
			name = "nested_float"
		}
		schema.StructArrayFields[0].Fields = append(schema.StructArrayFields[0].Fields, &schemapb.FieldSchema{
			FieldID: int64(12002 + i), Name: "struct_array[" + name + "]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
			TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
					Kind: &schemapb.TypeSchema_LeafType{LeafType: leaf},
				}},
			}}},
		})
	}
	schema.StructArrayFields[0].Fields = append(schema.StructArrayFields[0].Fields,
		&schemapb.FieldSchema{FieldID: 12004, Name: "struct_array[sub_float]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double})
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func TestIntegerArrayRejectsNonFiniteQueryValues(t *testing.T) {
	helper := nonFiniteArrayQuerySchema(t)
	for _, number := range []struct {
		name    string
		value   float64
		literal string
	}{
		{"nan", math.Float64frombits(0xfff8000000000001), "((-1.0)**0.5)"},
		{"positive_inf", math.Inf(1), "(2.0**1024)"},
		{"negative_inf", math.Inf(-1), "(-(2.0**1024))"},
	} {
		for _, mode := range []string{"literal", "template"} {
			for _, expression := range []string{
				"array_contains(ArrayField, {number})",
				"array_contains_any(ArrayField, {values})",
				"array_contains_all(ArrayField, {values})",
				"{number} in ArrayField",
				"ArrayField[0] == {number}",
				"ArrayField[0] < {number}",
				"ArrayField[0] in {values}",
				"ArrayField == {values}",
				"ArrayField != {values}",
				"ArrayField in {arrays}",
				"MATCH_ANY(struct_array, $[sub_int] == {number})",
				"MATCH_ANY(struct_array, $[sub_int] in {values})",
				"MATCH_ANY(struct_array, array_contains_any($[nested_int], {values}))",
				"MATCH_ALL(struct_array, array_contains_all($[nested_int], {values}))",
			} {
				t.Run(number.name+"/"+mode+"/"+expression, func(t *testing.T) {
					query, values := nonFiniteQueryValues(expression, mode, number.literal, number.value)
					parsed, err := ParseExpr(helper, query, values)
					if err == nil {
						t.Logf("accepted native expression: %s", parsed)
					}
					require.Error(t, err)
					require.Nil(t, parsed)
				})
			}
		}
	}
}

func nonFiniteQueryValues(expression, mode, literal string, value float64) (string, map[string]*schemapb.TemplateValue) {
	if mode == "literal" {
		return strings.NewReplacer("{number}", literal, "{values}", "["+literal+"]", "{arrays}", "[["+literal+"]]").Replace(expression), nil
	}
	array := &schemapb.TemplateArrayValue{Data: &schemapb.TemplateArrayValue_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{value}}}}
	return expression, map[string]*schemapb.TemplateValue{
		"number": {Val: &schemapb.TemplateValue_FloatVal{FloatVal: value}},
		"values": {Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: array}},
		"arrays": {Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{Data: &schemapb.TemplateArrayValue_ArrayData{
			ArrayData: &schemapb.TemplateArrayValueArray{Data: []*schemapb.TemplateArrayValue{array}},
		}}}},
	}
}

func TestFloatingArrayAcceptsNonFiniteQueryValues(t *testing.T) {
	helper := nonFiniteArrayQuerySchema(t)
	for _, number := range []struct {
		name    string
		value   float64
		literal string
	}{
		{"nan", math.NaN(), "((-1.0)**0.5)"},
		{"positive_inf", math.Inf(1), "(2.0**1024)"},
		{"negative_inf", math.Inf(-1), "(-(2.0**1024))"},
	} {
		for _, mode := range []string{"literal", "template"} {
			for _, expression := range []string{
				"array_contains(FloatArray, {number})",
				"array_contains_any(FloatArray, {values})",
				"array_contains_all(DoubleArray, {values})",
				"FloatArray[0] == {number}",
				"DoubleArray[0] < {number}",
				"DoubleArray[0] in {values}",
				"FloatArray == {values}",
				"DoubleArray != {values}",
				"DoubleArray in {arrays}",
				"MATCH_ANY(struct_array, $[sub_float] in {values})",
				"MATCH_ANY(struct_array, array_contains_any($[nested_float], {values}))",
				"MATCH_ALL(struct_array, array_contains_all($[nested_float], {values}))",
			} {
				t.Run(number.name+"/"+mode+"/"+expression, func(t *testing.T) {
					query, values := nonFiniteQueryValues(expression, mode, number.literal, number.value)
					parsed, err := ParseExpr(helper, query, values)
					require.NoError(t, err)
					require.NotNil(t, parsed)
				})
			}
		}
	}
}
