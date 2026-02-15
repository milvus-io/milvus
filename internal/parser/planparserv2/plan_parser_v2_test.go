package planparserv2

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func newTestSchema(EnableDynamicField bool) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
	}

	for name, value := range schemapb.DataType_value {
		dataType := schemapb.DataType(value)
		newField := &schemapb.FieldSchema{
			FieldID: int64(100 + value), Name: name + "Field", IsPrimaryKey: false, Description: "", DataType: dataType,
		}
		if dataType == schemapb.DataType_Array {
			newField.ElementType = schemapb.DataType_Int64
		}
		fields = append(fields, newField)
	}
	if EnableDynamicField {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: 130, Name: common.MetaFieldName, IsPrimaryKey: false, Description: "dynamic field", DataType: schemapb.DataType_JSON,
			IsDynamic: true,
		})
	}

	fields = append(fields, &schemapb.FieldSchema{
		FieldID: 131, Name: "StringArrayField", IsPrimaryKey: false, Description: "string array field",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_VarChar,
	})

	structArrayField := &schemapb.StructArrayFieldSchema{
		FieldID: 132, Name: "struct_array", Fields: []*schemapb.FieldSchema{
			{
				FieldID: 133, Name: "struct_array[sub_str]", IsPrimaryKey: false, Description: "sub struct array field for string",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_VarChar,
			},
			{
				FieldID: 134, Name: "struct_array[sub_int]", IsPrimaryKey: false, Description: "sub struct array field for int",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
		},
	}

	return &schemapb.CollectionSchema{
		Name:               "test",
		Description:        "schema for test used",
		AutoID:             true,
		Fields:             fields,
		StructArrayFields:  []*schemapb.StructArrayFieldSchema{structArrayField},
		EnableDynamicField: EnableDynamicField,
	}
}

func enableMatch(schema *schemapb.CollectionSchema) {
	for _, field := range schema.Fields {
		if typeutil.IsStringType(field.DataType) {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key: "enable_match", Value: "True",
			})
		}
	}
}

func newTestSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return schemaHelper
}

func assertValidExpr(t *testing.T, helper *typeutil.SchemaHelper, exprStr string) {
	expr, err := ParseExpr(helper, exprStr, nil)
	assert.NoError(t, err, exprStr)
	// fmt.Printf("expr: %s\n", exprStr)
	assert.NotNil(t, expr, exprStr)
	ShowExpr(expr)
}

func assertInvalidExpr(t *testing.T, helper *typeutil.SchemaHelper, exprStr string) {
	_, err := ParseExpr(helper, exprStr, nil)
	assert.Error(t, err, exprStr)
}

func TestExpr_Term(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`BoolField in [true, false]`,
		`Int8Field in [1, 2]`,
		`Int16Field in [3, 4]`,
		`Int32Field in [5, 6]`,
		`Int64Field in [7, 8]`,
		`FloatField in [9.0, 10.0]`,
		`DoubleField in [11.0, 12.0]`,
		`StringField in ["str13", "str14"]`,
		`VarCharField in ["str15", "str16"]`,
		`FloatField in [1373, 115]`,
		`Int64Field in [17]`,
		`Int64Field in []`,
		`Int64Field not in []`,
		`JSONField["A"] in [1, 10]`,
		`JSONField["A"] in []`,
		`$meta["A"] in [1, 10]`,
		`$meta["A"] in []`,
		`A in [1, 10]`,
		`A in []`,
		`A in ["abc", "def"]`,
		`A in ["1", "2", "abc", "def"]`,
		`A in ["1", 2, "abc", 2.2]`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}

func TestExpr_Call(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	testcases := []struct {
		CallExpr     string
		FunctionName string
		ParameterNum int
	}{
		{`hello123()`, "hello123", 0},
		{`lt(Int32Field)`, "lt", 1},
		// test parens
		{`lt((((Int32Field))))`, "lt", 1},
		{`empty(VarCharField,)`, "empty", 1},
		{`f2(Int64Field)`, "f2", 1},
		{`f2(Int64Field, 4)`, "f2", 2},
		{`f3(JSON_FIELD["A"], Int32Field)`, "f3", 2},
		{`f5(3+3, Int32Field)`, "f5", 2},
	}
	for _, testcase := range testcases {
		expr, err := ParseExpr(helper, testcase.CallExpr, nil)
		assert.NoError(t, err, testcase)
		assert.Equal(t, testcase.FunctionName, expr.GetCallExpr().FunctionName, testcase)
		assert.Equal(t, testcase.ParameterNum, len(expr.GetCallExpr().FunctionParameters), testcase)
		ShowExpr(expr)
	}

	expr, err := ParseExpr(helper, "xxx(1+1, !true, f(10+10))", nil)
	assert.NoError(t, err)
	assert.Equal(t, "xxx", expr.GetCallExpr().FunctionName)
	assert.Equal(t, 3, len(expr.GetCallExpr().FunctionParameters))
	assert.Equal(t, int64(2), expr.GetCallExpr().GetFunctionParameters()[0].GetValueExpr().GetValue().GetInt64Val())
	assert.Equal(t, false, expr.GetCallExpr().GetFunctionParameters()[1].GetValueExpr().GetValue().GetBoolVal())
	assert.Equal(t, int64(20), expr.GetCallExpr().GetFunctionParameters()[2].GetCallExpr().GetFunctionParameters()[0].GetValueExpr().GetValue().GetInt64Val())

	expr, err = ParseExpr(helper, "ceil(pow(1.5*Int32Field,0.58))", nil)
	assert.Error(t, err)
	assert.Nil(t, expr)
}

func TestExpr_Compare(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`Int8Field < Int16Field`,
		`Int16Field <= Int32Field`,
		`Int32Field > Int64Field`,
		`Int64Field >= FloatField`,
		`FloatField == DoubleField`,
		`StringField != VarCharField`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	exprStrs = []string{
		`BoolField == false + true`,
		`StringField == "1" + "2"`,
		`BoolField == false - true`,
		`StringField == "1" - "2"`,
		`BoolField == false * true`,
		`StringField == "1" * "2"`,
		`BoolField == false / true`,
		`StringField == "1" / "2"`,
		`BoolField == false % true`,
		`StringField == "1" % "2"`,
	}

	for _, exprStr := range exprStrs {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_UnaryRange(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`Int8Field < 0`,
		`Int16Field <= 1`,
		`Int32Field > 2`,
		`Int64Field >= 3`,
		`FloatField == 4.0`,
		`FloatField == 2`,
		`DoubleField != 5.0`,
		`StringField > "str6"`,
		`VarCharField <= "str7"`,
		`JSONField["A"] > 10`,
		`$meta["A"] > 10`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}

func TestExpr_Like(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	expr := `A like "8\\_0%"`
	plan, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	fmt.Println(plan)
	assert.Equal(t, planpb.OpType_PrefixMatch, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, "8_0", plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())

	expr = `A like "8_\\_0%"`
	plan, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	fmt.Println(plan)
	assert.Equal(t, planpb.OpType_Match, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, `8_\_0%`, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())

	expr = `A like "8\\%-0%"`
	plan, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	fmt.Println(plan)
	assert.Equal(t, planpb.OpType_PrefixMatch, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, `8%-0`, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())

	// Single-quoted LIKE with escaped percent (literal %)
	expr = `A like '8\%0'`
	plan, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	assert.Equal(t, planpb.OpType_Equal, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, `8%0`, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())

	// Single-quoted LIKE with escaped underscore (literal _)
	expr = `A like '8\_0%'`
	plan, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	assert.Equal(t, planpb.OpType_PrefixMatch, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, `8_0`, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())

	// Single-quoted LIKE with both escaped percent and underscore
	expr = `A like '8\_\%0'`
	plan, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	assert.Equal(t, planpb.OpType_Equal, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, `8_%0`, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())

	// Single-quoted LIKE with escaped percent as prefix match
	expr = `A like '8\%-0%'`
	plan, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	assert.NotNil(t, plan)
	assert.Equal(t, planpb.OpType_PrefixMatch, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetOp())
	assert.Equal(t, `8%-0`, plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetValue().GetStringVal())
}

func TestExpr_TextMatch(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`text_match(VarCharField, "query")`,
	}
	for _, exprStr := range exprStrs {
		assertInvalidExpr(t, helper, exprStr)
	}

	enableMatch(schema)
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	unsupported := []string{
		`text_match(not_exist, "query")`,
		`text_match(BoolField, "query")`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_TextMatch_MinShouldMatch(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	for _, v := range []int64{1, 2, 1000} {
		expr := fmt.Sprintf(`text_match(VarCharField, "query", minimum_should_match=%d)`, v)
		plan, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         10,
			MetricType:   "L2",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err, expr)
		assert.NotNil(t, plan)

		predicates := plan.GetVectorAnns().GetPredicates()
		assert.NotNil(t, predicates)
		ure := predicates.GetUnaryRangeExpr()
		assert.NotNil(t, ure)
		assert.Equal(t, planpb.OpType_TextMatch, ure.GetOp())
		assert.Equal(t, "query", ure.GetValue().GetStringVal())
		extra := ure.GetExtraValues()
		assert.Equal(t, 1, len(extra))
		assert.Equal(t, v, extra[0].GetInt64Val())
	}

	{
		expr := `text_match(VarCharField, "query", minimum_should_match=0)`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "minimum_should_match should be >= 1")
	}

	{
		expr := `text_match(VarCharField, "query", minimum_should_match=1001)`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "minimum_should_match should be <= 1000")
	}

	{
		expr := `text_match(VarCharField, "query", minimum_should_match=1.5)`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
	}

	{
		expr := `text_match(VarCharField, "query", minimum_should_match={min})`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
		// grammar rejects placeholder before visitor; accept either parse error or visitor error
		errMsg := err.Error()
		assert.True(t, strings.Contains(errMsg, "mismatched input") || strings.Contains(errMsg, "minimum_should_match should be a const integer expression"), errMsg)
	}

	{
		expr := `text_match(VarCharField, "query", minimum_should_match=9223372036854775808)`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid minimum_should_match value")
	}

	{
		expr := `text_match(VarCharField, "\中国")`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
	}

	{
		expr := `text_match(VarCharField, "query", minimum_should_match=9223372036854775808)`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid minimum_should_match value")
	}

	{
		expr := `text_match(VarCharField, "\中国")`
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err)
	}
}

func TestExpr_TextMatch_MinShouldMatch_NilValue_Coverage(t *testing.T) {
	// This test is specifically to cover the error case in validateAndExtractMinShouldMatch
	// which handles the edge case where minShouldMatchExpr is an ExprWithType

	// Test case 1: ExprWithType with a ColumnExpr
	// This will make getValueExpr return nil
	exprWithColumnExpr := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ColumnExpr{
				ColumnExpr: &planpb.ColumnExpr{
					Info: &planpb.ColumnInfo{
						FieldId:  100,
						DataType: schemapb.DataType_Int64,
					},
				},
			},
		},
		dataType: schemapb.DataType_Int64,
	}

	_, err := validateAndExtractMinShouldMatch(exprWithColumnExpr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "minimum_should_match should be a const integer expression")

	// Test case 2: ExprWithType with a ValueExpr but nil Value
	// This will make getValueExpr return a non-nil ValueExpr but GetValue() returns nil
	exprWithNilValue := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: nil,
				},
			},
		},
		dataType: schemapb.DataType_Int64,
	}

	_, err = validateAndExtractMinShouldMatch(exprWithNilValue)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "minimum_should_match should be a const integer expression")

	// Test case 3: Valid ExprWithType with proper value
	validExpr := &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewInt(10),
				},
			},
		},
		dataType: schemapb.DataType_Int64,
	}

	extraVals, err := validateAndExtractMinShouldMatch(validExpr)
	assert.NoError(t, err)
	assert.NotNil(t, extraVals)
	assert.Equal(t, 1, len(extraVals))
	assert.Equal(t, int64(10), extraVals[0].GetInt64Val())
}

func TestExpr_TextMatch_MinShouldMatch_Omitted(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	expr := `text_match(VarCharField, "query")`
	plan, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	predicates := plan.GetVectorAnns().GetPredicates()
	assert.NotNil(t, predicates)
	ure := predicates.GetUnaryRangeExpr()
	assert.NotNil(t, ure)
	assert.Equal(t, planpb.OpType_TextMatch, ure.GetOp())
	assert.Equal(t, "query", ure.GetValue().GetStringVal())
	// When omitted, ExtraValues should be empty
	assert.Equal(t, 0, len(ure.GetExtraValues()))
}

func TestExpr_TextMatch_MinShouldMatch_IntegerConstant(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	expr := `text_match(VarCharField, "query", minimum_should_match=10)`
	plan, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, plan)

	predicates := plan.GetVectorAnns().GetPredicates()
	assert.NotNil(t, predicates)
	ure := predicates.GetUnaryRangeExpr()
	assert.NotNil(t, ure)
	assert.Equal(t, planpb.OpType_TextMatch, ure.GetOp())
	assert.Equal(t, "query", ure.GetValue().GetStringVal())
	extra := ure.GetExtraValues()
	assert.Equal(t, 1, len(extra))
	assert.Equal(t, int64(10), extra[0].GetInt64Val())
}

func TestExpr_TextMatch_MinShouldMatch_NameTypos(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	invalid := []string{
		`text_match(VarCharField, "q", minimum_shouldmatch=1)`,
		`text_match(VarCharField, "q", min_should_match=1)`,
		`text_match(VarCharField, "q", minimumShouldMatch=1)`,
		`text_match(VarCharField, "q", minimum-should-match=1)`,
		`text_match(VarCharField, "q", minimum_should_matchx=1)`,
	}
	for _, expr := range invalid {
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err, expr)
	}
}

func TestExpr_TextMatch_MinShouldMatch_InvalidValueTypes(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	invalid := []string{
		`text_match(VarCharField, "q", minimum_should_match=10*10)`,
		`text_match(VarCharField, "q", minimum_should_match=nil)`,
		`text_match(VarCharField, "q", minimum_should_match=)`,
		`text_match(VarCharField, "q", minimum_should_match="10")`,
		`text_match(VarCharField, "q", minimum_should_match=true)`,
		`text_match(VarCharField, "q", minimum_should_match=a)`,
		`text_match(VarCharField, "q", minimum_should_match={min})`,
		`text_match(VarCharField, "q", minimum_should_match=1.0)`,
		`text_match(VarCharField, "q", minimum_should_match=-1)`,
	}
	for _, expr := range invalid {
		_, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
		assert.Error(t, err, expr)
	}
}

func TestExpr_TextMatch_MinShouldMatch_LeadingZerosAndOctal(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)
	{
		expr := `text_match(VarCharField, "query", minimum_should_match=001)`
		plan, err := CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{Topk: 10}, nil, nil)
		assert.NoError(t, err)
		ure := plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr()
		extra := ure.GetExtraValues()
		assert.Equal(t, 1, len(extra))
		assert.Equal(t, int64(1), extra[0].GetInt64Val())
	}
}

func TestExpr_TextMatch_MinShouldMatch_DuplicateOption(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	expr := `text_match(VarCharField, "query", minimum_should_match=2, minimum_should_match=3)`
	_, err = CreateSearchPlan(helper, expr, "FloatVectorField", &planpb.QueryInfo{}, nil, nil)
	assert.Error(t, err)
}

func TestExpr_PhraseMatch(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	enableMatch(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`phrase_match(VarCharField, "phrase")`,
		`phrase_match(StringField, "phrase")`,
		`phrase_match(StringField, "phrase", 1)`,
		`phrase_match(VarCharField, "phrase", 11223)`,
		`phrase_match(StringField, "phrase", 0)`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	unsupported := []string{
		`phrase_match(not_exist, "phrase")`,
		`phrase_match(BoolField, "phrase")`,
		`phrase_match(StringField, "phrase", -1)`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}

	unsupported = []string{
		`phrase_match(StringField, "phrase", -1)`,
		`phrase_match(StringField, "phrase", a)`,
		`phrase_match(StringField, "phrase", -a)`,
		`phrase_match(StringField, "phrase", 4294967296)`,
	}
	errMsgs := []string{
		`"slop" should not be a negative interger. "slop" passed: -1`,
		`"slop" should be a const integer expression with "uint32" value. "slop" expression passed: a`,
		`"slop" should be a const integer expression with "uint32" value. "slop" expression passed: -a`,
		`"slop" exceeds the range of "uint32". "slop" expression passed: 4294967296`,
	}
	for i, exprStr := range unsupported {
		_, err := ParseExpr(helper, exprStr, nil)
		assert.True(t, strings.Contains(err.Error(), errMsgs[i]), fmt.Sprintf("Error expected: %v, actual %v", errMsgs[i], err.Error()))
	}
}

func TestExpr_TextField(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	invalidExprs := []string{
		`TextField == "query"`,
		`text_match(TextField, "query")`,
	}

	for _, exprStr := range invalidExprs {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_IsNull(t *testing.T) {
	schema := newTestSchema(false)
	schema.EnableDynamicField = false
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`VarCharField is null`,
		`VarCharField IS NULL`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	unsupported := []string{
		`not_exist is null`,
		`FloatVectorField is null`,
		`BinaryVectorField is null`,
		`Float16VectorField is null`,
		`BFloat16VectorField is null`,
		`SparseFloatVectorField is null`,
		`Int8VectorField is null`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_IsNotNull(t *testing.T) {
	schema := newTestSchema(false)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`VarCharField is not null`,
		`VarCharField IS NOT NULL`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	unsupported := []string{
		`not_exist is not null`,
		`FloatVectorField is not null`,
		`BinaryVectorField is not null`,
		`Float16VectorField is not null`,
		`BFloat16VectorField is not null`,
		`SparseFloatVectorField is not null`,
		`Int8VectorField is not null`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_BinaryRange(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`1 < Int8Field < 2`,
		`3 <= Int16Field < 4`,
		`5 <= Int32Field <= 6`,
		`7 < Int64Field <= 8`,
		`9.0 < FloatField < 10.0`,
		`11.0 < DoubleField < 12.0`,
		`"str13" < StringField < "str14"`,
		`"str15" < VarCharField < "str16"`,
		`17 < DoubleField < 18`,
		`10 < A < 25`,

		`2 > Int8Field > 1`,
		`4 >= Int16Field >= 3`,
		`6 >= Int32Field >= 5`,
		`8 >= Int64Field > 7`,
		`10.0 > FloatField > 9.0`,
		`12.0 > DoubleField > 11.0`,
		`"str14" > StringField > "str13"`,
		`"str16" > VarCharField > "str15"`,
		`18 > DoubleField > 17`,
		`100 > B > 14`,
		`1 < JSONField < 3`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	invalidExprs := []string{
		`1 < ArrayField < 3`,
		`1 < A+B < 3`,
	}

	for _, exprStr := range invalidExprs {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_castValue(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStr := `Int64Field + 1.1 == 2.1`
	expr, err := ParseExpr(helper, exprStr, nil)
	assert.Error(t, err, exprStr)
	assert.Nil(t, expr, exprStr)

	exprStr = `FloatField +1 == 2`
	expr, err = ParseExpr(helper, exprStr, nil)
	assert.NoError(t, err, exprStr)
	assert.NotNil(t, expr, exprStr)
	assert.NotNil(t, expr.GetBinaryArithOpEvalRangeExpr())
	assert.NotNil(t, expr.GetBinaryArithOpEvalRangeExpr().GetRightOperand().GetFloatVal())
	assert.NotNil(t, expr.GetBinaryArithOpEvalRangeExpr().GetValue().GetFloatVal())
}

func TestExpr_BinaryArith(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`Int64Field % 10 == 9`,
		`Int64Field % 10 != 9`,
		`FloatField + 1.1 == 2.1`,
		`A % 10 != 2`,
		`Int8Field + 1 < 2`,
		`Int16Field - 3 <= 4`,
		`Int32Field * 5 > 6`,
		`Int64Field / 7 >= 8`,
		`FloatField + 11 < 12`,
		`DoubleField - 13 <= 14`,
		`A * 15 > 16`,
		`JSONField['A'] / 17 >= 18`,
		`ArrayField[0] % 19 >= 20`,
		`JSONField + 15 == 16`,
		`15 + JSONField == 16`,
		`Int64Field + (2**3) > 0`,
		`1 + FloatField > 100`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	// TODO: enable these after execution backend is ready.
	unsupported := []string{
		`ArrayField + 15 == 16`,
		`15 + ArrayField == 16`,
		`Int64Field + 1.1 = 2.1`,
		`Int64Field == 2.1`,
		`Int64Field >= 2.1`,
		`3 > Int64Field >= 2.1`,
		`Int64Field + (2**-1) > 0`,
		`Int64Field / 0 == 1`,
		`Int64Field % 0 == 1`,
		`FloatField / 0 == 1`,
		`FloatField % 0 == 1`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_Value(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`1`,
		`2.0`,
		`true`,
		`false`,
		`"str"`,
		`3 > 2`,
	}
	for _, exprStr := range exprStrs {
		expr := handleExpr(helper, exprStr)
		assert.NotNil(t, getExpr(expr).expr, exprStr)
		// fmt.Printf("expr: %s\n", exprStr)
		// ShowExpr(getExpr(expr).expr)
	}
}

func TestExpr_Identifier(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`BoolField`,
		`Int8Field`,
		`Int16Field`,
		`Int32Field`,
		`Int64Field`,
		`FloatField`,
		`DoubleField`,
		`StringField`,
		`VarCharField`,
		`JSONField["A"]`,
		`$meta["A"]`,
	}
	for _, exprStr := range exprStrs {
		expr := handleExpr(helper, exprStr)
		assert.NotNil(t, getExpr(expr).expr, exprStr)

		// fmt.Printf("expr: %s\n", exprStr)
		// ShowExpr(getExpr(expr).expr)
	}
}

func TestExpr_Constant(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		// ------------------- arithmetic operations ----------------
		`1.0 + 2.0`,
		`1.0 + 2`,
		`1 + 2.0`,
		`1 + 2`,
		`1.0 - 2.0`,
		`1.0 - 2`,
		`1 - 2.0`,
		`1 - 2`,
		`1.0 * 2.0`,
		`1.0 * 2`,
		`1 * 2.0`,
		`1 * 2`,
		`1.0 / 2.0`,
		`1.0 / 2`,
		`1 / 2.0`,
		`1 / 2`,
		`1 % 2`,
		// ------------------- logical operations ----------------
		`true and false`,
		`true or false`,
		`!true`,
		`!false`,
		// ------------------- relational operations ----------------
		`"1" < "2"`,
		`1.0 < 2.0`,
		`1.0 < 2`,
		`1 < 2.0`,
		`1 < 2`,
		`"1" <= "2"`,
		`1.0 <= 2.0`,
		`1.0 <= 2`,
		`1 <= 2.0`,
		`1 <= 2`,
		`"1" > "2"`,
		`1.0 > 2.0`,
		`1.0 > 2`,
		`1 > 2.0`,
		`1 > 2`,
		`"1" >= "2"`,
		`1.0 >= 2.0`,
		`1.0 >= 2`,
		`1 >= 2.0`,
		`1 >= 2`,
		`"1" == "2"`,
		`1.0 == 2.0`,
		`1.0 == 2`,
		`1 == 2.0`,
		`1 == 2`,
		`true == false`,
		`"1" != "2"`,
		`1.0 != 2.0`,
		`1.0 != 2`,
		`1 != 2.0`,
		`1 != 2`,
		`true != false`,
	}
	for _, exprStr := range exprStrs {
		expr := handleExpr(helper, exprStr)
		assert.NotNil(t, getExpr(expr).expr, exprStr)

		// fmt.Printf("expr: %s\n", exprStr)
		// ShowExpr(getExpr(expr).expr)
	}
}

func TestExpr_Combinations(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`not (Int8Field + 1 == 2)`,
		`(Int16Field - 3 == 4) and (Int32Field * 5 != 6)`,
		`(Int16Field - 3 == 4) AND (Int32Field * 5 != 6)`,
		`(Int64Field / 7 != 8) or (Int64Field % 10 == 9)`,
		`(Int64Field / 7 != 8) OR (Int64Field % 10 == 9)`,
		`Int64Field > 0 && VarCharField > "0"`,
		`Int64Field < 0 && VarCharField < "0"`,
		`A > 50 or B < 40`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}

func TestCreateRetrievePlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateRetrievePlan(schema, "Int64Field > 0", nil)
	assert.NoError(t, err)

	_, err = CreateRetrievePlan(schema, "id > -9223372036854775808", nil)
	assert.NoError(t, err)
}

func TestCreateSearchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err)
}

func TestCreateFloat16SearchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "Float16VectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err)
}

func TestCreateBFloat16earchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "BFloat16VectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err)
}

func TestCreateSparseFloatVectorSearchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "SparseFloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err)
}

func TestExpr_Invalid(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`invalid expression`,
		`"constant"`,
		// -------------- identifier not in schema --------------
		`not_in_schema`,
		// ------------------------ Add/Sub ---------------------
		`not_in_schema + 1`,
		`1 - not_in_schema`,
		`true + false`,
		`"str" + "text"`,
		`true + "str"`,
		`true - false`,
		`"str" - "text"`,
		`true - "str"`,
		`StringField + VarCharField`,
		`StringField - 2`,
		`2 + StringField`,
		// ------------------------ Mul/Div/Mod ---------------------
		`not_in_schema * 1`,
		`1 / not_in_schema`,
		`1 % not_in_schema`,
		`true * false`,
		`true / false`,
		`true % false`,
		`"str" * "text"`,
		`"str" / "text"`,
		`"str" % "text"`,
		`2 / 0`,
		`2 % 0`,
		`StringField % VarCharField`,
		`StringField * 2`,
		`2 / StringField`,
		//`JSONField / 2 == 1`,
		`2 % JSONField == 1`,
		`2 % Int64Field == 1`,
		`ArrayField / 2 == 1`,
		`2 / ArrayField == 1`,
		// ----------------------- ==/!= -------------------------
		//`not_in_schema != 1`, // maybe in json
		//`1 == not_in_schema`, // maybe in json
		`true == "str"`,
		`"str" != false`,
		`VarCharField != FloatField`,
		`FloatField == VarCharField`,
		`A == -9223372036854775809`,
		// ---------------------- relational --------------------
		//`not_in_schema < 1`, // maybe in json
		//`1 <= not_in_schema`, // maybe in json
		`true <= "str"`,
		`"str" >= false`,
		`VarCharField < FloatField`,
		`FloatField > VarCharField`,
		//`JSONField > 1`,
		//`1 < JSONField`,
		`ArrayField > 2`,
		`2 < ArrayField`,
		// https://github.com/milvus-io/milvus/issues/34139
		"\"Int64Field\" > 500 && \"Int64Field\" < 1000",
		"\"Int64Field\" == 500 || \"Int64Field\" != 1000",
		`"str" < 100`,
		`"str" <= 100`,
		`"str" > 100`,
		`"str" >= 100`,
		`"str" == 100`,
		`"str" != 100`,
		// ------------------------ like ------------------------
		`(VarCharField % 2) like "prefix%"`,
		`FloatField like "prefix%"`,
		//`value like "prefix%"`, // maybe in json
		// ------------------------ term ------------------------
		//`not_in_schema in [1, 2, 3]`, // maybe in json
		`1 in [1, 2, 3]`,
		`(Int8Field + 8) in [1, 2, 3]`,
		`Int8Field in [(true + 1)]`,
		`Int8Field in [Int16Field]`,
		`BoolField in [4.0]`,
		`VarCharField in [4.0]`,
		`Int32Field in [4.0]`,
		`FloatField in [5, 6.0, true]`,
		`1 in A`,
		// ----------------------- range -------------------------
		//`1 < not_in_schema < 2`, // maybe in json
		`1 < 3 < 2`,
		`1 < (Int8Field + Int16Field) < 2`,
		`(invalid_lower) < Int32Field < 2`,
		`1 < Int32Field < (invalid_upper)`,
		`(Int8Field) < Int32Field < 2`,
		`1 < Int32Field < (Int16Field)`,
		`1 < StringField < 2`,
		`1 < BoolField < 2`,
		`1.0 < Int32Field < 2.0`,
		`true < FloatField < false`,
		// `2 <= Int32Field <= 1`,
		`2 = Int32Field = 1`,
		`true = BoolField = false`,
		// ----------------------- unary ------------------------
		`-true`,
		`!"str"`,
		`!(not_in_schema)`,
		`-Int32Field`,
		`!(Int32Field)`,
		// ----------------------- or/and ------------------------
		`not_in_schema or true`,
		`false or not_in_schema`,
		`"str" or false`,
		`BoolField OR false`,
		`Int32Field OR Int64Field`,
		`not_in_schema and true`,
		`false AND not_in_schema`,
		`"str" and false`,
		`BoolField and false`,
		`Int32Field AND Int64Field`,
		// -------------------- unsupported ----------------------
		`1 ^ 2`,
		`1 & 2`,
		`1 ** 2`,
		`1 << 2`,
		`1 | 2`,
		// -------------------- cannot be independent ----------------------
		`BoolField`,
		`true`,
		`false`,
		`Int64Field > 100 and BoolField`,
		`Int64Field < 100 or false`, // maybe this can be optimized.
		`!BoolField`,
		// -------------------- array ----------------------
		//`A == [1, 2, 3]`,
		`Int64Field == [1, 2, 3]`,
		`Int64Field > [1, 2, 3]`,
		`Int64Field + [1, 2, 3] == 10`,
		`Int64Field % [1, 2, 3] == 10`,
		`[1, 2, 3] < Int64Field < [4, 5, 6]`,
		`Int64Field["A"] == 123`,
		`[1,2,3] == [4,5,6]`,
		`[1,2,3] == 1`,
	}
	for _, exprStr := range exprStrs {
		_, err := ParseExpr(helper, exprStr, nil)
		assert.Error(t, err, exprStr)
	}
}

func TestCreateRetrievePlan_Invalid(t *testing.T) {
	t.Run("invalid expr", func(t *testing.T) {
		schema := newTestSchemaHelper(t)
		_, err := CreateRetrievePlan(schema, "invalid expression", nil)
		assert.Error(t, err)
	})
}

func TestCreateSearchPlan_Invalid(t *testing.T) {
	t.Run("invalid expr", func(t *testing.T) {
		schema := newTestSchemaHelper(t)
		_, err := CreateSearchPlan(schema, "invalid expression", "", nil, nil, nil)
		assert.Error(t, err)
	})

	t.Run("invalid vector field", func(t *testing.T) {
		schema := newTestSchemaHelper(t)
		_, err := CreateSearchPlan(schema, "Int64Field > 0", "not_exist", nil, nil, nil)
		assert.Error(t, err)
	})

	t.Run("not vector type", func(t *testing.T) {
		schema := newTestSchemaHelper(t)
		_, err := CreateSearchPlan(schema, "Int64Field > 0", "VarCharField", nil, nil, nil)
		assert.Error(t, err)
	})
}

var listenerCnt int

type errorListenerTest struct {
	antlr.DefaultErrorListener
}

func (l *errorListenerTest) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	listenerCnt += 1
}

func (l *errorListenerTest) ReportAmbiguity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, exact bool, ambigAlts *antlr.BitSet, configs *antlr.ATNConfigSet) {
	listenerCnt += 1
}

func (l *errorListenerTest) Error() error {
	return nil
}

func Test_FixErrorListenerNotRemoved(t *testing.T) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	normal := "1 < Int32Field < (Int16Field)"
	for i := 0; i < 10; i++ {
		err := handleExpr(schemaHelper, normal)
		err1, ok := err.(error)
		assert.True(t, ok)
		assert.Error(t, err1)
	}
	assert.True(t, listenerCnt <= 10)
}

func Test_handleExpr(t *testing.T) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	ret1 := handleExpr(schemaHelper, "1 < Int32Field < (Int16Field)")
	err1, ok := ret1.(error)
	assert.True(t, ok)
	assert.Error(t, err1)
}

func Test_handleExpr_empty(t *testing.T) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	ret1 := handleExpr(schemaHelper, "")
	assert.True(t, isAlwaysTrueExpr(getExpr(ret1).expr))
	assert.Equal(t, schemapb.DataType_Bool, getExpr(ret1).dataType)
}

// test if handleExpr is thread-safe.
func Test_handleExpr_17126_26662(t *testing.T) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)
	normal := `VarCharField == "abcd\"defg"`

	n := 400
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ret := handleExpr(schemaHelper, normal)
			_, ok := ret.(error)
			assert.False(t, ok)
		}()
	}
	wg.Wait()
}

func Test_JSONExpr(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	// search
	exprs := []string{
		`$meta["A"] > 90`,
		`JSONField["A"] > 90`,
		`A < 10`,
		`JSONField["A"] <= 5`,
		`$meta["A"] <= 5`,
		`$meta["A"] >= 95`,
		`$meta["A"] == 5`,
		`$meta["A"] != 95`,
		`$meta["A"] > 90 && $meta["B"] < 5`,
		`$meta["A"] > 95 || $meta["B"] < 5`,
		`A > 95 || $meta["B"] < 5`,
		`not ($meta["A"] == 95)`,
		`$meta["A"] in [90, 91, 95, 97]`,
		`$meta["A"] not in [90, 91, 95, 97]`,
		`$meta["C"]["0"] in [90, 91, 95, 97]`,
		`$meta["C"]["0"] not in [90, 91, 95, 97]`,
		`C["0"] not in [90, 91, 95, 97]`,
		`C[0] in [90, 91, 95, 97]`,
		`C["0"] > 90`,
		`C["0"] < 90`,
		`C["0"] == 90`,
		`10 < C["0"] < 90`,
		`100 > C["0"] > 90`,
		`0 <= $meta["A"] < 5`,
		`0 <= A < 5`,
		`$meta["A"] + 5 == 10`,
		`$meta["A"] > 10 + 5`,
		`100 - 5 < $meta["A"]`,
		`100 == $meta["A"] + 6`,
		`exists $meta["A"]`,
		`exists $meta["A"]["B"]["C"] `,
		`exists $meta["A"] || exists JSONField["A"]`,
		`exists $meta["A"] && exists JSONField["A"]`,
		`A["B"][0] > 100`,
		`$meta[0] > 100`,
		`A["\"\"B\"\""] > 10`,
		`A["[\"B\"]"] == "abc\"bbb\"cc"`,
		`A['B'] == "abc\"bbb\"cc"`,
		`A['B'] == 'abc"cba'`,
		`A['B'] == 'abc\"cba'`,
		`A == [1,2,3]`,
		`A + 1.2 == 3.3`,
		`A + 1 == 2`,
		`JSONField > 0`,
		`JSONField == 0`,
		`JSONField < 100`,
		`0 < JSONField < 100`,
		`20 > JSONField > 0`,
		`JSONField + 5 > 0`,
		`JSONField > 2 + 5`,
		`JSONField * 2 > 5`,
		`JSONField / 2 > 5`,
		`JSONField % 10 > 5`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
	}
}

func Test_InvalidExprOnJSONField(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	// search
	exprs := []string{
		`exists $meta`,
		`exists JSONField`,
		`exists ArrayField`,
		`exists $meta["A"] > 10 `,
		`exists Int64Field`,
		`A[[""B""]] > 10`,
		`A["[""B""]"] > 10`,
		`A[[""B""]] > 10`,
		`A[B] > 10`,
		`A + B == 3.3`,
	}

	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err, expr)
	}
}

func Test_InvalidExprWithoutJSONField(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, Description: "id", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "vector", IsPrimaryKey: false, Description: "vector", DataType: schemapb.DataType_FloatVector},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      true,
		Fields:      fields,
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr := ""

	exprs := []string{
		`A == 0`,
		`JSON["A"] > 0`,
		`A < 100`,
		`0 < JSON["A"] < 100`,
		`0 < A < 100`,
		`100 > JSON["A"] > 0`,
		`100 > A > 0`,
	}

	for _, expr = range exprs {
		_, err = CreateSearchPlan(schemaHelper, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
	}
}

func Test_InvalidExprWithMultipleJSONField(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, Description: "id", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "vector", IsPrimaryKey: false, Description: "vector", DataType: schemapb.DataType_FloatVector},
		{FieldID: 102, Name: "json1", IsPrimaryKey: false, Description: "json field 1", DataType: schemapb.DataType_JSON},
		{FieldID: 103, Name: "json2", IsPrimaryKey: false, Description: "json field 2", DataType: schemapb.DataType_JSON},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      true,
		Fields:      fields,
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	expr := ""
	exprs := []string{
		`A == 0`,
		`A in [1, 2, 3]`,
		`A not in [1, 2, 3]`,
		`"1" in A`,
		`"1" not in A`,
	}

	for _, expr = range exprs {
		_, err = CreateSearchPlan(schemaHelper, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
	}
}

func Test_exprWithSingleQuotes(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	exprs := []string{
		`'abc' < StringField < "def"`,
		`'ab"c' < StringField < "d'ef"`,
		`'ab\"c' < StringField < "d\'ef"`,
		`'ab\'c' < StringField < "d\"ef"`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
	}

	invalidExprs := []string{
		`'abc'd' < StringField < "def"`,
		`'abc' < StringField < "def"g"`,
	}

	for _, expr = range invalidExprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
	}
}

func Test_JSONContains(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	exprs := []string{
		`json_contains(A, 10)`,
		`not json_contains(A, 10)`,
		`json_contains(A, 10.5)`,
		`not json_contains(A, 10.5)`,
		`json_contains(A, "10")`,
		`not json_contains(A, "10")`,
		`json_contains($meta["A"], 10)`,
		`not json_contains($meta["A"], 10)`,
		`json_contains(JSONField["x"], 5)`,
		`not json_contains(JSONField["x"], 5)`,
		`JSON_CONTAINS(JSONField["x"], 5)`,
		`json_Contains(JSONField, 5)`,
		`JSON_contains(JSONField, 5)`,
		`json_contains(A, [1,2,3])`,
		`array_contains(A, [1,2,3])`,
		`array_contains(ArrayField, 1)`,
		`json_contains(JSONField, 5)`,
		`json_contains($meta, 1)`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
	}
}

func Test_InvalidJSONContains(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	exprs := []string{
		`json_contains(10, A)`,
		`json_contains(1, [1,2,3])`,
		`json_contains([1,2,3], 1)`,
		`json_contains([1,2,3], [1,2,3])`,
		`json_contains([1,2,3], [1,2])`,
		`json_contains(A, B)`,
		`not json_contains(A, B)`,
		`json_contains(A, B > 5)`,
		`json_contains(StringField, "a")`,
		`json_contains(A, StringField > 5)`,
		`json_contains(A)`,
		`json_contains(A, 5, C)`,
		`json_contains(ArrayField, "abc")`,
		`json_contains(ArrayField, [1,2])`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
	}
}

func Test_isEmptyExpression(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{s: ""},
			want: true,
		},
		{
			args: args{s: "         "},
			want: true,
		},
		{
			args: args{s: "not empty"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isEmptyExpression(tt.args.s), "isEmptyExpression(%v)", tt.args.s)
		})
	}
}

func Test_EscapeString(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	exprs := []string{
		`A == "\"" || B == '\"'`,
		`A == "\n" || B == '\n'`,
		`A == "\367" || B == '\367'`,
		`A == "\3678" || B == '\3678'`,
		`A == "ab'c\'d" || B == 'abc"de\"'`,
		`A == "'" || B == '"'`,
		`A == "\'" || B == '\"' || C == '\''`,
		`A == "\\'" || B == '\\"' || C == '\''`,
		`A == "\\\'" || B == '\\\"' || C == '\\\''`,
		`A == "\\\\'" || B == '\\\\"' || C == '\\\''`,
		`A == "\\\\\'" || B == '\\\\\"' || C == '\\\\\''`,
		`A == "\\\\\\'" || B == '\\\\\\"' || C == '\\\\\''`,
		`str2 like 'abc\"def-%'`,
		`str2 like 'abc"def-%'`,
		`str4 like "abc\367-%"`,
		`str4 like "中国"`,
		`tag == '"blue"'`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
	}

	invalidExprs := []string{
		`A == "ab
c" || B == 'ab
c'`,
		`A == "\423" || B == '\378'`,
		`A == "\中国"`,
	}
	for _, expr = range invalidExprs {
		plan, err := CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
		fmt.Println(plan)
	}
}

// todo add null test

func Test_JSONContainsAll(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	var plan *planpb.PlanNode

	exprs := []string{
		`json_contains_all(A, [1,2,3])`,
		`json_contains_all(A, [1,"2",3.0])`,
		`JSON_CONTAINS_ALL(A, [1,"2",3.0])`,
		`array_contains_all(ArrayField, [1,2,3])`,
		`array_contains_all(ArrayField, [1])`,
		`array_contains_all(ArrayField, [1,2,3])`,
	}
	for _, expr = range exprs {
		plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetJsonContainsExpr())
		assert.Equal(t, planpb.JSONContainsExpr_ContainsAll, plan.GetVectorAnns().GetPredicates().GetJsonContainsExpr().GetOp())
	}

	invalidExprs := []string{
		`JSON_CONTAINS_ALL(A, 1)`,
		`JSON_CONTAINS_ALL(A, [abc])`,
		`JSON_CONTAINS_ALL(A, [2>a])`,
		`JSON_CONTAINS_ALL(A, [2>>a])`,
		`JSON_CONTAINS_ALL(A[""], [1,2,3])`,
		`JSON_CONTAINS_ALL(Int64Field, [1,2,3])`,
		`JSON_CONTAINS_ALL(A, B)`,
		`JSON_CONTAINS_ALL(ArrayField, [[1,2,3]])`,
	}
	for _, expr = range invalidExprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
	}
}

func Test_JSONContainsAny(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error
	var plan *planpb.PlanNode

	exprs := []string{
		`json_contains_any(A, [1,2,3])`,
		`json_contains_any(A, [1,"2",3.0])`,
		`JSON_CONTAINS_ANY(A, [1,"2",3.0])`,
		`JSON_CONTAINS_ANY(ArrayField, [1,2,3])`,
	}
	for _, expr = range exprs {
		plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetJsonContainsExpr())
		assert.Equal(t, planpb.JSONContainsExpr_ContainsAny, plan.GetVectorAnns().GetPredicates().GetJsonContainsExpr().GetOp())
	}

	invalidExprs := []string{
		`JSON_CONTAINS_ANY(A, 1)`,
		`JSON_CONTAINS_ANY(A, [abc])`,
		`JSON_CONTAINS_ANY(A, [2>a])`,
		`JSON_CONTAINS_ANY(A, [2>>a])`,
		`JSON_CONTAINS_ANY(A[""], [1,2,3])`,
		`JSON_CONTAINS_ANY(Int64Field, [1,2,3])`,
		`JSON_CONTAINS_ANY(ArrayField, [[1,2,3]])`,
		`JSON_CONTAINS_ANY(A, B)`,
	}
	for _, expr = range invalidExprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err)
	}
}

func Test_ArrayExpr(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error

	exprs := []string{
		`ArrayField == [1,2,3,4]`,
		`ArrayField[0] == 1`,
		`ArrayField[0] > 1`,
		`1 < ArrayField[0] < 3`,
		`StringArrayField[0] == "abc"`,
		`StringArrayField[0] < "abc"`,
		`"abc" < StringArrayField[0] < "efg"`,
		`array_contains(ArrayField, 1)`,
		`not ARRAY_CONTAINS(ArrayField, 1)`,
		`array_contains_all(ArrayField, [1,2,3,4])`,
		`not ARRAY_CONTAINS_ALL(ArrayField, [1,2,3,4])`,
		`array_contains_any(ArrayField, [1,2,3,4])`,
		`not ARRAY_CONTAINS_ANY(ArrayField, [1,2,3,4])`,
		`array_contains(StringArrayField, "abc")`,
		`not ARRAY_CONTAINS(StringArrayField, "abc")`,
		`array_contains_all(StringArrayField, ["a", "b", "c", "d"])`,
		`not ARRAY_CONTAINS_ALL(StringArrayField, ["a", "b", "c", "d"])`,
		`array_contains_any(StringArrayField, ["a", "b", "c", "d"])`,
		`not ARRAY_CONTAINS_ANY(StringArrayField, ["a", "b", "c", "d"])`,
		`StringArrayField[0] like "abd%"`,
		`+ArrayField[0] == 1`,
		`ArrayField[0] % 3 == 1`,
		`ArrayField[0] + 3 == 1`,
		`ArrayField[0] in [1,2,3]`,
		`ArrayField[0] in []`,
		`0 < ArrayField[0] < 100`,
		`100 > ArrayField[0] > 0`,
		`ArrayField[0] > 1`,
		`ArrayField[0] == 1`,
		`ArrayField in []`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err, expr)
	}

	invalidExprs := []string{
		`ArrayField == ["abc", "def"]`,
		`"abc" < ArrayField[0] < "def"`,
		`ArrayField[0] == "def"`,
		`ArrayField["0"] == 1`,
		`array_contains(ArrayField, "a")`,
		`array_contains(StringArrayField, 1)`,
		`array_contains_all(StringArrayField, ["abc", 123])`,
		`array_contains_any(StringArrayField, ["abc", 123])`,
		`StringArrayField like "abd%"`,
		`+ArrayField == 1`,
		`ArrayField % 3 == 1`,
		`ArrayField + 3 == 1`,
		`ArrayField in [1,2,3]`,
		`ArrayField[0] in [1, "abc",3.3]`,
		`0 < ArrayField < 100`,
		`100 > ArrayField > 0`,
		`ArrayField > 1`,
		`ArrayField == 1`,
		`ArrayField[] == 1`,
		`A[] == 1`,
		`ArrayField[0] + ArrayField[1] == 1`,
		`ArrayField == []`,
	}
	for _, expr = range invalidExprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err, expr)
	}
}

func Test_ArrayLength(t *testing.T) {
	schema := newTestSchemaHelper(t)
	expr := ""
	var err error

	exprs := []string{
		`array_length(ArrayField) == 10`,
		`array_length(A) != 10`,
		`array_length(StringArrayField) == 1`,
		`array_length(B) != 1`,
		`not (array_length(C[0]) == 1)`,
		`not (array_length(C["D"]) != 1)`,
		`array_length(StringArrayField) < 1`,
		`array_length(StringArrayField) <= 1`,
		`array_length(StringArrayField) > 5`,
		`array_length(StringArrayField) >= 5`,
	}
	for _, expr = range exprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err, expr)
	}

	invalidExprs := []string{
		`array_length(a > b) == 0`,
		`array_length(a, b) == 1`,
		`array_length(A)`,
		`array_length("A") / 10 == 2`,
		`array_length(Int64Field) == 2`,
		`array_length(a-b) == 2`,
		`0 < array_length(a-b) < 2`,
		`0 < array_length(StringArrayField) < 1`,
		`100 > array_length(ArrayField) > 10`,
		`array_length(A) % 10 == 2`,
		`array_length(A) / 10 == 2`,
		`array_length(A) + 1  == 2`,
		`array_length(JSONField) + 1  == 2`,
		`array_length(A)  == 2.2`,
	}
	for _, expr = range invalidExprs {
		_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.Error(t, err, expr)
	}
}

// Test randome sample with all other predicate expressions.
func TestRandomSampleWithFilter(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	enableMatch(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`random_sample(0.01)`,
		`random_sample(0.9999)`,
		`BoolField in [true, false] && random_sample(0.01)`,
		`Int8Field < Int16Field && random_sample(0.01)`,
		`Int8Field < Int16Field && Int16Field <= 1 && random_sample(0.01)`,
		`(Int8Field < Int16Field || Int16Field <= 1) && random_sample(0.01)`,
		`Int16Field <= 1 && random_sample(0.01)`,
		`VarCharField like "prefix%" && random_sample(0.01)`,
		`VarCharField is null && random_sample(0.01)`,
		`VarCharField IS NOT NULL && random_sample(0.01)`,
		`11.0 < DoubleField < 12.0 && random_sample(0.01)`,
		`1 < JSONField < 3 && random_sample(0.01)`,
		`Int64Field + 1 == 2 && random_sample(0.01)`,
		`Int64Field % 10 != 9 && random_sample(0.01)`,
		`A * 15 > 16 && random_sample(0.01)`,
		`(Int16Field - 3 == 4) and (Int32Field * 5 != 6) && random_sample(0.01)`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	exprStrsInvalid := []string{
		`random_sample(a)`,
		`random_sample(1)`,
		`random_sample(1.1)`,
		`random_sample(0)`,
		`random_sample(-1)`,
		`random_sample(0.01, Int8Field < Int16Field) + 1`,
		`random_sample(0.01, Int8Field < Int16Field) ** 2 == 1`,
		`not random_sample(0.01, Int8Field < Int16Field)`,
		`(Int16Field - 3 == 4) || random_sample(0.01)`,
		`random_sample(0.01) and (Int16Field - 3 == 4)`,
		`random_sample(0.01) or (Int16Field - 3 == 4)`,
		`random_sample(0.01, FloatField)`,
		`random_sample(0.01, 1.0 + 2.0)`,
		`random_sample(0.01, false)`,
		`random_sample(0.01, text_match(VarCharField, "query"))`,
		`random_sample(0.01, phrase_match(VarCharField, "query something"))`,
		`Int8Field < Int16Field || Int16Field <= 1 && random_sample(0.01)`,
	}
	for _, exprStr := range exprStrsInvalid {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func Test_SegmentScorers(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// helper to build a boost segment scorer function
	makeBoostRanker := func(filter string, weight string) *schemapb.FunctionSchema {
		params := []*commonpb.KeyValuePair{
			{Key: rerank.WeightKey, Value: weight},
			{Key: "reranker", Value: rerank.BoostName},
		}
		if filter != "" {
			params = append(params, &commonpb.KeyValuePair{Key: rerank.FilterKey, Value: filter})
		}
		return &schemapb.FunctionSchema{
			Params: params,
		}
	}

	t.Run("ok - single boost scorer", func(t *testing.T) {
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				makeBoostRanker("Int64Field > 0", "1.5"),
			},
		}
		plan, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{GroupByFieldId: -1}, nil, fs)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, 1, len(plan.Scorers))
		// filter should be parsed into Expr when provided
		assert.NotNil(t, plan.Scorers[0])
	})

	t.Run("ok - multiple boost scorers", func(t *testing.T) {
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				makeBoostRanker("Int64Field > 0", "1.0"),
				makeBoostRanker("", "2.0"),
			},
		}
		plan, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{GroupByFieldId: -1}, nil, fs)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.Equal(t, 2, len(plan.Scorers))
	})

	t.Run("error - not segment scorer flag", func(t *testing.T) {
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{{Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: rerank.WeightedName}}}},
		}
		plan, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{GroupByFieldId: -1}, nil, fs)
		assert.NoError(t, err)
		// not segment scorer means ignored
		assert.NotNil(t, plan)
		assert.Equal(t, 0, len(plan.Scorers))
	})

	t.Run("error - missing weight", func(t *testing.T) {
		// missing weight should cause CreateSearchScorer to fail
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				{Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: rerank.BoostName},
					// no weight
				}},
			},
		}
		_, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{}, nil, fs)
		assert.Error(t, err)
	})

	t.Run("error - invalid weight format", func(t *testing.T) {
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				makeBoostRanker("", "invalid_float"),
			},
		}
		_, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{}, nil, fs)
		assert.Error(t, err)
	})

	t.Run("error - scorer with group_by", func(t *testing.T) {
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				makeBoostRanker("", "1.0"),
			},
		}
		_, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{GroupByFieldId: 100}, nil, fs)
		assert.Error(t, err)
	})

	t.Run("error - scorer with search_iterator_v2", func(t *testing.T) {
		fs := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				makeBoostRanker("", "1.0"),
			},
		}
		_, err := CreateSearchPlan(schema, "", "FloatVectorField", &planpb.QueryInfo{SearchIteratorV2Info: &planpb.SearchIteratorV2Info{}}, nil, fs)
		assert.Error(t, err)
	})
}

func TestConcurrency(t *testing.T) {
	schemaHelper := newTestSchemaHelper(t)

	wg := sync.WaitGroup{}
	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				r := handleExpr(schemaHelper, fmt.Sprintf("array_length(ArrayField) == %d", j))
				err := getError(r)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkPlanCache(b *testing.B) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("cached", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := handleExpr(schemaHelper, "array_length(ArrayField) == 10")
			err := getError(r)
			assert.NoError(b, err)
		}
	})

	b.Run("uncached", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := handleExpr(schemaHelper, fmt.Sprintf("array_length(ArrayField) == %d", i))
			err := getError(r)
			assert.NoError(b, err)
		}
	})
}

func randomChineseString(length int) string {
	min := 0x4e00
	max := 0x9fa5

	result := make([]rune, length)
	for i := 0; i < length; i++ {
		result[i] = rune(rand.Intn(max-min+1) + min)
	}

	return string(result)
}

func BenchmarkWithString(b *testing.B) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(b, err)

	expr := ""
	for i := 0; i < 100; i++ {
		expr += fmt.Sprintf(`"%s",`, randomChineseString(rand.Intn(100)))
	}
	expr = "StringField in [" + expr + "]"

	for i := 0; i < b.N; i++ {
		plan, err := CreateSearchPlan(schemaHelper, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(b, err)
		assert.NotNil(b, plan)
	}
}

func Test_convertHanToASCII(t *testing.T) {
	type testcase struct {
		source string
		target string
	}
	testcases := []testcase{
		{`A in ["中国"]`, `A in ["\u4e2d\u56fd"]`},
		{`A in ["\中国"]`, `A in ["\中国"]`},
		{`A in ["\\中国"]`, `A in ["\\\u4e2d\u56fd"]`},
	}

	for _, c := range testcases {
		assert.Equal(t, c.target, convertHanToASCII(c.source))
	}
}

func BenchmarkTemplateWithString(b *testing.B) {
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(b, err)

	elements := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		elements[i] = generateTemplateValue(schemapb.DataType_String, fmt.Sprintf(`"%s",`, randomChineseString(rand.Intn(100))))
	}
	expr := "StringField in {list}"

	mv := map[string]*schemapb.TemplateValue{
		"list": generateTemplateValue(schemapb.DataType_Array, elements),
	}

	for i := 0; i < b.N; i++ {
		plan, err := CreateSearchPlan(schemaHelper, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, mv, nil)
		assert.NoError(b, err)
		assert.NotNil(b, plan)
	}
}

func TestNestedPathWithChinese(t *testing.T) {
	schema := newTestSchemaHelper(t)

	expr := `A["姓名"] == "小明"`
	plan, err := CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	paths := plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetColumnInfo().GetNestedPath()
	assert.NotNil(t, paths)
	assert.Equal(t, 2, len(paths))
	assert.Equal(t, "A", paths[0])
	assert.Equal(t, "姓名", paths[1])

	expr = `A["年份"]["月份"] == "九月"`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil, nil)
	assert.NoError(t, err, expr)
	paths = plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetColumnInfo().GetNestedPath()
	assert.NotNil(t, paths)
	assert.Equal(t, 3, len(paths))
	assert.Equal(t, "A", paths[0])
	assert.Equal(t, "年份", paths[1])
	assert.Equal(t, "月份", paths[2])
}

func Test_JSONPathNullExpr(t *testing.T) {
	schema := newTestSchemaHelper(t)

	exprPairs := [][]string{
		{`A["a"] is null`, `not exists A["a"]`},
		{`A["a"] is not null`, `exists A["a"]`},
		{`dyn_field is null`, `not exists dyn_field`},
		{`dyn_field is not null`, `exists dyn_field`},
	}

	for _, expr := range exprPairs {
		plan, err := CreateSearchPlan(schema, expr[0], "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, plan)

		plan2, err := CreateSearchPlan(schema, expr[1], "FloatVectorField", &planpb.QueryInfo{
			Topk:         0,
			MetricType:   "",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, plan2)

		planStr, err := proto.Marshal(plan)
		assert.NoError(t, err)
		plan2Str, err := proto.Marshal(plan2)
		assert.NoError(t, err)
		assert.Equal(t, planStr, plan2Str)
	}
}

// ============================================================================
// GIS Functions Tests
// ============================================================================

func TestExpr_GISFunctions(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test valid GIS function expressions
	validExprs := []string{
		// ST_EQUALS tests
		`st_equals(GeometryField, "POINT(0 0)")`,
		`ST_EQUALS(GeometryField, "POINT(1.5 2.3)")`,
		`st_equals(GeometryField, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")`,
		`st_equals(GeometryField, "LINESTRING(0 0, 1 1, 2 2)")`,
		`st_equals(GeometryField, "MULTIPOINT((0 0), (1 1))")`,

		// ST_INTERSECTS tests
		`st_intersects(GeometryField, "POINT(0 0)")`,
		`ST_INTERSECTS(GeometryField, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")`,
		`st_intersects(GeometryField, "LINESTRING(-1 -1, 1 1)")`,

		// ST_CONTAINS tests
		`st_contains(GeometryField, "POINT(0.5 0.5)")`,
		`ST_CONTAINS(GeometryField, "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))")`,

		// ST_WITHIN tests
		`st_within(GeometryField, "POLYGON((-2 -2, 2 -2, 2 2, -2 2, -2 -2))")`,
		`ST_WITHIN(GeometryField, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")`,

		// ST_TOUCHES tests
		`st_touches(GeometryField, "POINT(1 1)")`,
		`ST_TOUCHES(GeometryField, "LINESTRING(0 0, 1 0)")`,

		// ST_OVERLAPS tests
		`st_overlaps(GeometryField, "POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))")`,
		`ST_OVERLAPS(GeometryField, "POLYGON((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))")`,

		// ST_CROSSES tests
		`st_crosses(GeometryField, "LINESTRING(-1 0, 1 0)")`,
		`ST_CROSSES(GeometryField, "LINESTRING(0 -1, 0 1)")`,

		// ST_DWITHIN tests
		`st_dwithin(GeometryField, "POINT(0 0)", 1.0)`,
		`ST_DWITHIN(GeometryField, "POINT(1 1)", 5)`,
		`st_dwithin(GeometryField, "POINT(2.5 3.7)", 10.5)`,
		`ST_DWITHIN(GeometryField, "POINT(0.5 0.5)", 2.0)`,
		`st_dwithin(GeometryField, "POINT(1.0 1.0)", 1)`,

		// ST_ISVALID tests
		`st_isvalid(GeometryField)`,
		`ST_ISVALID(GeometryField)`,

		// Case insensitive tests
		`St_Equals(GeometryField, "POINT(0 0)")`,
		`sT_iNtErSeCts(GeometryField, "POINT(1 1)")`,
		`St_DWithin(GeometryField, "POINT(0 0)", 5.0)`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

func TestExpr_GISFunctionsInvalidExpressions(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test invalid GIS function expressions
	invalidExprs := []string{
		// Invalid field type
		`st_equals(Int64Field, "POINT(0 0)")`,
		`st_intersects(StringField, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")`,
		`st_dwithin(BoolField, "POINT(0 0)", 1.0)`,

		// Invalid WKT strings
		`st_equals(GeometryField, "INVALID WKT")`,
		`st_intersects(GeometryField, "POINT()")`,
		`st_contains(GeometryField, "POLYGON((0 0, 1 0))")`, // Unclosed polygon
		`st_within(GeometryField, "LINESTRING(0)")`,         // Incomplete linestring

		// Missing parameters
		`st_equals(GeometryField)`,
		`st_intersects()`,
		`st_dwithin(GeometryField, "POINT(0 0)")`,       // Missing distance parameter
		`st_contains(GeometryField, "POINT(0 0)", 1.0)`, // Extra parameter

		// Invalid distance parameter for ST_DWITHIN
		`st_dwithin(GeometryField, "POINT(0 0)", "abc")`, // String parameter
		`st_dwithin(GeometryField, "POINT(0 0)", "invalid")`,
		`st_dwithin(GeometryField, "POINT(0 0)", -1.0)`, // Negative distance
		`st_dwithin(GeometryField, "POINT(0 0)", true)`, // Boolean instead of number

		// Non-existent fields
		`st_equals(NonExistentField, "POINT(0 0)")`,
		`st_dwithin(UnknownGeometryField, "POINT(0 0)", 5.0)`,

		// ST_ISVALID invalid usage
		`st_isvalid(Int64Field)`,
		`st_isvalid()`,
		`st_isvalid(GeometryField, 1)`,
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

func TestExpr_GISFunctionsComplexExpressions(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test complex GIS expressions with logical operators
	complexExprs := []string{
		// AND combinations
		`st_equals(GeometryField, "POINT(0 0)") and st_intersects(GeometryField, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")`,
		`st_contains(GeometryField, "POINT(0.5 0.5)") AND st_within(GeometryField, "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))")`,
		`st_dwithin(GeometryField, "POINT(0 0)", 5.0) and Int64Field > 100`,
		`st_isvalid(GeometryField) and Int64Field > 0`,

		// OR combinations
		`st_equals(GeometryField, "POINT(0 0)") or st_equals(GeometryField, "POINT(1 1)")`,
		`st_intersects(GeometryField, "POINT(0 0)") OR st_touches(GeometryField, "POINT(1 1)")`,
		`st_dwithin(GeometryField, "POINT(0 0)", 1.0) or st_dwithin(GeometryField, "POINT(5 5)", 2.0)`,

		// NOT combinations
		`not st_equals(GeometryField, "POINT(0 0)")`,
		`!(st_intersects(GeometryField, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))`,
		`not (st_dwithin(GeometryField, "POINT(0 0)", 1.0))`,
		`not st_isvalid(GeometryField)`,
		// Mixed with other field types
		`st_contains(GeometryField, "POINT(0 0)") and StringField == "test"`,
		`st_dwithin(GeometryField, "POINT(0 0)", 5.0) or Int32Field in [1, 2, 3]`,
		`st_within(GeometryField, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))") and FloatField > 0.5`,

		// Nested expressions
		`(st_equals(GeometryField, "POINT(0 0)") and Int64Field > 0) or (st_intersects(GeometryField, "POINT(1 1)") and StringField != "")`,
		`st_dwithin(GeometryField, "POINT(0 0)", 5.0) and (Int32Field > 10 or BoolField == true)`,
	}

	for _, expr := range complexExprs {
		assertValidExpr(t, schema, expr)
	}
}

func TestExpr_GISFunctionsWithDifferentGeometryTypes(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test different WKT geometry types
	geometryTests := []struct {
		gisFunc     string
		geometryWKT string
		description string
	}{
		// Point geometries
		{"st_equals", "POINT(0 0)", "Simple point"},
		{"st_intersects", "POINT(1.5 2.3)", "Point with decimals"},
		{"st_dwithin", "POINT(-1 -1)", "Point with negative coordinates"},

		// LineString geometries
		{"st_intersects", "LINESTRING(0 0, 1 1)", "Simple linestring"},
		{"st_crosses", "LINESTRING(-1 0, 1 0)", "Horizontal linestring"},
		{"st_contains", "LINESTRING(0 0, 1 1, 2 2)", "Multi-segment linestring"},

		// Polygon geometries
		{"st_within", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", "Simple polygon"},
		{"st_overlaps", "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))", "Centered polygon"},
		{"st_contains", "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0), (0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))", "Polygon with hole"},

		// Multi geometries
		{"st_intersects", "MULTIPOINT((0 0), (1 1), (2 2))", "Multiple points"},
		{"st_crosses", "MULTILINESTRING((0 0, 1 0), (1 1, 2 1))", "Multiple linestrings"},
		{"st_overlaps", "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))", "Multiple polygons"},

		// Collection geometries
		{"st_intersects", "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 1, 2 2))", "Mixed geometry collection"},
	}

	for _, test := range geometryTests {
		exprStr := fmt.Sprintf(`%s(GeometryField, "%s")`, test.gisFunc, test.geometryWKT)
		if test.gisFunc == "st_dwithin" {
			exprStr = fmt.Sprintf(`%s(GeometryField, "%s", 5.0)`, test.gisFunc, test.geometryWKT)
		}

		assertValidExpr(t, schema, exprStr)
	}
}

func TestExpr_GISFunctionsWithVariousDistances(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test ST_DWITHIN with various distance values
	distanceTests := []struct {
		distance    interface{}
		shouldPass  bool
		description string
	}{
		// Valid distances (including zero)
		{0, true, "Zero distance (integer)"},
		{0.0, true, "Zero distance (float)"},
		{1, true, "Integer distance"},
		{1.0, true, "Float distance"},
		{0.5, true, "Small decimal distance"},
		{1000.0, true, "Large distance"},
		{99999999.999, true, "Very large distance"},
		{0.000001, true, "Very small distance"},

		// Valid distance expressions as strings that should be parsed
		{"0", true, "String zero integer"},
		{"0.0", true, "String zero float"},
		{"1", true, "String integer"},
		{"1.5", true, "String float"},
	}

	for _, test := range distanceTests {
		var exprStr string
		switch v := test.distance.(type) {
		case int:
			exprStr = fmt.Sprintf(`st_dwithin(GeometryField, "POINT(0 0)", %d)`, v)
		case float64:
			exprStr = fmt.Sprintf(`st_dwithin(GeometryField, "POINT(0 0)", %g)`, v)
		case string:
			exprStr = fmt.Sprintf(`st_dwithin(GeometryField, "POINT(0 0)", %s)`, v)
		}

		if test.shouldPass {
			assertValidExpr(t, schema, exprStr)
		} else {
			assertInvalidExpr(t, schema, exprStr)
		}
	}
}

func TestExpr_GISFunctionsPlanGeneration(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test that GIS expressions can be used in search plans
	gisExprs := []string{
		`st_equals(GeometryField, "POINT(0 0)")`,
		`st_intersects(GeometryField, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")`,
		`st_dwithin(GeometryField, "POINT(0 0)", 5.0)`,
		`st_contains(GeometryField, "POINT(0.5 0.5)") and Int64Field > 100`,
		`st_within(GeometryField, "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))") or StringField == "test"`,
	}

	for _, expr := range gisExprs {
		plan, err := CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         10,
			MetricType:   "L2",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err, "Failed to create plan for expression: %s", expr)
		assert.NotNil(t, plan, "Plan should not be nil for expression: %s", expr)
		assert.NotNil(t, plan.GetVectorAnns(), "Vector annotations should not be nil for expression: %s", expr)

		if plan.GetVectorAnns().GetPredicates() != nil {
			// Verify that the plan contains GIS function filter expressions
			// This ensures that the GIS expressions are properly parsed and converted to plan nodes
			predicates := plan.GetVectorAnns().GetPredicates()
			assert.NotNil(t, predicates, "Predicates should not be nil for GIS expression: %s", expr)
		}
	}
}

func TestExpr_GISFunctionsWithJSONFields(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test invalid usage with JSON fields - GIS functions should only work with geometry fields
	invalidJSONGISExprs := []string{
		`st_equals(JSONField, "POINT(0 0)")`,
		`st_intersects($meta["geometry"], "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")`,
		`st_dwithin(A, "POINT(0 0)", 5.0)`, // Dynamic field
		`st_contains(JSONField["geom"], "POINT(0.5 0.5)")`,
	}

	for _, expr := range invalidJSONGISExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

func TestExpr_GISFunctionsZeroDistance(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test zero distance specifically for ST_DWITHIN
	zeroDistanceExprs := []string{
		// Integer zero
		`st_dwithin(GeometryField, "POINT(0 0)", 0)`,
		// Float zero
		`st_dwithin(GeometryField, "POINT(1 1)", 0.0)`,
		// Zero in complex expressions
		`st_dwithin(GeometryField, "POINT(0 0)", 0) and Int64Field > 10`,
	}

	for _, expr := range zeroDistanceExprs {
		assertValidExpr(t, schema, expr)
	}

	// Test that zero distance expressions can generate valid search plans
	for _, expr := range zeroDistanceExprs {
		plan, err := CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
			Topk:         10,
			MetricType:   "L2",
			SearchParams: "",
			RoundDecimal: 0,
		}, nil, nil)
		assert.NoError(t, err, "Failed to create plan for zero distance expression: %s", expr)
		assert.NotNil(t, plan, "Plan should not be nil for zero distance expression: %s", expr)
	}

	// Test that negative distances are still invalid
	invalidNegativeExprs := []string{
		`st_dwithin(GeometryField, "POINT(0 0)", -1)`,
		`st_dwithin(GeometryField, "POINT(0 0)", -0.1)`,
		`st_dwithin(GeometryField, "POINT(0 0)", -100.5)`,
	}

	for _, expr := range invalidNegativeExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

func TestExpr_GISFunctionsInvalidParameterTypes(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test various invalid parameter types for ST_DWITHIN distance parameter
	invalidTypeExprs := []string{
		// String parameters (should be rejected)
		`st_dwithin(GeometryField, "POINT(0 0)", "abc")`,
		`st_dwithin(GeometryField, "POINT(0 0)", "123")`,    // Numeric string
		`st_dwithin(GeometryField, "POINT(0 0)", "123.45")`, // Float string
		`st_dwithin(GeometryField, "POINT(0 0)", "0")`,      // Zero string
		`st_dwithin(GeometryField, "POINT(0 0)", "-5")`,     // Negative string

		// Boolean parameters (should be rejected)
		`st_dwithin(GeometryField, "POINT(0 0)", true)`,
		`st_dwithin(GeometryField, "POINT(0 0)", false)`,

		// Array/complex parameters (should be rejected)
		`st_dwithin(GeometryField, "POINT(0 0)", [1, 2, 3])`,
		`st_dwithin(GeometryField, "POINT(0 0)", GeometryField)`, // Field reference instead of literal
	}

	for _, expr := range invalidTypeExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

func TestExpr_ElementFilter(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	// Valid expressions
	validExprs := []string{
		`element_filter(struct_array, 2 > $[sub_int] > 1)`,
		`element_filter(struct_array, $[sub_int] > 1)`,
		`element_filter(struct_array, $[sub_int] == 100)`,
		`element_filter(struct_array, $[sub_int] >= 0)`,
		`element_filter(struct_array, $[sub_int] <= 1000)`,
		`element_filter(struct_array, $[sub_int] != 0)`,

		`element_filter(struct_array, $[sub_str] == "1")`,
		`element_filter(struct_array, $[sub_str] != "")`,

		`element_filter(struct_array, $[sub_str] == "1" || $[sub_int] > 1)`,
		`element_filter(struct_array, $[sub_str] == "1" && $[sub_int] > 1)`,
		`element_filter(struct_array, $[sub_int] > 0 && $[sub_int] < 100)`,

		`element_filter(struct_array, ($[sub_int] > 0 && $[sub_int] < 100) || $[sub_str] == "default")`,
		`element_filter(struct_array, !($[sub_int] < 0))`,

		`Int64Field > 0 && element_filter(struct_array, $[sub_int] > 1)`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, helper, expr)
	}

	// Invalid expressions
	invalidExprs := []string{
		`element_filter(struct_array, element_filter(struct_array, $[sub_int] > 1))`,
		`element_filter(struct_array, $[sub_int] > 1 && element_filter(struct_array, $[sub_str] == "1"))`,

		`$[sub_int] > 1`,
		`Int64Field > 0 && $[sub_int] > 1`,

		`element_filter(struct_array, $[non_existent_field] > 1)`,
		`element_filter(non_existent_array, $[sub_int] > 1)`,

		`element_filter(struct_array)`, // missing element expression
		`element_filter()`,             // missing all parameters

		`element_filter(struct_array, $[sub_int] > 1) || element_filter(struct_array, $[sub_str] == "test")`,
		`element_filter(struct_array, $[sub_int] > 1) && Int64Field > 0`,
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, helper, expr)
	}
}

func TestExpr_Match(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	// Valid MATCH_ALL expressions
	validExprs := []string{
		// MATCH_ALL: all elements must match
		`MATCH_ALL(struct_array, $[sub_int] > 1)`,
		`MATCH_ALL(struct_array, $[sub_int] == 100)`,
		`MATCH_ALL(struct_array, $[sub_str] == "aaa")`,
		`MATCH_ALL(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100)`,
		`MATCH_ALL(struct_array, $[sub_str] != "" || $[sub_int] >= 0)`,

		// MATCH_ANY: at least one element must match
		`MATCH_ANY(struct_array, $[sub_int] > 1)`,
		`MATCH_ANY(struct_array, $[sub_int] == 100)`,
		`MATCH_ANY(struct_array, $[sub_str] == "aaa")`,
		`MATCH_ANY(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100)`,

		// MATCH_LEAST: at least N elements must match
		`MATCH_LEAST(struct_array, $[sub_int] > 1, threshold=3)`,
		`MATCH_LEAST(struct_array, $[sub_str] == "aaa", threshold=1)`,
		`MATCH_LEAST(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100, threshold=2)`,

		// MATCH_MOST: at most N elements must match
		`MATCH_MOST(struct_array, $[sub_int] > 1, threshold=3)`,
		`MATCH_MOST(struct_array, $[sub_str] == "aaa", threshold=0)`,
		`MATCH_MOST(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100, threshold=5)`,

		// MATCH_EXACT: exactly N elements must match
		`MATCH_EXACT(struct_array, $[sub_int] > 1, threshold=2)`,
		`MATCH_EXACT(struct_array, $[sub_str] == "aaa", threshold=0)`,
		`MATCH_EXACT(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100, threshold=3)`,

		// Combined with other expressions (match must be last)
		`Int64Field > 0 && MATCH_ALL(struct_array, $[sub_int] > 1)`,
		`Int64Field > 0 && MATCH_ANY(struct_array, $[sub_str] == "test")`,
		`Int64Field > 0 && MATCH_LEAST(struct_array, $[sub_int] > 1, threshold=2)`,

		// Complex predicates
		`MATCH_ALL(struct_array, ($[sub_int] > 0 && $[sub_int] < 100) || $[sub_str] == "default")`,
		`MATCH_ANY(struct_array, !($[sub_int] < 0))`,

		// Case insensitivity
		`match_all(struct_array, $[sub_int] > 1)`,
		`match_any(struct_array, $[sub_int] > 1)`,
		`match_least(struct_array, $[sub_int] > 1, threshold=2)`,
		`match_most(struct_array, $[sub_int] > 1, threshold=2)`,
		`match_exact(struct_array, $[sub_int] > 1, threshold=2)`,

		// Multiple match expressions with logical operators
		`MATCH_ALL(struct_array, $[sub_int] > 1) || MATCH_ANY(struct_array, $[sub_str] == "test")`,
		`MATCH_ALL(struct_array, $[sub_int] > 1) && MATCH_ANY(struct_array, $[sub_str] == "test")`,
		`MATCH_ANY(struct_array, $[sub_int] > 1) || Int64Field > 0`,
		`MATCH_ALL(struct_array, $[sub_int] > 1) && Int64Field > 0`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, helper, expr)
	}

	// Test proto structure assertions
	t.Run("MatchAll_Proto", func(t *testing.T) {
		expr, err := ParseExpr(helper, `MATCH_ALL(struct_array, $[sub_int] > 1)`, nil)
		assert.NoError(t, err)
		assert.NotNil(t, expr.GetMatchExpr())
		assert.Equal(t, "struct_array", expr.GetMatchExpr().GetStructName())
		assert.Equal(t, planpb.MatchType_MatchAll, expr.GetMatchExpr().GetMatchType())
		assert.Equal(t, int64(0), expr.GetMatchExpr().GetCount())
	})

	t.Run("MatchAny_Proto", func(t *testing.T) {
		expr, err := ParseExpr(helper, `MATCH_ANY(struct_array, $[sub_str] == "aaa")`, nil)
		assert.NoError(t, err)
		assert.NotNil(t, expr.GetMatchExpr())
		assert.Equal(t, "struct_array", expr.GetMatchExpr().GetStructName())
		assert.Equal(t, planpb.MatchType_MatchAny, expr.GetMatchExpr().GetMatchType())
		assert.Equal(t, int64(0), expr.GetMatchExpr().GetCount())
	})

	t.Run("MatchLeast_Proto", func(t *testing.T) {
		expr, err := ParseExpr(helper, `MATCH_LEAST(struct_array, $[sub_int] > 1, threshold=3)`, nil)
		assert.NoError(t, err)
		assert.NotNil(t, expr.GetMatchExpr())
		assert.Equal(t, "struct_array", expr.GetMatchExpr().GetStructName())
		assert.Equal(t, planpb.MatchType_MatchLeast, expr.GetMatchExpr().GetMatchType())
		assert.Equal(t, int64(3), expr.GetMatchExpr().GetCount())
	})

	t.Run("MatchMost_Proto", func(t *testing.T) {
		expr, err := ParseExpr(helper, `MATCH_MOST(struct_array, $[sub_str] == "aaa", threshold=5)`, nil)
		assert.NoError(t, err)
		assert.NotNil(t, expr.GetMatchExpr())
		assert.Equal(t, "struct_array", expr.GetMatchExpr().GetStructName())
		assert.Equal(t, planpb.MatchType_MatchMost, expr.GetMatchExpr().GetMatchType())
		assert.Equal(t, int64(5), expr.GetMatchExpr().GetCount())
	})

	t.Run("MatchExact_Proto", func(t *testing.T) {
		expr, err := ParseExpr(helper, `MATCH_EXACT(struct_array, $[sub_int] == 100, threshold=2)`, nil)
		assert.NoError(t, err)
		assert.NotNil(t, expr.GetMatchExpr())
		assert.Equal(t, "struct_array", expr.GetMatchExpr().GetStructName())
		assert.Equal(t, planpb.MatchType_MatchExact, expr.GetMatchExpr().GetMatchType())
		assert.Equal(t, int64(2), expr.GetMatchExpr().GetCount())
	})

	// Invalid expressions
	invalidExprs := []string{
		// Nested match expressions not allowed
		`MATCH_ALL(struct_array, MATCH_ANY(struct_array, $[sub_int] > 1))`,
		`MATCH_ANY(struct_array, $[sub_int] > 1 && MATCH_ALL(struct_array, $[sub_str] == "1"))`,

		// $[field] syntax outside match context
		`$[sub_int] > 1`,
		`Int64Field > 0 && $[sub_int] > 1`,

		// Non-existent fields
		`MATCH_ALL(struct_array, $[non_existent_field] > 1)`,
		`MATCH_ALL(non_existent_array, $[sub_int] > 1)`,

		// Missing parameters
		`MATCH_ALL(struct_array)`,
		`MATCH_ALL()`,
		`MATCH_ANY(struct_array)`,
		`MATCH_ANY()`,
		`MATCH_LEAST(struct_array, $[sub_int] > 1)`, // missing count
		`MATCH_MOST(struct_array, $[sub_int] > 1)`,  // missing count
		`MATCH_EXACT(struct_array, $[sub_int] > 1)`, // missing count

		// MATCH_ALL/MATCH_ANY should not have count parameter
		`MATCH_ALL(struct_array, $[sub_int] > 1, 3)`,
		`MATCH_ANY(struct_array, $[sub_int] > 1, 2)`,

		// Invalid count values
		`MATCH_LEAST(struct_array, $[sub_int] > 1, threshold=0)`,  // count must be positive for MATCH_LEAST
		`MATCH_LEAST(struct_array, $[sub_int] > 1, threshold=-1)`, // negative count
		`MATCH_MOST(struct_array, $[sub_int] > 1, threshold=-1)`,  // negative count
		`MATCH_EXACT(struct_array, $[sub_int] > 1, threshold=-1)`, // negative count
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, helper, expr)
	}
}

// ============================================================================
// Timestamptz Expression Tests
// These tests cover VisitTimestamptzCompareForward and VisitTimestamptzCompareReverse
// which are used for optimized timestamptz comparisons with optional INTERVAL arithmetic
// ============================================================================

func newTestSchemaWithTimestamptz(t *testing.T) *typeutil.SchemaHelper {
	// Create schema with Timestamptz field for testing
	// The newTestSchema already includes all DataType values including Timestamptz
	schema := newTestSchema(true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return schemaHelper
}

func TestExpr_TimestamptzCompareForward(t *testing.T) {
	schema := newTestSchemaWithTimestamptz(t)

	// Test valid timestamptz forward comparisons (column op ISO value)
	// Format: TimestamptzField [+|- INTERVAL 'duration'] <op> ISO 'timestamp'
	// Note: ISO keyword is required before the timestamp string literal
	validExprs := []string{
		// Simple comparisons without INTERVAL (quick path)
		`TimestamptzField > ISO '2025-01-01T00:00:00Z'`,
		`TimestamptzField >= ISO '2025-01-01T00:00:00Z'`,
		`TimestamptzField < ISO '2025-12-31T23:59:59Z'`,
		`TimestamptzField <= ISO '2025-06-15T12:00:00Z'`,
		`TimestamptzField == ISO '2025-03-20T10:30:00Z'`,
		`TimestamptzField != ISO '2025-08-10T08:00:00Z'`,

		// Comparisons with INTERVAL (slow path with arithmetic)
		`TimestamptzField + INTERVAL 'P1D' > ISO '2025-01-01T00:00:00Z'`,
		`TimestamptzField - INTERVAL 'P1D' < ISO '2025-12-31T23:59:59Z'`,
		`TimestamptzField + INTERVAL 'PT1H' >= ISO '2025-06-15T12:00:00Z'`,
		`TimestamptzField - INTERVAL 'PT30M' <= ISO '2025-03-20T10:30:00Z'`,
		`TimestamptzField + INTERVAL 'P1Y' == ISO '2026-01-01T00:00:00Z'`,
		`TimestamptzField - INTERVAL 'P6M' != ISO '2024-06-01T00:00:00Z'`,

		// Complex INTERVAL durations
		`TimestamptzField + INTERVAL 'P1Y2M3D' > ISO '2025-01-01T00:00:00Z'`,
		`TimestamptzField + INTERVAL 'PT10H30M15S' < ISO '2025-12-31T23:59:59Z'`,
		`TimestamptzField - INTERVAL 'P1Y2M3DT4H5M6S' >= ISO '2024-01-01T00:00:00Z'`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

func TestExpr_TimestamptzCompareReverse(t *testing.T) {
	schema := newTestSchemaWithTimestamptz(t)

	// Test valid timestamptz reverse comparisons (ISO value op column)
	// Format: ISO 'timestamp' <op> TimestamptzField [+|- INTERVAL 'duration']
	// Note: ISO keyword is required before the timestamp string
	// Note: Operator gets reversed internally (e.g., '>' becomes '<')
	validExprs := []string{
		// Simple reverse comparisons without INTERVAL (quick path)
		`ISO '2025-01-01T00:00:00Z' < TimestamptzField`,
		`ISO '2025-01-01T00:00:00Z' <= TimestamptzField`,
		`ISO '2025-12-31T23:59:59Z' > TimestamptzField`,
		`ISO '2025-06-15T12:00:00Z' >= TimestamptzField`,
		`ISO '2025-03-20T10:30:00Z' == TimestamptzField`,
		`ISO '2025-08-10T08:00:00Z' != TimestamptzField`,

		// Reverse comparisons with INTERVAL after field (slow path with arithmetic)
		`ISO '2025-01-01T00:00:00Z' < TimestamptzField + INTERVAL 'P1D'`,
		`ISO '2025-12-31T23:59:59Z' > TimestamptzField - INTERVAL 'P1D'`,
		`ISO '2025-06-15T12:00:00Z' <= TimestamptzField + INTERVAL 'PT1H'`,
		`ISO '2025-03-20T10:30:00Z' >= TimestamptzField - INTERVAL 'PT30M'`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

func TestExpr_TimestamptzCompareInvalid(t *testing.T) {
	schema := newTestSchemaWithTimestamptz(t)

	// Test invalid timestamptz expressions
	// Note: ISO keyword is required for timestamptz comparisons
	invalidExprs := []string{
		// Invalid field type for timestamptz operations (non-timestamptz field with INTERVAL)
		`Int64Field + INTERVAL 'P1D' > ISO '2025-01-01T00:00:00Z'`,
		`VarCharField + INTERVAL 'P1D' < ISO '2025-01-01T00:00:00Z'`,

		// Invalid timestamp format with ISO
		`TimestamptzField > ISO 'invalid-timestamp'`,
		`TimestamptzField < ISO '2025-13-01T00:00:00Z'`, // Invalid month
		`TimestamptzField > ISO '2025-01-32T00:00:00Z'`, // Invalid day

		// Invalid interval format
		`TimestamptzField + INTERVAL 'invalid' > ISO '2025-01-01T00:00:00Z'`,
		`TimestamptzField + INTERVAL '1D' > ISO '2025-01-01T00:00:00Z'`, // Missing P prefix
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

// ============================================================================
// Power Expression Tests
// These tests cover VisitPower for constant power operations
// ============================================================================

func TestExpr_Power(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test valid power expressions with constants
	validExprs := []string{
		// Integer powers
		`2 ** 3 == 8`,
		`3 ** 2 == 9`,
		`10 ** 0 == 1`,

		// Float powers
		`2.0 ** 3.0 == 8.0`,
		`4.0 ** 0.5 > 1.0`,

		// Negative exponents
		`2 ** -1 == 0.5`,

		// Used in arithmetic expressions
		`Int64Field + (2 ** 3) > 0`,
		`Int64Field * (10 ** 2) < 1000`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}

	// Test invalid power expressions - power requires constant operands
	invalidExprs := []string{
		// Power with field operands (not allowed)
		`Int64Field ** 2 == 100`,
		`2 ** Int64Field == 8`,
		`Int64Field ** Int64Field == 1`,
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

// ============================================================================
// Error Handling Tests
// These tests cover the int64OverflowError type and error handling paths
// ============================================================================

func TestInt64OverflowError(t *testing.T) {
	// Test int64OverflowError.Error() method - covers the Error() method at 0% coverage
	err := &int64OverflowError{literal: "9223372036854775808"}
	assert.Contains(t, err.Error(), "int64 overflow")
	assert.Contains(t, err.Error(), "9223372036854775808")

	// Test isInt64OverflowError helper function
	assert.True(t, isInt64OverflowError(err))
	assert.False(t, isInt64OverflowError(fmt.Errorf("some other error")))
	assert.False(t, isInt64OverflowError(nil))
}

// ============================================================================
// reverseCompareOp Tests
// This function is used internally to reverse comparison operators
// ============================================================================

func Test_reverseCompareOp(t *testing.T) {
	// Test all comparison operator reversals
	// This covers the reverseCompareOp function at 0% coverage
	tests := []struct {
		input    planpb.OpType
		expected planpb.OpType
	}{
		{planpb.OpType_LessThan, planpb.OpType_GreaterThan},
		{planpb.OpType_LessEqual, planpb.OpType_GreaterEqual},
		{planpb.OpType_GreaterThan, planpb.OpType_LessThan},
		{planpb.OpType_GreaterEqual, planpb.OpType_LessEqual},
		{planpb.OpType_Equal, planpb.OpType_Equal},
		{planpb.OpType_NotEqual, planpb.OpType_NotEqual},
		{planpb.OpType_Invalid, planpb.OpType_Invalid},
		{planpb.OpType_PrefixMatch, planpb.OpType_Invalid}, // Unknown ops return Invalid
	}

	for _, tt := range tests {
		result := reverseCompareOp(tt.input)
		assert.Equal(t, tt.expected, result, "reverseCompareOp(%v)", tt.input)
	}
}

// ============================================================================
// Additional Coverage Tests for Edge Cases
// ============================================================================

func TestExpr_AdditionalEdgeCases(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test valid edge case expressions
	validExprs := []string{
		// Floating point edge cases
		`FloatField > 1e10`,
		`DoubleField < 1e-10`,
		`FloatField == 3.14159265358979`,

		// Boolean expressions
		`true == true`,
		`false != true`,

		// Empty string comparison
		`StringField == ""`,
		`VarCharField != ""`,

		// JSON with complex nested paths
		`JSONField["level1"]["level2"]["level3"] > 0`,

		// Array length operations
		`array_length(ArrayField) > 0`,
		`array_length(ArrayField) == 10`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

func TestExpr_InvalidOperatorCombinations(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Test invalid operator combinations that should fail
	// These test the error paths in various Visit methods
	invalidExprs := []string{
		// Shift operations not supported
		`Int64Field << 2`,
		`Int64Field >> 2`,

		// Bitwise operations not supported
		`Int64Field & 0xFF`,
		`Int64Field | 0xFF`,
		`Int64Field ^ 0xFF`,

		// Type mismatches
		`"string" + 1`,
		`BoolField + 1`,
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

// TestExpr_VisitBooleanEdgeCases tests edge cases in VisitBoolean
// Boolean literals must be used in comparison expressions, not as standalone values
func TestExpr_VisitBooleanEdgeCases(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Valid boolean comparison expressions
	// Note: Standalone boolean values or fields are not valid filter expressions
	// They must be used in comparisons
	validExprs := []string{
		`true == true`,
		`false == false`,
		`true != false`,
		`BoolField == true`,
		`BoolField != false`,
		`BoolField == BoolField`,
		`not (BoolField == true)`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

// TestExpr_VisitFloatingEdgeCases tests edge cases in VisitFloating
func TestExpr_VisitFloatingEdgeCases(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Valid floating point literal expressions
	validExprs := []string{
		`FloatField > 0.0`,
		`FloatField < 1.0e10`,
		`FloatField >= -1.0e-10`,
		`FloatField <= 3.14159265`,
		`DoubleField == 2.718281828`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

// TestExpr_VisitRangeEdgeCases tests edge cases in VisitRange and VisitReverseRange
func TestExpr_VisitRangeEdgeCases(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Valid range expressions
	validExprs := []string{
		// Forward range: lower < field < upper
		`1 < Int64Field < 10`,
		`0.0 < FloatField < 1.0`,
		`"a" < StringField < "z"`,

		// Forward range with equal
		`1 <= Int64Field < 10`,
		`1 < Int64Field <= 10`,
		`1 <= Int64Field <= 10`,

		// Reverse range: upper > field > lower
		`10 > Int64Field > 1`,
		`1.0 > FloatField > 0.0`,
		`"z" > StringField > "a"`,

		// Reverse range with equal
		`10 >= Int64Field > 1`,
		`10 > Int64Field >= 1`,
		`10 >= Int64Field >= 1`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}

	// Invalid range expressions
	invalidExprs := []string{
		// Range on bool type is invalid
		`true < BoolField < false`,

		// Non-const bounds
		`Int64Field < Int32Field < Int64Field`,
	}

	for _, expr := range invalidExprs {
		assertInvalidExpr(t, schema, expr)
	}
}

// TestExpr_VisitUnaryEdgeCases tests edge cases in VisitUnary
// Unary operators (not/!) must produce boolean expressions for filter predicates
func TestExpr_VisitUnaryEdgeCases(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Valid unary expressions - must produce boolean filter predicates
	validExprs := []string{
		`not (Int64Field > 0)`,
		`!(Int64Field < 10)`,
		`not (BoolField == true)`,
		`not (true == false)`,
		`!(FloatField >= 1.0)`,
		// Unary negation used in comparison context
		`Int64Field > -1`,
		`Int64Field < -(-5)`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

// TestExpr_ConstantFolding tests constant folding in arithmetic expressions
func TestExpr_ConstantFolding(t *testing.T) {
	schema := newTestSchemaHelper(t)

	// Expressions where constants can be folded
	validExprs := []string{
		// Add/Sub constant folding
		`Int64Field > (1 + 2)`,
		`Int64Field < (10 - 5)`,
		`Int64Field == (1 + 2 + 3)`,

		// Mul/Div/Mod constant folding
		`Int64Field > (2 * 3)`,
		`Int64Field < (10 / 2)`,
		`Int64Field == (10 % 3)`,

		// Mixed operations
		`Int64Field > (2 * 3 + 4)`,
		`Int64Field < (10 - 2 * 3)`,

		// Float constant folding
		`FloatField > (1.0 + 2.0)`,
		`FloatField < (10.0 / 2.0)`,
	}

	for _, expr := range validExprs {
		assertValidExpr(t, schema, expr)
	}
}

// TestExpr_BooleanLiteral verifies how standalone "true"/"false" literals
// are parsed by the proxy expression parser.
//
// Key behavior:
//   - Standalone "true"/"false" are parsed into ValueExpr(BoolVal) with nodeDependent=true
//   - Because nodeDependent=true, canBeExecuted() returns false
//   - Therefore ParseExpr rejects them with "predicate is not a boolean expression"
//   - But combined expressions like "BoolField == true" or "1==1" work fine
//   - After rewriting, "1==1" becomes AlwaysTrueExpr, "1==2" becomes AlwaysFalseExpr
func TestExpr_BooleanLiteral(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	// Case 1: standalone "true" / "false" should fail ParseExpr
	// because VisitBoolean sets nodeDependent=true, and canBeExecuted requires nodeDependent=false
	standaloneBoolExprs := []string{
		"true",
		"false",
		"True",
		"False",
		"TRUE",
		"FALSE",
	}
	for _, exprStr := range standaloneBoolExprs {
		expr, err := ParseExpr(helper, exprStr, nil)
		assert.Error(t, err, "standalone %q should fail", exprStr)
		assert.Nil(t, expr, "standalone %q should return nil expr", exprStr)
		assert.Contains(t, err.Error(), "predicate is not a boolean expression",
			"standalone %q error message mismatch", exprStr)
	}

	// Case 2: verify that handleExpr (internal) does parse them into ValueExpr with Bool
	// This shows the ANTLR + visitor layer works, but the outer canBeExecuted gate blocks it
	for _, exprStr := range []string{"true", "false"} {
		ret := handleExpr(helper, exprStr)
		ewt, ok := ret.(*ExprWithType)
		require.True(t, ok, "handleExpr(%q) should return *ExprWithType", exprStr)
		assert.Equal(t, schemapb.DataType_Bool, ewt.dataType)
		assert.True(t, ewt.nodeDependent, "boolean literal should be nodeDependent")

		ve := ewt.expr.GetValueExpr()
		require.NotNil(t, ve, "should be ValueExpr for %q", exprStr)
		if exprStr == "true" {
			assert.True(t, ve.GetValue().GetBoolVal())
		} else {
			assert.False(t, ve.GetValue().GetBoolVal())
		}
	}

	// Case 3: boolean literals in valid combined expressions
	// These all produce executable boolean predicates
	validBoolExprs := []string{
		"BoolField == true",
		"BoolField == false",
		"BoolField != true",
		"BoolField != false",
		"BoolField in [true, false]",
	}
	for _, exprStr := range validBoolExprs {
		assertValidExpr(t, helper, exprStr)
	}

	// Case 4: constant-folded expressions become AlwaysTrueExpr / AlwaysFalseExpr
	// "1==1" constant-folds to ValueExpr(true), then rewriter converts to AlwaysTrueExpr
	exprTrue, err := ParseExpr(helper, "1==1", nil)
	require.NoError(t, err)
	assert.NotNil(t, exprTrue.GetAlwaysTrueExpr(),
		"1==1 should be rewritten to AlwaysTrueExpr")

	// "1==2" constant-folds to ValueExpr(false), then rewriter converts to AlwaysFalseExpr
	// AlwaysFalseExpr is represented as UnaryExpr(Not, AlwaysTrueExpr)
	exprFalse, err := ParseExpr(helper, "1==2", nil)
	require.NoError(t, err)
	ue := exprFalse.GetUnaryExpr()
	require.NotNil(t, ue, "1==2 should be rewritten to UnaryExpr(Not, AlwaysTrueExpr)")
	assert.Equal(t, planpb.UnaryExpr_Not, ue.GetOp())
	assert.NotNil(t, ue.GetChild().GetAlwaysTrueExpr())

	// Case 5: empty expression becomes AlwaysTrueExpr (special case in handleExprInternal)
	exprEmpty, err := ParseExpr(helper, "", nil)
	require.NoError(t, err)
	assert.NotNil(t, exprEmpty.GetAlwaysTrueExpr(),
		"empty expression should be AlwaysTrueExpr")

	// Case 6: "true and false" / "true or false" — two boolean literals connected by logical operators
	// Both sides are GenericValue (from VisitBoolean), so VisitLogicalAnd calls And() which
	// constant-folds to ValueExpr(BoolVal = true && false = false) with nodeDependent=false (default).
	// Since nodeDependent=false, canBeExecuted() passes, then rewriter converts to AlwaysFalseExpr.
	t.Run("true_and_false", func(t *testing.T) {
		expr, err := ParseExpr(helper, "true and false", nil)
		require.NoError(t, err, "\"true and false\" should be valid")
		// And(true, false) = false → rewriter → AlwaysFalseExpr = UnaryExpr(Not, AlwaysTrueExpr)
		ue := expr.GetUnaryExpr()
		require.NotNil(t, ue, "should be AlwaysFalseExpr (UnaryExpr Not)")
		assert.Equal(t, planpb.UnaryExpr_Not, ue.GetOp())
		assert.NotNil(t, ue.GetChild().GetAlwaysTrueExpr())
	})

	t.Run("true_and_true", func(t *testing.T) {
		expr, err := ParseExpr(helper, "true and true", nil)
		require.NoError(t, err, "\"true and true\" should be valid")
		// And(true, true) = true → rewriter → AlwaysTrueExpr
		assert.NotNil(t, expr.GetAlwaysTrueExpr(),
			"\"true and true\" should become AlwaysTrueExpr")
	})

	t.Run("true_or_false", func(t *testing.T) {
		expr, err := ParseExpr(helper, "true or false", nil)
		require.NoError(t, err, "\"true or false\" should be valid")
		// Or(true, false) = true → rewriter → AlwaysTrueExpr
		assert.NotNil(t, expr.GetAlwaysTrueExpr(),
			"\"true or false\" should become AlwaysTrueExpr")
	})

	t.Run("false_or_false", func(t *testing.T) {
		expr, err := ParseExpr(helper, "false or false", nil)
		require.NoError(t, err, "\"false or false\" should be valid")
		// Or(false, false) = false → rewriter → AlwaysFalseExpr
		ue := expr.GetUnaryExpr()
		require.NotNil(t, ue, "should be AlwaysFalseExpr")
		assert.Equal(t, planpb.UnaryExpr_Not, ue.GetOp())
		assert.NotNil(t, ue.GetChild().GetAlwaysTrueExpr())
	})

	t.Run("false_and_false", func(t *testing.T) {
		expr, err := ParseExpr(helper, "false and false", nil)
		require.NoError(t, err, "\"false and false\" should be valid")
		// And(false, false) = false → rewriter → AlwaysFalseExpr
		ue := expr.GetUnaryExpr()
		require.NotNil(t, ue, "should be AlwaysFalseExpr")
		assert.Equal(t, planpb.UnaryExpr_Not, ue.GetOp())
		assert.NotNil(t, ue.GetChild().GetAlwaysTrueExpr())
	})
}
