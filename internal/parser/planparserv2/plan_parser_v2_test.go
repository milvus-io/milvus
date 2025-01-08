package planparserv2

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

	return &schemapb.CollectionSchema{
		Name:               "test",
		Description:        "schema for test used",
		AutoID:             true,
		Fields:             fields,
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
		`JSONField["A"] > Int16Field`,
		`$meta["A"] > Int16Field`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
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

	exprStrs := []string{
		`VarCharField like "prefix%"`,
		`VarCharField like "equal"`,
		`JSONField["A"] like "name*"`,
		`$meta["A"] like "name*"`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	// TODO: enable these after regex-match is supported.
	//unsupported := []string{
	//	`VarCharField like "not_%_supported"`,
	//	`JSONField["A"] like "not_%_supported"`,
	//	`$meta["A"] like "not_%_supported"`,
	//}
	//for _, exprStr := range unsupported {
	//	assertInvalidExpr(t, helper, exprStr)
	//}
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

func TestExpr_PhraseMatch(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`phrase_match(VarCharField, "phrase")`,
		`phrase_match(StringField, "phrase")`,
		`phrase_match(StringField, "phrase", 1)`,
		`phrase_match(VarCharField, "phrase", 11223)`,
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
	assert.NoError(t, err, exprStr)
	assert.NotNil(t, expr, exprStr)
	assert.NotNil(t, expr.GetBinaryArithOpEvalRangeExpr())
	assert.NotNil(t, expr.GetBinaryArithOpEvalRangeExpr().GetRightOperand().GetFloatVal())
	assert.NotNil(t, expr.GetBinaryArithOpEvalRangeExpr().GetValue().GetFloatVal())

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
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	// TODO: enable these after execution backend is ready.
	unsupported := []string{
		`ArrayField + 15 == 16`,
		`15 + ArrayField == 16`,
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
}

func TestCreateSearchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil)
	assert.NoError(t, err)
}

func TestCreateFloat16SearchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "Float16VectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil)
	assert.NoError(t, err)
}

func TestCreateBFloat16earchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "BFloat16VectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil)
	assert.NoError(t, err)
}

func TestCreateSparseFloatVectorSearchPlan(t *testing.T) {
	schema := newTestSchemaHelper(t)
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "SparseFloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	}, nil)
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
		_, err := CreateSearchPlan(schema, "invalid expression", "", nil, nil)
		assert.Error(t, err)
	})

	t.Run("invalid vector field", func(t *testing.T) {
		schema := newTestSchemaHelper(t)
		_, err := CreateSearchPlan(schema, "Int64Field > 0", "not_exist", nil, nil)
		assert.Error(t, err)
	})

	t.Run("not vector type", func(t *testing.T) {
		schema := newTestSchemaHelper(t)
		_, err := CreateSearchPlan(schema, "Int64Field > 0", "VarCharField", nil, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
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
		}, nil)
		assert.Error(t, err, expr)
	}
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
		}, nil)
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
		}, mv)
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
	}, nil)
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
	}, nil)
	assert.NoError(t, err, expr)
	paths = plan.GetVectorAnns().GetPredicates().GetUnaryRangeExpr().GetColumnInfo().GetNestedPath()
	assert.NotNil(t, paths)
	assert.Equal(t, 3, len(paths))
	assert.Equal(t, "A", paths[0])
	assert.Equal(t, "年份", paths[1])
	assert.Equal(t, "月份", paths[2])
}
