package planparserv2

import (
	"sync"
	"testing"

	"github.com/milvus-io/milvus/pkg/common"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func newTestSchema() *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
	}

	for name, value := range schemapb.DataType_value {
		dataType := schemapb.DataType(value)
		newField := &schemapb.FieldSchema{
			FieldID: int64(100 + value), Name: name + "Field", IsPrimaryKey: false, Description: "", DataType: dataType,
		}
		fields = append(fields, newField)
	}
	fields = append(fields, &schemapb.FieldSchema{
		FieldID: 199, Name: common.MetaFieldName, IsPrimaryKey: false, Description: "dynamic field", DataType: schemapb.DataType_JSON,
		IsDynamic: true,
	})

	return &schemapb.CollectionSchema{
		Name:               "test",
		Description:        "schema for test used",
		AutoID:             true,
		Fields:             fields,
		EnableDynamicField: true,
	}
}

func assertValidExpr(t *testing.T, helper *typeutil.SchemaHelper, exprStr string) {
	_, err := ParseExpr(helper, exprStr)
	assert.NoError(t, err, exprStr)

	// expr, err := ParseExpr(helper, exprStr)
	// assert.NoError(t, err, exprStr)
	// fmt.Printf("expr: %s\n", exprStr)
	// ShowExpr(expr)
}

func assertInvalidExpr(t *testing.T, helper *typeutil.SchemaHelper, exprStr string) {
	_, err := ParseExpr(helper, exprStr)
	assert.Error(t, err, exprStr)
}

func TestExpr_Term(t *testing.T) {
	schema := newTestSchema()
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
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}

func TestExpr_Compare(t *testing.T) {
	schema := newTestSchema()
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
	schema := newTestSchema()
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
	schema := newTestSchema()
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
	unsupported := []string{
		`VarCharField like "not_%_supported"`,
		`JSONField["A"] like "not_%_supported"`,
		`$meta["A"] like "not_%_supported"`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_BinaryRange(t *testing.T) {
	schema := newTestSchema()
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
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}

func TestExpr_BinaryArith(t *testing.T) {
	schema := newTestSchema()
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`Int64Field % 10 == 9`,
		`Int64Field % 10 != 9`,
		`A % 10 != 2`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}

	// TODO: enable these after execution backend is ready.
	unsupported := []string{
		`Int8Field + 1 < 2`,
		`Int16Field - 3 <= 4`,
		`Int32Field * 5 > 6`,
		`Int64Field / 7 >= 8`,
		`FloatField + 11 < 12`,
		`DoubleField - 13 < 14`,
		`A - 15 < 16`,
	}
	for _, exprStr := range unsupported {
		assertInvalidExpr(t, helper, exprStr)
	}
}

func TestExpr_Value(t *testing.T) {
	schema := newTestSchema()
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`1`,
		`2.0`,
		`true`,
		`false`,
		`"str"`,
	}
	for _, exprStr := range exprStrs {
		expr := handleExpr(helper, exprStr)
		assert.NotNil(t, getExpr(expr).expr, exprStr)
		// fmt.Printf("expr: %s\n", exprStr)
		// ShowExpr(getExpr(expr).expr)
	}
}

func TestExpr_Identifier(t *testing.T) {
	schema := newTestSchema()
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
	schema := newTestSchema()
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
	schema := newTestSchema()
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStrs := []string{
		`not (Int8Field + 1 == 2)`,
		`(Int16Field - 3 == 4) and (Int32Field * 5 != 6)`,
		`(Int64Field / 7 != 8) or (Int64Field % 10 == 9)`,
		`Int64Field > 0 && VarCharField > "0"`,
		`Int64Field < 0 && VarCharField < "0"`,
		`A > 50 or B < 40`,
	}
	for _, exprStr := range exprStrs {
		assertValidExpr(t, helper, exprStr)
	}
}

func TestCreateRetrievePlan(t *testing.T) {
	schema := newTestSchema()
	_, err := CreateRetrievePlan(schema, "Int64Field > 0")
	assert.NoError(t, err)
}

func TestCreateSearchPlan(t *testing.T) {
	schema := newTestSchema()
	_, err := CreateSearchPlan(schema, `$meta["A"] != 10`, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
}

func TestExpr_Invalid(t *testing.T) {
	schema := newTestSchema()
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
		`BoolField or false`,
		`Int32Field or Int64Field`,
		`not_in_schema and true`,
		`false and not_in_schema`,
		`"str" and false`,
		`BoolField and false`,
		`Int32Field and Int64Field`,
		// -------------------- unsupported ----------------------
		`1 ^ 2`,
		`1 & 2`,
		`1 ** 2`,
		`1 << 2`,
		`1 | 2`,
	}
	for _, exprStr := range exprStrs {
		_, err := ParseExpr(helper, exprStr)
		assert.Error(t, err, exprStr)
	}
}

func TestCreateRetrievePlan_Invalid(t *testing.T) {
	t.Run("invalid schema", func(t *testing.T) {
		schema := newTestSchema()
		schema.Fields = append(schema.Fields, schema.Fields[0])
		_, err := CreateRetrievePlan(schema, "")
		assert.Error(t, err)
	})

	t.Run("invalid expr", func(t *testing.T) {
		schema := newTestSchema()
		_, err := CreateRetrievePlan(schema, "invalid expression")
		assert.Error(t, err)
	})
}

func TestCreateSearchPlan_Invalid(t *testing.T) {
	t.Run("invalid schema", func(t *testing.T) {
		schema := newTestSchema()
		schema.Fields = append(schema.Fields, schema.Fields[0])
		_, err := CreateSearchPlan(schema, "", "", nil)
		assert.Error(t, err)
	})

	t.Run("invalid expr", func(t *testing.T) {
		schema := newTestSchema()
		_, err := CreateSearchPlan(schema, "invalid expression", "", nil)
		assert.Error(t, err)
	})

	t.Run("invalid vector field", func(t *testing.T) {
		schema := newTestSchema()
		_, err := CreateSearchPlan(schema, "Int64Field > 0", "not_exist", nil)
		assert.Error(t, err)
	})

	t.Run("not vector type", func(t *testing.T) {
		schema := newTestSchema()
		_, err := CreateSearchPlan(schema, "Int64Field > 0", "VarCharField", nil)
		assert.Error(t, err)
	})
}

func Test_handleExpr(t *testing.T) {
	schema := newTestSchema()
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	ret1 := handleExpr(schemaHelper, "1 < Int32Field < (Int16Field)")
	err1, ok := ret1.(error)
	assert.True(t, ok)
	assert.Error(t, err1)
}

// test if handleExpr is thread-safe.
func Test_handleExpr_17126(t *testing.T) {
	schema := newTestSchema()
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)
	normal := "Int64Field > 0"
	abnormal := "1 < Int32Field < (Int16Field)"

	n := 4 // default parallel in regression.
	m := 16
	var wg sync.WaitGroup
	for i := 0; i < n*m; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				ret := handleExpr(schemaHelper, normal)
				_, ok := ret.(error)
				assert.False(t, ok)
			} else {
				ret := handleExpr(schemaHelper, abnormal)
				err, ok := ret.(error)
				assert.True(t, ok)
				assert.Error(t, err)
			}
		}()
	}
	wg.Wait()
}

func Test_JSONExpr(t *testing.T) {
	schema := newTestSchema()
	expr := ""
	// search
	expr = `$meta["A"] > 90`
	_, err := CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `JSONField["A"] > 90`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A < 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `JSONField["A"] <= 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] <= 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] >= 95`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] == 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] != 95`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] > 90 && $meta["B"] < 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] > 95 || $meta["B"] < 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A > 95 || $meta["B"] < 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `not ($meta["A"] == 95)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] in [90, 91, 95, 97]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] not in [90, 91, 95, 97]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["C"]["0"] in [90, 91, 95, 97]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["C"]["0"] not in [90, 91, 95, 97]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `C["0"] not in [90, 91, 95, 97]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `C[0] in [90, 91, 95, 97]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `C["0"] > 90`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `C["0"] < 90`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `C["0"] == 90`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `10 < C["0"] < 90`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `100 > C["0"] > 90`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `0 <= $meta["A"] < 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `0 <= A < 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] + 5 == 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta["A"] > 10 + 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `100 - 5 < $meta["A"]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `100 == $meta["A"] + 6`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `exists $meta["A"]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `exists $meta["A"]["B"]["C"] `
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A["B"][0] > 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A["B"][0] > 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `$meta[0] > 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A["\"\"B\"\""] > 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A["[\"B\"]"] == "abc\"bbb\"cc"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A['B'] == "abc\"bbb\"cc"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A['B'] == 'abc"cba'`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `A['B'] == 'abc\"cba'`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
}

func Test_InvalidExprOnJSONField(t *testing.T) {
	schema := newTestSchema()
	expr := ""
	var err error
	expr = `exists $meta`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `exists JSONField`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `$meta > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `$meta > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `JSONField == 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `$meta < 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `0 < $meta < 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `20 > $meta > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `$meta + 5 > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `$meta > 2 + 5`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `exists $meta["A"] > 10 `
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `exists Int64Field `
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A[[""B""]] > 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A["[""B""]"] > 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A[[""B""]] > 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A[B] > 10`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)
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

	expr := ""
	var err error
	expr = `A == 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `JSON["A"] > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A < 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `0 < JSON["A"] < 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `0 < A < 100`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `100 > JSON["A"] > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `100 > A > 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)
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

	expr := ""
	var err error
	expr = `A == 0`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A in [1, 2, 3]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `A not in [1, 2, 3]`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `"1" in A`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `"1" not in A`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)
}

func Test_exprWithSingleQuotes(t *testing.T) {
	schema := newTestSchema()
	expr := ""
	var err error
	expr = `'abc' < StringField < "def"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `'ab"c' < StringField < "d'ef"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `'ab\"c' < StringField < "d\'ef"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `'ab\'c' < StringField < "d\"ef"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)

	expr = `'abc'd' < StringField < "def"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `'abc' < StringField < "def"g"`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)
}

func Test_JSONContains(t *testing.T) {
	schema := newTestSchema()
	expr := ""
	var err error
	expr = `json_contains(A, 10)`
	plan, err := CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetTermExpr())

	expr = `not json_contains(A, 10)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetUnaryExpr())

	expr = `json_contains(A, 10.5)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetTermExpr())

	expr = `not json_contains(A, 10.5)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetUnaryExpr())

	expr = `json_contains(A, "10")`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetTermExpr())

	expr = `not json_contains(A, "10")`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetUnaryExpr())

	expr = `json_contains($meta["A"], 10)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetTermExpr())

	expr = `not json_contains($meta["A"], 10)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetUnaryExpr())

	expr = `json_contains(JSONField["x"], 5)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetTermExpr())

	expr = `not json_contains(JSONField["x"], 5)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetUnaryExpr())

	expr = `JSON_CONTAINS(JSONField["x"], 5)`
	plan, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.NoError(t, err)
	assert.NotNil(t, plan.GetVectorAnns().GetPredicates().GetTermExpr())
}

func Test_InvalidJSONContains(t *testing.T) {
	schema := newTestSchema()
	expr := ""
	var err error
	expr = `json_contains(10, A)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(1, [1,2,3])`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains([1,2,3], 1)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains([1,2,3], [1,2,3])`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains([1,2,3], [1,2])`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(A, [1,2,3])`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains($meta, 1)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(A, B)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `not json_contains(A, B)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(A, B > 5)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(StringField, "a")`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(A, StringField > 5)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(A)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(A, 5, C)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_contains(JSONField, 5)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `json_Contains(JSONField, 5)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)

	expr = `JSON_contains(JSONField, 5)`
	_, err = CreateSearchPlan(schema, expr, "FloatVectorField", &planpb.QueryInfo{
		Topk:         0,
		MetricType:   "",
		SearchParams: "",
		RoundDecimal: 0,
	})
	assert.Error(t, err)
}
