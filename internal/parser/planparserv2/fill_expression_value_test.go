package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type FillExpressionValueSuite struct {
	suite.Suite
}

func TestFillExpressionValue(t *testing.T) {
	suite.Run(t, new(FillExpressionValueSuite))
}

type testcase struct {
	expr   string
	values map[string]*schemapb.TemplateValue
}

func (s *FillExpressionValueSuite) assertValidExpr(helper *typeutil.SchemaHelper, exprStr string, templateValues map[string]*schemapb.TemplateValue) {
	expr, err := ParseExpr(helper, exprStr, templateValues)
	s.NoError(err, exprStr)
	s.NotNil(expr, exprStr)
	ShowExpr(expr)
}

func (s *FillExpressionValueSuite) assertInvalidExpr(helper *typeutil.SchemaHelper, exprStr string, templateValues map[string]*schemapb.TemplateValue) {
	expr, err := ParseExpr(helper, exprStr, templateValues)
	s.Error(err, exprStr)
	s.Nil(expr, exprStr)
}

func (s *FillExpressionValueSuite) TestTermExpr() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field in {age}`, map[string]*schemapb.TemplateValue{
				"age": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3), int64(4)})),
			}},
			{`FloatField in {age}`, map[string]*schemapb.TemplateValue{
				"age": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Double, []float64{1.1, 2.2, 3.3, 4.4})),
			}},
			{`A in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData(2.2),
						generateJSONData("abc"),
						generateJSONData(false),
					})),
			}},

			{`ArrayField in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Array,
						[]*schemapb.TemplateArrayValue{
							generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3)}),
							generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(4), int64(5), int64(6)}),
							generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(7), int64(8), int64(9)}),
						})),
			}},
			{`ArrayField[0] in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3)})),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertValidExpr(schemaH, c.expr, c.values)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field in {age}`, map[string]*schemapb.TemplateValue{
				"age": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_String, []string{"abc", "def"})),
			}},
			{`StringField in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData("abc"),
						generateJSONData(2.2),
						generateJSONData(false),
					})),
			}},
			{"ArrayField[0] in {list}", map[string]*schemapb.TemplateValue{
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData("abc"),
						generateJSONData(3.2),
					})),
			}},
			{"Int64Field not in {not_list}", map[string]*schemapb.TemplateValue{
				"not_list": generateTemplateValue(schemapb.DataType_Int64, int64(33)),
			}},
			{"Int64Field not in {not_list}", map[string]*schemapb.TemplateValue{
				"age": generateTemplateValue(schemapb.DataType_Int64, int64(33)),
			}},
			{`Int64Field in {empty_list}`, map[string]*schemapb.TemplateValue{
				"empty_list": generateTemplateValue(schemapb.DataType_Array, &schemapb.TemplateArrayValue{}),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}

func (s *FillExpressionValueSuite) TestUnaryRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field == 10`, nil},
			{`Int64Field > {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
			{`FloatField < {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, float64(12.3)),
			}},
			{`DoubleField != {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, float64(3.5)),
			}},
			{`ArrayField[0] >= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
			}},
			{`BoolField == {bool}`, map[string]*schemapb.TemplateValue{
				"bool": generateTemplateValue(schemapb.DataType_Bool, false),
			}},
			{`{str} != StringField`, map[string]*schemapb.TemplateValue{
				"str": generateTemplateValue(schemapb.DataType_String, "abc"),
			}},
			{`{target} > Int64Field`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertValidExpr(schemaH, c.expr, c.values)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field == 10.5`, nil},
			{`Int64Field > {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, 11.2),
			}},
			{`FloatField < {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_String, "abc"),
			}},
			{`DoubleField != {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Bool, false),
			}},
			{`ArrayField[0] >= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, 3.5),
			}},
			{`BoolField == {bool}`, map[string]*schemapb.TemplateValue{
				"bool": generateTemplateValue(schemapb.DataType_String, "abc"),
			}},
			{`{str} != StringField`, map[string]*schemapb.TemplateValue{
				"str": generateTemplateValue(schemapb.DataType_Int64, int64(5)),
			}},
			{`{int} != StringField`, map[string]*schemapb.TemplateValue{
				"int": generateTemplateValue(schemapb.DataType_Int64, int64(5)),
			}},
			{`{str} != StringField`, map[string]*schemapb.TemplateValue{
				"int": generateTemplateValue(schemapb.DataType_Int64, int64(5)),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`10 < Int64Field < 20`, nil},
			{`{max} > Int64Field > {min}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`{min} <= FloatField <= {max}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Float, float64(11)),
				"max": generateTemplateValue(schemapb.DataType_Float, float64(22)),
			}},
			{`{min} < DoubleField < {max}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Double, float64(11)),
				"max": generateTemplateValue(schemapb.DataType_Double, float64(22)),
			}},
			{`{max} >= ArrayField[0] >= {min}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} > Int64Field >= 10`, map[string]*schemapb.TemplateValue{
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`30 >= Int64Field > {min}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
			{`10 < Int64Field <= {max}`, map[string]*schemapb.TemplateValue{
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`{min} <= Int64Field < 20`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertValidExpr(schemaH, c.expr, c.values)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`10 < Int64Field < 20.5`, nil},
			{`{max} > Int64Field > {min}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
				"max": generateTemplateValue(schemapb.DataType_Double, 22.5),
			}},
			{`{min} <= FloatField <= {max}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_String, "abc"),
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
			{`{min} < DoubleField < {max}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Int64, int64(33)),
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} >= ArrayField[0] >= {min}`, map[string]*schemapb.TemplateValue{
				"min": generateTemplateValue(schemapb.DataType_Double, 11.5),
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} >= Int64Field >= {min}`, map[string]*schemapb.TemplateValue{
				"max": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} > Int64Field`, map[string]*schemapb.TemplateValue{
				"max": generateTemplateValue(schemapb.DataType_Bool, false),
			}},
			{`{$meta} > Int64Field`, map[string]*schemapb.TemplateValue{
				"$meta": generateTemplateValue(schemapb.DataType_Int64, int64(22)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryArithOpEvalRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field + 5.5 == 10.5`, nil},
			{`Int64Field - {offset} >= {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Double, 3.5),
				"target": generateTemplateValue(schemapb.DataType_Double, 11.5),
			}},
			{`Int64Field * 3.5 <= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, 11.5),
			}},
			{`Int64Field / {offset} > 11.5`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Double, 3.5),
			}},
			{`ArrayField[0] % {offset} < 11`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
			}},
			{`array_length(ArrayField) == {length}`, map[string]*schemapb.TemplateValue{
				"length": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
			}},
			{`array_length(ArrayField) > {length}`, map[string]*schemapb.TemplateValue{
				"length": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertValidExpr(schemaH, c.expr, c.values)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field + 6 == 12.5`, nil},
			{`Int64Field - {offset} == {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Int64, int64(4)),
				"target": generateTemplateValue(schemapb.DataType_Double, 13.5),
			}},
			{`Int64Field * 6 == {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, 13.5),
			}},
			{`Int64Field / {offset} == 11.5`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Int64, int64(6)),
			}},
			{`Int64Field % {offset} < 11`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Double, 3.5),
			}},
			{`Int64Field + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Double, 3.5),
			}},
			{`Int64Field + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_String, "abc"),
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(15)),
			}},
			{`Int64Field + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Int64, int64(15)),
				"target": generateTemplateValue(schemapb.DataType_String, "def"),
			}},
			{`ArrayField + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Double, 3.5),
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(5)),
			}},
			{`ArrayField[0] + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3)})),
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(5)),
			}},
			{`array_length(ArrayField) == {length}`, map[string]*schemapb.TemplateValue{
				"length": generateTemplateValue(schemapb.DataType_String, "abc"),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}

func (s *FillExpressionValueSuite) TestJSONContainsExpression() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`json_contains(A, 5)`, nil},
			{`json_contains(A, {age})`, map[string]*schemapb.TemplateValue{
				"age": generateTemplateValue(schemapb.DataType_Int64, int64(18)),
			}},
			{`json_contains(A, {str})`, map[string]*schemapb.TemplateValue{
				"str": generateTemplateValue(schemapb.DataType_String, "abc"),
			}},
			{`json_contains(A, {bool})`, map[string]*schemapb.TemplateValue{
				"bool": generateTemplateValue(schemapb.DataType_Bool, false),
			}},
			{`json_contains_any(JSONField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData("abc"),
						generateJSONData(2.2),
						generateJSONData(false),
					})),
			}},
			{`json_contains_any(JSONField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3), int64(4)})),
			}},
			{`json_contains_any(JSONField["A"], {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3), int64(4)})),
			}},
			{`json_contains_all(JSONField["A"], {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData("abc"),
						generateJSONData(2.2),
						generateJSONData(false),
					})),
			}},
			{`json_contains_all(JSONField["A"], {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Array, []*schemapb.TemplateArrayValue{
						generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3)}),
						generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(4), int64(5), int64(6)}),
					})),
			}},
			{`json_contains(ArrayField, {int})`, map[string]*schemapb.TemplateValue{
				"int": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
			}},
			{`json_contains_any(ArrayField, {list})`, map[string]*schemapb.TemplateValue{
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3), int64(4)})),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			s.assertValidExpr(schemaH, c.expr, c.values)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`json_contains(ArrayField[0], {str})`, map[string]*schemapb.TemplateValue{
				"str": generateTemplateValue(schemapb.DataType_String, "abc"),
			}},
			{`json_contains_any(JSONField, {not_array})`, map[string]*schemapb.TemplateValue{
				"not_array": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
			}},
			{`json_contains_all(JSONField, {not_array})`, map[string]*schemapb.TemplateValue{
				"not_array": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
			}},
			{`json_contains_all(JSONField, {not_array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3)})),
			}},
			{`json_contains_all(ArrayField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData("abc"),
						generateJSONData(2.2),
						generateJSONData(false),
					})),
			}},
			{`json_contains(ArrayField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{int64(1), int64(2), int64(3)})),
			}},
			{`json_contains_any(ArrayField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData(int64(1)),
						generateJSONData("abc"),
						generateJSONData(2.2),
						generateJSONData(false),
					})),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryExpression() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field > {int} && StringField in {list}`, map[string]*schemapb.TemplateValue{
				"int": generateTemplateValue(schemapb.DataType_Int64, int64(10)),
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_String, []string{"abc", "def", "ghi"})),
			}},
			{`{max} > FloatField >= {min} or BoolField == {bool}`, map[string]*schemapb.TemplateValue{
				"min":  generateTemplateValue(schemapb.DataType_Int64, int64(10)),
				"max":  generateTemplateValue(schemapb.DataType_Float, 22.22),
				"bool": generateTemplateValue(schemapb.DataType_Bool, true),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			s.assertValidExpr(schemaH, c.expr, c.values)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field > {int} && StringField in {list}`, map[string]*schemapb.TemplateValue{
				"int": generateTemplateValue(schemapb.DataType_String, "abc"),
				"list": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
						generateJSONData("abc"),
						generateJSONData(int64(10)),
						generateJSONData("ghi"),
					})),
			}},
			{`{max} > FloatField >= {min} or BoolField == {bool}`, map[string]*schemapb.TemplateValue{
				"min":  generateTemplateValue(schemapb.DataType_Int64, int64(10)),
				"bool": generateTemplateValue(schemapb.DataType_Bool, true),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}
