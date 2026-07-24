package planparserv2

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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
			{`Int64Field in {empty_list}`, map[string]*schemapb.TemplateValue{
				"empty_list": generateTemplateValue(schemapb.DataType_Array, &schemapb.TemplateArrayValue{}),
			}},
			{`A in {empty_list}`, map[string]*schemapb.TemplateValue{
				"empty_list": generateTemplateValue(schemapb.DataType_Array, &schemapb.TemplateArrayValue{}),
			}},
			{`ArrayField in {empty_list}`, map[string]*schemapb.TemplateValue{
				"empty_list": generateTemplateValue(schemapb.DataType_Array, &schemapb.TemplateArrayValue{}),
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
		}

		schemaH := newTestSchemaHelper(s.T())
		for _, c := range testcases {
			s.assertInvalidExpr(schemaH, c.expr, c.values)
		}
	})
}

func (s *FillExpressionValueSuite) TestEmptyTermRejectsUnsupportedTargetTypes() {
	schemaH := newTestSchemaHelper(s.T())
	emptyTemplate := map[string]*schemapb.TemplateValue{
		"empty": generateTemplateValue(schemapb.DataType_Array, &schemapb.TemplateArrayValue{}),
	}
	targetFields := []string{
		"BinaryVectorField",
		"FloatVectorField",
		"Float16VectorField",
		"BFloat16VectorField",
		"SparseFloatVectorField",
		"Int8VectorField",
		"ArrayOfVectorField",
		"GeometryField",
	}
	rightHandSides := []struct {
		name   string
		value  string
		params map[string]*schemapb.TemplateValue
	}{
		{name: "inline", value: "[]"},
		{name: "template", value: "{empty}", params: emptyTemplate},
	}

	for _, field := range targetFields {
		for _, op := range []string{"in", "not in"} {
			for _, rhs := range rightHandSides {
				s.Run(field+"/"+op+"/"+rhs.name, func() {
					expr, err := ParseExpr(schemaH, field+" "+op+" "+rhs.value, rhs.params)
					s.Require().Error(err)
					s.Require().Nil(expr)
					s.Contains(err.Error(), "term expression is not supported")
				})
			}
		}
	}
}

func (s *FillExpressionValueSuite) TestEmptyArrayComparisonNormalization() {
	schemaH := newTestSchemaHelper(s.T())
	emptyArray := generateTemplateValue(schemapb.DataType_Array, &schemapb.TemplateArrayValue{})

	testcases := []struct {
		expr string
		op   planpb.OpType
	}{
		{expr: `ArrayField == {empty}`, op: planpb.OpType_Equal},
		{expr: `{empty} == ArrayField`, op: planpb.OpType_Equal},
		{expr: `ArrayField != {empty}`, op: planpb.OpType_NotEqual},
		{expr: `{empty} != ArrayField`, op: planpb.OpType_NotEqual},
	}
	for _, testcase := range testcases {
		s.Run(testcase.expr, func() {
			expr, err := ParseExpr(schemaH, testcase.expr, map[string]*schemapb.TemplateValue{
				"empty": emptyArray,
			})
			s.NoError(err)
			arrayLength := expr.GetBinaryArithOpEvalRangeExpr()
			s.NotNil(arrayLength)
			s.Equal(planpb.ArithOpType_ArrayLength, arrayLength.GetArithOp())
			s.Equal(testcase.op, arrayLength.GetOp())
			s.Equal(int64(0), arrayLength.GetValue().GetInt64Val())
		})
	}
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

func (s *FillExpressionValueSuite) TestSpecialStringTemplate() {
	schema := newTestSchema(true)
	enableMatch(schema)
	schemaH, err := typeutil.CreateSchemaHelper(schema)
	s.NoError(err)

	s.Run("like pattern", func() {
		expr, err := ParseExpr(schemaH, `VarCharField like {pattern}`, map[string]*schemapb.TemplateValue{
			"pattern": generateTemplateValue(schemapb.DataType_VarChar, "prefix%"),
		})
		s.NoError(err)
		rangeExpr := expr.GetUnaryRangeExpr()
		s.NotNil(rangeExpr)
		s.Equal(planpb.OpType_PrefixMatch, rangeExpr.GetOp())
		s.Equal("prefix", rangeExpr.GetValue().GetStringVal())
	})

	s.Run("text match query", func() {
		expr, err := ParseExpr(schemaH, `text_match(VarCharField, {query})`, map[string]*schemapb.TemplateValue{
			"query": generateTemplateValue(schemapb.DataType_VarChar, "vector database"),
		})
		s.NoError(err)
		rangeExpr := expr.GetUnaryRangeExpr()
		s.NotNil(rangeExpr)
		s.Equal(planpb.OpType_TextMatch, rangeExpr.GetOp())
		s.Equal("vector database", rangeExpr.GetValue().GetStringVal())
	})

	s.Run("phrase match query", func() {
		expr, err := ParseExpr(schemaH, `phrase_match(VarCharField, {query}, 1)`, map[string]*schemapb.TemplateValue{
			"query": generateTemplateValue(schemapb.DataType_VarChar, "vector database"),
		})
		s.NoError(err)
		rangeExpr := expr.GetUnaryRangeExpr()
		s.NotNil(rangeExpr)
		s.Equal(planpb.OpType_PhraseMatch, rangeExpr.GetOp())
		s.Equal("vector database", rangeExpr.GetValue().GetStringVal())
		s.Equal(int64(1), rangeExpr.GetExtraValues()[0].GetInt64Val())
	})

	s.Run("GIS WKT", func() {
		expr, err := ParseExpr(schemaH, `st_contains(GeometryField, {wkt})`, map[string]*schemapb.TemplateValue{
			"wkt": generateTemplateValue(schemapb.DataType_VarChar, "POINT(0 0)"),
		})
		s.NoError(err)
		gisExpr := expr.GetGisfunctionFilterExpr()
		s.NotNil(gisExpr)
		s.Equal(planpb.GISFunctionFilterExpr_Contains, gisExpr.GetOp())
		s.Equal("POINT(0 0)", gisExpr.GetWktString())
	})

	s.Run("ST_DWithin WKT", func() {
		expr, err := ParseExpr(schemaH, `st_dwithin(GeometryField, {wkt}, 1.5)`, map[string]*schemapb.TemplateValue{
			"wkt": generateTemplateValue(schemapb.DataType_VarChar, "POINT(0 0)"),
		})
		s.NoError(err)
		gisExpr := expr.GetGisfunctionFilterExpr()
		s.NotNil(gisExpr)
		s.Equal(planpb.GISFunctionFilterExpr_DWithin, gisExpr.GetOp())
		s.Equal("POINT(0 0)", gisExpr.GetWktString())
		s.Equal(float64(1.5), gisExpr.GetDistance())
	})

	s.Run("extra parameters are still constants", func() {
		s.assertInvalidExpr(schemaH, `text_match(VarCharField, "vector database", minimum_should_match={n})`, map[string]*schemapb.TemplateValue{
			"n": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		})
		s.assertInvalidExpr(schemaH, `phrase_match(VarCharField, "vector database", {slop})`, map[string]*schemapb.TemplateValue{
			"slop": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		})
		s.assertInvalidExpr(schemaH, `st_dwithin(GeometryField, "POINT(0 0)", {distance})`, map[string]*schemapb.TemplateValue{
			"distance": generateTemplateValue(schemapb.DataType_Double, float64(1.5)),
		})
		s.assertInvalidExpr(schemaH, `random_sample({ratio})`, map[string]*schemapb.TemplateValue{
			"ratio": generateTemplateValue(schemapb.DataType_Double, float64(0.1)),
		})
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
			{`Int64Field + 5 == 10`, nil},
			{`Int64Field - {offset} >= {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
			{`Int64Field * 3 <= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateTemplateValue(schemapb.DataType_Int64, int64(11)),
			}},
			{`Int64Field / {offset} > 11`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
			}},
			{`FloatField / {offset} > 11.3`, map[string]*schemapb.TemplateValue{
				"offset": generateTemplateValue(schemapb.DataType_Double, 3.3),
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

	s.Run("template elements same type", func() {
		schemaH := newTestSchemaHelper(s.T())
		testcases := []struct {
			name     string
			expr     string
			values   map[string]*schemapb.TemplateValue
			expected bool
		}{
			{
				name: "typed homogeneous array",
				expr: `json_contains_any(JSONField, {array})`,
				values: map[string]*schemapb.TemplateValue{
					"array": generateTemplateValue(schemapb.DataType_Array,
						generateTemplateArrayValue(schemapb.DataType_Int64, []int64{1, 2, 3})),
				},
				expected: true,
			},
			{
				name: "JSON homogeneous array",
				expr: `json_contains_all(JSONField, {array})`,
				values: map[string]*schemapb.TemplateValue{
					"array": generateTemplateValue(schemapb.DataType_Array,
						generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
							generateJSONData(int64(1)),
							generateJSONData(int64(2)),
							generateJSONData(int64(3)),
						})),
				},
				expected: true,
			},
			{
				name: "JSON heterogeneous array",
				expr: `json_contains_any(JSONField, {array})`,
				values: map[string]*schemapb.TemplateValue{
					"array": generateTemplateValue(schemapb.DataType_Array,
						generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
							generateJSONData(int64(1)),
							generateJSONData("1"),
						})),
				},
				expected: false,
			},
			{
				name: "empty array",
				expr: `json_contains_any(JSONField, {array})`,
				values: map[string]*schemapb.TemplateValue{
					"array": generateTemplateValue(schemapb.DataType_Array,
						generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{})),
				},
				expected: true,
			},
			{
				name: "singleton array",
				expr: `json_contains_all(JSONField, {array})`,
				values: map[string]*schemapb.TemplateValue{
					"array": generateTemplateValue(schemapb.DataType_Array,
						generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
							generateJSONData(1.5),
						})),
				},
				expected: true,
			},
		}

		for _, testcase := range testcases {
			s.Run(testcase.name, func() {
				expr, err := ParseExpr(schemaH, testcase.expr, testcase.values)
				s.NoError(err)
				s.NotNil(expr)
				contains := expr.GetJsonContainsExpr()
				s.NotNil(contains)
				s.Equal(testcase.expected, contains.GetElementsSameType())
			})
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

func (s *FillExpressionValueSuite) TestBinaryRangeWithMixedNumericTypesForJSON() {
	// Mixed JSON numeric bounds must preserve their concrete literal types so
	// large int64 values are not rounded before precise segcore comparison.
	schemaH := newTestSchemaHelper(s.T())

	s.Run("lower int64 upper float should preserve types", func() {
		// A is a dynamic field (JSON type)
		exprStr := `{min} < A < {max}`
		templateValues := map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Int64, int64(9007199254740993)),
			"max": generateTemplateValue(schemapb.DataType_Double, float64(9007199254740996)),
		}

		expr, err := ParseExpr(schemaH, exprStr, templateValues)
		s.NoError(err)
		s.NotNil(expr)

		bre := expr.GetBinaryRangeExpr()
		s.NotNil(bre, "expected BinaryRangeExpr")
		s.IsType(&planpb.GenericValue_Int64Val{}, bre.GetLowerValue().GetVal())
		s.Equal(int64(9007199254740993), bre.GetLowerValue().GetInt64Val())
		s.IsType(&planpb.GenericValue_FloatVal{}, bre.GetUpperValue().GetVal())
		s.Equal(float64(9007199254740996), bre.GetUpperValue().GetFloatVal())
	})

	s.Run("lower float upper int64 should preserve types", func() {
		exprStr := `{min} < A < {max}`
		templateValues := map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Double, float64(9007199254740992)),
			"max": generateTemplateValue(schemapb.DataType_Int64, int64(9007199254740995)),
		}

		expr, err := ParseExpr(schemaH, exprStr, templateValues)
		s.NoError(err)
		s.NotNil(expr)

		bre := expr.GetBinaryRangeExpr()
		s.NotNil(bre, "expected BinaryRangeExpr")
		s.IsType(&planpb.GenericValue_FloatVal{}, bre.GetLowerValue().GetVal())
		s.Equal(float64(9007199254740992), bre.GetLowerValue().GetFloatVal())
		s.IsType(&planpb.GenericValue_Int64Val{}, bre.GetUpperValue().GetVal())
		s.Equal(int64(9007199254740995), bre.GetUpperValue().GetInt64Val())
	})

	s.Run("both int64 should remain int64", func() {
		exprStr := `{min} < A < {max}`
		templateValues := map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Int64, int64(10)),
			"max": generateTemplateValue(schemapb.DataType_Int64, int64(100)),
		}

		expr, err := ParseExpr(schemaH, exprStr, templateValues)
		s.NoError(err)
		s.NotNil(expr)

		// Verify both bounds remain int64 type
		bre := expr.GetBinaryRangeExpr()
		s.NotNil(bre, "expected BinaryRangeExpr")
		s.Equal(int64(10), bre.GetLowerValue().GetInt64Val())
		s.Equal(int64(100), bre.GetUpperValue().GetInt64Val())
	})

	s.Run("both float should remain float", func() {
		exprStr := `{min} < A < {max}`
		templateValues := map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Double, float64(10.5)),
			"max": generateTemplateValue(schemapb.DataType_Double, float64(100.5)),
		}

		expr, err := ParseExpr(schemaH, exprStr, templateValues)
		s.NoError(err)
		s.NotNil(expr)

		// Verify both bounds remain float type
		bre := expr.GetBinaryRangeExpr()
		s.NotNil(bre, "expected BinaryRangeExpr")
		s.Equal(float64(10.5), bre.GetLowerValue().GetFloatVal())
		s.Equal(float64(100.5), bre.GetUpperValue().GetFloatVal())
	})

	s.Run("adjacent mixed bounds above 2^53 should remain valid", func() {
		expr, err := ParseExpr(schemaH, `{min} <= A < {max}`, map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Double, float64(9007199254740992)),
			"max": generateTemplateValue(schemapb.DataType_Int64, int64(9007199254740993)),
		})
		s.Require().NoError(err)

		bre := expr.GetBinaryRangeExpr()
		s.Require().NotNil(bre)
		s.IsType(&planpb.GenericValue_FloatVal{}, bre.GetLowerValue().GetVal())
		s.Equal(float64(9007199254740992), bre.GetLowerValue().GetFloatVal())
		s.IsType(&planpb.GenericValue_Int64Val{}, bre.GetUpperValue().GetVal())
		s.Equal(int64(9007199254740993), bre.GetUpperValue().GetInt64Val())
	})

	s.Run("reverse adjacent mixed bounds above 2^53 should remain valid", func() {
		expr, err := ParseExpr(schemaH, `{max} > A >= {min}`, map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Double, float64(9007199254740992)),
			"max": generateTemplateValue(schemapb.DataType_Int64, int64(9007199254740993)),
		})
		s.Require().NoError(err)

		bre := expr.GetBinaryRangeExpr()
		s.Require().NotNil(bre)
		s.IsType(&planpb.GenericValue_FloatVal{}, bre.GetLowerValue().GetVal())
		s.IsType(&planpb.GenericValue_Int64Val{}, bre.GetUpperValue().GetVal())
	})

	s.Run("truly reversed mixed bounds should fail", func() {
		s.assertInvalidExpr(schemaH, `{min} <= A < {max}`, map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Double, float64(9007199254740994)),
			"max": generateTemplateValue(schemapb.DataType_Int64, int64(9007199254740993)),
		})
	})

	s.Run("NaN and different dynamic types are deferred to execution", func() {
		s.assertValidExpr(schemaH, `{min} < A < {max}`, map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Double, math.NaN()),
			"max": generateTemplateValue(schemapb.DataType_Int64, int64(10)),
		})
		s.assertValidExpr(schemaH, `{min} < A < {max}`, map[string]*schemapb.TemplateValue{
			"min": generateTemplateValue(schemapb.DataType_Int64, int64(1)),
			"max": generateTemplateValue(schemapb.DataType_String, "z"),
		})
	})
}

func (s *FillExpressionValueSuite) TestBinaryArithOpEvalRangeDivisionByZero() {
	schemaH := newTestSchemaHelper(s.T())

	s.Run("division by integer zero should fail", func() {
		exprStr := `Int64Field / {divisor} == {value}`
		templateValues := map[string]*schemapb.TemplateValue{
			"divisor": generateTemplateValue(schemapb.DataType_Int64, int64(0)),
			"value":   generateTemplateValue(schemapb.DataType_Int64, int64(5)),
		}
		s.assertInvalidExpr(schemaH, exprStr, templateValues)
	})

	s.Run("division by float zero should fail", func() {
		exprStr := `FloatField / {divisor} == {value}`
		templateValues := map[string]*schemapb.TemplateValue{
			"divisor": generateTemplateValue(schemapb.DataType_Double, float64(0.0)),
			"value":   generateTemplateValue(schemapb.DataType_Double, float64(5.0)),
		}
		s.assertInvalidExpr(schemaH, exprStr, templateValues)
	})

	s.Run("modulo by integer zero should fail", func() {
		exprStr := `Int64Field % {divisor} == {value}`
		templateValues := map[string]*schemapb.TemplateValue{
			"divisor": generateTemplateValue(schemapb.DataType_Int64, int64(0)),
			"value":   generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		}
		s.assertInvalidExpr(schemaH, exprStr, templateValues)
	})

	s.Run("division by non-zero should succeed", func() {
		exprStr := `Int64Field / {divisor} == {value}`
		templateValues := map[string]*schemapb.TemplateValue{
			"divisor": generateTemplateValue(schemapb.DataType_Int64, int64(2)),
			"value":   generateTemplateValue(schemapb.DataType_Int64, int64(5)),
		}
		s.assertValidExpr(schemaH, exprStr, templateValues)
	})

	s.Run("modulo by non-zero should succeed", func() {
		exprStr := `Int64Field % {divisor} == {value}`
		templateValues := map[string]*schemapb.TemplateValue{
			"divisor": generateTemplateValue(schemapb.DataType_Int64, int64(3)),
			"value":   generateTemplateValue(schemapb.DataType_Int64, int64(1)),
		}
		s.assertValidExpr(schemaH, exprStr, templateValues)
	})
}

func (s *FillExpressionValueSuite) TestTermExprWithMixedNumericTypesForJSON() {
	// Mixed JSON membership is split into homogeneous predicates so segcore
	// never receives a TermExpr whose type disagrees with one of its values.
	schemaH := newTestSchemaHelper(s.T())

	s.Run("mixed int and float should split by concrete type", func() {
		// A is a dynamic field (JSON type)
		exprStr := `A in [1, 2.5, 3, 4.5]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)
		assertJSONMembershipKinds(s.T(), expr, map[string]int{"int64": 2, "float": 2})
	})

	s.Run("all integers should remain int64", func() {
		exprStr := `A in [1, 2, 3, 4]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)

		// Verify all values remain int64 type
		te := expr.GetTermExpr()
		s.NotNil(te, "expected TermExpr")
		s.Len(te.GetValues(), 4)
		s.Equal(int64(1), te.GetValues()[0].GetInt64Val())
		s.Equal(int64(2), te.GetValues()[1].GetInt64Val())
		s.Equal(int64(3), te.GetValues()[2].GetInt64Val())
		s.Equal(int64(4), te.GetValues()[3].GetInt64Val())
	})

	s.Run("all floats should remain float", func() {
		exprStr := `A in [1.5, 2.5, 3.5, 4.5]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)

		// Verify all values remain float type
		te := expr.GetTermExpr()
		s.NotNil(te, "expected TermExpr")
		s.Len(te.GetValues(), 4)
		s.Equal(float64(1.5), te.GetValues()[0].GetFloatVal())
		s.Equal(float64(2.5), te.GetValues()[1].GetFloatVal())
		s.Equal(float64(3.5), te.GetValues()[2].GetFloatVal())
		s.Equal(float64(4.5), te.GetValues()[3].GetFloatVal())
	})

	s.Run("single float with integers should keep exact literal types", func() {
		exprStr := `A in [1, 2, 3.0, 4]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)

		assertJSONMembershipKinds(s.T(), expr, map[string]int{"int64": 3, "float": 1})
	})

	s.Run("JSONField with mixed int and float should split", func() {
		exprStr := `JSONField["x"] in [10, 20.5, 30]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)

		assertJSONMembershipKinds(s.T(), expr, map[string]int{"int64": 2, "float": 1})
	})

	s.Run("non-JSON field should not be affected by mixed type normalization", func() {
		// Use >= 10 values to stay above the integer IN threshold and keep TermExpr form
		exprStr := `Int64Field in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)

		// Verify values remain as integers for non-JSON field
		te := expr.GetTermExpr()
		s.NotNil(te, "expected TermExpr")
		s.Len(te.GetValues(), 10)
		for i := 0; i < 10; i++ {
			s.Equal(int64(i+1), te.GetValues()[i].GetInt64Val())
		}
	})

	s.Run("not in with mixed int and float should negate split membership", func() {
		exprStr := `A not in [1, 2.5, 3]`

		expr, err := ParseExpr(schemaH, exprStr, nil)
		s.NoError(err)
		s.NotNil(expr)

		ue := expr.GetUnaryExpr()
		s.NotNil(ue, "expected UnaryExpr")
		s.Equal(planpb.UnaryExpr_Not, ue.GetOp())
		assertJSONMembershipKinds(s.T(), ue.GetChild(), map[string]int{"int64": 2, "float": 1})
	})

	s.Run("mixed template values should split by concrete type", func() {
		expr, err := ParseExpr(schemaH, `A in {list}`, map[string]*schemapb.TemplateValue{
			"list": generateTemplateValue(schemapb.DataType_Array,
				generateTemplateArrayValue(schemapb.DataType_JSON, [][]byte{
					generateJSONData(int64(1)),
					generateJSONData(2.5),
					generateJSONData("3"),
					generateJSONData(true),
				})),
		})
		s.NoError(err)
		s.NotNil(expr)
		assertJSONMembershipKinds(s.T(), expr, map[string]int{
			"bool": 1, "int64": 1, "float": 1, "string": 1,
		})
	})
}

func assertJSONMembershipKinds(t *testing.T, expr *planpb.Expr, expected map[string]int) {
	t.Helper()
	actual := make(map[string]int)
	var visit func(*planpb.Expr)
	visit = func(current *planpb.Expr) {
		if current == nil {
			return
		}
		if term := current.GetTermExpr(); term != nil {
			var termKind string
			for _, value := range term.GetValues() {
				kind := genericValueKind(value)
				if termKind == "" {
					termKind = kind
				}
				require.Equal(t, termKind, kind, "TermExpr must be homogeneous")
				actual[kind]++
			}
			return
		}
		if unaryRange := current.GetUnaryRangeExpr(); unaryRange != nil {
			if unaryRange.GetOp() == planpb.OpType_Equal {
				actual[genericValueKind(unaryRange.GetValue())]++
			}
			return
		}
		if binary := current.GetBinaryExpr(); binary != nil {
			visit(binary.GetLeft())
			visit(binary.GetRight())
			return
		}
		if unary := current.GetUnaryExpr(); unary != nil {
			visit(unary.GetChild())
		}
	}
	visit(expr)
	require.Equal(t, expected, actual)
}

func genericValueKind(value *planpb.GenericValue) string {
	switch value.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return "bool"
	case *planpb.GenericValue_Int64Val:
		return "int64"
	case *planpb.GenericValue_FloatVal:
		return "float"
	case *planpb.GenericValue_StringVal:
		return "string"
	case *planpb.GenericValue_ArrayVal:
		return "array"
	default:
		return "other"
	}
}

// assertNoUnfilledPlaceholder walks the expression tree and asserts that
// every scalar predicate has its GenericValue populated. This is precisely
// the invariant violated by issue #49141: under unary NOT, template
// placeholders were left unset (val_case == VAL_NOT_SET), leading to
// QueryNode assertion failures or empty-Values terms that flip the
// semantics of the surrounding NOT.
func (s *FillExpressionValueSuite) assertNoUnfilledPlaceholder(e *planpb.Expr) {
	if e == nil {
		return
	}
	switch x := e.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		te := x.TermExpr
		s.NotEmpty(te.GetValues(), "TermExpr values should be filled (template=%q)", te.GetTemplateVariableName())
		for _, v := range te.GetValues() {
			s.NotNil(v.GetVal(), "TermExpr element GenericValue must have val set")
		}
	case *planpb.Expr_UnaryRangeExpr:
		ure := x.UnaryRangeExpr
		s.NotNil(ure.GetValue(), "UnaryRangeExpr value should be filled (template=%q)", ure.GetTemplateVariableName())
		s.NotNil(ure.GetValue().GetVal(), "UnaryRangeExpr GenericValue must have val set")
	case *planpb.Expr_BinaryRangeExpr:
		bre := x.BinaryRangeExpr
		s.NotNil(bre.GetLowerValue().GetVal(), "BinaryRangeExpr lower value must be filled")
		s.NotNil(bre.GetUpperValue().GetVal(), "BinaryRangeExpr upper value must be filled")
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		bao := x.BinaryArithOpEvalRangeExpr
		s.NotNil(bao.GetValue().GetVal(), "BinaryArithOpEvalRangeExpr value must be filled")
		s.NotNil(bao.GetRightOperand().GetVal(), "BinaryArithOpEvalRangeExpr right operand must be filled")
	case *planpb.Expr_JsonContainsExpr:
		jc := x.JsonContainsExpr
		s.NotEmpty(jc.GetElements(), "JSONContainsExpr elements should be filled (template=%q)", jc.GetTemplateVariableName())
		for _, v := range jc.GetElements() {
			s.NotNil(v.GetVal(), "JSONContainsExpr element GenericValue must have val set")
		}
	case *planpb.Expr_BinaryExpr:
		s.assertNoUnfilledPlaceholder(x.BinaryExpr.GetLeft())
		s.assertNoUnfilledPlaceholder(x.BinaryExpr.GetRight())
	case *planpb.Expr_UnaryExpr:
		s.assertNoUnfilledPlaceholder(x.UnaryExpr.GetChild())
	case *planpb.Expr_BinaryArithExpr:
		s.assertNoUnfilledPlaceholder(x.BinaryArithExpr.GetLeft())
		s.assertNoUnfilledPlaceholder(x.BinaryArithExpr.GetRight())
	}
}

// TestUnaryNotWithTemplate regression-tests issue #49141: templated
// expressions wrapped by unary NOT were not propagating IsTemplate to
// the outer Expr, so FillExpressionValue short-circuited and left
// placeholders unfilled (causing VAL_NOT_SET errors or silent wrong results).
func (s *FillExpressionValueSuite) TestUnaryNotWithTemplate() {
	schemaH := newTestSchemaHelper(s.T())

	cases := []struct {
		name      string
		templExpr string
		values    map[string]*schemapb.TemplateValue
	}{
		{
			"not wrapping templated term expr",
			`not (Int64Field in {vals})`,
			map[string]*schemapb.TemplateValue{
				"vals": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{10, 20, 99})),
			},
		},
		{
			"not wrapping templated unary range expr",
			`not (Int64Field > {x})`,
			map[string]*schemapb.TemplateValue{
				"x": generateTemplateValue(schemapb.DataType_Int64, int64(15)),
			},
		},
		{
			"not wrapping compound expr with template",
			`not ((Int64Field > {x}) and (StringField == "user"))`,
			map[string]*schemapb.TemplateValue{
				"x": generateTemplateValue(schemapb.DataType_Int64, int64(15)),
			},
		},
		{
			"not wrapping templated json contains",
			`not (json_contains_any(JSONField["nums"], {vals}))`,
			map[string]*schemapb.TemplateValue{
				"vals": generateTemplateValue(schemapb.DataType_Array,
					generateTemplateArrayValue(schemapb.DataType_Int64, []int64{2, 9})),
			},
		},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			expr, err := ParseExpr(schemaH, c.templExpr, c.values)
			s.NoError(err, c.templExpr)
			s.NotNil(expr)
			s.assertNoUnfilledPlaceholder(expr)
		})
	}
}
