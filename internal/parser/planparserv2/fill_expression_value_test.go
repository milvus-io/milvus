package planparserv2

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
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

func (s *FillExpressionValueSuite) jsonMarshal(v interface{}) []byte {
	r, err := json.Marshal(v)
	s.NoError(err)
	return r
}

func (s *FillExpressionValueSuite) TestTermExpr() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field in {age}`, map[string]*schemapb.TemplateValue{
				"age": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
				}),
			}},
			{`FloatField in {age}`, map[string]*schemapb.TemplateValue{
				"age": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Float, 1.1),
					generateExpressionFieldData(schemapb.DataType_Float, 2.2),
					generateExpressionFieldData(schemapb.DataType_Float, 3.3),
					generateExpressionFieldData(schemapb.DataType_Float, 4.4),
				}),
			}},
			{`A in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Float, 2.2),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Bool, false),
				}),
			}},
			{`ArrayField in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
					}),
					generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(5)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(6)),
					}),
					generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData(schemapb.DataType_Int64, int64(7)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(8)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(9)),
					}),
				}),
			}},
			{`ArrayField[0] in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
				}),
			}},
			{`Int64Field in {empty_list}`, map[string]*schemapb.TemplateValue{
				"empty_list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.NoError(err)
			s.NotNil(plan)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field in {age}`, map[string]*schemapb.TemplateValue{
				"age": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_String, "def"),
				}),
			}},
			{`StringField in {list}`, map[string]*schemapb.TemplateValue{
				"list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Float, 2.2),
					generateExpressionFieldData(schemapb.DataType_Bool, false),
				}),
			}},
			{"ArrayField[0] in {list}", map[string]*schemapb.TemplateValue{
				"list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Float, 3.2),
				}),
			}},
			{"Int64Field not in {not_list}", map[string]*schemapb.TemplateValue{
				"not_list": generateExpressionFieldData(schemapb.DataType_Int64, int64(33)),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)
			s.Error(err)
			s.Nil(plan)
			fmt.Println(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestUnaryRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field == 10`, nil},
			{`Int64Field > {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
			}},
			{`FloatField < {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Float, float64(12.3)),
			}},
			{`DoubleField != {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
			}},
			{`ArrayField[0] >= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
			}},
			{`BoolField == {bool}`, map[string]*schemapb.TemplateValue{
				"bool": generateExpressionFieldData(schemapb.DataType_Bool, false),
			}},
			{`{str} != StringField`, map[string]*schemapb.TemplateValue{
				"str": generateExpressionFieldData(schemapb.DataType_String, "abc"),
			}},
			{`{target} > Int64Field`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field == 10.5`, nil},
			{`Int64Field > {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Double, 11.2),
			}},
			{`FloatField < {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_String, "abc"),
			}},
			{`DoubleField != {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Bool, false),
			}},
			{`ArrayField[0] >= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
			}},
			{`BoolField == {bool}`, map[string]*schemapb.TemplateValue{
				"bool": generateExpressionFieldData(schemapb.DataType_String, "abc"),
			}},
			{`{str} != StringField`, map[string]*schemapb.TemplateValue{
				"str": generateExpressionFieldData(schemapb.DataType_Int64, int64(5)),
			}},
			{`{int} != StringField`, map[string]*schemapb.TemplateValue{
				"int": generateExpressionFieldData(schemapb.DataType_Int64, int64(5)),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`10 < Int64Field < 20`, nil},
			{`{max} > Int64Field > {min}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`{min} <= FloatField <= {max}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Float, float64(11)),
				"max": generateExpressionFieldData(schemapb.DataType_Float, float64(22)),
			}},
			{`{min} < DoubleField < {max}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Double, float64(11)),
				"max": generateExpressionFieldData(schemapb.DataType_Double, float64(22)),
			}},
			{`{max} >= ArrayField[0] >= {min}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} > Int64Field >= 10`, map[string]*schemapb.TemplateValue{
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`30 >= Int64Field > {min}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
			}},
			{`10 < Int64Field <= {max}`, map[string]*schemapb.TemplateValue{
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`{min} <= Int64Field < 20`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`10 < Int64Field < 20.5`, nil},
			{`{max} > Int64Field > {min}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
				"max": generateExpressionFieldData(schemapb.DataType_Double, 22.5),
			}},
			{`{min} <= FloatField <= {max}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_String, "abc"),
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(11)),
			}},
			{`{min} < DoubleField < {max}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Int64, int64(33)),
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} >= ArrayField[0] >= {min}`, map[string]*schemapb.TemplateValue{
				"min": generateExpressionFieldData(schemapb.DataType_Double, 11.5),
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} >= Int64Field >= {min}`, map[string]*schemapb.TemplateValue{
				"max": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
			{`{max} > Int64Field`, map[string]*schemapb.TemplateValue{
				"max": generateExpressionFieldData(schemapb.DataType_Bool, false),
			}},
			{`{$meta} > Int64Field`, map[string]*schemapb.TemplateValue{
				"$meta": generateExpressionFieldData(schemapb.DataType_Int64, int64(22)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryArithOpEvalRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field + 5.5 == 10.5`, nil},
			{`Int64Field - {offset} >= {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
				"target": generateExpressionFieldData(schemapb.DataType_Double, 11.5),
			}},
			{`Int64Field * 3.5 <= {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Double, 11.5),
			}},
			{`Int64Field / {offset} > 11.5`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
			}},
			{`ArrayField[0] % {offset} < 11`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field + 6 == 12.5`, nil},
			{`Int64Field - {offset} == {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
				"target": generateExpressionFieldData(schemapb.DataType_Double, 13.5),
			}},
			{`Int64Field * 6 == {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Double, 13.5),
			}},
			{`Int64Field / {offset} == 11.5`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Int64, int64(6)),
			}},
			{`Int64Field % {offset} < 11`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
			}},
			{`Int64Field + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"target": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
			}},
			{`Int64Field + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_String, "abc"),
				"target": generateExpressionFieldData(schemapb.DataType_Int64, int64(15)),
			}},
			{`Int64Field + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Int64, int64(15)),
				"target": generateExpressionFieldData(schemapb.DataType_String, "def"),
			}},
			{`ArrayField + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Double, 3.5),
				"target": generateExpressionFieldData(schemapb.DataType_Int64, int64(5)),
			}},
			{`ArrayField[0] + {offset} < {target}`, map[string]*schemapb.TemplateValue{
				"offset": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
				}),
				"target": generateExpressionFieldData(schemapb.DataType_Int64, int64(5)),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestJSONContainsExpression() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`json_contains(A, 5)`, nil},
			{`json_contains(A, {age})`, map[string]*schemapb.TemplateValue{
				"age": generateExpressionFieldData(schemapb.DataType_Int64, int64(18)),
			}},
			{`json_contains(A, {str})`, map[string]*schemapb.TemplateValue{
				"str": generateExpressionFieldData(schemapb.DataType_String, "abc"),
			}},
			{`json_contains(A, {bool})`, map[string]*schemapb.TemplateValue{
				"bool": generateExpressionFieldData(schemapb.DataType_Bool, false),
			}},
			{`json_contains_any(JSONField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Double, 2.2),
					generateExpressionFieldData(schemapb.DataType_Bool, false),
				}),
			}},
			{`json_contains_any(JSONField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
				}),
			}},
			{`json_contains_any(JSONField["A"], {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
				}),
			}},
			{`json_contains_all(JSONField["A"], {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Double, 2.2),
					generateExpressionFieldData(schemapb.DataType_Bool, false),
				}),
			}},
			{`json_contains_all(JSONField["A"], {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
					}),
					generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(5)),
						generateExpressionFieldData(schemapb.DataType_Int64, int64(6)),
					}),
				}),
			}},
			{`json_contains(ArrayField, {int})`, map[string]*schemapb.TemplateValue{
				"int": generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
			}},
			{`json_contains_any(ArrayField, {list})`, map[string]*schemapb.TemplateValue{
				"list": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(4)),
				}),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`json_contains(ArrayField[0], {str})`, map[string]*schemapb.TemplateValue{
				"str": generateExpressionFieldData(schemapb.DataType_String, "abc"),
			}},
			{`json_contains_any(JSONField, {not_array})`, map[string]*schemapb.TemplateValue{
				"not_array": generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
			}},
			{`json_contains_all(JSONField, {not_array})`, map[string]*schemapb.TemplateValue{
				"not_array": generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
			}},
			{`json_contains_all(JSONField, {not_array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
				}),
			}},
			{`json_contains_all(ArrayField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Double, 2.2),
					generateExpressionFieldData(schemapb.DataType_Bool, false),
				}),
			}},
			{`json_contains(ArrayField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(2)),
					generateExpressionFieldData(schemapb.DataType_Int64, int64(3)),
				}),
			}},
			{`json_contains_any(ArrayField, {array})`, map[string]*schemapb.TemplateValue{
				"array": generateExpressionFieldData(schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData(schemapb.DataType_Int64, int64(1)),
					generateExpressionFieldData(schemapb.DataType_String, "abc"),
					generateExpressionFieldData(schemapb.DataType_Double, 2.2),
					generateExpressionFieldData(schemapb.DataType_Bool, false),
				}),
			}},
		}

		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values)

			s.Error(err)
			s.Nil(plan)
		}
	})
}
