package planparserv2

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

type FillExpressionValueSuite struct {
	suite.Suite
}

func (s *FillExpressionValueSuite) SetupTest() {

}

func TestFillExpressionValue(t *testing.T) {
	suite.Run(t, new(FillExpressionValueSuite))
}

type testcase struct {
	expr   string
	values []*schemapb.FieldData
}

func (s *FillExpressionValueSuite) jsonMarshal(v interface{}) []byte {
	r, err := json.Marshal(v)
	s.NoError(err)
	return r
}

func (s *FillExpressionValueSuite) TestTermExpr() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field in {age}`, []*schemapb.FieldData{
				generateExpressionFieldData("age", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3), int64(4)}),
				}),
			}},
			{`FloatField in {age}`, []*schemapb.FieldData{
				generateExpressionFieldData("age", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Double, []interface{}{1.1, 2.2, 3.3, 4.4}),
				}),
			}},
			{`A in {list}`, []*schemapb.FieldData{
				generateExpressionFieldData("list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(int64(1)), s.jsonMarshal("abc"), s.jsonMarshal(float32(2.2)), s.jsonMarshal(false)}),
				}),
			}},
			{`ArrayField in {list}`, []*schemapb.FieldData{
				generateExpressionFieldData("list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3)}),
						generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(3), int64(4), int64(5)}),
					}),
				}),
			}},
			{`ArrayField[0] in {list}`, []*schemapb.FieldData{
				generateExpressionFieldData("list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int32, []interface{}{int32(1), int32(2), int32(3), int32(4), int32(5)}),
				}),
			}},
			{`Int64Field in {empty_list}`, []*schemapb.FieldData{
				generateExpressionFieldData("empty_list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int32, []interface{}{}),
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
			}, c.values...)

			s.NoError(err)
			s.NotNil(plan)
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field in {age}`, []*schemapb.FieldData{
				generateExpressionFieldData("age", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_String, []interface{}{"abc", "def"}),
				}),
			}},
			{`StringField in {list}`, []*schemapb.FieldData{
				generateExpressionFieldData("list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(int8(1)), s.jsonMarshal("abc"), s.jsonMarshal(float32(2.2)), s.jsonMarshal(false)}),
				}),
			}},
			{"ArrayField[0] in {list}", []*schemapb.FieldData{
				generateExpressionFieldData("list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(int16(1)), s.jsonMarshal("abc"), s.jsonMarshal(int32(3))}),
				}),
			}},
			{"Int64Field not in {not_list}", []*schemapb.FieldData{
				generateExpressionFieldData("not_list", schemapb.DataType_String, []interface{}{"abc"}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)
			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestUnaryRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field == 10`, nil},
			{`Int64Field > {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Int64, []interface{}{int64(11)}),
			}},
			{`FloatField < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Float, []interface{}{float32(12.3)}),
			}},
			{`DoubleField != {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{3.5}),
			}},
			{`ArrayField[0] >= {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Int64, []interface{}{int64(3)}),
			}},
			{`BoolField == {bool}`, []*schemapb.FieldData{
				generateExpressionFieldData("bool", schemapb.DataType_Bool, []interface{}{false}),
			}},
			{`{str} != StringField`, []*schemapb.FieldData{
				generateExpressionFieldData("str", schemapb.DataType_String, []interface{}{"abc"}),
			}},
			{`{target} > Int64Field`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Int64, []interface{}{int64(11)}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field == 10.5`, nil},
			{`Int64Field > {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{11.2}),
			}},
			{`FloatField < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_String, []interface{}{"abc"}),
			}},
			{`DoubleField != {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Bool, []interface{}{false}),
			}},
			{`ArrayField[0] >= {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{3.5}),
			}},
			{`BoolField == {bool}`, []*schemapb.FieldData{
				generateExpressionFieldData("bool", schemapb.DataType_String, []interface{}{"abc"}),
			}},
			{`{str} != StringField`, []*schemapb.FieldData{
				generateExpressionFieldData("str", schemapb.DataType_Int64, []interface{}{int64(5)}),
			}},
			{`{str} != StringField`, []*schemapb.FieldData{
				generateExpressionFieldData("int", schemapb.DataType_Int64, []interface{}{int64(5)}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)

			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`10 < Int64Field < 20`, nil},
			{`{max} > Int64Field > {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(11)}),
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{min} <= FloatField <= {max}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Float, []interface{}{float32(11)}),
				generateExpressionFieldData("max", schemapb.DataType_Float, []interface{}{float32(22)}),
			}},
			{`{min} < DoubleField < {max}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Double, []interface{}{float64(11)}),
				generateExpressionFieldData("max", schemapb.DataType_Double, []interface{}{float64(22)}),
			}},
			{`{max} >= ArrayField[0] >= {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(11)}),
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{max} > Int64Field >= 10`, []*schemapb.FieldData{
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`30 >= Int64Field > {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(11)}),
			}},
			{`10 < Int64Field <= {max}`, []*schemapb.FieldData{
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{min} <= Int64Field < 20`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(11)}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`10 < Int64Field < 20.5`, nil},
			{`{max} > Int64Field > {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(11)}),
				generateExpressionFieldData("max", schemapb.DataType_Double, []interface{}{22.5}),
			}},
			{`{min} <= FloatField <= {max}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_String, []interface{}{"abc"}),
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(11)}),
			}},
			{`{min} < DoubleField < {max}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(33)}),
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{max} >= ArrayField[0] >= {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Double, []interface{}{11.5}),
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{max} >= Int64Field >= {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("max", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{max} >= Int64Field >= {min}`, []*schemapb.FieldData{
				generateExpressionFieldData("min", schemapb.DataType_Int64, []interface{}{int64(22)}),
			}},
			{`{max} > Int64Field`, []*schemapb.FieldData{
				generateExpressionFieldData("max", schemapb.DataType_Bool, []interface{}{false}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)

			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestBinaryArithOpEvalRange() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`Int64Field + 5.5 == 10.5`, nil},
			{`Int64Field - {offset}  >= {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Double, []interface{}{3.5}),
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{11.5}),
			}},
			{`Int64Field * 3.5  <= {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{11.5}),
			}},
			{`Int64Field / {offset}  > 11.5`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Double, []interface{}{3.5}),
			}},
			{`ArrayField[0] % {offset}  < 11`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Int64, []interface{}{int64(3)}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`Int64Field + 6 == 12.5`, nil},
			{`Int64Field - {offset}  == {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Int64, []interface{}{int64(4)}),
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{13.5}),
			}},
			{`Int64Field * 6  == {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{13.5}),
			}},
			{`Int64Field / {offset}  == 11.5`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Int64, []interface{}{int64(6)}),
			}},
			{`Int64Field % {offset}  < 11`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Double, []interface{}{3.5}),
			}},
			{`Int64Field + {offset}  < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("target", schemapb.DataType_Double, []interface{}{3.5}),
			}},
			{`Int64Field + {offset}  < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Double, []interface{}{3.5}),
			}},
			{`Int64Field + {offset}  < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_String, []interface{}{"abc"}),
				generateExpressionFieldData("target", schemapb.DataType_Int64, []interface{}{int64(15)}),
			}},
			{`Int64Field + {offset}  < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Int64, []interface{}{int64(15)}),
				generateExpressionFieldData("target", schemapb.DataType_String, []interface{}{"def"}),
			}},
			{`ArrayField + {offset}  < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Double, []interface{}{3.5}),
				generateExpressionFieldData("target", schemapb.DataType_Int64, []interface{}{int64(5)}),
			}},
			{`ArrayField[0] + {offset}  < {target}`, []*schemapb.FieldData{
				generateExpressionFieldData("offset", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3)}),
				}),
				generateExpressionFieldData("target", schemapb.DataType_Int64, []interface{}{int64(5)}),
			}},
		}
		schemaH := newTestSchemaHelper(s.T())

		for _, c := range testcases {
			plan, err := CreateSearchPlan(schemaH, c.expr, "FloatVectorField", &planpb.QueryInfo{
				Topk:         0,
				MetricType:   "",
				SearchParams: "",
				RoundDecimal: 0,
			}, c.values...)

			s.Error(err)
			s.Nil(plan)
		}
	})
}

func (s *FillExpressionValueSuite) TestJSONContainsExpression() {
	s.Run("normal case", func() {
		testcases := []testcase{
			{`json_contains(A, 5)`, nil},
			{`json_contains(A, {age})`, []*schemapb.FieldData{
				generateExpressionFieldData("age", schemapb.DataType_Int64, []interface{}{int64(18)}),
			}},
			{`json_contains(A, {str})`, []*schemapb.FieldData{
				generateExpressionFieldData("str", schemapb.DataType_String, []interface{}{"abc"}),
			}},
			{`json_contains(A, {bool})`, []*schemapb.FieldData{
				generateExpressionFieldData("bool", schemapb.DataType_Bool, []interface{}{false}),
			}},
			{`json_contains_any(JSONField, {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(1), s.jsonMarshal("abc"), s.jsonMarshal("2.2"), s.jsonMarshal(false)}),
				}),
			}},
			{`json_contains_any(JSONField, {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3), int64(4)}),
				}),
			}},
			{`json_contains_any(JSONField["A"], {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3), int64(4)}),
				}),
			}},
			{`json_contains_all(JSONField["A"], {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(1), s.jsonMarshal("abc"), s.jsonMarshal("2.2"), s.jsonMarshal(false)}),
				}),
			}},
			{`json_contains_all(JSONField["A"], {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Array, []interface{}{
						generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3)}),
						generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(4), int64(5), int64(6)}),
					}),
				}),
			}},
			{`json_contains(ArrayField, {int})`, []*schemapb.FieldData{
				generateExpressionFieldData("int", schemapb.DataType_Int64, []interface{}{int64(1)}),
			}},
			{`json_contains_any(ArrayField, {list})`, []*schemapb.FieldData{
				generateExpressionFieldData("list", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3), int64(4)}),
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
			}, c.values...)

			s.NoError(err)
			s.NotNil(plan)
			s.NotNil(plan.GetVectorAnns())
			s.NotNil(plan.GetVectorAnns().GetPredicates())
		}
	})

	s.Run("failed case", func() {
		testcases := []testcase{
			{`json_contains(ArrayField[0], {str})`, []*schemapb.FieldData{
				generateExpressionFieldData("str", schemapb.DataType_String, []interface{}{"abc"}),
			}},
			{`json_contains_any(JSONField, {not_array})`, []*schemapb.FieldData{
				generateExpressionFieldData("not_array", schemapb.DataType_Int64, []interface{}{int64(1)}),
			}},
			{`json_contains_all(JSONField, {not_array})`, []*schemapb.FieldData{
				generateExpressionFieldData("not_array", schemapb.DataType_Int64, []interface{}{int64(1)}),
			}},
			{`json_contains_all(JSONField, {not_array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3)}),
				}),
			}},
			{`json_contains_all(ArrayField, {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(1), s.jsonMarshal("abc"), s.jsonMarshal("2.2"), s.jsonMarshal(false)}),
				}),
			}},
			{`json_contains(ArrayField, {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_Int64, []interface{}{int64(1), int64(2), int64(3)}),
				}),
			}},
			{`json_contains_any(ArrayField, {array})`, []*schemapb.FieldData{
				generateExpressionFieldData("array", schemapb.DataType_Array, []interface{}{
					generateExpressionFieldData("", schemapb.DataType_JSON, []interface{}{s.jsonMarshal(1), s.jsonMarshal("abc"), s.jsonMarshal("2.2"), s.jsonMarshal(false)}),
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
			}, c.values...)

			s.Error(err)
			s.Nil(plan)
		}
	})
}
