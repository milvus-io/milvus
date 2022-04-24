// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"fmt"
	"testing"

	ant_ast "github.com/antonmedv/expr/ast"
	ant_parser "github.com/antonmedv/expr/parser"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func newTestSchema() *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
	}

	for name, value := range schemapb.DataType_value {
		dataType := schemapb.DataType(value)
		if !typeutil.IsIntegerType(dataType) && !typeutil.IsFloatingType(dataType) && !typeutil.IsVectorType(dataType) {
			continue
		}
		newField := &schemapb.FieldSchema{
			FieldID: int64(100 + value), Name: name + "Field", IsPrimaryKey: false, Description: "", DataType: dataType,
		}
		fields = append(fields, newField)
	}

	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      true,
		Fields:      fields,
	}
}

func TestParseExpr_Naive(t *testing.T) {
	schemaPb := newTestSchema()
	schema, err := typeutil.CreateSchemaHelper(schemaPb)
	assert.Nil(t, err)

	t.Run("test UnaryNode", func(t *testing.T) {
		exprStrs := []string{
			"Int64Field > +1",
			"Int64Field > -1",
			"FloatField > +1.0",
			"FloatField > -1.0",
		}
		for _, exprStr := range exprStrs {
			exprProto, err := parseExpr(schema, exprStr)
			assert.Nil(t, err)
			str := proto.MarshalTextString(exprProto)
			println(str)
		}
	})

	t.Run("test UnaryNode invalid", func(t *testing.T) {
		exprStrs := []string{
			"Int64Field > +aa",
			"FloatField > -aa",
		}
		for _, exprStr := range exprStrs {
			exprProto, err := parseExpr(schema, exprStr)
			assert.Error(t, err)
			assert.Nil(t, exprProto)
		}
	})

	t.Run("test BinaryNode", func(t *testing.T) {
		exprStrs := []string{
			// "+"
			"FloatField > 1 + 2",
			"FloatField > 1 + 2.0",
			"FloatField > 1.0 + 2",
			"FloatField > 1.0 + 2.0",
			// "-"
			"FloatField > 1 - 2",
			"FloatField > 1 - 2.0",
			"FloatField > 1.0 - 2",
			"FloatField > 1.0 - 2.0",
			// "*"
			"FloatField > 1 * 2",
			"FloatField > 1 * 2.0",
			"FloatField > 1.0 * 2",
			"FloatField > 1.0 * 2.0",
			// "/"
			"FloatField > 1 / 2",
			"FloatField > 1 / 2.0",
			"FloatField > 1.0 / 2",
			"FloatField > 1.0 / 2.0",
			// "%"
			"FloatField > 1 % 2",
			// "**"
			"FloatField > 1 ** 2",
			"FloatField > 1 ** 2.0",
			"FloatField > 1.0 ** 2",
			"FloatField > 1.0 ** 2.0",
		}
		for _, exprStr := range exprStrs {
			exprProto, err := parseExpr(schema, exprStr)
			assert.Nil(t, err)
			str := proto.MarshalTextString(exprProto)
			println(str)
		}
	})

	t.Run("test BinaryNode invalid", func(t *testing.T) {
		exprStrs := []string{
			// "+"
			"FloatField > 1 + aa",
			"FloatField > aa + 2.0",
			// "-"
			"FloatField > 1 - aa",
			"FloatField > aa - 2.0",
			// "*"
			"FloatField > 1 * aa",
			"FloatField > aa * 2.0",
			// "/"
			"FloatField > 1 / 0",
			"FloatField > 1 / 0.0",
			"FloatField > 1.0 / 0",
			"FloatField > 1.0 / 0.0",
			"FloatField > 1 / aa",
			"FloatField > aa / 2.0",
			// "%"
			"FloatField > 1 % aa",
			"FloatField > 1 % 0",
			"FloatField > 1 % 0.0",
			// "**"
			"FloatField > 1 ** aa",
			"FloatField > aa ** 2.0",
		}
		for _, exprStr := range exprStrs {
			exprProto, err := parseExpr(schema, exprStr)
			assert.Error(t, err)
			assert.Nil(t, exprProto)
		}
	})

	t.Run("test BinaryArithOpNode", func(t *testing.T) {
		exprStrs := []string{
			// "+"
			"FloatField + 1.2 == 3",
			"Int64Field + 3 == 5",
			"1.2 + FloatField != 3",
			"3 + Int64Field != 5",
			// "-"
			"FloatField - 1.2 == 3",
			"Int64Field - 3 != 5",
			// "*"
			"FloatField * 1.2 == 3",
			"Int64Field * 3 == 5",
			"1.2 * FloatField != 3",
			"3 * Int64Field != 5",
			// "/"
			"FloatField / 1.2 == 3",
			"Int64Field / 3 != 5",
			// "%"
			"Int64Field % 7 == 5",
		}
		for _, exprStr := range exprStrs {
			exprProto, err := parseExpr(schema, exprStr)
			assert.Nil(t, err)
			str := proto.MarshalTextString(exprProto)
			println(str)
		}
	})

	t.Run("test BinaryArithOpNode invalid", func(t *testing.T) {
		exprStrs := []string{
			// "+"
			"FloatField + FloatField == 20",
			"Int64Field + Int64Field != 10",
			// "-"
			"FloatField - FloatField == 20.0",
			"Int64Field - Int64Field != 10",
			"10 - FloatField == 20",
			"20 - Int64Field != 10",
			// "*"
			"FloatField * FloatField == 20",
			"Int64Field * Int64Field != 10",
			// "/"
			"FloatField / FloatField == 20",
			"Int64Field / Int64Field != 10",
			"FloatField / 0 == 20",
			"Int64Field / 0 != 10",
			// "%"
			"Int64Field % Int64Field != 10",
			"FloatField % 0 == 20",
			"Int64Field % 0 != 10",
			"FloatField % 2.3 == 20",
		}
		for _, exprStr := range exprStrs {
			exprProto, err := parseExpr(schema, exprStr)
			assert.Error(t, err)
			assert.Nil(t, exprProto)
		}
	})
}

func TestParsePlanNode_Naive(t *testing.T) {
	exprStrs := []string{
		"not (Int64Field > 3)",
		"not (3 > Int64Field)",
		"Int64Field in [1, 2, 3]",
		"Int64Field < 3 and (Int64Field > 2 || Int64Field == 1)",
		"DoubleField in [1.0, 2, 3]",
		"DoubleField in [1.0, 2, 3] && Int64Field < 3 or Int64Field > 2",
	}

	schema := newTestSchema()
	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	// Note: use pointer to string to represent nullable string
	// TODO: change it to better solution
	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "FloatVectorField", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}

}

func TestExternalParser(t *testing.T) {
	ast, err := ant_parser.Parse("!(1 < a < 2 or b in [1, 2, 3]) or (c < 3 and b > 5)")
	// NOTE: probe ast here via IDE
	assert.Nil(t, err)

	println(ast.Node.Location().Column)
}

func TestExprPlan_Str(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	// without filter
	planProto, err := createQueryPlan(schema, "", "fakevec", queryInfo)
	assert.Nil(t, err)
	dbgStr := proto.MarshalTextString(planProto)
	println(dbgStr)

	exprStrs := []string{
		"age >= 420000 && age < 420010", // range
		"age == 420000 || age == 420001 || age == 420002 || age == 420003 || age == 420004", // term
		"age not in [1, 2, 3]",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}

func TestExprMultiRange_Str(t *testing.T) {
	exprStrs := []string{
		"3 < FloatN < 4.0",
		"3 < age1 < 5 < age2 < 7 < FloatN < 9.0 < FloatN2",
		"1 + 1 < age1 < 2 * 2",
		"1 - 1 < age1 < 3 / 2",
		"1.0 - 1 < FloatN < 3 / 2",
		"2 ** 10 > FloatN >= 7 % 4",
		"0.1 ** 2 < FloatN < 2 ** 0.1",
		"0.1 ** 1.1 < FloatN < 3.1 / 4",
		"4.1 / 3 < FloatN < 0.0 / 5.0",
		"BoolN1 == True",
		"True == BoolN1",
		"BoolN1 == False",
	}
	invalidExprs := []string{
		"BoolN1 == 1",
		"BoolN1 == 0",
		"BoolN1 > 0",
	}

	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age1", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "age2", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "FloatN", DataType: schemapb.DataType_Float},
		{FieldID: 104, Name: "FloatN2", DataType: schemapb.DataType_Float},
		{FieldID: 105, Name: "BoolN1", DataType: schemapb.DataType_Bool},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
	for offset, exprStr := range invalidExprs {
		fmt.Printf("invalid case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Error(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}

func TestExprFieldCompare_Str(t *testing.T) {
	exprStrs := []string{
		"age1 < age2",
		"3 < age1 <= age2 < 4",
	}

	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age1", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "age2", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "FloatN", DataType: schemapb.DataType_Float},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}

func TestExprBinaryArithOp_Str(t *testing.T) {
	exprStrs := []string{
		// Basic arithmetic
		"(age1 + 5) == 2",
		// Float data type
		"(FloatN - 5.2) == 0",
		// Other operators
		"(age1 - 5) == 1",
		"(age1 * 5) == 6",
		"(age1 / 5) == 1",
		"(age1 % 5) == 0",
		// Allow for commutative property for + and *
		"(6 + age1) != 2",
		"(age1 * 4) != 9",
		"(5 * FloatN) != 0",
		"(9 * FloatN) != 0",
	}

	unsupportedExprStrs := []string{
		// Comparison operators except for "==" and "!=" are unsupported
		"(age1 + 2) > 4",
		"(age1 + 2) >= 4",
		"(age1 + 2) < 4",
		"(age1 + 2) <= 4",
		// Functional nodes at the right of the comparison are not allowed
		"0 == (age1 + 3)",
		// Field as the right operand for -, /, and % operators are not supported
		"(10 - age1) == 0",
		"(20 / age1) == 0",
		"(30 % age1) == 0",
		// Modulo is not supported in the parser but the engine can handle it since fmod is used
		"(FloatN % 2.1) == 0",
		// Different data types are not supported
		"(age1 + 20.16) == 35.16",
		// Left operand of the function must be an identifier
		"(10.5 / floatN) == 5.75",
	}

	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "fakevec", DataType: schemapb.DataType_FloatVector},
		{FieldID: 101, Name: "age1", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "FloatN", DataType: schemapb.DataType_Float},
	}

	schema := &schemapb.CollectionSchema{
		Name:        "default-collection",
		Description: "",
		AutoID:      true,
		Fields:      fields,
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	for offset, exprStr := range exprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Nil(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
	for offset, exprStr := range unsupportedExprStrs {
		fmt.Printf("case %d: %s\n", offset, exprStr)
		planProto, err := createQueryPlan(schema, exprStr, "fakevec", queryInfo)
		assert.Error(t, err)
		dbgStr := proto.MarshalTextString(planProto)
		println(dbgStr)
	}
}

func TestPlanParseAPIs(t *testing.T) {
	t.Run("get compare op type", func(t *testing.T) {
		var op planpb.OpType
		var reverse bool

		reverse = false
		op = getCompareOpType(">", reverse)
		assert.Equal(t, planpb.OpType_GreaterThan, op)
		op = getCompareOpType(">=", reverse)
		assert.Equal(t, planpb.OpType_GreaterEqual, op)
		op = getCompareOpType("<", reverse)
		assert.Equal(t, planpb.OpType_LessThan, op)
		op = getCompareOpType("<=", reverse)
		assert.Equal(t, planpb.OpType_LessEqual, op)
		op = getCompareOpType("==", reverse)
		assert.Equal(t, planpb.OpType_Equal, op)
		op = getCompareOpType("!=", reverse)
		assert.Equal(t, planpb.OpType_NotEqual, op)
		op = getCompareOpType("*", reverse)
		assert.Equal(t, planpb.OpType_Invalid, op)

		reverse = true
		op = getCompareOpType(">", reverse)
		assert.Equal(t, planpb.OpType_LessThan, op)
		op = getCompareOpType(">=", reverse)
		assert.Equal(t, planpb.OpType_LessEqual, op)
		op = getCompareOpType("<", reverse)
		assert.Equal(t, planpb.OpType_GreaterThan, op)
		op = getCompareOpType("<=", reverse)
		assert.Equal(t, planpb.OpType_GreaterEqual, op)
		op = getCompareOpType("==", reverse)
		assert.Equal(t, planpb.OpType_Equal, op)
		op = getCompareOpType("!=", reverse)
		assert.Equal(t, planpb.OpType_NotEqual, op)
		op = getCompareOpType("*", reverse)
		assert.Equal(t, planpb.OpType_Invalid, op)
	})

	t.Run("parse bool node", func(t *testing.T) {
		var nodeRaw1, nodeRaw2, nodeRaw3, nodeRaw4 ant_ast.Node
		nodeRaw1 = &ant_ast.IdentifierNode{
			Value: "True",
		}
		boolNode1 := parseBoolNode(&nodeRaw1)
		assert.Equal(t, boolNode1.Value, true)

		nodeRaw2 = &ant_ast.IdentifierNode{
			Value: "False",
		}
		boolNode2 := parseBoolNode(&nodeRaw2)
		assert.Equal(t, boolNode2.Value, false)

		nodeRaw3 = &ant_ast.IdentifierNode{
			Value: "abcd",
		}
		assert.Nil(t, parseBoolNode(&nodeRaw3))

		nodeRaw4 = &ant_ast.BoolNode{
			Value: true,
		}
		assert.Nil(t, parseBoolNode(&nodeRaw4))
	})
}
