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

func Test_getMatchOperator(t *testing.T) {
	var opStr string

	opStr = "startsWith"
	assert.Equal(t, planpb.OpType_PrefixMatch, getMatchOperator(opStr))

	opStr = "endsWith"
	assert.Equal(t, planpb.OpType_PostfixMatch, getMatchOperator(opStr))
}

func Test_handleMatchExpr(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "str",
				FieldID:  100,
				DataType: schemapb.DataType_VarChar,
			},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	pc := &parserContext{
		schema: helper,
	}

	t.Run("normal case", func(t *testing.T) {
		node := &ant_ast.BinaryNode{
			Operator: "startsWith",
			Left:     &ant_ast.IdentifierNode{Value: "str"},
			Right:    &ant_ast.StringNode{Value: "prefix"},
		}
		expr, err := pc.handleMatchExpr(node)
		assert.NoError(t, err)
		matchExpr, ok := expr.GetExpr().(*planpb.Expr_MatchExpr)
		assert.True(t, ok)
		assert.Equal(t, planpb.OpType_PrefixMatch, matchExpr.MatchExpr.GetOp())
		assert.Equal(t, "prefix", matchExpr.MatchExpr.GetValue().GetStringVal())
		assert.Equal(t, int64(100), matchExpr.MatchExpr.GetColumnInfo().GetFieldId())
		assert.Equal(t, schemapb.DataType_VarChar, matchExpr.MatchExpr.GetColumnInfo().GetDataType())
	})

	t.Run("invalid operator", func(t *testing.T) {
		node := &ant_ast.BinaryNode{
			Operator: "invalid",
		}
		_, err := pc.handleMatchExpr(node)
		assert.Error(t, err)
	})

	t.Run("left is not identifier", func(t *testing.T) {
		node := &ant_ast.BinaryNode{
			Operator: "startsWith",
			Left:     &ant_ast.StringNode{Value: "prefix"},
		}
		_, err := pc.handleMatchExpr(node)
		assert.Error(t, err)
	})

	t.Run("left identifier not in schema", func(t *testing.T) {
		node := &ant_ast.BinaryNode{
			Operator: "startsWith",
			Left:     &ant_ast.IdentifierNode{Value: "not_in_schema"},
			Right:    &ant_ast.StringNode{Value: "prefix"},
		}
		_, err := pc.handleMatchExpr(node)
		assert.Error(t, err)
	})

	t.Run("right node is not of string node", func(t *testing.T) {
		node := &ant_ast.BinaryNode{
			Operator: "startsWith",
			Left:     &ant_ast.IdentifierNode{Value: "not_in_schema"},
			Right:    &ant_ast.IntegerNode{Value: 1},
		}
		_, err := pc.handleMatchExpr(node)
		assert.Error(t, err)
	})
}

func Test_handleBinaryExpr_matchExpr(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "str",
				FieldID:  100,
				DataType: schemapb.DataType_VarChar,
			},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	pc := &parserContext{
		schema: helper,
	}

	node := &ant_ast.BinaryNode{
		Operator: "startsWith",
		Left:     &ant_ast.IdentifierNode{Value: "str"},
		Right:    &ant_ast.StringNode{Value: "prefix"},
	}
	expr, err := pc.handleBinaryExpr(node)
	assert.NoError(t, err)
	matchExpr, ok := expr.GetExpr().(*planpb.Expr_MatchExpr)
	assert.True(t, ok)
	assert.Equal(t, planpb.OpType_PrefixMatch, matchExpr.MatchExpr.GetOp())
	assert.Equal(t, "prefix", matchExpr.MatchExpr.GetValue().GetStringVal())
	assert.Equal(t, int64(100), matchExpr.MatchExpr.GetColumnInfo().GetFieldId())
	assert.Equal(t, schemapb.DataType_VarChar, matchExpr.MatchExpr.GetColumnInfo().GetDataType())
}

func Test_showMatchPlan(t *testing.T) {
	var exprStr string
	var err error
	var expr *planpb.Expr
	var serializedExpr string

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "str",
				FieldID:  100,
				DataType: schemapb.DataType_VarChar,
			},
			{
				Name:     "int64",
				FieldID:  101,
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	// only prefix match.
	exprStr = `str startsWith "prefix"`
	fmt.Println(exprStr)
	expr, err = parseExpr(helper, exprStr)
	assert.NoError(t, err)
	serializedExpr = proto.MarshalTextString(expr)
	fmt.Println(serializedExpr)

	// prefix match && postfix match.
	exprStr = `str startsWith "prefix" && str endsWith "postfix"`
	fmt.Println(exprStr)
	expr, err = parseExpr(helper, exprStr)
	assert.NoError(t, err)
	serializedExpr = proto.MarshalTextString(expr)
	fmt.Println(serializedExpr)

	// !((prefix match && postfix match) || in)
	exprStr = `!((str startsWith "prefix" && str endsWith "postfix") || (int64 in [1, 2, 3]))`
	fmt.Println(exprStr)
	expr, err = parseExpr(helper, exprStr)
	assert.NoError(t, err)
	serializedExpr = proto.MarshalTextString(expr)
	fmt.Println(serializedExpr)
}
