// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"fmt"
	"math"
	"strings"

	ant_ast "github.com/antonmedv/expr/ast"
	ant_parser "github.com/antonmedv/expr/parser"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type ParserContext struct {
	schema *typeutil.SchemaHelper
}

type optimizer struct {
	err error
}

func (*optimizer) Enter(*ant_ast.Node) {}

func (optimizer *optimizer) Exit(node *ant_ast.Node) {
	patch := func(newNode ant_ast.Node) {
		ant_ast.Patch(node, newNode)
	}

	switch node := (*node).(type) {
	case *ant_ast.UnaryNode:
		switch node.Operator {
		case "-":
			if i, ok := node.Node.(*ant_ast.IntegerNode); ok {
				patch(&ant_ast.IntegerNode{Value: -i.Value})
			} else if i, ok := node.Node.(*ant_ast.FloatNode); ok {
				patch(&ant_ast.FloatNode{Value: -i.Value})
			} else {
				optimizer.err = fmt.Errorf("can only make a number negative")
				return
			}
		case "+":
			if i, ok := node.Node.(*ant_ast.IntegerNode); ok {
				patch(&ant_ast.IntegerNode{Value: i.Value})
			} else if i, ok := node.Node.(*ant_ast.FloatNode); ok {
				patch(&ant_ast.FloatNode{Value: i.Value})
			} else {
				optimizer.err = fmt.Errorf("can only make a number positive")
				return
			}
		}

	case *ant_ast.BinaryNode:
		floatNodeLeft, leftFloat := node.Left.(*ant_ast.FloatNode)
		integerNodeLeft, leftInteger := node.Left.(*ant_ast.IntegerNode)
		floatNodeRight, rightFloat := node.Right.(*ant_ast.FloatNode)
		integerNodeRight, rightInteger := node.Right.(*ant_ast.IntegerNode)

		switch node.Operator {
		case "+":
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value + floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value + float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) + floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value + integerNodeRight.Value})
			} else {
				optimizer.err = fmt.Errorf("can only add two number")
				return
			}
		case "-":
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value - floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value - float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) - floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value - integerNodeRight.Value})
			} else {
				optimizer.err = fmt.Errorf("can only subtract two number")
				return
			}
		case "*":
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value * floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value * float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) * floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value * integerNodeRight.Value})
			} else {
				optimizer.err = fmt.Errorf("can only multiply two number")
				return
			}
		case "/":
			if leftFloat && rightFloat {
				if floatNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("number divide by zero")
					return
				}
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value / floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("number divide by zero")
					return
				}
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value / float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				if floatNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("number divide by zero")
					return
				}
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) / floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("number divide by zero")
					return
				}
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value / integerNodeRight.Value})
			} else {
				optimizer.err = fmt.Errorf("can only divide two number")
				return
			}
		case "%":
			if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value % integerNodeRight.Value})
			} else {
				optimizer.err = fmt.Errorf("can only modulus two integer")
				return
			}
		case "**":
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: math.Pow(floatNodeLeft.Value, floatNodeRight.Value)})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: math.Pow(floatNodeLeft.Value, float64(integerNodeRight.Value))})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: math.Pow(float64(integerNodeLeft.Value), floatNodeRight.Value)})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: int(math.Pow(float64(integerNodeLeft.Value), float64(integerNodeRight.Value)))})
			} else {
				optimizer.err = fmt.Errorf("can only pow two number")
				return
			}
		}
	}
}

func parseExpr(schema *typeutil.SchemaHelper, exprStr string) (*planpb.Expr, error) {
	if exprStr == "" {
		return nil, nil
	}
	ast, err := ant_parser.Parse(exprStr)
	if err != nil {
		return nil, err
	}

	optimizer := &optimizer{}
	ant_ast.Walk(&ast.Node, optimizer)
	if optimizer.err != nil {
		return nil, optimizer.err
	}

	context := ParserContext{schema}
	expr, err := context.handleExpr(&ast.Node)
	if err != nil {
		return nil, err
	}

	return expr, nil
}

func (context *ParserContext) createColumnInfo(field *schemapb.FieldSchema) *planpb.ColumnInfo {
	return &planpb.ColumnInfo{
		FieldId:      field.FieldID,
		DataType:     field.DataType,
		IsPrimaryKey: field.IsPrimaryKey,
	}
}

func isSameOrder(opStr1, opStr2 string) bool {
	isLess1 := (opStr1 == "<") || (opStr1 == "<=")
	isLess2 := (opStr2 == "<") || (opStr2 == "<=")
	return isLess1 == isLess2
}

func getCompareOpType(opStr string, reverse bool) planpb.OpType {
	type OpType = planpb.OpType
	var op planpb.OpType

	if !reverse {
		switch opStr {
		case "<":
			op = planpb.OpType_LessThan
		case ">":
			op = planpb.OpType_GreaterThan
		case "<=":
			op = planpb.OpType_LessEqual
		case ">=":
			op = planpb.OpType_GreaterEqual
		case "==":
			op = planpb.OpType_Equal
		case "!=":
			op = planpb.OpType_NotEqual
		default:
			op = planpb.OpType_Invalid
		}
	} else {
		switch opStr {
		case ">":
			op = planpb.OpType_LessThan
		case "<":
			op = planpb.OpType_GreaterThan
		case ">=":
			op = planpb.OpType_LessEqual
		case "<=":
			op = planpb.OpType_GreaterEqual
		case "==":
			op = planpb.OpType_Equal
		case "!=":
			op = planpb.OpType_NotEqual
		default:
			op = planpb.OpType_Invalid
		}
	}
	return op
}

func getLogicalOpType(opStr string) planpb.BinaryExpr_BinaryOp {
	switch opStr {
	case "&&", "and":
		return planpb.BinaryExpr_LogicalAnd
	case "||", "or":
		return planpb.BinaryExpr_LogicalOr
	default:
		return planpb.BinaryExpr_Invalid
	}
}

func parseBoolNode(nodeRaw *ant_ast.Node) *ant_ast.BoolNode {
	switch node := (*nodeRaw).(type) {
	case *ant_ast.IdentifierNode:
		val := strings.ToLower(node.Value)
		if val == "true" {
			return &ant_ast.BoolNode{
				Value: true,
			}
		} else if val == "false" {
			return &ant_ast.BoolNode{
				Value: false,
			}
		} else {
			return nil
		}
	default:
		return nil
	}
}

func (context *ParserContext) createCmpExpr(left, right ant_ast.Node, operator string) (*planpb.Expr, error) {
	if boolNode := parseBoolNode(&left); boolNode != nil {
		left = boolNode
	}
	if boolNode := parseBoolNode(&right); boolNode != nil {
		right = boolNode
	}
	idNodeLeft, leftIDNode := left.(*ant_ast.IdentifierNode)
	idNodeRight, rightIDNode := right.(*ant_ast.IdentifierNode)

	if leftIDNode && rightIDNode {
		leftField, err := context.handleIdentifier(idNodeLeft)
		if err != nil {
			return nil, err
		}
		rightField, err := context.handleIdentifier(idNodeRight)
		if err != nil {
			return nil, err
		}
		op := getCompareOpType(operator, false)
		if op == planpb.OpType_Invalid {
			return nil, fmt.Errorf("invalid binary operator(%s)", operator)
		}
		expr := &planpb.Expr{
			Expr: &planpb.Expr_CompareExpr{
				CompareExpr: &planpb.CompareExpr{
					LeftColumnInfo:  context.createColumnInfo(leftField),
					RightColumnInfo: context.createColumnInfo(rightField),
					Op:              op,
				},
			},
		}
		return expr, nil
	}

	var idNode *ant_ast.IdentifierNode
	var isReversed bool
	var valueNode *ant_ast.Node
	if leftIDNode {
		idNode = idNodeLeft
		isReversed = false
		valueNode = &right
	} else if rightIDNode {
		idNode = idNodeRight
		isReversed = true
		valueNode = &left
	} else {
		return nil, fmt.Errorf("compare expr has no identifier")
	}

	field, err := context.handleIdentifier(idNode)
	if err != nil {
		return nil, err
	}

	val, err := context.handleLeafValue(valueNode, field.DataType)
	if err != nil {
		return nil, err
	}

	op := getCompareOpType(operator, isReversed)
	if op == planpb.OpType_Invalid {
		return nil, fmt.Errorf("invalid binary operator(%s)", operator)
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: context.createColumnInfo(field),
				Op:         op,
				Value:      val,
			},
		},
	}
	return expr, nil
}

func (context *ParserContext) handleCmpExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	return context.createCmpExpr(node.Left, node.Right, node.Operator)
}

func (context *ParserContext) handleLogicalExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	op := getLogicalOpType(node.Operator)
	if op == planpb.BinaryExpr_Invalid {
		return nil, fmt.Errorf("invalid logical operator(%s)", node.Operator)
	}

	leftExpr, err := context.handleExpr(&node.Left)
	if err != nil {
		return nil, err
	}

	rightExpr, err := context.handleExpr(&node.Right)
	if err != nil {
		return nil, err
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    op,
				Left:  leftExpr,
				Right: rightExpr,
			},
		},
	}
	return expr, nil
}

func (context *ParserContext) handleArrayExpr(node *ant_ast.Node, dataType schemapb.DataType) ([]*planpb.GenericValue, error) {
	arrayNode, ok2 := (*node).(*ant_ast.ArrayNode)
	if !ok2 {
		return nil, fmt.Errorf("right operand of the InExpr must be array")
	}
	var arr []*planpb.GenericValue
	for _, element := range arrayNode.Nodes {
		// use value inside
		// #nosec G601
		val, err := context.handleLeafValue(&element, dataType)
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
	}
	return arr, nil
}

func (context *ParserContext) handleInExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	if node.Operator != "in" && node.Operator != "not in" {
		return nil, fmt.Errorf("invalid operator(%s)", node.Operator)
	}
	idNode, ok := node.Left.(*ant_ast.IdentifierNode)
	if !ok {
		return nil, fmt.Errorf("left operand of the InExpr must be identifier")
	}
	field, err := context.handleIdentifier(idNode)
	if err != nil {
		return nil, err
	}
	arrayData, err := context.handleArrayExpr(&node.Right, field.DataType)
	if err != nil {
		return nil, err
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: context.createColumnInfo(field),
				Values:     arrayData,
			},
		},
	}

	if node.Operator == "not in" {
		return context.createNotExpr(expr)
	}
	return expr, nil
}

func (context *ParserContext) combineUnaryRangeExpr(a, b *planpb.UnaryRangeExpr) *planpb.Expr {
	if a.Op == planpb.OpType_LessEqual || a.Op == planpb.OpType_LessThan {
		a, b = b, a
	}

	lowerInclusive := (a.Op == planpb.OpType_GreaterEqual)
	upperInclusive := (b.Op == planpb.OpType_LessEqual)

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:     a.ColumnInfo,
				LowerInclusive: lowerInclusive,
				UpperInclusive: upperInclusive,
				LowerValue:     a.Value,
				UpperValue:     b.Value,
			},
		},
	}
	return expr
}

func (context *ParserContext) handleMultiCmpExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	exprs := []*planpb.Expr{}
	curNode := node

	// handle multiple relational operator
	for {
		binNodeLeft, LeftOk := curNode.Left.(*ant_ast.BinaryNode)
		if !LeftOk {
			expr, err := context.handleCmpExpr(curNode)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
			break
		}
		if isSameOrder(node.Operator, binNodeLeft.Operator) {
			expr, err := context.createCmpExpr(binNodeLeft.Right, curNode.Right, curNode.Operator)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
			curNode = binNodeLeft
		} else {
			return nil, fmt.Errorf("illegal multi-range expr")
		}
	}

	// combine UnaryRangeExpr to BinaryRangeExpr
	var lastExpr *planpb.UnaryRangeExpr
	for i := len(exprs) - 1; i >= 0; i-- {
		if expr, ok := exprs[i].Expr.(*planpb.Expr_UnaryRangeExpr); ok {
			if lastExpr != nil && expr.UnaryRangeExpr.ColumnInfo.FieldId == lastExpr.ColumnInfo.FieldId {
				binaryRangeExpr := context.combineUnaryRangeExpr(expr.UnaryRangeExpr, lastExpr)
				exprs = append(exprs[0:i], append([]*planpb.Expr{binaryRangeExpr}, exprs[i+2:]...)...)
				lastExpr = nil
			} else {
				lastExpr = expr.UnaryRangeExpr
			}
		} else {
			lastExpr = nil
		}
	}

	// use `&&` to connect exprs
	combinedExpr := exprs[len(exprs)-1]
	for i := len(exprs) - 2; i >= 0; i-- {
		expr := exprs[i]
		combinedExpr = &planpb.Expr{
			Expr: &planpb.Expr_BinaryExpr{
				BinaryExpr: &planpb.BinaryExpr{
					Op:    planpb.BinaryExpr_LogicalAnd,
					Left:  combinedExpr,
					Right: expr,
				},
			},
		}
	}
	return combinedExpr, nil
}

func (context *ParserContext) handleBinaryExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	switch node.Operator {
	case "<", "<=", ">", ">=":
		return context.handleMultiCmpExpr(node)
	case "==", "!=":
		return context.handleCmpExpr(node)
	case "and", "or", "&&", "||":
		return context.handleLogicalExpr(node)
	case "in", "not in":
		return context.handleInExpr(node)
	}
	return nil, fmt.Errorf("unsupported binary operator %s", node.Operator)
}

func (context *ParserContext) createNotExpr(childExpr *planpb.Expr) (*planpb.Expr, error) {
	expr := &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op:    planpb.UnaryExpr_Not,
				Child: childExpr,
			},
		},
	}
	return expr, nil
}

func (context *ParserContext) handleLeafValue(nodeRaw *ant_ast.Node, dataType schemapb.DataType) (gv *planpb.GenericValue, err error) {
	switch node := (*nodeRaw).(type) {
	case *ant_ast.FloatNode:
		if typeutil.IsFloatingType(dataType) {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: node.Value,
				},
			}
		} else {
			return nil, fmt.Errorf("type mismatch")
		}
	case *ant_ast.IntegerNode:
		if typeutil.IsFloatingType(dataType) {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_FloatVal{
					FloatVal: float64(node.Value),
				},
			}
		} else if typeutil.IsIntegerType(dataType) {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: int64(node.Value),
				},
			}
		} else if dataType == schemapb.DataType_Bool {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_BoolVal{},
			}
			if node.Value == 1 {
				gv.Val.(*planpb.GenericValue_BoolVal).BoolVal = true
			} else {
				gv.Val.(*planpb.GenericValue_BoolVal).BoolVal = false
			}
		} else {
			return nil, fmt.Errorf("type mismatch")
		}
	case *ant_ast.BoolNode:
		if typeutil.IsBoolType(dataType) {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_BoolVal{
					BoolVal: node.Value,
				},
			}
		} else {
			return nil, fmt.Errorf("type mismatch")
		}
	default:
		return nil, fmt.Errorf("unsupported leaf node")
	}

	return gv, nil
}

func (context *ParserContext) handleIdentifier(node *ant_ast.IdentifierNode) (*schemapb.FieldSchema, error) {
	fieldName := node.Value
	field, err := context.schema.GetFieldFromName(fieldName)
	return field, err
}

func (context *ParserContext) handleUnaryExpr(node *ant_ast.UnaryNode) (*planpb.Expr, error) {
	switch node.Operator {
	case "!", "not":
		subExpr, err := context.handleExpr(&node.Node)
		if err != nil {
			return nil, err
		}
		return context.createNotExpr(subExpr)
	default:
		return nil, fmt.Errorf("invalid unary operator(%s)", node.Operator)
	}
}

func (context *ParserContext) handleExpr(nodeRaw *ant_ast.Node) (*planpb.Expr, error) {
	switch node := (*nodeRaw).(type) {
	case *ant_ast.IdentifierNode,
		*ant_ast.FloatNode,
		*ant_ast.IntegerNode,
		*ant_ast.BoolNode:
		return nil, fmt.Errorf("scalar expr is not supported yet")
	case *ant_ast.UnaryNode:
		expr, err := context.handleUnaryExpr(node)
		if err != nil {
			return nil, err
		}
		return expr, nil
	case *ant_ast.BinaryNode:
		return context.handleBinaryExpr(node)
	default:
		return nil, fmt.Errorf("unsupported node (%s)", node.Type().String())
	}
}

func CreateQueryPlan(schemaPb *schemapb.CollectionSchema, exprStr string, vectorFieldName string, queryInfo *planpb.QueryInfo) (*planpb.PlanNode, error) {
	schema, err := typeutil.CreateSchemaHelper(schemaPb)
	if err != nil {
		return nil, err
	}

	expr, err := parseExpr(schema, exprStr)
	if err != nil {
		return nil, err
	}
	vectorField, err := schema.GetFieldFromName(vectorFieldName)
	if err != nil {
		return nil, err
	}
	fieldID := vectorField.FieldID
	dataType := vectorField.DataType

	if !typeutil.IsVectorType(dataType) {
		return nil, fmt.Errorf("field (%s) to search is not of vector data type", vectorFieldName)
	}

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				IsBinary:       dataType == schemapb.DataType_BinaryVector,
				Predicates:     expr,
				QueryInfo:      queryInfo,
				PlaceholderTag: "$0",
				FieldId:        fieldID,
			},
		},
	}
	return planNode, nil
}

func CreateExprPlan(schemaPb *schemapb.CollectionSchema, exprStr string) (*planpb.PlanNode, error) {
	schema, err := typeutil.CreateSchemaHelper(schemaPb)
	if err != nil {
		return nil, err
	}

	expr, err := parseExpr(schema, exprStr)
	if err != nil {
		return nil, err
	}

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: expr,
		},
	}
	return planNode, nil
}
