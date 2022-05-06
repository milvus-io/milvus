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
	"math"
	"strings"

	ant_ast "github.com/antonmedv/expr/ast"
	ant_parser "github.com/antonmedv/expr/parser"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type parserContext struct {
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
				optimizer.err = fmt.Errorf("invalid data type")
				return
			}
		case "+":
			if i, ok := node.Node.(*ant_ast.IntegerNode); ok {
				patch(&ant_ast.IntegerNode{Value: i.Value})
			} else if i, ok := node.Node.(*ant_ast.FloatNode); ok {
				patch(&ant_ast.FloatNode{Value: i.Value})
			} else {
				optimizer.err = fmt.Errorf("invalid data type")
				return
			}
		}

	case *ant_ast.BinaryNode:
		floatNodeLeft, leftFloat := node.Left.(*ant_ast.FloatNode)
		integerNodeLeft, leftInteger := node.Left.(*ant_ast.IntegerNode)
		floatNodeRight, rightFloat := node.Right.(*ant_ast.FloatNode)
		integerNodeRight, rightInteger := node.Right.(*ant_ast.IntegerNode)

		// Check IdentifierNodes
		identifierNodeLeft, leftIdentifier := node.Left.(*ant_ast.IdentifierNode)
		identifierNodeRight, rightIdentifier := node.Right.(*ant_ast.IdentifierNode)

		switch node.Operator {
		case "+":
			funcName, err := getFuncNameByNodeOp(node.Operator)
			if err != nil {
				optimizer.err = err
				return
			}
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value + floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value + float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) + floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value + integerNodeRight.Value})
			} else if leftIdentifier && rightFloat {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, floatNodeRight}})
			} else if leftIdentifier && rightInteger {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, integerNodeRight}})
			} else if leftFloat && rightIdentifier {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeRight, floatNodeLeft}})
			} else if leftInteger && rightIdentifier {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeRight, integerNodeLeft}})
			} else {
				optimizer.err = fmt.Errorf("invalid data type")
				return
			}
		case "-":
			funcName, err := getFuncNameByNodeOp(node.Operator)
			if err != nil {
				optimizer.err = err
				return
			}
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value - floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value - float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) - floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value - integerNodeRight.Value})
			} else if leftIdentifier && rightFloat {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, floatNodeRight}})
			} else if leftIdentifier && rightInteger {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, integerNodeRight}})
			} else if leftFloat && rightIdentifier {
				optimizer.err = fmt.Errorf("field as right operand is not yet supported for (%s) operator", node.Operator)
				return
			} else if leftInteger && rightIdentifier {
				optimizer.err = fmt.Errorf("field as right operand is not yet supported for (%s) operator", node.Operator)
				return
			} else {
				optimizer.err = fmt.Errorf("invalid data type")
				return
			}
		case "*":
			funcName, err := getFuncNameByNodeOp(node.Operator)
			if err != nil {
				optimizer.err = err
				return
			}
			if leftFloat && rightFloat {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value * floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value * float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) * floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value * integerNodeRight.Value})
			} else if leftIdentifier && rightFloat {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, floatNodeRight}})
			} else if leftIdentifier && rightInteger {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, integerNodeRight}})
			} else if leftFloat && rightIdentifier {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeRight, floatNodeLeft}})
			} else if leftInteger && rightIdentifier {
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeRight, integerNodeLeft}})
			} else {
				optimizer.err = fmt.Errorf("invalid data type")
				return
			}
		case "/":
			funcName, err := getFuncNameByNodeOp(node.Operator)
			if err != nil {
				optimizer.err = err
				return
			}
			if leftFloat && rightFloat {
				if floatNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("divide by zero")
					return
				}
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value / floatNodeRight.Value})
			} else if leftFloat && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("divide by zero")
					return
				}
				patch(&ant_ast.FloatNode{Value: floatNodeLeft.Value / float64(integerNodeRight.Value)})
			} else if leftInteger && rightFloat {
				if floatNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("divide by zero")
					return
				}
				patch(&ant_ast.FloatNode{Value: float64(integerNodeLeft.Value) / floatNodeRight.Value})
			} else if leftInteger && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("divide by zero")
					return
				}
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value / integerNodeRight.Value})
			} else if leftIdentifier && rightFloat {
				if floatNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("divide by zero")
					return
				}
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, floatNodeRight}})
			} else if leftIdentifier && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("divide by zero")
					return
				}
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, integerNodeRight}})
			} else if leftFloat && rightIdentifier {
				optimizer.err = fmt.Errorf("field as right operand is not yet supported for (%s) operator", node.Operator)
				return
			} else if leftInteger && rightIdentifier {
				optimizer.err = fmt.Errorf("field as right operand is not yet supported for (%s) operator", node.Operator)
				return
			} else {
				optimizer.err = fmt.Errorf("invalid data type")
				return
			}
		case "%":
			funcName, err := getFuncNameByNodeOp(node.Operator)
			if err != nil {
				optimizer.err = err
				return
			}
			if leftInteger && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("modulo by zero")
					return
				}
				patch(&ant_ast.IntegerNode{Value: integerNodeLeft.Value % integerNodeRight.Value})
			} else if leftIdentifier && rightInteger {
				if integerNodeRight.Value == 0 {
					optimizer.err = fmt.Errorf("modulo by zero")
					return
				}
				patch(&ant_ast.FunctionNode{Name: funcName, Arguments: []ant_ast.Node{identifierNodeLeft, integerNodeRight}})
			} else if leftInteger && rightIdentifier {
				optimizer.err = fmt.Errorf("field as right operand is not yet supported for (%s) operator", node.Operator)
				return
			} else {
				optimizer.err = fmt.Errorf("invalid data type")
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
				optimizer.err = fmt.Errorf("invalid data type")
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

	pc := parserContext{schema}
	expr, err := pc.handleExpr(&ast.Node)
	if err != nil {
		return nil, err
	}

	return expr, nil
}

func createColumnInfo(field *schemapb.FieldSchema) *planpb.ColumnInfo {
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

var opMap = map[planpb.OpType]string{
	planpb.OpType_Invalid:      "invalid",
	planpb.OpType_GreaterThan:  ">",
	planpb.OpType_GreaterEqual: ">=",
	planpb.OpType_LessThan:     "<",
	planpb.OpType_LessEqual:    "<=",
	planpb.OpType_Equal:        "==",
	planpb.OpType_NotEqual:     "!=",
}

func getCompareOpType(opStr string, reverse bool) (op planpb.OpType) {
	switch opStr {
	case ">":
		if reverse {
			op = planpb.OpType_LessThan
		} else {
			op = planpb.OpType_GreaterThan
		}
	case "<":
		if reverse {
			op = planpb.OpType_GreaterThan
		} else {
			op = planpb.OpType_LessThan
		}
	case ">=":
		if reverse {
			op = planpb.OpType_LessEqual
		} else {
			op = planpb.OpType_GreaterEqual
		}
	case "<=":
		if reverse {
			op = planpb.OpType_GreaterEqual
		} else {
			op = planpb.OpType_LessEqual
		}
	case "==":
		op = planpb.OpType_Equal
	case "!=":
		op = planpb.OpType_NotEqual
	case "startsWith":
		op = planpb.OpType_PrefixMatch
	case "endsWith":
		op = planpb.OpType_PostfixMatch
	default:
		op = planpb.OpType_Invalid
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

func getArithOpType(funcName string) (planpb.ArithOpType, error) {
	var op planpb.ArithOpType

	switch funcName {
	case "add":
		op = planpb.ArithOpType_Add
	case "sub":
		op = planpb.ArithOpType_Sub
	case "mul":
		op = planpb.ArithOpType_Mul
	case "div":
		op = planpb.ArithOpType_Div
	case "mod":
		op = planpb.ArithOpType_Mod
	default:
		return op, fmt.Errorf("unsupported or invalid arith op type: %s", funcName)
	}
	return op, nil
}

func getFuncNameByNodeOp(nodeOp string) (string, error) {
	var funcName string

	switch nodeOp {
	case "+":
		funcName = "add"
	case "-":
		funcName = "sub"
	case "*":
		funcName = "mul"
	case "/":
		funcName = "div"
	case "%":
		funcName = "mod"
	default:
		return funcName, fmt.Errorf("no defined funcName assigned to nodeOp: %s", nodeOp)
	}
	return funcName, nil
}

func parseBoolNode(nodeRaw *ant_ast.Node) *ant_ast.BoolNode {
	switch node := (*nodeRaw).(type) {
	case *ant_ast.IdentifierNode:
		// bool node only accept value 'true' or 'false'
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

func (pc *parserContext) createCmpExpr(left, right ant_ast.Node, operator string) (*planpb.Expr, error) {
	if boolNode := parseBoolNode(&left); boolNode != nil {
		left = boolNode
	}
	if boolNode := parseBoolNode(&right); boolNode != nil {
		right = boolNode
	}
	idNodeLeft, okLeft := left.(*ant_ast.IdentifierNode)
	idNodeRight, okRight := right.(*ant_ast.IdentifierNode)

	if okLeft && okRight {
		leftField, err := pc.handleIdentifier(idNodeLeft)
		if err != nil {
			return nil, err
		}
		rightField, err := pc.handleIdentifier(idNodeRight)
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
					LeftColumnInfo:  createColumnInfo(leftField),
					RightColumnInfo: createColumnInfo(rightField),
					Op:              op,
				},
			},
		}
		return expr, nil
	}

	var idNode *ant_ast.IdentifierNode
	var reverse bool
	var valueNode *ant_ast.Node
	if okLeft {
		idNode = idNodeLeft
		reverse = false
		valueNode = &right
	} else if okRight {
		idNode = idNodeRight
		reverse = true
		valueNode = &left
	} else {
		return nil, fmt.Errorf("compare expr has no identifier")
	}

	field, err := pc.handleIdentifier(idNode)
	if err != nil {
		return nil, err
	}

	val, err := pc.handleLeafValue(valueNode, field.DataType)
	if err != nil {
		return nil, err
	}

	op := getCompareOpType(operator, reverse)
	if op == planpb.OpType_Invalid {
		return nil, fmt.Errorf("invalid binary operator(%s)", operator)
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: createColumnInfo(field),
				Op:         op,
				Value:      val,
			},
		},
	}
	return expr, nil
}

func (pc *parserContext) createBinaryArithOpEvalExpr(left *ant_ast.FunctionNode, right *ant_ast.Node, operator string) (*planpb.Expr, error) {
	switch operator {
	case "==", "!=":
		binArithOp, err := pc.handleFunction(left)
		if err != nil {
			return nil, fmt.Errorf("createBinaryArithOpEvalExpr: %v", err)
		}
		op := getCompareOpType(operator, false)
		val, err := pc.handleLeafValue(right, binArithOp.ColumnInfo.DataType)
		if err != nil {
			return nil, err
		}

		expr := &planpb.Expr{
			Expr: &planpb.Expr_BinaryArithOpEvalRangeExpr{
				BinaryArithOpEvalRangeExpr: &planpb.BinaryArithOpEvalRangeExpr{
					ColumnInfo:   binArithOp.ColumnInfo,
					ArithOp:      binArithOp.ArithOp,
					RightOperand: binArithOp.RightOperand,
					Op:           op,
					Value:        val,
				},
			},
		}
		return expr, nil
	}
	return nil, fmt.Errorf("operator(%s) not yet supported for function nodes", operator)
}

func (pc *parserContext) handleCmpExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	return pc.createCmpExpr(node.Left, node.Right, node.Operator)
}

func (pc *parserContext) handleBinaryArithCmpExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	leftNode, funcNodeLeft := node.Left.(*ant_ast.FunctionNode)
	rightNode, funcNodeRight := node.Right.(*ant_ast.FunctionNode)

	if funcNodeLeft && funcNodeRight {
		return nil, fmt.Errorf("left and right are both expression are not supported")
	} else if funcNodeRight {
		// Only the right node is a function node
		op := getCompareOpType(node.Operator, true)
		if op == planpb.OpType_Invalid {
			return nil, fmt.Errorf("invalid right expression")
		}
		return pc.createBinaryArithOpEvalExpr(rightNode, &node.Left, opMap[op])
	} else if funcNodeLeft {
		// Only the left node is a function node
		return pc.createBinaryArithOpEvalExpr(leftNode, &node.Right, node.Operator)
	} else {
		// Both left and right are not function nodes, pass to createCmpExpr
		return pc.createCmpExpr(node.Left, node.Right, node.Operator)
	}
}

func (pc *parserContext) handleLogicalExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	op := getLogicalOpType(node.Operator)
	if op == planpb.BinaryExpr_Invalid {
		return nil, fmt.Errorf("invalid logical operator(%s)", node.Operator)
	}

	leftExpr, err := pc.handleExpr(&node.Left)
	if err != nil {
		return nil, err
	}

	rightExpr, err := pc.handleExpr(&node.Right)
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

func (pc *parserContext) handleArrayExpr(node *ant_ast.Node, dataType schemapb.DataType) ([]*planpb.GenericValue, error) {
	arrayNode, ok2 := (*node).(*ant_ast.ArrayNode)
	if !ok2 {
		return nil, fmt.Errorf("right operand of the InExpr must be array")
	}
	var arr []*planpb.GenericValue
	for _, element := range arrayNode.Nodes {
		// use value inside
		// #nosec G601
		val, err := pc.handleLeafValue(&element, dataType)
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
	}
	return arr, nil
}

func (pc *parserContext) handleInExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	if node.Operator != "in" && node.Operator != "not in" {
		return nil, fmt.Errorf("invalid operator(%s)", node.Operator)
	}
	idNode, ok := node.Left.(*ant_ast.IdentifierNode)
	if !ok {
		return nil, fmt.Errorf("left operand of the InExpr must be identifier")
	}
	field, err := pc.handleIdentifier(idNode)
	if err != nil {
		return nil, err
	}
	arrayData, err := pc.handleArrayExpr(&node.Right, field.DataType)
	if err != nil {
		return nil, err
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: createColumnInfo(field),
				Values:     arrayData,
			},
		},
	}

	if node.Operator == "not in" {
		return pc.createNotExpr(expr)
	}
	return expr, nil
}

func (pc *parserContext) combineUnaryRangeExpr(a, b *planpb.UnaryRangeExpr) *planpb.Expr {
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

func (pc *parserContext) handleMultiCmpExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	exprs := []*planpb.Expr{}
	curNode := node

	// handle multiple relational operators
	for {
		binNodeLeft, LeftOk := curNode.Left.(*ant_ast.BinaryNode)
		if !LeftOk {
			expr, err := pc.handleCmpExpr(curNode)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
			break
		}
		if isSameOrder(node.Operator, binNodeLeft.Operator) {
			expr, err := pc.createCmpExpr(binNodeLeft.Right, curNode.Right, curNode.Operator)
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
				binaryRangeExpr := pc.combineUnaryRangeExpr(expr.UnaryRangeExpr, lastExpr)
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

func (pc *parserContext) handleBinaryExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	_, leftArithExpr := node.Left.(*ant_ast.FunctionNode)
	_, rightArithExpr := node.Right.(*ant_ast.FunctionNode)

	if leftArithExpr || rightArithExpr {
		return pc.handleBinaryArithCmpExpr(node)
	}

	switch node.Operator {
	case "<", "<=", ">", ">=":
		return pc.handleMultiCmpExpr(node)
	case "==", "!=", "startsWith", "endsWith":
		return pc.handleCmpExpr(node)
	case "and", "or", "&&", "||":
		return pc.handleLogicalExpr(node)
	case "in", "not in":
		return pc.handleInExpr(node)
	}
	return nil, fmt.Errorf("unsupported binary operator %s", node.Operator)
}

func (pc *parserContext) createNotExpr(childExpr *planpb.Expr) (*planpb.Expr, error) {
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

func (pc *parserContext) handleLeafValue(nodeRaw *ant_ast.Node, dataType schemapb.DataType) (gv *planpb.GenericValue, err error) {
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
	case *ant_ast.StringNode:
		if typeutil.IsStringType(dataType) {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{
					StringVal: node.Value,
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

func (pc *parserContext) handleFunction(node *ant_ast.FunctionNode) (*planpb.BinaryArithOp, error) {
	funcArithOp, err := getArithOpType(node.Name)
	if err != nil {
		return nil, err
	}

	idNode, ok := node.Arguments[0].(*ant_ast.IdentifierNode)
	if !ok {
		return nil, fmt.Errorf("left operand of the function must be an identifier")
	}

	field, err := pc.handleIdentifier(idNode)
	if err != nil {
		return nil, err
	}

	valueNode := node.Arguments[1]
	val, err := pc.handleLeafValue(&valueNode, field.DataType)
	if err != nil {
		return nil, err
	}

	arithOp := &planpb.BinaryArithOp{
		ColumnInfo:   createColumnInfo(field),
		ArithOp:      funcArithOp,
		RightOperand: val,
	}

	return arithOp, nil
}

func (pc *parserContext) handleIdentifier(node *ant_ast.IdentifierNode) (*schemapb.FieldSchema, error) {
	fieldName := node.Value
	field, err := pc.schema.GetFieldFromName(fieldName)
	return field, err
}

func (pc *parserContext) handleUnaryExpr(node *ant_ast.UnaryNode) (*planpb.Expr, error) {
	switch node.Operator {
	case "!", "not":
		subExpr, err := pc.handleExpr(&node.Node)
		if err != nil {
			return nil, err
		}
		return pc.createNotExpr(subExpr)
	default:
		return nil, fmt.Errorf("invalid unary operator(%s)", node.Operator)
	}
}

func (pc *parserContext) handleExpr(nodeRaw *ant_ast.Node) (*planpb.Expr, error) {
	switch node := (*nodeRaw).(type) {
	case *ant_ast.IdentifierNode,
		*ant_ast.FloatNode,
		*ant_ast.IntegerNode,
		*ant_ast.BoolNode,
		*ant_ast.StringNode:
		return nil, fmt.Errorf("scalar expr is not supported yet")
	case *ant_ast.UnaryNode:
		expr, err := pc.handleUnaryExpr(node)
		if err != nil {
			return nil, err
		}
		return expr, nil
	case *ant_ast.BinaryNode:
		return pc.handleBinaryExpr(node)
	default:
		return nil, fmt.Errorf("unsupported node (%s)", node.Type().String())
	}
}

func createQueryPlan(schemaPb *schemapb.CollectionSchema, exprStr string, vectorFieldName string, queryInfo *planpb.QueryInfo) (*planpb.PlanNode, error) {
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

func createExprPlan(schemaPb *schemapb.CollectionSchema, exprStr string) (*planpb.PlanNode, error) {
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
