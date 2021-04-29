package proxynode

import (
	"fmt"

	ant_ast "github.com/antonmedv/expr/ast"
	ant_parser "github.com/antonmedv/expr/parser"

	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

//func parseQueryExprNaive(schema *typeutil.SchemaHelper, exprStr string) (*planpb.Expr, error) {
//	// TODO: handle more cases
//	// TODO: currently just A > 3
//
//	tmps := strings.Split(exprStr, ">")
//	if len(tmps) != 2 {
//		return nil, errors.New("unsupported yet")
//	}
//	for i, str := range tmps {
//		tmps[i] = strings.TrimSpace(str)
//	}
//	fieldName := tmps[0]
//	tmpValue, err := strconv.ParseInt(tmps[1], 10, 64)
//	if err != nil {
//		return nil, err
//	}
//	fieldValue := &planpb.GenericValue{
//		Val: &planpb.GenericValue_Int64Val{Int64Val: tmpValue},
//	}
//
//	field, err := schema.GetFieldFromName(fieldName)
//	if err != nil {
//		return nil, err
//	}
//
//	expr := &planpb.Expr{
//		Expr: &planpb.Expr_RangeExpr{
//			RangeExpr: &planpb.RangeExpr{
//				ColumnInfo: &planpb.ColumnInfo{
//					FieldId:  field.FieldID,
//					DataType: field.DataType,
//				},
//				Ops:    []planpb.RangeExpr_OpType{planpb.RangeExpr_GreaterThan},
//				Values: []*planpb.GenericValue{fieldValue},
//			},
//		},
//	}
//	return expr, nil
//}

func parseQueryExpr(schema *typeutil.SchemaHelper, exprStrNullable *string) (*planpb.Expr, error) {
	if exprStrNullable == nil {
		return nil, nil
	}
	exprStr := *exprStrNullable
	return parseQueryExprAdvanced(schema, exprStr)
}

type ParserContext struct {
	schema *typeutil.SchemaHelper
}

func parseQueryExprAdvanced(schema *typeutil.SchemaHelper, exprStr string) (*planpb.Expr, error) {
	ast, err := ant_parser.Parse(exprStr)
	if err != nil {
		return nil, err
	}
	context := ParserContext{schema}

	return context.parseExpr(&ast.Node)
}

func (context *ParserContext) createColumnInfo(field *schemapb.FieldSchema) *planpb.ColumnInfo {
	return &planpb.ColumnInfo{
		FieldId:  field.FieldID,
		DataType: field.DataType,
	}
}

func createSingleOps(opStr string, reverse bool) planpb.RangeExpr_OpType {
	type OpType = planpb.RangeExpr_OpType
	var op planpb.RangeExpr_OpType
	if !reverse {
		switch opStr {
		case "<":
			op = planpb.RangeExpr_LessThan
		case ">":
			op = planpb.RangeExpr_GreaterThan
		case "<=":
			op = planpb.RangeExpr_LessEqual
		case ">=":
			op = planpb.RangeExpr_GreaterEqual
		case "==":
			op = planpb.RangeExpr_Equal
		case "!=":
			op = planpb.RangeExpr_NotEqual
		default:
			op = planpb.RangeExpr_Invalid
		}
	} else {
		switch opStr {
		case ">":
			op = planpb.RangeExpr_LessThan
		case "<":
			op = planpb.RangeExpr_GreaterThan
		case ">=":
			op = planpb.RangeExpr_LessEqual
		case "<=":
			op = planpb.RangeExpr_GreaterEqual
		case "==":
			op = planpb.RangeExpr_Equal
		case "!=":
			op = planpb.RangeExpr_NotEqual
		default:
			op = planpb.RangeExpr_Invalid
		}
	}
	return op
}

func (context *ParserContext) handleCmpExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	idNode, leftOk := node.Left.(*ant_ast.IdentifierNode)
	if !leftOk {
		return nil, fmt.Errorf("compare require left to be identifier")
	}
	field, err := context.handleIdentifier(idNode)
	if err != nil {
		return nil, err
	}

	val, err := context.handleLeafValue(&node.Right, field.DataType)
	if err != nil {
		return nil, err
	}

	op := createSingleOps(node.Operator, false)
	if op == planpb.RangeExpr_Invalid {
		return nil, fmt.Errorf("invalid binary operator %s", node.Operator)
	}

	final := &planpb.Expr{
		Expr: &planpb.Expr_RangeExpr{
			RangeExpr: &planpb.RangeExpr{
				ColumnInfo: context.createColumnInfo(field),
				Ops:        []planpb.RangeExpr_OpType{op},
				Values:     []*planpb.GenericValue{val},
			},
		},
	}
	return final, nil
}

func (context *ParserContext) handleLogicalExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (context *ParserContext) handleInExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (context *ParserContext) handleBinaryExpr(node *ant_ast.BinaryNode) (*planpb.Expr, error) {
	// TODO
	switch node.Operator {
	case "<", "<=", ">", ">=", "==", "!=":
		return context.handleCmpExpr(node)
	case "and", "or":
		return context.handleLogicalExpr(node)
	case "in", "not in":
		return context.handleInExpr(node)
	}
	return nil, fmt.Errorf("unimplemented")
}

func (context *ParserContext) handleNotExpr(node *ant_ast.UnaryNode) (*planpb.Expr, error) {
	return nil, fmt.Errorf("unimplemented")
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
		} else if typeutil.IsIntergerType(dataType) {
			gv = &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: int64(node.Value),
				},
			}
		} else {
			return nil, fmt.Errorf("type mismatch")
		}
	case *ant_ast.BoolNode:
		if typeutil.IsFloatingType(dataType) {
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
		return context.handleNotExpr(node)
	default:
		return nil, fmt.Errorf("invalid unary operator(%s)", node.Operator)
	}
}

func (context *ParserContext) parseExpr(nodeRaw *ant_ast.Node) (*planpb.Expr, error) {
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

func CreateQueryPlan(schemaPb *schemapb.CollectionSchema, exprStrNullable *string, vectorFieldName string, queryInfo *planpb.QueryInfo) (*planpb.PlanNode, error) {
	schema, err := typeutil.CreateSchemaHelper(schemaPb)
	if err != nil {
		return nil, err
	}

	expr, err := parseQueryExpr(schema, exprStrNullable)
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
